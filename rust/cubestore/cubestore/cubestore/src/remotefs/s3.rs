use crate::app_metrics;
use crate::di_service;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::Region;
use aws_config::meta::region::RegionProviderChain;
use aws_config::SdkConfig;
use aws_types::credentials::SharedCredentialsProvider;
use chrono::{DateTime, Utc};
use datafusion::cube_ext;
use log::{debug, info};
use regex::{NoExpand, Regex};
use std::env;
use std::fmt;
use std::fmt::Formatter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::sync::Mutex;

pub struct S3RemoteFs {
    dir: PathBuf,
    client: Client,
    bucket_name: String,
    sub_path: Option<String>,
    delete_mut: Mutex<()>,
}

impl fmt::Debug for S3RemoteFs {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut s = f.debug_struct("S3RemoteFs");
        s.field("dir", &self.dir).field("sub_path", &self.sub_path).field("bucket_name", &self.bucket_name);
        s.finish_non_exhaustive()
    }
}

impl S3RemoteFs {
    pub async fn new(
        dir: PathBuf,
        region: String,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        let region_provider = RegionProviderChain::default_provider().or_else(Region::new(&region));
        let config = aws_config::from_env().region(region_provider).load().await;

        let client = Client::new(&config);

        Ok(Arc::new(Self {
            dir,
            client,
            bucket_name,
            sub_path,
            delete_mut: Mutex::new(()),
        }))
    }
}

di_service!(S3RemoteFs, [RemoteFs]);

#[async_trait]
impl RemoteFs for S3RemoteFs {
    async fn temp_upload_path(&self, remote_path: String) -> Result<String, CubeError> {
        CommonRemoteFsUtils::temp_upload_path(self, remote_path).await
    }

    async fn uploads_dir(&self) -> Result<String, CubeError> {
        CommonRemoteFsUtils::uploads_dir(self).await
    }

    async fn check_upload_file(
        &self,
        remote_path: String,
        expected_size: u64,
    ) -> Result<(), CubeError> {
        CommonRemoteFsUtils::check_upload_file(self, remote_path, expected_size).await
    }

    async fn upload_file(
        &self,
        temp_upload_path: String,
        remote_path: String,
    ) -> Result<u64, CubeError> {
        {
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec![
                    "operation:upload_file".to_string(),
                    "driver:s3".to_string(),
                ]),
            );

            let time = SystemTime::now();
            debug!("Uploading {}", remote_path);
            let path = self.s3_path(&remote_path);

            let body = aws_sdk_s3::ByteStream::from_path(Path::new(&temp_upload_path)).await?;
            let put_object_output = self.client.put_object()
                .bucket(&self.bucket_name)
                .key(path)
                .body(body)
                .send()
                .await?;

            info!("Uploaded {} ({:?})", remote_path, time.elapsed()?);
            if put_object_output.e_tag().is_none() {
                return Err(CubeError::user(format!(
                    "S3 upload returned no ETag"
                )));
            }
        }
        let size = fs::metadata(&temp_upload_path).await?.len();
        self.check_upload_file(remote_path.clone(), size).await?;

        let local_path = self.dir.as_path().join(&remote_path);
        if Path::new(&temp_upload_path) != local_path {
            fs::create_dir_all(local_path.parent().unwrap())
                .await
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Create dir {}: {}",
                        local_path.parent().as_ref().unwrap().to_string_lossy(),
                        e
                    ))
                })?;
            fs::rename(&temp_upload_path, local_path.clone()).await?;
        }
        Ok(fs::metadata(local_path).await?.len())
    }

    async fn download_file(
        &self,
        remote_path: String,
        _expected_file_size: Option<u64>,
    ) -> Result<String, CubeError> {
        let local_file = self.dir.as_path().join(&remote_path);
        let local_dir = local_file.parent().unwrap();
        let downloads_dir = local_dir.join("downloads");

        let local_file_str = local_file.to_str().unwrap().to_string(); // return value.

        fs::create_dir_all(&downloads_dir).await?;
        if !local_file.exists() {
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec![
                    "operation:download_file".to_string(),
                    "driver:s3".to_string(),
                ]),
            );
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);
            let path = self.s3_path(&remote_path);

            let get_object_output = self.client.get_object()
                .bucket(&self.bucket_name)
                .key(path)
                .send()
                .await?;

            let mut body = get_object_output.body.collect().await?;

            let (mut temp_file, temp_path) =
                NamedTempFile::new_in(&downloads_dir)?.into_parts();
            temp_file.write_all(&body.into_bytes())?;
            temp_file.flush()?;

            temp_path.persist(local_file)?;

            info!("Downloaded {} ({:?})", remote_path, time.elapsed()?);
        }
        Ok(local_file_str)
    }

    async fn delete_file(&self, remote_path: String) -> Result<(), CubeError> {
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            1,
            Some(&vec![
                "operation:delete_file".to_string(),
                "driver:s3".to_string(),
            ]),
        );
        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);
        let path = self.s3_path(&remote_path);

        self.client.delete_object()
            .bucket(&self.bucket_name)
            .key(path)
            .send()
            .await?;

        info!("Deleting {} ({:?})", remote_path, time.elapsed()?);

        let _guard = acquire_lock("delete file", self.delete_mut.lock()).await?;
        let local = self.dir.as_path().join(remote_path);
        if fs::metadata(local.clone()).await.is_ok() {
            fs::remove_file(local.clone()).await?;
            LocalDirRemoteFs::remove_empty_paths(self.dir.as_path().to_path_buf(), local.clone())
                .await?;
        }

        Ok(())
    }

    async fn list(&self, remote_prefix: String) -> Result<Vec<String>, CubeError> {
        Ok(self
            .list_with_metadata(remote_prefix)
            .await?
            .into_iter()
            .map(|f| f.remote_path)
            .collect::<Vec<_>>())
    }

    async fn list_with_metadata(
        &self,
        remote_prefix: String,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let path = self.s3_path(&remote_prefix);
        let list = self.client.list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(path)
            .send()
            .await?;

        let leading_slash = Regex::new(format!("^{}", self.s3_path("")).as_str()).unwrap();
        let result = list.contents.unwrap_or_default()
            .into_iter()
            .map(|o| -> Result<RemoteFile, CubeError> {
                Ok(RemoteFile {
                    remote_path: leading_slash.replace(&o.key.unwrap_or_default(), NoExpand("")).to_string(),
                    updated: DateTime::parse_from_rfc3339(&o.last_modified.unwrap_or_default())?
                        .with_timezone(&Utc),
                    file_size: o.size.unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(result)
    }

    async fn local_path(&self) -> Result<String, CubeError> {
        Ok(self.dir.to_str().unwrap().to_owned())
    }

    async fn local_file(&self, remote_path: String) -> Result<String, CubeError> {
        let buf = self.dir.join(remote_path);
        fs::create_dir_all(buf.parent().unwrap()).await?;
        Ok(buf.to_str().unwrap().to_string())
    }
}

impl S3RemoteFs {
    fn s3_path(&self, remote_path: &str) -> String {
        format!(
            "{}{}",
            self.sub_path
                .as_ref()
                .map(|p| format!("{}/", p.to_string()))
                .unwrap_or_else(|| "".to_string()),
            remote_path
        )
    }
}
