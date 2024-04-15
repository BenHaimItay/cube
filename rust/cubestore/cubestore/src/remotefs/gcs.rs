use crate::app_metrics;
use crate::di_service;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use cloud_storage::{ListRequest, Object};
use datafusion::cube_ext;
use futures::{TryStreamExt, StreamExt};
use log::{debug, info};
use regex::{NoExpand, Regex};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::SystemTime;
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tokio_util::codec::{BytesCodec, FramedRead};


#[derive(Debug)]
pub struct GCSRemoteFs {
    dir: PathBuf,
    bucket: String,
    sub_path: Option<String>,
    delete_mut: Mutex<()>,
}

impl GCSRemoteFs {
    pub fn new(
        dir: PathBuf,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        Ok(Arc::new(Self {
            dir,
            bucket: bucket_name.to_string(),
            sub_path,
            delete_mut: Mutex::new(()),
        }))
    }
}

di_service!(GCSRemoteFs, [RemoteFs]);

#[async_trait]
impl RemoteFs for GCSRemoteFs {
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
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            1,
            Some(&vec![
                "operation:upload_file".to_string(),
                "driver:gcs".to_string(),
            ]),
        );
        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);
        let file = File::open(temp_upload_path.clone()).await?;
        let stream = FramedRead::new(file, BytesCodec::new());
        let stream = stream.map(|r| r.map(|b| b.to_vec()));
        Object::create_streamed(
            self.bucket.as_str(),
            stream,
            None,
            self.gcs_path(&remote_path).as_str(),
            "application/octet-stream",
        )
        .await?;

        let size = fs::metadata(temp_upload_path.clone()).await?.len();
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
        info!("Uploaded {} ({:?})", remote_path, time.elapsed()?);
        Ok(fs::metadata(local_path).await?.len())
    }

    async fn download_file(
        &self,
        remote_path: String,
        _expected_file_size: Option<u64>,
    ) -> Result<String, CubeError> {
        let mut local_file = self.dir.as_path().join(&remote_path);
        let local_dir = local_file.parent().unwrap();
        let downloads_dirs = local_dir.join("downloads");

        fs::create_dir_all(&downloads_dirs).await?;
        if !local_file.exists() {
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&vec![
                    "operation:download_file".to_string(),
                    "driver:gcs".to_string(),
                ]),
            );
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);
            let (temp_file, temp_path) =
                cube_ext::spawn_blocking(move || NamedTempFile::new_in(downloads_dirs))
                    .await??
                    .into_parts();
            let mut writer = BufWriter::new(tokio::fs::File::from_std(temp_file));
            let mut stream = Object::download_streamed(
                self.bucket.as_str(),
                self.gcs_path(&remote_path).as_str(),
            )
            .await?;

            let mut c = 0;
            while let Some(byte) = stream.next().await {
                // TODO it might be very slow
                writer.write_all(&[byte?]).await?;
                c += 1;
            }
            writer.flush().await?;

            local_file = cube_ext::spawn_blocking(move || -> Result<PathBuf, PathPersistError> {
                temp_path.persist(&local_file)?;
                Ok(local_file)
            })
            .await??;

            info!(
                "Downloaded {} ({:?}) ({} bytes)",
                remote_path,
                time.elapsed()?,
                c
            );
        }
        Ok(local_file.into_os_string().into_string().unwrap())
    }

    async fn delete_file(&self, remote_path: String) -> Result<(), CubeError> {
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            1,
            Some(&vec![
                "operation:delete_file".to_string(),
                "driver:gcs".to_string(),
            ]),
        );
        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);
        Object::delete(self.bucket.as_str(), self.gcs_path(&remote_path).as_str()).await?;
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
            let prefix = self.gcs_path(&remote_prefix);
            let request = ListRequest {
                prefix: Some(prefix.into()),
                ..Default::default()
            };
            let list= Object::list(self.bucket.as_str(), request)
                .await?
                .map_ok(|object_list| object_list.items)
                .try_concat()
                .await?;

            let leading_slash = Regex::new(format!("^{}", self.gcs_path("")).as_str()).unwrap();
            let result: Vec<RemoteFile> = list.into_iter()
                .map(|obj| RemoteFile {
                    remote_path: leading_slash.replace(&obj.name, NoExpand("")).to_string(),
                    updated: obj.updated.clone(),
                    file_size: obj.size,
                })
                .collect::<Vec<RemoteFile>>();

            let mut pages_count = result.len() / 1_000;
            if result.len() % 1_000 > 0 {
                pages_count += 1;
            }
            if pages_count > 100 {
                log::warn!("gcs list returned more than 100 pages: {}", pages_count);
            }
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                pages_count as i64,
                Some(&vec![
                    "operation:list".to_string(),
                    "driver:gcs".to_string(),
                ]),
            );
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

impl GCSRemoteFs {
    fn gcs_path(&self, remote_path: &str) -> String {
        format!(
            "{}/{}",
            self.sub_path
                .as_ref()
                .map(|p| p.to_string())
                .unwrap_or_else(|| "".to_string()),
            remote_path
        )
    }
}
