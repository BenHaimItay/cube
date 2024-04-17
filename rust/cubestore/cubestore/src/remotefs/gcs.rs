use async_trait::async_trait;
use cloud_storage::{Object, ListRequest};
use datafusion::cube_ext;
use futures::{TryStreamExt, StreamExt};
use log::{debug, info};
use regex::NoExpand;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::SystemTime;
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::codec::{BytesCodec, FramedRead};
use crate::util::lock::acquire_lock;
use crate::{CubeError, app_metrics, remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFs, RemoteFile}, di_service};

static INIT_CREDENTIALS: Once = Once::new();

fn ensure_credentials_init() {
    // The cloud storage library uses env vars to get access tokens.
    // We decided CubeStore needs its own alias for it, so rewrite and hope no one read it before.
    // TODO: switch to something that allows to configure without env vars.
    // TODO: remove `SERVICE_ACCOUNT` completely.
    INIT_CREDENTIALS.call_once(|| {
        let mut creds_json = None;
        if let Ok(c) = std::env::var("CUBESTORE_GCP_CREDENTIALS") {
            match decode_credentials(&c) {
                Ok(s) => creds_json = Some((s, "CUBESTORE_GCP_CREDENTIALS".to_string())),
                Err(e) => log::error!("Could not decode 'CUBESTORE_GCP_CREDENTIALS': {}", e),
            }
        }
        let mut creds_path = match std::env::var("CUBESTORE_GCP_KEY_FILE") {
            Ok(s) => Some((s, "CUBESTORE_GCP_KEY_FILE".to_string())),
            Err(_) => None,
        };

        // TODO: this handles deprecated variable names, remove them.
        for (var, is_path) in &[
            ("SERVICE_ACCOUNT", true),
            ("GOOGLE_APPLICATION_CREDENTIALS", true),
            ("SERVICE_ACCOUNT_JSON", false),
            ("GOOGLE_APPLICATION_CREDENTIALS_JSON", false),
        ] {
            for var in &[&format!("CUBESTORE_GCP_{}", var), *var] {
                if let Ok(var_value) = std::env::var(&var) {
                    let (upgrade_var, read_value) = if *is_path {
                        ("CUBESTORE_GCP_KEY_FILE", &mut creds_path)
                    } else {
                        ("CUBESTORE_GCP_CREDENTIALS", &mut creds_json)
                    };

                    match read_value {
                        None => {
                            *read_value = Some((var_value, var.to_string()));
                            log::warn!(
                                "Environment variable '{}' is deprecated and will be ignored in future versions, use '{}' instead",
                                var,
                                upgrade_var
                            );
                        }
                        Some((prev_val, prev_var)) => {
                            if prev_val != &var_value {
                                log::warn!(
                                    "Values of '{}' and '{}' differ, preferring the latter",
                                    var,
                                    prev_var
                                );
                            }
                        }
                    }
                }
            }
        }

        match creds_json {
            Some((v, _)) => std::env::set_var("SERVICE_ACCOUNT_JSON", v),
            None => std::env::remove_var("SERVICE_ACCOUNT_JSON"),
        }
        match creds_path {
            Some((v, _)) => std::env::set_var("SERVICE_ACCOUNT", v),
            None => std::env::remove_var("SERVICE_ACCOUNT"),
        }
    })
}

fn decode_credentials(creds_base64: &str) -> Result<String, CubeError> {
    Ok(String::from_utf8(base64::decode(creds_base64)?)?)
}

#[derive(Debug)]
pub struct GCSRemoteFs {
    dir: PathBuf,
    bucket: String,
    sub_path: Option<String>,
    delete_mut: AsyncMutex<()>,
    use_service_account: bool,
}

impl GCSRemoteFs {
    pub fn new(dir: PathBuf, bucket_name: String, sub_path: Option<String>) -> Result<Arc<Self>, CubeError> {
        let use_service_account = env::var("CUBESTORE_GCP_CREDENTIALS").is_ok() ||
                                  env::var("CUBESTORE_GCP_KEY_FILE").is_ok() ||
                                  env::var("GOOGLE_APPLICATION_CREDENTIALS").is_ok();

        if use_service_account {
            ensure_credentials_init();
        }

        Ok(Arc::new(Self {
            dir,
            bucket: bucket_name,
            sub_path,
            delete_mut: AsyncMutex::new(()),
            use_service_account,
        }))
    }

    fn gcs_path(&self, path: &str) -> String {
        match &self.sub_path {
            Some(sub_path) => format!("{}/{}", sub_path, path),
            None => path.to_string(),
        }
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

    async fn check_upload_file(&self, remote_path: String, expected_size: u64) -> Result<(), CubeError> {
        CommonRemoteFsUtils::check_upload_file(self, remote_path, expected_size).await
    }

    async fn upload_file(&self, temp_upload_path: String, remote_path: String) -> Result<u64, CubeError> {
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            1,
            Some(&["operation:upload_file".to_string(), "driver:gcs".to_string()])
        );
        let time = SystemTime::now();
        debug!("Uploading {}", remote_path);
        let file = File::open(&temp_upload_path).await?;
        let stream = FramedRead::new(file, BytesCodec::new()).map(|r| r.map(|b| b.to_vec()));
        Object::create_streamed(
            self.bucket.as_str(),
            stream,
            None,
            &self.gcs_path(&remote_path),
            "application/octet-stream"
        ).await?;
        let size = fs::metadata(&temp_upload_path).await?.len();
        self.check_upload_file(remote_path.clone(), size).await?;

        let local_path = self.dir.join(&remote_path);
        if &temp_upload_path != local_path.to_str().unwrap() {
            fs::create_dir_all(local_path.parent().unwrap()).await?;
            fs::rename(&temp_upload_path, &local_path).await?;
        }
        info!("Uploaded {} in {:?}", remote_path, time.elapsed()?);
        Ok(fs::metadata(local_path).await?.len())
    }

    async fn download_file(&self, remote_path: String, _expected_file_size: Option<u64>) -> Result<String, CubeError> {
        let local_file = self.dir.join(&remote_path);
        let downloads_dir = local_file.parent().unwrap().join("downloads");
        fs::create_dir_all(&downloads_dir).await.map_err(|e| CubeError::internal(format!("Failed to create downloads directory: {}", e)))?;

        if !local_file.exists() {
            app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
                1,
                Some(&["operation:download_file".to_string(), "driver:gcs".to_string()])
            );
            let time = SystemTime::now();
            debug!("Downloading {}", remote_path);

            let (temp_file, temp_path) = cube_ext::spawn_blocking(move || NamedTempFile::new_in(&downloads_dir))
                .await?
                .map_err(|e| CubeError::internal(format!("Failed to create a temporary file: {}", e)))?
                .into_parts();

            let mut writer = BufWriter::new(File::from_std(temp_file));
            let mut stream = Object::download_streamed(
                self.bucket.as_str(),
                &self.gcs_path(&remote_path),
            ).await.map_err(|e| CubeError::internal(format!("Failed to start download stream: {}", e)))?;

            let mut total_bytes = 0;
            while let Some(Ok(chunk)) = stream.next().await {
                writer.write_all(&chunk).await.map_err(|e| CubeError::internal(format!("Failed to write to file: {}", e)))?;
                total_bytes += chunk.len();
            }
            writer.flush().await.map_err(|e| CubeError::internal(format!("Failed to flush writer: {}", e)))?;

            cube_ext::spawn_blocking(move || -> Result<_, PathPersistError> {
                temp_path.persist(&local_file)?;
                Ok(())
            }).await.map_err(|e| CubeError::internal(format!("Failed to persist temporary file: {}", e)))??;

            info!("Downloaded '{}' ({} bytes) in {:?}", remote_path, total_bytes, time.elapsed()?);
        }

        Ok(local_file.into_os_string().into_string().unwrap())
    }

    async fn delete_file(&self, remote_path: String) -> Result<(), CubeError> {
        // Measure operation and add metrics
        app_metrics::REMOTE_FS_OPERATION_CORE.add_with_tags(
            1,
            Some(&["operation:delete_file".to_string(), "driver:gcs".to_string()])
        );

        let time = SystemTime::now();
        debug!("Deleting {}", remote_path);

        Object::delete(self.bucket.as_str(), &self.gcs_path(&remote_path)).await
            .map_err(|e| CubeError::internal(format!("Failed to delete remote file: {}", e)))?;

        info!("Deleted remote file '{}' ({:?})", remote_path, time.elapsed()?);

        let _guard = self.delete_mut.lock().await;
        let local_path = self.dir.join(&remote_path);
        if fs::metadata(&local_path).await.is_ok() {
            fs::remove_file(&local_path).await
                .map_err(|e| CubeError::internal(format!("Failed to delete local file: {}", e)))?;
            if let Some(parent) = local_path.parent() {
                if fs::read_dir(parent).await?.next().await.is_none() {
                    fs::remove_dir(parent).await
                        .map_err(|e| CubeError::internal(format!("Failed to remove empty directory: {}", e)))?;
                }
            }

            info!("Deleted local file '{}' ({:?})", local_path.display(), time.elapsed()?);
        }

        Ok(())
    }

    async fn list(&self, remote_prefix: String) -> Result<Vec<String>, CubeError> {
        debug!("Listing files with prefix: {}", remote_prefix);
        let full_prefix = self.gcs_path(&remote_prefix);
        let mut result = vec![];
        let mut page_token = None;
        loop {
            let response = Object::list_prefix(
                &self.bucket,
                &full_prefix,
                page_token,
            )
            .await
            .map_err(|e| CubeError::internal(format!("Failed listing objects: {}", e)))?;

            for object in response.items.iter() {
                if let Some(name) = object.name.strip_prefix(&full_prefix) {
                    result.push(name.trim_start_matches('/').to_string());
                }
            }

            if let Some(next_token) = response.next_page_token {
                page_token = Some(next_token);
            } else {
                break;
            }
        }

        info!("Found {} files with prefix: {}", result.len(), remote_prefix);
        Ok(result)
    }

    async fn list_with_metadata(
        &self,
        remote_prefix: String,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let full_prefix = self.gcs_path(&remote_prefix);
        let leading_slash_regex = Regex::new(&format!("^{}", Regex::escape(&full_prefix))).unwrap();

        let mut results = vec![];
        let mut page_token = None;

        loop {
            let mut request = ListRequest {
                prefix: Some(full_prefix.clone()),
                page_token,
                ..Default::default()
            };

            let response = Object::list(self.bucket.as_str(), request)
                .await
                .map_err(|e| CubeError::internal(format!("Failed to list objects with prefix '{}': {}", full_prefix, e)))?;

            for object in response.items {
                if let Some(name) = object.name.strip_prefix(&full_prefix) {
                    let cleaned_name = leading_slash_regex.replace(name, NoExpand("")).to_string();
                    results.push(RemoteFile {
                        remote_path: cleaned_name.trim_start_matches('/').to_string(),
                        updated: object.updated,
                        file_size: object.size.try_into().unwrap_or(0),
                    });
                }
            }

            if let Some(next_page_token) = response.next_page_token {
                if next_page_token.is_empty() {
                    break;
                } else {
                    page_token = Some(next_page_token);
                }
            } else {
                break;
            }
        }

        Ok(results)
    }

    async fn local_path(&self) -> Result<String, CubeError> {
        Ok(self.dir.to_str().unwrap().to_owned())
    }

}
