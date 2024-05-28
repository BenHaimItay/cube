use crate::app_metrics;
use crate::di_service;
use crate::remotefs::{CommonRemoteFsUtils, LocalDirRemoteFs, RemoteFile, RemoteFs};
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::cube_ext;
use log::{debug, info};
use regex::{NoExpand, Regex};
use s3::creds::Credentials;
use s3::{Bucket, Region};
use std::env;
use std::fmt;
use std::fmt::Formatter;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::sync::Mutex;

pub struct S3RemoteFs {
    dir: PathBuf,
    bucket: std::sync::RwLock<Bucket>,
    sub_path: Option<String>,
    delete_mut: Mutex<()>,
}

impl fmt::Debug for S3RemoteFs {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut s = f.debug_struct("S3RemoteFs");
        s.field("dir", &self.dir).field("sub_path", &self.sub_path);
        // Do not expose AWS credentials.
        match self.bucket.try_read() {
            Ok(bucket) => s
                .field("bucket_name", &bucket.name)
                .field("bucket_region", &bucket.region),
            Err(_) => s.field("bucket", &"<locked>"),
        };
        s.finish_non_exhaustive()
    }
}

impl S3RemoteFs {
    pub fn new(
        dir: PathBuf,
        region: String,
        bucket_name: String,
        sub_path: Option<String>,
    ) -> Result<Arc<Self>, CubeError> {
        // Use environment variables if present, otherwise use default credentials
        let key_id = env::var("CUBESTORE_AWS_ACCESS_KEY_ID").ok();
        let access_key = env::var("CUBESTORE_AWS_SECRET_ACCESS_KEY").ok();
        let credentials = if key_id.is_some() && access_key.is_some() {
            Credentials::new(key_id.as_deref(), access_key.as_deref(), None, None, None)?
        } else {
            Credentials::default()?
        };

        let region = region.parse::<Region>()?;
        let bucket = std::sync::RwLock::new(Bucket::new(&bucket_name, region.clone(), credentials)?);
        let fs = Arc::new(Self {
            dir,
            bucket,
            sub_path,
            delete_mut: Mutex::new(()),
        });
        spawn_creds_refresh_loop(key_id, access_key, bucket_name, region, &fs);
        Ok(fs)
    }
}

fn spawn_creds_refresh_loop(
    key_id: Option<String>,
    access_key: Option<String>,
    bucket_name: String,
    region: Region,
    fs: &Arc<S3RemoteFs>,
) {
    // Refresh credentials. TODO: use expiration time.
    let refresh_every = refresh_interval_from_env();
    if refresh_every.as_secs() == 0 {
        return;
    }
    let fs = Arc::downgrade(fs);
    std::thread::spawn(move || {
        log::debug!("Started S3 credentials refresh loop");
        loop {
            std::thread::sleep(refresh_every);
            let fs = match fs.upgrade() {
                None => {
                    log::debug!("Stopping S3 credentials refresh loop");
                    return;
                }
                Some(fs) => fs,
            };
            let c = match if key_id.is_some() && access_key.is_some() {
                Credentials::new(key_id.as_deref(), access_key.as_deref(), None, None, None)
            } else {
                Credentials::default()
            } {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Failed to refresh S3 credentials: {}", e);
                    continue;
                }
            };
            let b = match Bucket::new(&bucket_name, region.clone(), c) {
                Ok(b) => b,
                Err(e) => {
                    log::error!("Failed to refresh S3 credentials: {}", e);
                    continue;
                }
            };
            *fs.bucket.write().unwrap() = b;
            log::debug!("Successfully refreshed S3 credentials")
        }
    });
}

fn refresh_interval_from_env() -> Duration {
    let mut mins = 180; // 3 hours by default.
    if let Ok(s) = std::env::var("CUBESTORE_AWS_CREDS_REFRESH_EVERY_MINS") {
        match s.parse::<u64>() {
            Ok(i) => mins = i,
            Err(e) => log::error!("Could not parse CUBESTORE_AWS_CREDS_REFRESH_EVERY_MINS. Refreshing every {} minutes. Error: {}", mins, e),
        };
    };
    Duration::from_secs(60 * mins)
}

di_service!(S3RemoteFs, [RemoteFs]);

#[async_trait]
impl RemoteFs for S3RemoteFs {
    // ... (remaining implementation unchanged)
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
