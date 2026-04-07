use anyhow::Result;
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::fs;
use uuid::Uuid;

#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Uploads raw bytes and returns a storage key
    async fn put(&self, data: &[u8], mime_type: &str) -> Result<String>;

    /// Generates a public accessible URL (Pre-signed URL for S3, or Static URL for Local)
    async fn get_access_url(&self, storage_key: &str) -> Result<String>;
}

pub struct LocalFileSystemStore {
    base_path: PathBuf,
    base_url: String,
}

impl LocalFileSystemStore {
    pub fn new(base_path: PathBuf, base_url: String) -> Self {
        Self {
            base_path,
            base_url,
        }
    }
}

#[async_trait]
impl ObjectStore for LocalFileSystemStore {
    async fn put(&self, data: &[u8], mime_type: &str) -> Result<String> {
        // create_dir_all is idempotent; no need for a racy exists() check
        fs::create_dir_all(&self.base_path).await?;

        let id = Uuid::new_v4();
        // Simple extension mapping (can be expanded)
        let ext = match mime_type {
            "image/png" => "png",
            "image/jpeg" => "jpg",
            "image/webp" => "webp",
            _ => "bin",
        };
        let filename = format!("{}.{}", id, ext);
        let file_path = self.base_path.join(&filename);

        fs::write(&file_path, data).await?;

        // Return a local storage key scheme
        Ok(format!("local://{}", filename))
    }

    async fn get_access_url(&self, storage_key: &str) -> Result<String> {
        // storage_key expected format: local://filename.ext
        let filename = storage_key
            .strip_prefix("local://")
            .ok_or_else(|| anyhow::anyhow!("Invalid storage key format"))?;

        Ok(format!("{}/{}", self.base_url, filename))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_local_file_system_store_put_and_get() {
        let dir = tempdir().unwrap();
        let store = LocalFileSystemStore::new(
            dir.path().to_path_buf(),
            "http://localhost/assets".to_string(),
        );

        let data = b"fake image data";
        let key = store.put(data, "image/png").await.unwrap();

        assert!(key.starts_with("local://"));
        assert!(key.ends_with(".png"));

        let url = store.get_access_url(&key).await.unwrap();
        assert!(url.starts_with("http://localhost/assets/"));
        assert!(url.ends_with(".png"));

        let invalid_key = "s3://foo.png";
        assert!(store.get_access_url(invalid_key).await.is_err());

        let jpeg_key = store.put(data, "image/jpeg").await.unwrap();
        assert!(jpeg_key.ends_with(".jpg"));

        let webp_key = store.put(data, "image/webp").await.unwrap();
        assert!(webp_key.ends_with(".webp"));

        let bin_key = store.put(data, "application/octet-stream").await.unwrap();
        assert!(bin_key.ends_with(".bin"));
    }
}
