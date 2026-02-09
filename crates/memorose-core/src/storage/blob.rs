use async_trait::async_trait;
use anyhow::Result;
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
        // Ensure directory exists
        if !self.base_path.exists() {
            fs::create_dir_all(&self.base_path).await?;
        }

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
        let filename = storage_key.strip_prefix("local://")
            .ok_or_else(|| anyhow::anyhow!("Invalid storage key format"))?;
        
        Ok(format!("{}/{}", self.base_url, filename))
    }
}
