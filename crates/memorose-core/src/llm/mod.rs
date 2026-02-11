pub mod gemini;

pub use gemini::GeminiClient;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionOutput {
    pub content: String,
    pub valid_at: Option<String>,
}

#[async_trait]
pub trait LLMClient: Send + Sync {
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Batch embed multiple texts. Returns embeddings in the same order as input.
    /// Default implementation falls back to individual embed() calls.
    async fn embed_batch(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        let mut results = Vec::new();
        for text in texts {
            results.push(self.embed(&text).await?);
        }
        Ok(results)
    }

    async fn generate(&self, prompt: &str) -> Result<String>;
    async fn compress(&self, text: &str) -> Result<CompressionOutput>;
    async fn summarize_group(&self, texts: Vec<String>) -> Result<String>;
    async fn describe_image(&self, image_url_or_base64: &str) -> Result<String>;
    async fn transcribe(&self, audio_url_or_base64: &str) -> Result<String>;
    async fn describe_video(&self, video_url: &str) -> Result<String>;
}
