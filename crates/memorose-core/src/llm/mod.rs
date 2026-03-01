pub mod gemini;
pub mod openai;

pub use gemini::GeminiClient;
pub use openai::OpenAIClient;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use memorose_common::config::{LLMConfig, LLMProvider};

pub fn create_llm_client(config: &LLMConfig) -> Option<Arc<dyn LLMClient>> {
    match config.provider {
        LLMProvider::Gemini => {
            let api_key = config.google_api_key.clone()?;
            Some(Arc::new(GeminiClient::with_base_url(
                api_key,
                config.model.clone(),
                config.embedding_model.clone(),
                config.get_base_url().unwrap_or_else(|| "https://generativelanguage.googleapis.com".to_string()),
            )))
        }
        LLMProvider::OpenAI => {
            let api_key = config.openai_api_key.clone()?;
            Some(Arc::new(OpenAIClient::new(
                api_key,
                config.model.clone(),
                config.embedding_model.clone(),
                config.get_base_url(),
            )))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionOutput {
    pub content: String,
    pub valid_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LLMResponse<T> {
    pub data: T,
    pub usage: memorose_common::TokenUsage,
}

#[async_trait]
pub trait LLMClient: Send + Sync {
    async fn embed(&self, text: &str) -> Result<LLMResponse<Vec<f32>>>;

    /// Batch embed multiple texts. Returns embeddings in the same order as input.
    /// Default implementation falls back to individual embed() calls.
    async fn embed_batch(&self, texts: Vec<String>) -> Result<LLMResponse<Vec<Vec<f32>>>> {
        let mut results = Vec::new();
        let mut total_usage = memorose_common::TokenUsage::default();
        
        for text in texts {
            let res = self.embed(&text).await?;
            results.push(res.data);
            total_usage.prompt_tokens += res.usage.prompt_tokens;
            total_usage.completion_tokens += res.usage.completion_tokens;
            total_usage.total_tokens += res.usage.total_tokens;
        }
        Ok(LLMResponse { data: results, usage: total_usage })
    }

    async fn generate(&self, prompt: &str) -> Result<LLMResponse<String>>;
    async fn compress(&self, text: &str, is_agent: bool) -> Result<LLMResponse<CompressionOutput>>;
    async fn summarize_group(&self, texts: Vec<String>) -> Result<LLMResponse<String>>;
    
    // Multi-modal placeholders
    async fn describe_image(&self, image_url_or_base64: &str) -> Result<LLMResponse<String>>;
    async fn transcribe(&self, audio_url_or_base64: &str) -> Result<LLMResponse<String>>;
    async fn describe_video(&self, video_url: &str) -> Result<LLMResponse<String>>;
}
