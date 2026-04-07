pub mod gemini;
pub mod openai;

pub use gemini::GeminiClient;
pub use openai::OpenAIClient;

use anyhow::Result;
use async_trait::async_trait;
use memorose_common::config::{LLMConfig, LLMProvider};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Represents embedding input that can be text or multimodal content.
#[derive(Debug, Clone)]
pub enum EmbedInput {
    Text(String),
    Multimodal { parts: Vec<EmbedPart> },
}

/// A part of a multimodal embedding input.
#[derive(Debug, Clone)]
pub enum EmbedPart {
    Text(String),
    InlineData { mime_type: String, data: String }, // base64-encoded
}

impl EmbedInput {
    /// Extract text content from this input (for fallback embedding or display).
    pub fn as_text(&self) -> String {
        match self {
            EmbedInput::Text(t) => t.clone(),
            EmbedInput::Multimodal { parts } => parts
                .iter()
                .filter_map(|p| match p {
                    EmbedPart::Text(t) => Some(t.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" "),
        }
    }

    /// Returns true if this input contains non-text parts.
    pub fn has_multimodal_parts(&self) -> bool {
        match self {
            EmbedInput::Text(_) => false,
            EmbedInput::Multimodal { parts } => parts
                .iter()
                .any(|p| matches!(p, EmbedPart::InlineData { .. })),
        }
    }
}

pub fn create_llm_client(config: &LLMConfig) -> Option<Arc<dyn LLMClient>> {
    match config.provider {
        LLMProvider::Gemini => {
            let api_key = config.google_api_key.clone()?;
            Some(Arc::new(GeminiClient::with_base_url(
                api_key,
                config.model.clone(),
                config.embedding_model.clone(),
                config
                    .get_base_url()
                    .unwrap_or_else(|| "https://generativelanguage.googleapis.com".to_string()),
                config.embedding_output_dim,
                config.embedding_task_type.clone(),
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
        Ok(LLMResponse {
            data: results,
            usage: total_usage,
        })
    }

    /// Embed a single input that may be text or multimodal content.
    /// Default implementation extracts text and calls embed().
    async fn embed_content(&self, input: EmbedInput) -> Result<LLMResponse<Vec<f32>>> {
        self.embed(&input.as_text()).await
    }

    /// Batch embed multiple inputs that may be text or multimodal.
    /// Default implementation extracts text and calls embed_batch().
    async fn embed_content_batch(
        &self,
        inputs: Vec<EmbedInput>,
    ) -> Result<LLMResponse<Vec<Vec<f32>>>> {
        let texts: Vec<String> = inputs.iter().map(|i| i.as_text()).collect();
        self.embed_batch(texts).await
    }

    async fn generate(&self, prompt: &str) -> Result<LLMResponse<String>>;
    async fn compress(&self, text: &str, is_agent: bool) -> Result<LLMResponse<CompressionOutput>>;
    async fn summarize_group(&self, texts: Vec<String>) -> Result<LLMResponse<String>>;

    // Multi-modal placeholders
    async fn describe_image(&self, image_url_or_base64: &str) -> Result<LLMResponse<String>>;
    async fn transcribe(&self, audio_url_or_base64: &str) -> Result<LLMResponse<String>>;
    async fn describe_video(&self, video_url: &str) -> Result<LLMResponse<String>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyLLM;

    #[async_trait::async_trait]
    impl LLMClient for DummyLLM {
        async fn embed(&self, _text: &str) -> anyhow::Result<LLMResponse<Vec<f32>>> {
            Ok(LLMResponse {
                data: vec![1.0, 2.0, 3.0],
                usage: memorose_common::TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 0,
                    total_tokens: 1,
                },
            })
        }

        async fn generate(&self, _prompt: &str) -> anyhow::Result<LLMResponse<String>> { unimplemented!() }
        async fn compress(&self, _text: &str, _is_agent: bool) -> anyhow::Result<LLMResponse<CompressionOutput>> { unimplemented!() }
        async fn summarize_group(&self, _texts: Vec<String>) -> anyhow::Result<LLMResponse<String>> { unimplemented!() }
        async fn describe_image(&self, _image_url_or_base64: &str) -> anyhow::Result<LLMResponse<String>> { unimplemented!() }
        async fn transcribe(&self, _audio_url_or_base64: &str) -> anyhow::Result<LLMResponse<String>> { unimplemented!() }
        async fn describe_video(&self, _video_url: &str) -> anyhow::Result<LLMResponse<String>> { unimplemented!() }
    }

    #[tokio::test]
    async fn test_default_embed_batch_calls_embed() {
        let llm = DummyLLM;
        let res = llm.embed_batch(vec!["a".into(), "b".into()]).await.unwrap();
        assert_eq!(res.data.len(), 2);
        assert_eq!(res.data[0], vec![1.0, 2.0, 3.0]);
        assert_eq!(res.usage.total_tokens, 2);
    }

    #[tokio::test]
    async fn test_default_embed_content_calls_embed() {
        let llm = DummyLLM;
        let input = EmbedInput::Text("test".into());
        let res = llm.embed_content(input).await.unwrap();
        assert_eq!(res.data, vec![1.0, 2.0, 3.0]);
    }

    #[tokio::test]
    async fn test_default_embed_content_batch_calls_embed_batch() {
        let llm = DummyLLM;
        let inputs = vec![EmbedInput::Text("test1".into()), EmbedInput::Text("test2".into())];
        let res = llm.embed_content_batch(inputs).await.unwrap();
        assert_eq!(res.data.len(), 2);
        assert_eq!(res.usage.total_tokens, 2);
    }
}
