use anyhow::{Result, anyhow};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use super::LLMClient;
use base64::{Engine as _, engine::general_purpose};

pub struct GeminiClient {
    client: Client,
    api_key: String,
    base_url: String,
    model: String,
    embedding_model: String,
}

impl GeminiClient {
    pub fn new(api_key: String, model: String, embedding_model: String) -> Self {
        Self::with_base_url(api_key, model, embedding_model, "https://generativelanguage.googleapis.com".to_string())
    }

    pub fn with_base_url(api_key: String, model: String, embedding_model: String, base_url: String) -> Self {
        let api_key = api_key.trim().to_string();
        tracing::debug!(
            "GeminiClient initialized: api_key_len={}, api_key_prefix={}, model={}, embedding_model={}, base_url={}",
            api_key.len(),
            if api_key.len() >= 10 { &api_key[..10] } else { &api_key },
            model,
            embedding_model,
            base_url
        );
        Self {
            client: Client::new(),
            api_key,
            base_url,
            model,
            embedding_model,
        }
    }
}

// ============== Generate API Structures ==============

#[derive(Serialize)]
struct GenerateRequest {
    contents: Vec<Content>,
    #[serde(rename = "systemInstruction", skip_serializing_if = "Option::is_none")]
    system_instruction: Option<Content>,
}

#[derive(Serialize, Deserialize)]
struct Content {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    parts: Vec<Part>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
enum Part {
    Text { text: String },
    Inline { #[serde(rename = "inline_data")] inline_data: InlineData },
}

#[derive(Serialize, Deserialize, Clone)]
struct InlineData {
    mime_type: String,
    data: String,
}

#[derive(Deserialize)]
struct GenerateResponse {
    candidates: Option<Vec<Candidate>>,
    error: Option<ApiError>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<GeminiUsageMetadata>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiUsageMetadata {
    prompt_token_count: Option<u32>,
    candidates_token_count: Option<u32>,
    total_token_count: Option<u32>,
}

#[derive(Deserialize)]
struct Candidate {
    content: Content,
}

#[derive(Deserialize)]
struct ApiError {
    message: String,
}

// ============== Embedding API Structures ==============

#[derive(Serialize)]
struct EmbedRequest {
    model: String,
    content: EmbedContent,
}

#[derive(Serialize)]
struct EmbedContent {
    parts: Vec<Part>,
}

#[derive(Deserialize)]
struct EmbedResponse {
    embedding: Option<Embedding>,
    error: Option<ApiError>,
}

#[derive(Deserialize)]
struct Embedding {
    values: Vec<f32>,
}

// ============== Batch Embedding API Structures ==============

#[derive(Serialize)]
struct BatchEmbedRequest {
    requests: Vec<EmbedRequest>,
}

#[derive(Deserialize)]
struct BatchEmbedResponse {
    embeddings: Option<Vec<Embedding>>,
    error: Option<ApiError>,
}

const GEMINI_BATCH_EMBED_MAX_SIZE: usize = 100;

#[async_trait]
impl LLMClient for GeminiClient {
    async fn generate(&self, prompt: &str) -> Result<super::LLMResponse<String>> {
        self.call_generate(None, prompt).await
    }

    async fn embed(&self, text: &str) -> Result<super::LLMResponse<Vec<f32>>> {
        let clean_model = self.embedding_model.trim_start_matches("models/");
        let model_name = format!("models/{}", clean_model);
        let url = format!(
            "{}/v1beta/models/{}:embedContent?key={}",
            self.base_url.trim_end_matches('/'),
            clean_model,
            self.api_key.trim()
        );

        let request = EmbedRequest {
            model: model_name,
            content: EmbedContent {
                parts: vec![Part::Text { text: text.to_string() }],
            },
        };

        let response = self.client.post(&url).json(&request).send().await?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(anyhow!("Gemini Embedding API error ({}): {}", status, body));
        }

        let parsed: EmbedResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow!("Failed to parse Gemini embedding response: {} - body: {}", e, body))?;

        if let Some(err) = parsed.error {
            return Err(anyhow!("Gemini Embedding API error: {}", err.message));
        }

        let data = parsed
            .embedding
            .map(|e| e.values)
            .ok_or_else(|| anyhow!("No embedding in Gemini response"))?;

        Ok(super::LLMResponse { data, usage: Default::default() })
    }

    async fn embed_batch(&self, texts: Vec<String>) -> Result<super::LLMResponse<Vec<Vec<f32>>>> {
        if texts.is_empty() {
            return Ok(super::LLMResponse { data: vec![], usage: Default::default() });
        }

        let clean_model = self.embedding_model.trim_start_matches("models/");
        let model_name = format!("models/{}", clean_model);
        let url = format!(
            "{}/v1beta/models/{}:batchEmbedContents?key={}",
            self.base_url.trim_end_matches('/'),
            clean_model,
            self.api_key.trim()
        );

        let mut all_embeddings = Vec::with_capacity(texts.len());

        for chunk in texts.chunks(GEMINI_BATCH_EMBED_MAX_SIZE) {
            let requests: Vec<EmbedRequest> = chunk
                .iter()
                .map(|text| EmbedRequest {
                    model: model_name.clone(),
                    content: EmbedContent {
                        parts: vec![Part::Text {
                            text: text.to_string(),
                        }],
                    },
                })
                .collect();

            let batch_request = BatchEmbedRequest { requests };

            tracing::debug!(
                "Batch embedding chunk size={} (total={})",
                chunk.len(),
                texts.len()
            );

            let response = self.client.post(&url).json(&batch_request).send().await?;

            let status = response.status();
            let body = response.text().await?;

            if !status.is_success() {
                return Err(anyhow!(
                    "Gemini Batch Embedding API error ({}): {}",
                    status,
                    body
                ));
            }

            let parsed: BatchEmbedResponse = serde_json::from_str(&body).map_err(|e| {
                anyhow!(
                    "Failed to parse Gemini batch embedding response: {} - body: {}",
                    e,
                    body
                )
            })?;

            if let Some(err) = parsed.error {
                return Err(anyhow!("Gemini Batch Embedding API error: {}", err.message));
            }

            let embeddings = parsed
                .embeddings
                .ok_or_else(|| anyhow!("No embeddings in Gemini batch response"))?;

            if embeddings.len() != chunk.len() {
                return Err(anyhow!(
                    "Gemini batch embedding count mismatch: requested={}, received={}",
                    chunk.len(),
                    embeddings.len()
                ));
            }

            all_embeddings.extend(embeddings.into_iter().map(|e| e.values));
        }

        Ok(super::LLMResponse { data: all_embeddings, usage: Default::default() })
    }

    async fn compress(&self, text: &str, is_agent: bool) -> Result<super::LLMResponse<super::CompressionOutput>> {
        let system_prompt = if is_agent {
            // PROCEDURAL (Agent) PROMPT
            "You are an expert at extracting and summarizing Agent execution trajectories and experiences. \
            Your task is to produce a comprehensive summary of the agent's actions, logic, and outcomes. \
            \
            CRITICAL RULES: \
            - RECONSTRUCT THE STATE MACHINE: Clearly identify the Goal, the Plan, the Action taken, the Observation (Result), and the Reflection (what was learned). \
            - PRESERVE ERRORS: If an API call failed, record exactly what failed and the stated reason. \
            - BE VERBOSE ON LOGIC: Do not just give the final answer. The step-by-step logic and tool usage is the core of this memory. \
            - OMIT USER CHITCHAT: Focus purely on the agent's internal workings. \
            \
            Output ONLY valid JSON: \
            {\"content\": \"detailed agent trajectory and reflection\", \"valid_at\": null}"
        } else {
            // FACTUAL (User) PROMPT
            "You are an expert at extracting core facts, preferences, and profiles about a HUMAN user from text. \
            Compress the following event into a concise, high-density factual statement. \
            \
            CRITICAL RULES: \
            - PRESERVE ALL specific numbers, quantities, dates, and proper nouns exactly. \
            - EXTRACT PREFERENCES: e.g., 'User likes X', 'User is allergic to Y'. \
            - OMIT AGENT/SYSTEM TEXT: Disregard anything the AI assistant said. Focus 100% on the human. \
            - Keep the first-person perspective (use 'I' if the original uses it) when referring to the user. \
            \
            If the text contains specific time references (e.g., 'last week'), extract the estimated UTC timestamp. \
            \
            Output ONLY valid JSON: \
            {\"content\": \"compressed factual summary\", \"valid_at\": \"ISO8601 timestamp or null\"}"
        };
        
        let response = self.call_generate(Some(system_prompt), text).await?;
        
        let clean_json = response.data.trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        let parsed: super::CompressionOutput = serde_json::from_str(clean_json)
            .map_err(|e| anyhow!("Failed to parse compression JSON: {} - body: {}", e, clean_json))?;

        Ok(super::LLMResponse { data: parsed, usage: response.usage })
    }

    async fn summarize_group(&self, texts: Vec<String>) -> Result<super::LLMResponse<String>> {
        let combined = texts.join("\n---\n");
        let prompt = format!("Synthesize the following related memories into a single, high-level abstract insight that captures the underlying pattern or knowledge:\n\n{}", combined);
        self.generate(&prompt).await
    }

    async fn describe_image(&self, image_url_or_base64: &str) -> Result<super::LLMResponse<String>> {
        let (mime_type, data) = if image_url_or_base64.starts_with("http") {
             let resp = self.client.get(image_url_or_base64).send().await?;
             let headers = resp.headers();
             let mime = headers.get("content-type")
                 .and_then(|v| v.to_str().ok())
                 .unwrap_or("image/jpeg")
                 .to_string();
             let bytes = resp.bytes().await?;
             (mime, general_purpose::STANDARD.encode(&bytes))
        } else {
             // Assume it's base64, default to jpeg if unknown
             ("image/jpeg".to_string(), image_url_or_base64.to_string())
        };

        let prompt = "Describe this image in detail, focusing on objects, actions, and text visible.";
        
        self.call_generate_parts(None, vec![
             Part::Text { text: prompt.to_string() },
             Part::Inline { inline_data: InlineData { mime_type, data } }
        ]).await
    }

    async fn transcribe(&self, audio_url_or_base64: &str) -> Result<super::LLMResponse<String>> {
        let (mime_type, data) = if audio_url_or_base64.starts_with("http") {
             let resp = self.client.get(audio_url_or_base64).send().await?;
             let headers = resp.headers();
             let mime = headers.get("content-type")
                 .and_then(|v| v.to_str().ok())
                 .unwrap_or("audio/mp3")
                 .to_string();
             let bytes = resp.bytes().await?;
             (mime, general_purpose::STANDARD.encode(&bytes))
        } else {
             ("audio/mp3".to_string(), audio_url_or_base64.to_string())
        };

        let prompt = "Transcribe the following audio verbatim. Identify speakers if possible.";

        self.call_generate_parts(None, vec![
             Part::Text { text: prompt.to_string() },
             Part::Inline { inline_data: InlineData { mime_type, data } }
        ]).await
    }

    async fn describe_video(&self, video_url: &str) -> Result<super::LLMResponse<String>> {
        let (mime_type, data) = if video_url.starts_with("http") {
             let resp = self.client.get(video_url).send().await?;
             let headers = resp.headers();
             let mime = headers.get("content-type")
                 .and_then(|v| v.to_str().ok())
                 .unwrap_or("video/mp4")
                 .to_string();
             let bytes = resp.bytes().await?;
             (mime, general_purpose::STANDARD.encode(&bytes))
        } else {
             ("video/mp4".to_string(), video_url.to_string())
        };

        let prompt = "Describe this video in detail: what happens, key scenes, visible text, people, actions, and any spoken dialogue.";

        self.call_generate_parts(None, vec![
             Part::Text { text: prompt.to_string() },
             Part::Inline { inline_data: InlineData { mime_type, data } }
        ]).await
    }
}

impl GeminiClient {
    async fn call_generate(&self, system_prompt: Option<&str>, user_prompt: &str) -> Result<super::LLMResponse<String>> {
        self.call_generate_parts(system_prompt, vec![Part::Text { text: user_prompt.to_string() }]).await
    }

    async fn call_generate_parts(&self, system_prompt: Option<&str>, parts: Vec<Part>) -> Result<super::LLMResponse<String>> {
        let clean_model = self.model.trim_start_matches("models/");
        let url = format!(
            "{}/v1beta/models/{}:generateContent?key={}",
            self.base_url.trim_end_matches('/'), 
            clean_model, 
            self.api_key.trim()
        );

        let request = GenerateRequest {
            contents: vec![Content {
                role: Some("user".to_string()),
                parts,
            }],
            system_instruction: system_prompt.map(|s| Content {
                role: None,
                parts: vec![Part::Text { text: s.to_string() }],
            }),
        };

        tracing::debug!("Gemini generate request to: {} (key masked)", url.replace(&self.api_key, "***"));
        
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        
        if !status.is_success() {
            tracing::error!("Gemini API failed: status={}, api_key_empty={}", status, self.api_key.is_empty());
            return Err(anyhow!("Gemini API error ({}): {}", status, body));
        }

        let parsed: GenerateResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow!("Failed to parse Gemini response: {} - body: {}", e, body))?;

        if let Some(err) = parsed.error {
            return Err(anyhow!("Gemini API error: {}", err.message));
        }

        let text = parsed.candidates
            .and_then(|c| c.into_iter().next())
            .and_then(|c| c.content.parts.into_iter().next())
            .map(|p| match p {
                Part::Text { text } => text,
                _ => "".to_string(), // Should not happen for text-only response expectation, or handle properly
            })
            .ok_or_else(|| anyhow!("No content in Gemini response"))?;

        let usage = parsed.usage_metadata.map(|m| memorose_common::TokenUsage {
            prompt_tokens: m.prompt_token_count.unwrap_or(0),
            completion_tokens: m.candidates_token_count.unwrap_or(0),
            total_tokens: m.total_token_count.unwrap_or(0),
        }).unwrap_or_default();

        Ok(super::LLMResponse { data: text, usage })
    }
}

#[cfg(test)]
#[path = "gemini_tests.rs"]
mod tests;