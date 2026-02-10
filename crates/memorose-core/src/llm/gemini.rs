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

#[async_trait]
impl LLMClient for GeminiClient {
    async fn generate(&self, prompt: &str) -> Result<String> {
        self.call_generate(None, prompt).await
    }

    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let clean_model = self.embedding_model.trim_start_matches("models/");
        let url = format!(
            "{}/v1beta/models/{}:embedContent?key={}",
            self.base_url.trim_end_matches('/'), 
            clean_model, 
            self.api_key.trim()
        );

        let request = EmbedRequest {
            model: format!("models/{}", self.embedding_model),
            content: EmbedContent {
                parts: vec![Part::Text { text: text.to_string() }],
            },
        };

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await?;

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

        parsed.embedding
            .map(|e| e.values)
            .ok_or_else(|| anyhow!("No embedding in Gemini response"))
    }

    async fn compress(&self, text: &str) -> Result<super::CompressionOutput> {
        let system_prompt = "You are an expert at summarizing memories for an AI system. \
            Compress the following event into a concise, high-density factual statement. \
            \
            CRITICAL RULES: \
            - PRESERVE ALL specific numbers, quantities, amounts, durations, dates, prices, counts, and measurements exactly as stated. \
            - PRESERVE ALL proper nouns (names of people, places, brands, products). \
            - PRESERVE ALL key relationships (who did what, with whom, for whom). \
            - Keep the first-person perspective (use 'I' if the original uses it). \
            - Do NOT omit factual details to save space. Density over brevity. \
            \
            If the text contains specific time references (e.g., 'last week', 'in 2020', 'two days ago'), \
            extract the estimated UTC timestamp. \
            \
            Output ONLY valid JSON: \
            {\"content\": \"compressed summary with ALL facts preserved\", \"valid_at\": \"ISO8601 timestamp or null\"}";
        
        let result = self.call_generate(Some(system_prompt), text).await?;
        
        let clean_json = result.trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        serde_json::from_str(clean_json)
            .map_err(|e| anyhow!("Failed to parse compression JSON: {} - body: {}", e, clean_json))
    }

    async fn summarize_group(&self, texts: Vec<String>) -> Result<String> {
        let combined = texts.join("\n---\n");
        let prompt = format!("Synthesize the following related memories into a single, high-level abstract insight that captures the underlying pattern or knowledge:\n\n{}", combined);
        self.generate(&prompt).await
    }

    async fn describe_image(&self, image_url_or_base64: &str) -> Result<String> {
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

    async fn transcribe(&self, audio_url_or_base64: &str) -> Result<String> {
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

    async fn describe_video(&self, video_url: &str) -> Result<String> {
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
    async fn call_generate(&self, system_prompt: Option<&str>, user_prompt: &str) -> Result<String> {
        self.call_generate_parts(system_prompt, vec![Part::Text { text: user_prompt.to_string() }]).await
    }

    async fn call_generate_parts(&self, system_prompt: Option<&str>, parts: Vec<Part>) -> Result<String> {
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

        Ok(text)
    }
}

#[cfg(test)]
#[path = "gemini_tests.rs"]
mod tests;