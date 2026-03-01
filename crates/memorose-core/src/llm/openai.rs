use anyhow::{Result, anyhow};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use super::{LLMClient, CompressionOutput};
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Debug, Clone, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<Message>,
    temperature: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
    usage: Option<Usage>,
}

#[derive(Debug, Clone, Deserialize)]
struct Usage {
    prompt_tokens: u32,
    completion_tokens: Option<u32>,
    total_tokens: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct Choice {
    message: ResponseMessage,
}

#[derive(Debug, Clone, Deserialize)]
struct ResponseMessage {
    content: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct EmbedRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedData>,
    usage: Option<Usage>,
}

#[derive(Debug, Clone, Deserialize)]
struct EmbedData {
    embedding: Vec<f32>,
}

pub struct OpenAIClient {
    client: Client,
    api_key: String,
    base_url: String,
    model: String,
    embedding_model: String,
}

impl OpenAIClient {
    pub fn new(api_key: String, model: String, embedding_model: String, base_url: Option<String>) -> Self {
        let actual_base_url = base_url.unwrap_or_else(|| "https://api.openai.com/v1".to_string());
        
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap_or_default();

        Self {
            client,
            api_key,
            base_url: actual_base_url,
            model,
            embedding_model,
        }
    }

    async fn call_chat_completion(&self, system_prompt: Option<&str>, user_prompt: &str, is_json: bool) -> Result<super::LLMResponse<String>> {
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        
        let mut messages = Vec::new();
        if let Some(sys) = system_prompt {
            messages.push(Message { role: "system".to_string(), content: sys.to_string() });
        }
        messages.push(Message { role: "user".to_string(), content: user_prompt.to_string() });

        let mut req = ChatRequest {
            model: self.model.clone(),
            messages,
            temperature: 0.1,
            response_format: None,
        };

        if is_json {
            req.response_format = Some(json!({ "type": "json_object" }));
        }

        let res = self.client.post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&req)
            .send()
            .await?;

        let status = res.status();
        let body = res.text().await?;

        if !status.is_success() {
            return Err(anyhow!("OpenAI API error ({}): {}", status, body));
        }

        let parsed: ChatResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow!("Failed to parse OpenAI response: {} - body: {}", e, body))?;

        let content = parsed.choices.first()
            .and_then(|c| c.message.content.clone())
            .ok_or_else(|| anyhow!("No content in OpenAI response"))?;

        let usage = parsed.usage.map(|u| memorose_common::TokenUsage {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens.unwrap_or(0),
            total_tokens: u.total_tokens,
        }).unwrap_or_default();

        Ok(super::LLMResponse { data: content, usage })
    }
}

#[async_trait]
impl LLMClient for OpenAIClient {
    async fn generate(&self, prompt: &str) -> Result<super::LLMResponse<String>> {
        self.call_chat_completion(None, prompt, false).await
    }

    async fn compress(&self, text: &str, is_agent: bool) -> Result<super::LLMResponse<CompressionOutput>> {
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
        
        let response = self.call_chat_completion(Some(system_prompt), text, true).await?;
        
        let parsed: CompressionOutput = serde_json::from_str(&response.data)
            .map_err(|e| anyhow!("Failed to parse JSON compression output: {} - Output: {}", e, response.data))?;
            
        Ok(super::LLMResponse { data: parsed, usage: response.usage })
    }

    async fn summarize_group(&self, texts: Vec<String>) -> Result<super::LLMResponse<String>> {
        let system_prompt = "Synthesize these related memory fragments into a coherent, single high-level insight.";
        let combined = texts.join("\n---\n");
        self.call_chat_completion(Some(system_prompt), &combined, false).await
    }

    async fn embed(&self, text: &str) -> Result<super::LLMResponse<Vec<f32>>> {
        let results = self.embed_batch(vec![text.to_string()]).await?;
        let emb = results.data.into_iter().next().ok_or_else(|| anyhow!("Empty embedding response"))?;
        Ok(super::LLMResponse { data: emb, usage: results.usage })
    }

    async fn embed_batch(&self, texts: Vec<String>) -> Result<super::LLMResponse<Vec<Vec<f32>>>> {
        let url = format!("{}/embeddings", self.base_url.trim_end_matches('/'));
        
        let req = EmbedRequest {
            model: self.embedding_model.clone(),
            input: texts,
        };

        let res = self.client.post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&req)
            .send()
            .await?;

        let status = res.status();
        let body = res.text().await?;

        if !status.is_success() {
            return Err(anyhow!("OpenAI Embedding API error ({}): {}", status, body));
        }

        let parsed: EmbedResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow!("Failed to parse OpenAI embedding response: {} - body: {}", e, body))?;

        let embeddings = parsed.data.into_iter().map(|d| d.embedding).collect();
        
        let usage = parsed.usage.map(|u| memorose_common::TokenUsage {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens.unwrap_or(0),
            total_tokens: u.total_tokens,
        }).unwrap_or_default();

        Ok(super::LLMResponse { data: embeddings, usage })
    }

    async fn describe_image(&self, image_url_or_base64: &str) -> Result<super::LLMResponse<String>> {
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        
        let image_url = if image_url_or_base64.starts_with("http") {
            image_url_or_base64.to_string()
        } else {
            format!("data:image/jpeg;base64,{}", image_url_or_base64)
        };

        let req = MultiModalChatRequest {
            model: self.model.clone(),
            messages: vec![MultiModalMessage {
                role: "user".to_string(),
                content: MessageContent::Parts(vec![
                    ContentPart::Text {
                        r#type: "text".to_string(),
                        text: "Describe this image in detail, focusing on objects, actions, and text visible.".to_string(),
                    },
                    ContentPart::ImageUrl {
                        r#type: "image_url".to_string(),
                        image_url: ImageUrlDetail { url: image_url },
                    },
                ]),
            }],
            temperature: 0.1,
            max_tokens: 300,
        };

        let res = self.client.post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&req)
            .send()
            .await?;

        let status = res.status();
        let body = res.text().await?;

        if !status.is_success() {
            return Err(anyhow!("OpenAI Vision API error ({}): {}", status, body));
        }

        let parsed: ChatResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow!("Failed to parse OpenAI Vision response: {} - body: {}", e, body))?;

        let content = parsed.choices.first()
            .and_then(|c| c.message.content.clone())
            .ok_or_else(|| anyhow!("No content in OpenAI Vision response"))?;

        let usage = parsed.usage.map(|u| memorose_common::TokenUsage {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens.unwrap_or(0),
            total_tokens: u.total_tokens,
        }).unwrap_or_default();

        Ok(super::LLMResponse { data: content, usage })
    }

    async fn transcribe(&self, _audio_url_or_base64: &str) -> Result<super::LLMResponse<String>> {
        Err(anyhow!("transcribe not fully implemented for OpenAIClient yet"))
    }

    async fn describe_video(&self, _video_url: &str) -> Result<super::LLMResponse<String>> {
        Err(anyhow!("describe_video not fully implemented for OpenAIClient yet"))
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum ContentPart {
    Text {
        r#type: String, // "text"
        text: String,
    },
    ImageUrl {
        r#type: String, // "image_url"
        image_url: ImageUrlDetail,
    },
}

#[derive(Debug, Clone, Serialize)]
struct ImageUrlDetail {
    url: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum MessageContent {
    String(String),
    Parts(Vec<ContentPart>),
}

#[derive(Debug, Clone, Serialize)]
struct MultiModalMessage {
    role: String,
    content: MessageContent,
}

#[derive(Debug, Clone, Serialize)]
struct MultiModalChatRequest {
    model: String,
    messages: Vec<MultiModalMessage>,
    temperature: f32,
    max_tokens: u32,
}

#[cfg(test)]
#[path = "openai_tests.rs"]
mod openai_tests;
