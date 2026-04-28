use crate::storage::kv::KvStore;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use memorose_common::config::{LLMConfig, RerankerConfig, RerankerType};
use memorose_common::MemoryUnit;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait Reranker: Send + Sync {
    async fn rerank(
        &self,
        query: &str,
        store: &KvStore,
        candidates: Vec<(MemoryUnit, f32)>,
    ) -> Result<Vec<(MemoryUnit, f32)>>;
    async fn apply_feedback(
        &self,
        store: &KvStore,
        cited_ids: Vec<String>,
        retrieved_ids: Vec<String>,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct WeightedReranker {}

impl WeightedReranker {
    pub fn new() -> Self {
        Self {}
    }

    async fn get_weights(&self, store: &KvStore) -> Result<RerankerWeights> {
        let key = b"reranker:weights";
        let val = store.get(key)?;

        match val {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Ok(RerankerWeights::default()),
        }
    }

    async fn save_weights(&self, store: &KvStore, weights: RerankerWeights) -> Result<()> {
        let key = b"reranker:weights";
        let val = serde_json::to_vec(&weights)?;
        store.put(key, &val)?;
        Ok(())
    }

    fn calculate_recency(&self, unit: &MemoryUnit) -> f32 {
        let now = chrono::Utc::now();
        let age_secs = now
            .signed_duration_since(unit.transaction_time)
            .num_seconds() as f32;
        let half_life = 7.0 * 24.0 * 3600.0;
        (0.5f32).powf(age_secs / half_life)
    }
}

#[async_trait]
impl Reranker for WeightedReranker {
    async fn rerank(
        &self,
        _query: &str,
        store: &KvStore,
        candidates: Vec<(MemoryUnit, f32)>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let weights = self.get_weights(store).await?;

        let mut reranked = Vec::new();
        for (unit, sim_score) in candidates {
            let recency = self.calculate_recency(&unit);
            let final_score = sim_score * weights.similarity_weight
                + unit.importance * weights.importance_weight
                + recency * weights.recency_weight;

            reranked.push((unit, final_score));
        }

        reranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        Ok(reranked)
    }

    async fn apply_feedback(
        &self,
        store: &KvStore,
        cited_ids: Vec<String>,
        retrieved_ids: Vec<String>,
    ) -> Result<()> {
        let mut weights = self.get_weights(store).await?;

        for id in retrieved_ids {
            let is_cited = cited_ids.contains(&id);
            let reward = if is_cited { 1.0 } else { -1.0 };
            let learning_rate = 0.01;

            if is_cited {
                weights.similarity_weight += learning_rate * reward;
            } else {
                weights.similarity_weight += learning_rate * reward;
            }
            weights.similarity_weight = weights.similarity_weight.max(0.1).min(2.0);
        }

        self.save_weights(store, weights).await?;
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RerankerWeights {
    similarity_weight: f32,
    importance_weight: f32,
    recency_weight: f32,
}

impl Default for RerankerWeights {
    fn default() -> Self {
        Self {
            similarity_weight: 1.0,
            importance_weight: 0.2,
            recency_weight: 0.1,
        }
    }
}

// ---------------------------------------------------------
// HttpReranker (Custom Model / BGE-Reranker via Webhook)
// ---------------------------------------------------------

#[derive(Serialize)]
struct HttpRerankRequest {
    query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_n: Option<usize>,
    candidates: Vec<HttpCandidate>,
}

#[derive(Serialize)]
struct HttpCandidate {
    id: String,
    text: String,
    base_score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct HttpRerankResponse {
    results: Vec<HttpRerankResult>,
}

#[derive(Debug, Deserialize)]
struct HttpRerankResult {
    id: String,
    score: f32,
}

pub struct HttpReranker {
    endpoint: String,
    client: Client,
    headers: HashMap<String, String>,
    model: Option<String>,
    top_n: Option<usize>,
    include_metadata: bool,
}

impl HttpReranker {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: Client::builder()
                .no_proxy()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            headers: HashMap::new(),
            model: None,
            top_n: None,
            include_metadata: false,
        }
    }

    pub fn from_config(config: &RerankerConfig) -> Result<Self> {
        let endpoint = config.endpoint.clone().ok_or_else(|| {
            anyhow!(
                "HTTP reranker provider '{}' requires an endpoint",
                config.provider.as_deref().unwrap_or("custom")
            )
        })?;

        let client = Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(config.timeout_secs.max(1)))
            .build()?;

        Ok(Self {
            endpoint,
            client,
            headers: config.headers.clone(),
            model: config.model.clone(),
            top_n: config.top_n,
            include_metadata: config.include_metadata,
        })
    }
}

pub fn build_reranker(config: &RerankerConfig) -> Arc<dyn Reranker> {
    build_reranker_with_llm_config(config, None)
}

pub fn build_reranker_with_llm_config(
    config: &RerankerConfig,
    llm_config: Option<&LLMConfig>,
) -> Arc<dyn Reranker> {
    match config.r#type {
        RerankerType::Weighted => Arc::new(WeightedReranker::new()),
        RerankerType::Arbitrator => {
            let Some(llm_config) = llm_config else {
                tracing::warn!(
                    provider = config.provider.as_deref().unwrap_or("arbitrator"),
                    "Arbitrator mode requires LLM config; falling back to weighted reranker"
                );
                return Arc::new(WeightedReranker::new());
            };
            match ArbitratorReranker::from_config(config, llm_config) {
                Ok(arbitrator) if config.fallback_to_weighted => Arc::new(FallbackReranker::new(
                    Arc::new(arbitrator),
                    Arc::new(WeightedReranker::new()),
                )),
                Ok(arbitrator) => Arc::new(arbitrator),
                Err(err) => {
                    tracing::warn!(
                        provider = config.provider.as_deref().unwrap_or("arbitrator"),
                        error = %err,
                        "Failed to build arbitrator reranker; falling back to weighted reranker"
                    );
                    Arc::new(WeightedReranker::new())
                }
            }
        }
        RerankerType::Http => build_http_reranker(config),
    }
}

fn build_http_reranker(config: &RerankerConfig) -> Arc<dyn Reranker> {
    let Some(_) = config.endpoint else {
        tracing::warn!(
            provider = config.provider.as_deref().unwrap_or("http"),
            "Reranker provider has no endpoint; falling back to weighted reranker"
        );
        return Arc::new(WeightedReranker::new());
    };

    match HttpReranker::from_config(config) {
        Ok(http) if config.fallback_to_weighted => Arc::new(FallbackReranker::new(
            Arc::new(http),
            Arc::new(WeightedReranker::new()),
        )),
        Ok(http) => Arc::new(http),
        Err(err) => {
            tracing::warn!(
                provider = config.provider.as_deref().unwrap_or("http"),
                error = %err,
                "Failed to build configured reranker; falling back to weighted reranker"
            );
            Arc::new(WeightedReranker::new())
        }
    }
}

pub struct ArbitratorReranker {
    client: Arc<dyn crate::llm::LLMClient>,
    model: Option<String>,
    max_candidates: Option<usize>,
    include_metadata: bool,
}

fn build_arbitrator_llm_config(config: &RerankerConfig, llm_config: &LLMConfig) -> LLMConfig {
    let mut arbitrator_llm_config = llm_config.clone();
    if let Some(model) = config
        .model
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty())
    {
        arbitrator_llm_config.model = model.to_string();
    }
    arbitrator_llm_config
}

impl ArbitratorReranker {
    pub fn from_config(config: &RerankerConfig, llm_config: &LLMConfig) -> Result<Self> {
        let arbitrator_llm_config = build_arbitrator_llm_config(config, llm_config);
        let client = crate::llm::create_llm_client(&arbitrator_llm_config).ok_or_else(|| {
            anyhow!("arbitrator reranker requires an API key for the configured LLM")
        })?;
        Ok(Self {
            client,
            model: Some(arbitrator_llm_config.model),
            max_candidates: config.max_candidates,
            include_metadata: config.include_metadata,
        })
    }

    pub fn new(
        client: Arc<dyn crate::llm::LLMClient>,
        model: Option<String>,
        max_candidates: Option<usize>,
        include_metadata: bool,
    ) -> Self {
        Self {
            client,
            model,
            max_candidates,
            include_metadata,
        }
    }

    fn build_prompt(&self, query: &str, candidates: &[(MemoryUnit, f32)]) -> Result<String> {
        let candidates_json: Vec<serde_json::Value> = candidates
            .iter()
            .map(|(unit, base_score)| {
                let mut value = serde_json::json!({
                    "id": unit.id.to_string(),
                    "text": unit.content,
                    "base_score": base_score,
                });
                if self.include_metadata {
                    value["metadata"] = serde_json::json!({
                        "importance": unit.importance,
                        "level": unit.level,
                        "user_id": unit.user_id,
                        "org_id": unit.org_id,
                        "domain": unit.domain,
                        "memory_type": unit.memory_type,
                        "namespace_key": unit.namespace_key,
                    });
                }
                value
            })
            .collect();

        let payload = serde_json::json!({
            "query": query,
            "model": self.model,
            "selection_mode": "dynamic",
            "max_candidates": self.max_candidates,
            "candidates": candidates_json,
        });

        Ok(format!(
            "You are a strict arbitrator for memory retrieval.\n\
Return ONLY valid JSON in this schema: {{\"results\":[{{\"id\":\"...\",\"score\":0.0}}]}}.\n\
Scores must be floats from 0.0 to 1.0. Include only candidate ids from the input. \
Choose only the candidates that are genuinely relevant to the query. Return fewer results when few candidates are useful, and return an empty results array when none are useful. Do not pad to a fixed count.\n\n{}",
            serde_json::to_string(&payload)?
        ))
    }

    fn parse_response(&self, response: &str) -> Result<HttpRerankResponse> {
        let trimmed = response.trim();
        let json_text = trimmed
            .strip_prefix("```json")
            .and_then(|s| s.strip_suffix("```"))
            .or_else(|| {
                trimmed
                    .strip_prefix("```")
                    .and_then(|s| s.strip_suffix("```"))
            })
            .unwrap_or(trimmed)
            .trim();
        Ok(serde_json::from_str(json_text)?)
    }
}

#[async_trait]
impl Reranker for ArbitratorReranker {
    async fn rerank(
        &self,
        query: &str,
        _store: &KvStore,
        candidates: Vec<(MemoryUnit, f32)>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let arbitrated_candidates: Vec<(MemoryUnit, f32)> = self
            .max_candidates
            .map(|limit| candidates.iter().take(limit).cloned().collect())
            .unwrap_or_else(|| candidates.clone());
        let prompt = self.build_prompt(query, &arbitrated_candidates)?;
        let response = self.client.generate(&prompt).await?;
        let resp_data = self.parse_response(&response.data)?;

        let mut score_map = HashMap::new();
        for result in resp_data.results {
            score_map.insert(result.id, result.score.clamp(0.0, 1.0));
        }

        let mut reranked = Vec::new();
        for (unit, _) in arbitrated_candidates {
            if let Some(score) = score_map.get(&unit.id.to_string()) {
                reranked.push((unit, *score));
            }
        }
        reranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(reranked)
    }

    async fn apply_feedback(
        &self,
        _store: &KvStore,
        _cited_ids: Vec<String>,
        _retrieved_ids: Vec<String>,
    ) -> Result<()> {
        Ok(())
    }
}

struct FallbackReranker {
    primary: Arc<dyn Reranker>,
    fallback: Arc<dyn Reranker>,
}

impl FallbackReranker {
    fn new(primary: Arc<dyn Reranker>, fallback: Arc<dyn Reranker>) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl Reranker for FallbackReranker {
    async fn rerank(
        &self,
        query: &str,
        store: &KvStore,
        candidates: Vec<(MemoryUnit, f32)>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        match self.primary.rerank(query, store, candidates.clone()).await {
            Ok(reranked) => Ok(reranked),
            Err(err) => {
                tracing::warn!(error = %err, "Primary reranker failed; using weighted fallback");
                self.fallback.rerank(query, store, candidates).await
            }
        }
    }

    async fn apply_feedback(
        &self,
        store: &KvStore,
        cited_ids: Vec<String>,
        retrieved_ids: Vec<String>,
    ) -> Result<()> {
        if let Err(err) = self
            .primary
            .apply_feedback(store, cited_ids.clone(), retrieved_ids.clone())
            .await
        {
            tracing::debug!(error = %err, "Primary reranker feedback hook failed");
        }
        self.fallback
            .apply_feedback(store, cited_ids, retrieved_ids)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use memorose_common::config::{LLMConfig, LLMProvider, RerankerConfig};
    use memorose_common::{MemoryType, SharePolicy};
    use serde_json::json;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use uuid::Uuid;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    struct MockRerankLlm {
        response: String,
    }

    #[async_trait]
    impl crate::llm::LLMClient for MockRerankLlm {
        async fn embed(&self, _text: &str) -> Result<crate::llm::LLMResponse<Vec<f32>>> {
            Ok(crate::llm::LLMResponse::default())
        }

        async fn generate(&self, prompt: &str) -> Result<crate::llm::LLMResponse<String>> {
            assert!(prompt.contains("strict arbitrator"));
            assert!(prompt.contains("plugin candidate"));
            Ok(crate::llm::LLMResponse {
                data: self.response.clone(),
                usage: memorose_common::TokenUsage::default(),
            })
        }

        async fn compress(
            &self,
            _text: &str,
            _is_agent: bool,
        ) -> Result<crate::llm::LLMResponse<crate::llm::CompressionOutput>> {
            unimplemented!()
        }

        async fn summarize_group(
            &self,
            _texts: Vec<String>,
        ) -> Result<crate::llm::LLMResponse<String>> {
            unimplemented!()
        }

        async fn describe_image(
            &self,
            _image_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            unimplemented!()
        }

        async fn transcribe(
            &self,
            _audio_url_or_base64: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            unimplemented!()
        }

        async fn describe_video(
            &self,
            _video_url: &str,
        ) -> Result<crate::llm::LLMResponse<String>> {
            unimplemented!()
        }
    }

    fn build_memory(content: &str, importance: f32, age_days: i64) -> MemoryUnit {
        let mut unit = MemoryUnit::new(
            None,
            "user-1".to_string(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            content.to_string(),
            None,
        );
        unit.importance = importance;
        unit.transaction_time = Utc::now() - Duration::days(age_days);
        unit.last_accessed_at = unit.transaction_time;
        unit.share_policy = SharePolicy::default();
        unit
    }

    #[tokio::test]
    async fn test_weighted_reranker_returns_empty_for_no_candidates() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let reranker = WeightedReranker::new();

        let reranked = reranker.rerank("query", &store, Vec::new()).await?;
        assert!(reranked.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_weighted_reranker_prefers_recent_and_important_memories() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let reranker = WeightedReranker::new();
        reranker
            .save_weights(
                &store,
                RerankerWeights {
                    similarity_weight: 0.1,
                    importance_weight: 1.0,
                    recency_weight: 1.0,
                },
            )
            .await?;

        let old_high_similarity = build_memory("old", 0.1, 30);
        let fresh_important = build_memory("fresh", 1.0, 0);

        let reranked = reranker
            .rerank(
                "query",
                &store,
                vec![
                    (old_high_similarity.clone(), 0.9),
                    (fresh_important.clone(), 0.6),
                ],
            )
            .await?;

        assert_eq!(reranked.len(), 2);
        assert_eq!(reranked[0].0.content, "fresh");
        assert!(reranked[0].1 > reranked[1].1);
        Ok(())
    }

    #[tokio::test]
    async fn test_weighted_reranker_apply_feedback_updates_and_clamps_weights() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let reranker = WeightedReranker::new();

        for idx in 0..300 {
            reranker
                .apply_feedback(&store, Vec::new(), vec![format!("uncited-{idx}")])
                .await?;
        }

        let weights = reranker.get_weights(&store).await?;
        assert!((weights.similarity_weight - 0.1).abs() < 1e-6);
        Ok(())
    }

    #[tokio::test]
    async fn test_weighted_reranker_reads_persisted_weights() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let reranker = WeightedReranker::new();

        store.put(
            b"reranker:weights",
            &serde_json::to_vec(&RerankerWeights {
                similarity_weight: 1.4,
                importance_weight: 0.7,
                recency_weight: 0.2,
            })?,
        )?;

        let weights = reranker.get_weights(&store).await?;
        assert!((weights.similarity_weight - 1.4).abs() < 1e-6);
        assert!((weights.importance_weight - 0.7).abs() < 1e-6);
        assert!((weights.recency_weight - 0.2).abs() < 1e-6);
        Ok(())
    }

    #[tokio::test]
    async fn test_http_reranker_returns_empty_for_no_candidates() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let reranker = HttpReranker::new("http://localhost:9".to_string());

        let reranked = reranker.rerank("query", &store, Vec::new()).await?;
        assert!(reranked.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_http_reranker_invalid_endpoint_errors() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let reranker = HttpReranker::new("not-a-valid-url".to_string());

        let err = reranker
            .rerank(
                "query",
                &store,
                vec![(build_memory("candidate", 0.5, 1), 0.6)],
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(!err.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_http_reranker_apply_feedback_is_noop() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let reranker = HttpReranker::new("http://localhost:9".to_string());

        reranker
            .apply_feedback(&store, vec!["a".to_string()], vec!["b".to_string()])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_http_reranker_uses_plugin_config() -> Result<()> {
        let mock_server = MockServer::start().await;
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer plugin-key".to_string());

        let config = RerankerConfig {
            r#type: RerankerType::Http,
            provider: Some("jina".to_string()),
            endpoint: Some(format!("{}/rerank", mock_server.uri())),
            headers,
            model: Some("jina-reranker-v2-base-multilingual".to_string()),
            top_n: Some(1),
            max_candidates: None,
            timeout_secs: 2,
            fallback_to_weighted: false,
            include_metadata: true,
        };

        let candidate = build_memory("plugin candidate", 0.7, 1);
        Mock::given(method("POST"))
            .and(path("/rerank"))
            .and(header("Authorization", "Bearer plugin-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "results": [{ "id": candidate.id.to_string(), "score": 0.99 }]
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let reranker = HttpReranker::from_config(&config)?;
        let reranked = reranker
            .rerank("plugin query", &store, vec![(candidate.clone(), 0.2)])
            .await?;

        assert_eq!(reranked.len(), 1);
        assert_eq!(reranked[0].0.id, candidate.id);
        assert!((reranked[0].1 - 0.99).abs() < 1e-6);

        let requests = mock_server
            .received_requests()
            .await
            .expect("request recording should be enabled");
        let body = String::from_utf8(requests[0].body.clone())?;
        assert!(body.contains("\"query\":\"plugin query\""));
        assert!(body.contains("\"model\":\"jina-reranker-v2-base-multilingual\""));
        assert!(body.contains("\"top_n\":1"));
        assert!(body.contains("\"text\":\"plugin candidate\""));
        assert!(body.contains("\"metadata\""));
        assert!(body.contains("\"importance\":"));
        Ok(())
    }

    #[tokio::test]
    async fn test_plugin_reranker_falls_back_to_weighted_when_enabled() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let config = RerankerConfig {
            r#type: RerankerType::Http,
            provider: Some("custom".to_string()),
            endpoint: Some("http://127.0.0.1:9/rerank".to_string()),
            fallback_to_weighted: true,
            ..RerankerConfig::default()
        };
        let reranker = build_reranker(&config);

        let old_high_similarity = build_memory("old", 0.1, 30);
        let fresh_important = build_memory("fresh", 1.0, 0);
        let reranked = reranker
            .rerank(
                "query",
                &store,
                vec![
                    (old_high_similarity.clone(), 0.9),
                    (fresh_important.clone(), 0.6),
                ],
            )
            .await?;

        assert_eq!(reranked.len(), 2);
        Ok(())
    }

    #[test]
    fn test_arbitrator_config_overrides_llm_model() {
        let llm_config = LLMConfig {
            provider: LLMProvider::Gemini,
            openai_api_key: None,
            google_api_key: Some("test-key".to_string()),
            base_url: Some("https://example.test".to_string()),
            model: "main-model".to_string(),
            embedding_model: "embedding-model".to_string(),
            embedding_dim: 1536,
            embedding_output_dim: Some(768),
            embedding_task_type: Some("RETRIEVAL_DOCUMENT".to_string()),
            stt_provider: None,
            stt_model: None,
        };
        let reranker_config = RerankerConfig {
            r#type: RerankerType::Arbitrator,
            model: Some("arbitrator-model".to_string()),
            ..RerankerConfig::default()
        };

        let arbitrator_llm_config = build_arbitrator_llm_config(&reranker_config, &llm_config);

        assert_eq!(arbitrator_llm_config.model, "arbitrator-model");
        assert_eq!(arbitrator_llm_config.embedding_model, "embedding-model");
        assert_eq!(
            arbitrator_llm_config.base_url.as_deref(),
            Some("https://example.test")
        );
        assert_eq!(arbitrator_llm_config.embedding_output_dim, Some(768));
    }

    #[tokio::test]
    async fn test_arbitrator_reranker_selects_relevant_candidates_dynamically() -> Result<()> {
        let temp_dir = tempdir()?;
        let store = KvStore::open(temp_dir.path())?;
        let candidate = build_memory("plugin candidate", 0.7, 1);
        let other = build_memory("other candidate", 0.1, 2);
        let llm = Arc::new(MockRerankLlm {
            response: serde_json::json!({
                "results": [
                    { "id": candidate.id.to_string(), "score": 0.94 }
                ]
            })
            .to_string(),
        });
        let reranker = ArbitratorReranker::new(
            llm,
            Some("gemini-3.1-flash-lite-preview".to_string()),
            Some(2),
            true,
        );

        let reranked = reranker
            .rerank(
                "plugin query",
                &store,
                vec![(other.clone(), 0.8), (candidate.clone(), 0.2)],
            )
            .await?;

        assert_eq!(reranked.len(), 1);
        assert_eq!(reranked[0].0.id, candidate.id);
        assert!((reranked[0].1 - 0.94).abs() < 1e-6);
        Ok(())
    }
}

#[async_trait]
impl Reranker for HttpReranker {
    async fn rerank(
        &self,
        query: &str,
        _store: &KvStore,
        candidates: Vec<(MemoryUnit, f32)>,
    ) -> Result<Vec<(MemoryUnit, f32)>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let http_candidates: Vec<HttpCandidate> = candidates
            .iter()
            .map(|(u, s)| HttpCandidate {
                id: u.id.to_string(),
                text: u.content.clone(),
                base_score: *s,
                metadata: self.include_metadata.then(|| {
                    serde_json::json!({
                        "importance": u.importance,
                        "level": u.level,
                        "user_id": u.user_id,
                        "org_id": u.org_id,
                        "domain": u.domain,
                        "memory_type": u.memory_type,
                        "namespace_key": u.namespace_key,
                        "transaction_time": u.transaction_time,
                        "access_count": u.access_count,
                    })
                }),
            })
            .collect();

        let req = HttpRerankRequest {
            query: query.to_string(),
            model: self.model.clone(),
            top_n: self.top_n,
            candidates: http_candidates,
        };

        let mut request = self.client.post(&self.endpoint).json(&req);
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        let res = request.send().await?;

        if !res.status().is_success() {
            return Err(anyhow!("HTTP Reranker failed with status {}", res.status()));
        }

        let resp_data: HttpRerankResponse = res.json().await?;

        let mut score_map = std::collections::HashMap::new();
        for r in resp_data.results {
            score_map.insert(r.id, r.score);
        }

        let mut reranked = Vec::new();
        for (unit, base_score) in candidates {
            let final_score = *score_map.get(&unit.id.to_string()).unwrap_or(&base_score);
            reranked.push((unit, final_score));
        }

        reranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        if let Some(top_n) = self.top_n {
            reranked.truncate(top_n);
        }

        Ok(reranked)
    }

    async fn apply_feedback(
        &self,
        _store: &KvStore,
        _cited_ids: Vec<String>,
        _retrieved_ids: Vec<String>,
    ) -> Result<()> {
        // We could send a feedback webhook here if the external reranker supports online learning.
        // For now, no-op.
        Ok(())
    }
}
