use anyhow::{Result, anyhow};
use memorose_common::MemoryUnit;
use crate::storage::kv::KvStore;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait Reranker: Send + Sync {
    async fn rerank(&self, query: &str, store: &KvStore, candidates: Vec<(MemoryUnit, f32)>) -> Result<Vec<(MemoryUnit, f32)>>;
    async fn apply_feedback(&self, store: &KvStore, cited_ids: Vec<String>, retrieved_ids: Vec<String>) -> Result<()>;
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
        let age_secs = now.signed_duration_since(unit.transaction_time).num_seconds() as f32;
        let half_life = 7.0 * 24.0 * 3600.0;
        (0.5f32).powf(age_secs / half_life)
    }
}

#[async_trait]
impl Reranker for WeightedReranker {
    async fn rerank(&self, _query: &str, store: &KvStore, candidates: Vec<(MemoryUnit, f32)>) -> Result<Vec<(MemoryUnit, f32)>> {
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

    async fn apply_feedback(&self, store: &KvStore, cited_ids: Vec<String>, retrieved_ids: Vec<String>) -> Result<()> {
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
    candidates: Vec<HttpCandidate>,
}

#[derive(Serialize)]
struct HttpCandidate {
    id: String,
    text: String,
    base_score: f32,
}

#[derive(Deserialize)]
struct HttpRerankResponse {
    results: Vec<HttpRerankResult>,
}

#[derive(Deserialize)]
struct HttpRerankResult {
    id: String,
    score: f32,
}

pub struct HttpReranker {
    endpoint: String,
    client: Client,
}

impl HttpReranker {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: Client::builder().timeout(std::time::Duration::from_secs(10)).build().unwrap_or_default(),
        }
    }
}

#[async_trait]
impl Reranker for HttpReranker {
    async fn rerank(&self, query: &str, _store: &KvStore, candidates: Vec<(MemoryUnit, f32)>) -> Result<Vec<(MemoryUnit, f32)>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let http_candidates: Vec<HttpCandidate> = candidates.iter().map(|(u, s)| HttpCandidate {
            id: u.id.to_string(),
            text: u.content.clone(),
            base_score: *s,
        }).collect();

        let req = HttpRerankRequest {
            query: query.to_string(),
            candidates: http_candidates,
        };

        let res = self.client.post(&self.endpoint)
            .json(&req)
            .send()
            .await?;

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

        Ok(reranked)
    }

    async fn apply_feedback(&self, _store: &KvStore, _cited_ids: Vec<String>, _retrieved_ids: Vec<String>) -> Result<()> {
        // We could send a feedback webhook here if the external reranker supports online learning.
        // For now, no-op.
        Ok(())
    }
}
