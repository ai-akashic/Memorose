use anyhow::Result;
use memorose_common::MemoryUnit;
use crate::storage::kv::KvStore;
use async_trait::async_trait;

#[async_trait]
pub trait Reranker: Send + Sync {
    async fn rerank(&self, store: &KvStore, candidates: Vec<(MemoryUnit, f32)>) -> Result<Vec<(MemoryUnit, f32)>>;
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
    async fn rerank(&self, store: &KvStore, candidates: Vec<(MemoryUnit, f32)>) -> Result<Vec<(MemoryUnit, f32)>> {
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
