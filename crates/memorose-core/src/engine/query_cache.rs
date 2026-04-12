use anyhow::Result;
use memorose_common::GraphEdge;
use std::collections::HashMap;
use uuid::Uuid;

impl super::MemoroseEngine {
    pub async fn batch_get_neighbors(
        &self,
        user_id: &str,
        node_ids: &[Uuid],
    ) -> Result<HashMap<Uuid, Vec<GraphEdge>>> {
        self.batch_executor
            .batch_get_outgoing_edges(user_id, node_ids)
            .await
    }

    /// 带缓存的邻居查询（用于热点查询）
    pub async fn get_neighbors_cached(
        &self,
        user_id: &str,
        node_id: Uuid,
    ) -> Result<Vec<GraphEdge>> {
        use crate::graph::CacheKey;

        let cache_key = CacheKey::OneHopNeighbors {
            user_id: user_id.to_string(),
            node_id,
            direction: crate::graph::cache::Direction::Outgoing,
        };

        // Try to get from cache
        if let Some(cached) = self.query_cache.get_edges(&cache_key).await {
            return Ok(cached);
        }

        // 缓存未命中，查询数据库
        let edges = self.graph.get_outgoing_edges(user_id, node_id).await?;

        // 写入缓存
        self.query_cache.put_edges(cache_key, edges.clone()).await;

        Ok(edges)
    }

    /// 多跳遍历（使用批量优化）
    pub async fn multi_hop_traverse(
        &self,
        user_id: &str,
        start_nodes: Vec<Uuid>,
        max_hops: usize,
        min_weight: Option<f32>,
    ) -> Result<Vec<Uuid>> {
        self.batch_executor
            .batch_multi_hop_traverse(user_id, start_nodes, max_hops, min_weight)
            .await
    }

    /// 失效用户的查询缓存（在写入边时调用）
    pub async fn invalidate_query_cache(&self, user_id: &str) {
        self.query_cache.invalidate_user(user_id).await;
    }

    /// Get cache statistics
    pub async fn query_cache_stats(&self) -> crate::graph::cache::CacheStats {
        self.query_cache.stats().await
    }
}
