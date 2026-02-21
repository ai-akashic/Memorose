// Query Cache - Borrowed from lance-graph's query cache ideas
// Caches frequently used query results to avoid redundant computations

use uuid::Uuid;
use std::time::Duration;
use moka::future::Cache;
use memorose_common::GraphEdge;

/// Cache key types
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum CacheKey {
    /// 1-hop neighborhood cache
    OneHopNeighbors {
        user_id: String,
        node_id: Uuid,
        direction: Direction,
    },
    /// Multi-hop traversal cache
    MultiHopTraversal {
        user_id: String,
        start_nodes: Vec<Uuid>,
        max_hops: usize,
    },
    /// Community detection results cache
    CommunityDetection {
        user_id: String,
        algorithm: String,
    },
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Direction {
    Outgoing,
    Incoming,
}

/// Query result cache
pub struct QueryCache {
    /// Edge query result cache
    edge_cache: Cache<CacheKey, Vec<GraphEdge>>,
    /// Node ID list cache
    node_list_cache: Cache<CacheKey, Vec<Uuid>>,
    /// Cache configuration
    config: CacheConfig,
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Cache time-to-live
    pub ttl: Duration,
    /// Maximum number of cache entries
    pub max_entries: usize,
    /// Whether caching is enabled
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),  // 5 minutes
            max_entries: 10000,
            enabled: true,
        }
    }
}

impl QueryCache {
    pub fn new(config: CacheConfig) -> Self {
        let edge_cache = Cache::builder()
            .time_to_live(config.ttl)
            .max_capacity(config.max_entries as u64)
            .build();

        let node_list_cache = Cache::builder()
            .time_to_live(config.ttl)
            .max_capacity(config.max_entries as u64)
            .build();

        Self {
            edge_cache,
            node_list_cache,
            config,
        }
    }

    /// Retrieve cached edge query results
    pub async fn get_edges(&self, key: &CacheKey) -> Option<Vec<GraphEdge>> {
        if !self.config.enabled {
            return None;
        }

        if let Some(value) = self.edge_cache.get(key).await {
            tracing::debug!("Cache HIT for {:?}", key);
            return Some(value);
        }

        tracing::debug!("Cache MISS for {:?}", key);
        None
    }

    /// Cache edge query results
    pub async fn put_edges(&self, key: CacheKey, edges: Vec<GraphEdge>) {
        if !self.config.enabled {
            return;
        }
        self.edge_cache.insert(key, edges).await;
    }

    /// Retrieve cached node list
    pub async fn get_node_list(&self, key: &CacheKey) -> Option<Vec<Uuid>> {
        if !self.config.enabled {
            return None;
        }
        self.node_list_cache.get(key).await
    }

    /// Cache node list
    pub async fn put_node_list(&self, key: CacheKey, nodes: Vec<Uuid>) {
        if !self.config.enabled {
            return;
        }
        self.node_list_cache.insert(key, nodes).await;
    }

    /// Invalidate all caches for a specific user (e.g., when the user adds new edges)
    pub async fn invalidate_user(&self, user_id: &str) {
        let uid = user_id.to_string();
        let _ = self.edge_cache.invalidate_entries_if(move |k: &CacheKey, _v| {
            Self::key_matches_user(k, &uid)
        });

        let uid2 = user_id.to_string();
        let _ = self.node_list_cache.invalidate_entries_if(move |k: &CacheKey, _v| {
            Self::key_matches_user(k, &uid2)
        });

        tracing::info!("Invalidated cache for user: {}", user_id);
    }

    fn key_matches_user(key: &CacheKey, user_id: &str) -> bool {
        match key {
            CacheKey::OneHopNeighbors { user_id: uid, .. } => uid == user_id,
            CacheKey::MultiHopTraversal { user_id: uid, .. } => uid == user_id,
            CacheKey::CommunityDetection { user_id: uid, .. } => uid == user_id,
        }
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        self.edge_cache.run_pending_tasks().await;
        self.node_list_cache.run_pending_tasks().await;

        CacheStats {
            edge_cache_size: self.edge_cache.entry_count() as usize,
            node_cache_size: self.node_list_cache.entry_count() as usize,
            max_entries: self.config.max_entries,
        }
    }

    /// Clear all caches
    pub async fn clear(&self) {
        self.edge_cache.invalidate_all();
        self.node_list_cache.invalidate_all();
        tracing::info!("Cleared all query caches");
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub edge_cache_size: usize,
    pub node_cache_size: usize,
    pub max_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_hit_miss() {
        let cache = QueryCache::new(CacheConfig::default());

        let key = CacheKey::OneHopNeighbors {
            user_id: "user1".to_string(),
            node_id: Uuid::new_v4(),
            direction: Direction::Outgoing,
        };

        // Initially it should MISS
        assert!(cache.get_edges(&key).await.is_none());

        // Write to cache
        let edges = vec![];
        cache.put_edges(key.clone(), edges.clone()).await;

        // Now it should HIT
        assert!(cache.get_edges(&key).await.is_some());
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let mut config = CacheConfig::default();
        config.ttl = Duration::from_millis(100);  // 100ms TTL

        let cache = QueryCache::new(config);

        let key = CacheKey::OneHopNeighbors {
            user_id: "user1".to_string(),
            node_id: Uuid::new_v4(),
            direction: Direction::Outgoing,
        };

        cache.put_edges(key.clone(), vec![]).await;

        // Immediate query should hit
        assert!(cache.get_edges(&key).await.is_some());

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Now it should be invalidated
        assert!(cache.get_edges(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_user_invalidation() {
        let cache = QueryCache::new(CacheConfig::default());

        let key1 = CacheKey::OneHopNeighbors {
            user_id: "user1".to_string(),
            node_id: Uuid::new_v4(),
            direction: Direction::Outgoing,
        };

        let key2 = CacheKey::OneHopNeighbors {
            user_id: "user2".to_string(),
            node_id: Uuid::new_v4(),
            direction: Direction::Outgoing,
        };

        cache.put_edges(key1.clone(), vec![]).await;
        cache.put_edges(key2.clone(), vec![]).await;

        // Invalidate cache for user1
        cache.invalidate_user("user1").await;
        
        // Let background tasks complete
        cache.edge_cache.run_pending_tasks().await;
        cache.node_list_cache.run_pending_tasks().await;

        // Cache for user1 should be cleared
        assert!(cache.get_edges(&key1).await.is_none());

        // Cache for user2 should be preserved
        assert!(cache.get_edges(&key2).await.is_some());
    }
}