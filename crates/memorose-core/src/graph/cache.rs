// Query Cache - 借鉴 lance-graph 的查询缓存思想
// 缓存常用的查询结果，避免重复计算

use uuid::Uuid;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::sync::Arc;
use tokio::sync::RwLock;
use memorose_common::GraphEdge;

/// 缓存键类型
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum CacheKey {
    /// 1-hop 邻居缓存
    OneHopNeighbors {
        user_id: String,
        node_id: Uuid,
        direction: Direction,
    },
    /// 多跳查询缓存
    MultiHopTraversal {
        user_id: String,
        start_nodes: Vec<Uuid>,
        max_hops: usize,
    },
    /// 社区检测结果缓存
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

/// 缓存条目
#[derive(Debug, Clone)]
struct CacheEntry<T> {
    value: T,
    created_at: Instant,
    access_count: usize,
}

impl<T> CacheEntry<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            created_at: Instant::now(),
            access_count: 0,
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }
}

/// 查询结果缓存
pub struct QueryCache {
    /// 边查询结果缓存
    edge_cache: Arc<RwLock<HashMap<CacheKey, CacheEntry<Vec<GraphEdge>>>>>,
    /// 节点 ID 列表缓存
    node_list_cache: Arc<RwLock<HashMap<CacheKey, CacheEntry<Vec<Uuid>>>>>,
    /// 缓存配置
    config: CacheConfig,
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// 缓存生存时间
    pub ttl: Duration,
    /// 最大缓存条目数
    pub max_entries: usize,
    /// 是否启用缓存
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),  // 5 分钟
            max_entries: 10000,
            enabled: true,
        }
    }
}

impl QueryCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            edge_cache: Arc::new(RwLock::new(HashMap::new())),
            node_list_cache: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// 获取缓存的边查询结果
    pub async fn get_edges(&self, key: &CacheKey) -> Option<Vec<GraphEdge>> {
        if !self.config.enabled {
            return None;
        }

        let mut cache = self.edge_cache.write().await;

        if let Some(entry) = cache.get_mut(key) {
            if entry.is_expired(self.config.ttl) {
                cache.remove(key);
                return None;
            }

            entry.access_count += 1;
            tracing::debug!(
                "Cache HIT for {:?} (access_count={})",
                key,
                entry.access_count
            );
            return Some(entry.value.clone());
        }

        tracing::debug!("Cache MISS for {:?}", key);
        None
    }

    /// 缓存边查询结果
    pub async fn put_edges(&self, key: CacheKey, edges: Vec<GraphEdge>) {
        if !self.config.enabled {
            return;
        }

        let mut cache = self.edge_cache.write().await;

        // LRU 驱逐策略
        if cache.len() >= self.config.max_entries {
            self.evict_lru(&mut cache).await;
        }

        cache.insert(key, CacheEntry::new(edges));
    }

    /// 获取缓存的节点列表
    pub async fn get_node_list(&self, key: &CacheKey) -> Option<Vec<Uuid>> {
        if !self.config.enabled {
            return None;
        }

        let mut cache = self.node_list_cache.write().await;

        if let Some(entry) = cache.get_mut(key) {
            if entry.is_expired(self.config.ttl) {
                cache.remove(key);
                return None;
            }

            entry.access_count += 1;
            return Some(entry.value.clone());
        }

        None
    }

    /// 缓存节点列表
    pub async fn put_node_list(&self, key: CacheKey, nodes: Vec<Uuid>) {
        if !self.config.enabled {
            return;
        }

        let mut cache = self.node_list_cache.write().await;

        if cache.len() >= self.config.max_entries {
            // 简单驱逐：移除最旧的条目
            if let Some(oldest_key) = cache.iter()
                .min_by_key(|(_, v)| v.created_at)
                .map(|(k, _)| k.clone())
            {
                cache.remove(&oldest_key);
            }
        }

        cache.insert(key, CacheEntry::new(nodes));
    }

    /// LRU 驱逐策略：移除访问次数最少且最旧的条目
    async fn evict_lru(&self, cache: &mut HashMap<CacheKey, CacheEntry<Vec<GraphEdge>>>) {
        if let Some(lru_key) = cache.iter()
            .min_by_key(|(_, v)| (v.access_count, v.created_at))
            .map(|(k, _)| k.clone())
        {
            cache.remove(&lru_key);
            tracing::debug!("Evicted LRU cache entry: {:?}", lru_key);
        }
    }

    /// 失效指定用户的所有缓存（例如用户新增边时）
    pub async fn invalidate_user(&self, user_id: &str) {
        let mut edge_cache = self.edge_cache.write().await;
        let mut node_cache = self.node_list_cache.write().await;

        edge_cache.retain(|key, _| !self.key_matches_user(key, user_id));
        node_cache.retain(|key, _| !self.key_matches_user(key, user_id));

        tracing::info!("Invalidated cache for user: {}", user_id);
    }

    fn key_matches_user(&self, key: &CacheKey, user_id: &str) -> bool {
        match key {
            CacheKey::OneHopNeighbors { user_id: uid, .. } => uid == user_id,
            CacheKey::MultiHopTraversal { user_id: uid, .. } => uid == user_id,
            CacheKey::CommunityDetection { user_id: uid, .. } => uid == user_id,
        }
    }

    /// 获取缓存统计信息
    pub async fn stats(&self) -> CacheStats {
        let edge_count = self.edge_cache.read().await.len();
        let node_count = self.node_list_cache.read().await.len();

        CacheStats {
            edge_cache_size: edge_count,
            node_cache_size: node_count,
            max_entries: self.config.max_entries,
        }
    }

    /// 清空所有缓存
    pub async fn clear(&self) {
        self.edge_cache.write().await.clear();
        self.node_list_cache.write().await.clear();
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

        // 初始应该 MISS
        assert!(cache.get_edges(&key).await.is_none());

        // 写入缓存
        let edges = vec![];
        cache.put_edges(key.clone(), edges.clone()).await;

        // 现在应该 HIT
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

        // 立即查询应该命中
        assert!(cache.get_edges(&key).await.is_some());

        // 等待过期
        tokio::time::sleep(Duration::from_millis(150)).await;

        // 现在应该失效
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

        // 失效 user1 的缓存
        cache.invalidate_user("user1").await;

        // user1 的缓存应该被清除
        assert!(cache.get_edges(&key1).await.is_none());

        // user2 的缓存应该保留
        assert!(cache.get_edges(&key2).await.is_some());
    }
}
