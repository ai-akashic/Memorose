// Batch Executor - 借鉴 lance-graph 的批量执行理念
// 但直接基于 LanceDB 的 Arrow 接口，避免依赖冲突

use crate::storage::graph::GraphStore;
use memorose_common::GraphEdge;
use uuid::Uuid;
use anyhow::Result;
use std::collections::{HashMap, HashSet};

/// 批量边查询执行器
pub struct BatchExecutor {
    pub(crate) graph_store: GraphStore,
}

impl BatchExecutor {
    pub fn new(graph_store: GraphStore) -> Self {
        Self { graph_store }
    }

    /// 获取内部 GraphStore 的引用（用于高级操作）
    pub fn graph_store(&self) -> &GraphStore {
        &self.graph_store
    }

    /// 批量查询多个节点的出边（真正的批量优化）
    pub async fn batch_get_outgoing_edges(
        &self,
        user_id: &str,
        source_nodes: &[Uuid],
    ) -> Result<HashMap<Uuid, Vec<GraphEdge>>> {
        // ✅ 使用 GraphStore 的批量 API（单次 SQL IN 查询）
        self.graph_store.batch_get_outgoing_edges(user_id, source_nodes).await
    }

    /// 批量查询入边
    pub async fn batch_get_incoming_edges(
        &self,
        user_id: &str,
        target_nodes: &[Uuid],
    ) -> Result<HashMap<Uuid, Vec<GraphEdge>>> {
        // ✅ 使用 GraphStore 的批量 API
        self.graph_store.batch_get_incoming_edges(user_id, target_nodes).await
    }

    /// 多跳遍历的批量优化版本
    ///
    /// 示例：找到距离起始节点 2 跳以内的所有节点
    pub async fn batch_multi_hop_traverse(
        &self,
        user_id: &str,
        start_nodes: Vec<Uuid>,
        max_hops: usize,
        weight_threshold: Option<f32>,
    ) -> Result<Vec<Uuid>> {
        let mut current_frontier = start_nodes.clone();
        let mut all_visited: HashSet<Uuid> = start_nodes.into_iter().collect();

        for hop in 0..max_hops {
            if current_frontier.is_empty() {
                break;
            }

            // 关键优化：批量获取当前层所有节点的边
            let edges_map = self.batch_get_outgoing_edges(user_id, &current_frontier).await?;

            let mut next_frontier = Vec::new();

            // 收集下一层节点
            for edges in edges_map.values() {
                for edge in edges {
                    // 应用权重过滤
                    if let Some(threshold) = weight_threshold {
                        if edge.weight < threshold {
                            continue;
                        }
                    }

                    // 避免重复访问
                    if !all_visited.contains(&edge.target_id) {
                        all_visited.insert(edge.target_id);
                        next_frontier.push(edge.target_id);
                    }
                }
            }

            current_frontier = next_frontier;

            // 性能监控
            tracing::debug!(
                "Hop {}: visited {} nodes, frontier size = {}",
                hop + 1,
                all_visited.len(),
                current_frontier.len()
            );
        }

        Ok(all_visited.into_iter().collect())
    }

    /// 预取优化：预先加载常用的邻居信息
    pub async fn prefetch_neighborhoods(
        &self,
        user_id: &str,
        node_ids: &[Uuid],
    ) -> Result<NeighborhoodCache> {
        // 一次性加载所有邻居（使用批量 API）
        let outgoing = self.batch_get_outgoing_edges(user_id, node_ids).await?;
        let incoming = self.batch_get_incoming_edges(user_id, node_ids).await?;

        Ok(NeighborhoodCache {
            outgoing,
            incoming,
        })
    }
}

/// 邻居缓存（用于预取优化）
pub struct NeighborhoodCache {
    outgoing: HashMap<Uuid, Vec<GraphEdge>>,
    incoming: HashMap<Uuid, Vec<GraphEdge>>,
}

impl NeighborhoodCache {
    pub fn get_neighbors(&self, node_id: &Uuid) -> Vec<Uuid> {
        let mut neighbors = Vec::new();

        if let Some(out_edges) = self.outgoing.get(node_id) {
            neighbors.extend(out_edges.iter().map(|e| e.target_id));
        }

        if let Some(in_edges) = self.incoming.get(node_id) {
            neighbors.extend(in_edges.iter().map(|e| e.source_id));
        }

        neighbors
    }

    pub fn get_degree(&self, node_id: &Uuid) -> usize {
        let out_degree = self.outgoing.get(node_id).map(|v| v.len()).unwrap_or(0);
        let in_degree = self.incoming.get(node_id).map(|v| v.len()).unwrap_or(0);
        out_degree + in_degree
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_batch_vs_sequential_performance() {
        // 演示批量查询的性能优势

        // 假设有 100 个节点需要查询
        let node_ids: Vec<Uuid> = (0..100).map(|_| Uuid::new_v4()).collect();

        // ❌ 传统方式（伪代码）:
        // let mut all_edges = Vec::new();
        // for node_id in &node_ids {
        //     let edges = graph.get_outgoing_edges(user_id, *node_id).await?;
        //     all_edges.extend(edges);
        // }
        // 预期延迟: 100 次查询 × 2ms = 200ms

        // ✅ 批量方式:
        // let executor = BatchExecutor::new(graph_store);
        // let edges_map = executor.batch_get_outgoing_edges(user_id, &node_ids).await?;
        // 预期延迟: 1 次查询 × 5ms = 5ms
        //
        // 性能提升: 40x
    }

    #[test]
    fn test_neighborhood_cache() {
        let cache = NeighborhoodCache {
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
        };

        let node = Uuid::new_v4();
        assert_eq!(cache.get_degree(&node), 0);
    }
}
