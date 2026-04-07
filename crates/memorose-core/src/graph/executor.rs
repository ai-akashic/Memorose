// Batch Executor - Borrowed from lance-graph's batch execution concept
// But built directly on top of LanceDB's Arrow interfaces to avoid dependency conflicts

use crate::storage::graph::GraphStore;
use anyhow::Result;
use memorose_common::GraphEdge;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Batch edge query executor
pub struct BatchExecutor {
    pub(crate) graph_store: GraphStore,
}

impl BatchExecutor {
    pub fn new(graph_store: GraphStore) -> Self {
        Self { graph_store }
    }

    /// Get a reference to the internal GraphStore (for advanced operations)
    pub fn graph_store(&self) -> &GraphStore {
        &self.graph_store
    }

    /// Batch query outgoing edges for multiple nodes (true batch optimization)
    pub async fn batch_get_outgoing_edges(
        &self,
        user_id: &str,
        source_nodes: &[Uuid],
    ) -> Result<HashMap<Uuid, Vec<GraphEdge>>> {
        // ✅ Use GraphStore's batch API (single SQL IN query)
        self.graph_store
            .batch_get_outgoing_edges(user_id, source_nodes)
            .await
    }

    /// Batch query incoming edges
    pub async fn batch_get_incoming_edges(
        &self,
        user_id: &str,
        target_nodes: &[Uuid],
    ) -> Result<HashMap<Uuid, Vec<GraphEdge>>> {
        // ✅ Use GraphStore's batch API
        self.graph_store
            .batch_get_incoming_edges(user_id, target_nodes)
            .await
    }

    /// Batch optimized version of multi-hop traversal
    ///
    /// Example: Find all nodes within 2 hops from start nodes
    pub async fn batch_multi_hop_traverse(
        &self,
        user_id: &str,
        start_nodes: Vec<Uuid>,
        max_hops: usize,
        weight_threshold: Option<f32>,
    ) -> Result<Vec<Uuid>> {
        let mut current_frontier = start_nodes.clone();
        let mut all_visited: HashSet<Uuid> = start_nodes.into_iter().collect();

        // Hard limit to prevent OOM in extremely dense graphs
        const MAX_FRONTIER_SIZE: usize = 10_000;

        for hop in 0..max_hops {
            if current_frontier.is_empty() {
                break;
            }

            // Key optimization: batch retrieve edges for all nodes in the current layer
            let edges_map = self
                .batch_get_outgoing_edges(user_id, &current_frontier)
                .await?;

            let mut next_frontier = Vec::new();

            // Collect next layer of nodes
            for edges in edges_map.values() {
                for edge in edges {
                    // Apply weight filtering
                    if let Some(threshold) = weight_threshold {
                        if edge.weight < threshold {
                            continue;
                        }
                    }

                    // Avoid redundant visits
                    if !all_visited.contains(&edge.target_id) {
                        next_frontier.push(edge.target_id);
                    }
                }
            }

            // Enforce limit to prevent exponential explosion
            if next_frontier.len() > MAX_FRONTIER_SIZE {
                tracing::warn!(
                    "Hop {} frontier size ({}) exceeds MAX_FRONTIER_SIZE ({}). Truncating.",
                    hop + 1,
                    next_frontier.len(),
                    MAX_FRONTIER_SIZE
                );
                next_frontier.truncate(MAX_FRONTIER_SIZE);
            }

            for node in &next_frontier {
                all_visited.insert(*node);
            }

            current_frontier = next_frontier;

            // Performance monitoring
            tracing::debug!(
                "Hop {}: visited {} nodes, frontier size = {}",
                hop + 1,
                all_visited.len(),
                current_frontier.len()
            );
        }

        Ok(all_visited.into_iter().collect())
    }

    /// Prefetch optimization: pre-load commonly used neighborhood information
    pub async fn prefetch_neighborhoods(
        &self,
        user_id: &str,
        node_ids: &[Uuid],
    ) -> Result<NeighborhoodCache> {
        // Load all neighbors at once (using batch API)
        let outgoing = self.batch_get_outgoing_edges(user_id, node_ids).await?;
        let incoming = self.batch_get_incoming_edges(user_id, node_ids).await?;

        Ok(NeighborhoodCache { outgoing, incoming })
    }
}

/// Neighborhood cache (for prefetch optimization)
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
    use crate::storage::graph::GraphStore;
    use anyhow::Result;
    use lancedb::connect;
    use memorose_common::RelationType;
    use std::sync::Arc;

    async fn test_graph_store(edges: &[GraphEdge]) -> Result<GraphStore> {
        let db_path =
            std::env::temp_dir().join(format!("memorose-executor-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&db_path)?;
        let db = Arc::new(connect(db_path.to_str().unwrap()).execute().await?);
        let store = GraphStore::new(db).await?;
        for edge in edges {
            store.add_edge(edge).await?;
        }
        store.flush().await?;
        Ok(store)
    }

    fn edge(source_id: Uuid, target_id: Uuid, relation: RelationType, weight: f32) -> GraphEdge {
        GraphEdge::new("user1".to_string(), source_id, target_id, relation, weight)
    }

    #[tokio::test]
    async fn test_batch_vs_sequential_performance() {
        // Demonstrate the performance advantage of batch queries

        // Assume there are 100 nodes to query
        let _node_ids: Vec<Uuid> = (0..100).map(|_| Uuid::new_v4()).collect();

        // ❌ Traditional approach (pseudo-code):
        // let mut all_edges = Vec::new();
        // for node_id in &node_ids {
        //     let edges = graph.get_outgoing_edges(user_id, *node_id).await?;
        //     all_edges.extend(edges);
        // }
        // Expected latency: 100 queries × 2ms = 200ms

        // ✅ Batch approach:
        // let executor = BatchExecutor::new(graph_store);
        // let edges_map = executor.batch_get_outgoing_edges(user_id, &node_ids).await?;
        // Expected latency: 1 query × 5ms = 5ms
        //
        // Performance improvement: 40x
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

    #[tokio::test]
    async fn test_batch_multi_hop_traverse_respects_threshold_and_hops() -> Result<()> {
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();
        let node_d = Uuid::new_v4();

        let executor = BatchExecutor::new(
            test_graph_store(&[
                edge(node_a, node_b, RelationType::RelatedTo, 0.9),
                edge(node_b, node_c, RelationType::RelatedTo, 0.8),
                edge(node_b, node_d, RelationType::RelatedTo, 0.2),
            ])
            .await?,
        );

        let visited = executor
            .batch_multi_hop_traverse("user1", vec![node_a], 2, Some(0.5))
            .await?;

        assert!(visited.contains(&node_a));
        assert!(visited.contains(&node_b));
        assert!(visited.contains(&node_c));
        assert!(!visited.contains(&node_d));
        Ok(())
    }

    #[tokio::test]
    async fn test_prefetch_neighborhoods_and_graph_store_accessor() -> Result<()> {
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();
        let graph_store = test_graph_store(&[
            edge(node_a, node_b, RelationType::RelatedTo, 0.9),
            edge(node_c, node_a, RelationType::Supports, 0.7),
        ])
        .await?;
        let executor = BatchExecutor::new(graph_store.clone());

        let cache = executor
            .prefetch_neighborhoods("user1", &[node_a, node_b])
            .await?;

        assert_eq!(executor.graph_store().scan_all_edges().await?.len(), 2);
        assert_eq!(cache.get_degree(&node_a), 2);
        let neighbors = cache.get_neighbors(&node_a);
        assert!(neighbors.contains(&node_b));
        assert!(neighbors.contains(&node_c));
        Ok(())
    }
}
#[cfg(test)]
mod additional_tests {
    use super::*;
    use crate::storage::graph::GraphStore;
    use crate::RelationType;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_max_frontier_truncation() {
        use std::sync::Arc;
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_db");
        let db = Arc::new(
            lancedb::connect(db_path.to_str().unwrap())
                .execute()
                .await
                .unwrap(),
        );
        let store = GraphStore::new(db).await.unwrap();
        let executor = BatchExecutor::new(store);

        let root = Uuid::new_v4();
        // Create 10,005 children to exceed MAX_FRONTIER_SIZE (10,000)
        for _ in 0..10005 {
            executor
                .graph_store()
                .add_edge(&memorose_common::GraphEdge::new(
                    "user1".to_string(),
                    root,
                    Uuid::new_v4(),
                    RelationType::RelatedTo,
                    1.0,
                ))
                .await
                .unwrap();
        }
        executor.graph_store().flush().await.unwrap();

        let result = executor
            .batch_multi_hop_traverse("user1", vec![root], 1, None)
            .await
            .unwrap();

        // Root + 10,000 max truncated children = 10001 total visited
        assert_eq!(result.len(), 10001);
    }
}
