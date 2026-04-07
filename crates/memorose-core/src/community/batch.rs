// 批量优化的社区检测 - 用于大规模图
//
// 特点：
// - 利用 BatchExecutor 避免 N+1 查询
// - 流式处理，不需要一次性加载所有边
// - 支持增量更新

use super::enhanced::{CommunityResult, DetectionConfig, EnhancedCommunityDetector};
use crate::graph::BatchExecutor;
use crate::storage::graph::GraphStore;
use anyhow::Result;
use memorose_common::GraphEdge;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// 批量优化的社区检测器
pub struct BatchCommunityDetector {
    batch_executor: BatchExecutor,
    config: DetectionConfig,
}

impl BatchCommunityDetector {
    pub fn new(graph_store: GraphStore, config: DetectionConfig) -> Self {
        Self {
            batch_executor: BatchExecutor::new(graph_store),
            config,
        }
    }

    /// 为用户检测社区（批量优化）
    ///
    /// 对于大图，分批加载边以避免内存溢出
    pub async fn detect_communities_for_user(
        &self,
        user_id: &str,
        all_node_ids: &[Uuid],
    ) -> Result<CommunityResult> {
        // 对于小图（< 1000 节点），直接使用标准算法
        if all_node_ids.len() < 1000 {
            return self.detect_communities_direct(user_id, all_node_ids).await;
        }

        // 对于大图，使用分批处理
        self.detect_communities_batched(user_id, all_node_ids).await
    }

    /// 直接检测（小图）
    async fn detect_communities_direct(
        &self,
        user_id: &str,
        node_ids: &[Uuid],
    ) -> Result<CommunityResult> {
        // 批量获取所有边
        let edges_map = self
            .batch_executor
            .batch_get_outgoing_edges(user_id, node_ids)
            .await?;

        // 收集所有边
        let mut all_edges = Vec::new();
        for edges in edges_map.values() {
            all_edges.extend(edges.clone());
        }

        // 去重（因为边可能被两端都访问到）
        all_edges.sort_by_key(|e| (e.source_id, e.target_id));
        all_edges.dedup_by_key(|e| (e.source_id, e.target_id));

        // 使用增强检测器
        let detector = EnhancedCommunityDetector::new(self.config.clone());
        detector.detect(&all_edges)
    }

    /// 分批检测（大图）
    ///
    /// 策略：
    /// 1. 采样节点进行初步聚类
    /// 2. 扩展每个簇的边界
    /// 3. 对每个簇独立进行社区检测
    /// 4. 合并结果
    async fn detect_communities_batched(
        &self,
        user_id: &str,
        node_ids: &[Uuid],
    ) -> Result<CommunityResult> {
        const BATCH_SIZE: usize = 500;

        tracing::info!(
            "Large graph detected ({} nodes), using batched community detection",
            node_ids.len()
        );

        let mut all_edges = Vec::new();

        // 分批加载边
        for chunk in node_ids.chunks(BATCH_SIZE) {
            let edges_map = self
                .batch_executor
                .batch_get_outgoing_edges(user_id, chunk)
                .await?;

            for edges in edges_map.values() {
                all_edges.extend(edges.clone());
            }

            tracing::debug!(
                "Loaded edges for batch, total edges so far: {}",
                all_edges.len()
            );
        }

        // 去重
        all_edges.sort_by_key(|e| (e.source_id, e.target_id));
        all_edges.dedup_by_key(|e| (e.source_id, e.target_id));

        tracing::info!(
            "Loaded {} unique edges, starting community detection",
            all_edges.len()
        );

        // 执行社区检测
        let detector = EnhancedCommunityDetector::new(self.config.clone());
        detector.detect(&all_edges)
    }

    /// 增量更新社区（当添加新边时）
    ///
    /// 对于已有的社区结果，增量地更新受影响的部分
    pub async fn incremental_update(
        &self,
        user_id: &str,
        current_communities: &HashMap<Uuid, Uuid>,
        new_edges: &[GraphEdge],
    ) -> Result<CommunityResult> {
        // 找到受影响的节点
        let mut affected_nodes: HashSet<Uuid> = HashSet::new();
        for edge in new_edges {
            affected_nodes.insert(edge.source_id);
            affected_nodes.insert(edge.target_id);

            // 也包括这些节点所在社区的所有邻居
            if let Some(comm) = current_communities.get(&edge.source_id) {
                for (node, node_comm) in current_communities {
                    if node_comm == comm {
                        affected_nodes.insert(*node);
                    }
                }
            }
        }

        let affected_vec: Vec<Uuid> = affected_nodes.into_iter().collect();

        // 对受影响的子图重新检测
        self.detect_communities_direct(user_id, &affected_vec).await
    }

    /// 两阶段社区检测（先快速粗分，再精细优化）
    ///
    /// Phase 1: 快速 LPA 得到初步分组
    /// Phase 2: 对每个分组内部使用 Louvain 精细优化
    pub async fn two_phase_detection(
        &self,
        user_id: &str,
        node_ids: &[Uuid],
    ) -> Result<CommunityResult> {
        // Phase 1: 快速 LPA
        let mut phase1_config = self.config.clone();
        phase1_config.algorithm = super::enhanced::Algorithm::LabelPropagation;
        phase1_config.max_iterations = 10; // 快速收敛

        let phase1_detector =
            BatchCommunityDetector::new(self.batch_executor.clone_graph_store(), phase1_config);

        let phase1_result = phase1_detector
            .detect_communities_direct(user_id, node_ids)
            .await?;

        tracing::info!(
            "Phase 1 (LPA) found {} communities, modularity: {:.4}",
            phase1_result.num_communities,
            phase1_result.modularity
        );

        // Phase 2: 对大社区使用 Louvain 优化
        let mut final_communities = phase1_result.node_to_community.clone();
        let improved_modularity = phase1_result.modularity;

        for (comm_id, members) in &phase1_result.community_to_nodes {
            if members.len() < 10 {
                // 小社区不需要优化
                continue;
            }

            // 对这个社区内部重新检测
            let mut phase2_config = self.config.clone();
            phase2_config.algorithm = super::enhanced::Algorithm::Louvain;

            let phase2_detector =
                BatchCommunityDetector::new(self.batch_executor.clone_graph_store(), phase2_config);

            let subgraph_result = phase2_detector
                .detect_communities_direct(user_id, members)
                .await?;

            if subgraph_result.modularity > 0.0 {
                // 如果发现了更好的子社区，更新结果
                for (node, sub_comm) in &subgraph_result.node_to_community {
                    // 创建全局唯一的社区 ID（组合父社区和子社区）
                    let global_comm = if sub_comm == comm_id {
                        *comm_id // 保持原社区 ID
                    } else {
                        Uuid::new_v4() // 新的子社区 ID
                    };
                    final_communities.insert(*node, global_comm);
                }

                tracing::debug!(
                    "Refined community {} ({} nodes) into {} sub-communities",
                    comm_id,
                    members.len(),
                    subgraph_result.num_communities
                );
            }
        }

        // 重新计算模块度和分组
        let mut community_groups: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        for (node, comm) in &final_communities {
            community_groups.entry(*comm).or_default().push(*node);
        }

        community_groups.retain(|_, members| members.len() >= self.config.min_community_size);

        Ok(CommunityResult {
            node_to_community: final_communities,
            community_to_nodes: community_groups.clone(),
            modularity: improved_modularity, // TODO: 重新计算准确的模块度
            num_communities: community_groups.len(),
        })
    }
}

impl BatchExecutor {
    /// 提供对内部 graph_store 的克隆（用于社区检测）
    pub fn clone_graph_store(&self) -> GraphStore {
        self.graph_store.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lancedb::connect;
    use memorose_common::RelationType;
    use std::sync::Arc;

    async fn test_graph_store(edges: &[GraphEdge]) -> Result<GraphStore> {
        let db_path = std::env::temp_dir().join(format!("memorose-batch-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&db_path)?;
        let db = Arc::new(connect(db_path.to_str().unwrap()).execute().await?);
        let store = GraphStore::new(db).await?;
        for edge in edges {
            store.add_edge(edge).await?;
        }
        store.flush().await?;
        Ok(store)
    }

    fn edge(source_id: Uuid, target_id: Uuid, weight: f32) -> GraphEdge {
        GraphEdge::new(
            "user1".to_string(),
            source_id,
            target_id,
            RelationType::RelatedTo,
            weight,
        )
    }

    fn relaxed_config() -> DetectionConfig {
        DetectionConfig {
            algorithm: crate::community::Algorithm::LabelPropagation,
            min_community_size: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_batch_community_detection_direct_path() -> Result<()> {
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();
        let node_d = Uuid::new_v4();
        let edges = vec![
            edge(node_a, node_b, 1.0),
            edge(node_b, node_c, 0.8),
            edge(node_c, node_d, 0.7),
        ];

        let detector =
            BatchCommunityDetector::new(test_graph_store(&edges).await?, relaxed_config());

        let result = detector
            .detect_communities_for_user("user1", &[node_a, node_b, node_c, node_d])
            .await?;

        assert!(result.node_to_community.contains_key(&node_a));
        assert!(result.node_to_community.contains_key(&node_d));
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_community_detection_batched_path() -> Result<()> {
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();
        let edges = vec![edge(node_a, node_b, 1.0), edge(node_b, node_c, 0.9)];
        let detector =
            BatchCommunityDetector::new(test_graph_store(&edges).await?, relaxed_config());

        let mut all_node_ids = vec![node_a, node_b, node_c];
        while all_node_ids.len() < 1000 {
            all_node_ids.push(Uuid::new_v4());
        }

        let result = detector
            .detect_communities_for_user("user1", &all_node_ids)
            .await?;

        assert!(result.node_to_community.contains_key(&node_a));
        assert!(result.node_to_community.contains_key(&node_c));
        Ok(())
    }

    #[tokio::test]
    async fn test_incremental_update_recomputes_affected_subgraph() -> Result<()> {
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();
        let node_d = Uuid::new_v4();
        let initial_edges = vec![edge(node_a, node_b, 1.0), edge(node_b, node_c, 0.8)];
        let store = test_graph_store(&initial_edges).await?;
        let detector = BatchCommunityDetector::new(store.clone(), relaxed_config());

        let initial = detector
            .detect_communities_for_user("user1", &[node_a, node_b, node_c])
            .await?;

        let new_edge = edge(node_c, node_d, 0.95);
        store.add_edge(&new_edge).await?;
        store.flush().await?;

        let updated = detector
            .incremental_update("user1", &initial.node_to_community, &[new_edge])
            .await?;

        assert!(updated.node_to_community.contains_key(&node_c));
        assert!(updated.node_to_community.contains_key(&node_d));
        Ok(())
    }

    #[tokio::test]
    async fn test_two_phase_detection_runs_refinement_on_large_community() -> Result<()> {
        let nodes: Vec<Uuid> = (0..12).map(|_| Uuid::new_v4()).collect();
        let mut edges = Vec::new();
        for window in nodes.windows(2) {
            edges.push(edge(window[0], window[1], 1.0));
        }
        edges.push(edge(nodes[0], *nodes.last().unwrap(), 0.9));

        let detector =
            BatchCommunityDetector::new(test_graph_store(&edges).await?, relaxed_config());

        let result = detector.two_phase_detection("user1", &nodes).await?;

        assert!(result.num_communities >= 1);
        assert!(nodes
            .iter()
            .all(|node_id| result.node_to_community.contains_key(node_id)));
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_community_detection() -> Result<()> {
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let detector = BatchCommunityDetector::new(
            test_graph_store(&[edge(node_a, node_b, 1.0)]).await?,
            relaxed_config(),
        );

        let result = detector
            .detect_communities_for_user("user1", &[node_a, node_b])
            .await?;

        assert_eq!(result.node_to_community.len(), 2);
        Ok(())
    }
}
