use anyhow::Result;
use memorose_common::{GraphEdge, MemoryUnit, RelationType};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use super::types::PendingMaterializationJob;

impl super::MemoroseEngine {
    // ── Community Detection ─────────────────────────────────────────

    /// Graph-driven L2 Generation for a specific user.
    pub async fn process_communities(&self, user_id: &str) -> Result<()> {
        self.process_communities_with_limits(user_id, 3, usize::MAX)
            .await?;
        Ok(())
    }

    /// Graph-driven L2 generation with configurable thresholds/limits.
    /// Returns number of L2 units created in this run.
    pub async fn process_communities_with_limits(
        &self,
        user_id: &str,
        min_members: usize,
        max_groups: usize,
    ) -> Result<usize> {
        let edges = self.graph.get_all_edges_for_user(user_id).await?;

        if edges.is_empty() {
            return Ok(0);
        }

        let communities = tokio::task::spawn_blocking(move || {
            crate::community::CommunityDetector::detect_communities(&edges)
        })
        .await?;

        let mut community_groups: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        for (node_id, community_id) in communities {
            community_groups
                .entry(community_id)
                .or_default()
                .push(node_id);
        }

        let min_members = min_members.max(1);
        let mut created = 0usize;

        for (_comm_id, members) in community_groups {
            if created >= max_groups {
                break;
            }

            if members.len() < min_members {
                continue;
            }

            let member_ids: Vec<String> = members.iter().map(|id| id.to_string()).collect();
            let units = self.fetch_units(user_id, member_ids.clone()).await?;

            if units.is_empty() {
                continue;
            }

            let texts: Vec<String> = units.iter().map(|u| u.content.clone()).collect();

            let insight = self.arbitrator.summarize_community(texts).await?;

            let mut l2_unit = MemoryUnit::new(
                None,
                user_id.to_string(),
                None,
                Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                insight.summary,
                None,
            );
            l2_unit.level = 2;
            l2_unit.keywords.push(insight.name.clone());
            l2_unit.keywords.extend(insight.keywords);
            l2_unit.references = members.clone();
            let l2_id = l2_unit.id;
            let uid2 = user_id.to_string();
            let post_publish_edges = members
                .iter()
                .map(|member_id| {
                    GraphEdge::new(
                        uid2.clone(),
                        l2_id,
                        *member_id,
                        RelationType::DerivedFrom,
                        1.0,
                    )
                })
                .collect::<Vec<_>>();
            self.enqueue_materialization_jobs(vec![PendingMaterializationJob::new(
                l2_unit,
                post_publish_edges,
                None,
            )])?;

            created += 1;
            tracing::info!(
                "Created L2 Insight '{}' from {} members for user {}",
                insight.name,
                units.len(),
                user_id
            );
        }

        Ok(created)
    }

    /// 增强版社区检测（支持多种算法）
    ///
    /// 使用 Louvain、加权 LPA 等高级算法，并提供模块度评估
    pub async fn detect_communities_enhanced(
        &self,
        user_id: &str,
        config: crate::community::DetectionConfig,
    ) -> Result<crate::community::CommunityResult> {
        use crate::community::{BatchCommunityDetector, EnhancedCommunityDetector};

        // Get all nodes for the user
        let edges = self.graph.get_all_edges_for_user(user_id).await?;

        if edges.is_empty() {
            return Ok(crate::community::CommunityResult {
                node_to_community: HashMap::new(),
                community_to_nodes: HashMap::new(),
                modularity: 0.0,
                num_communities: 0,
            });
        }

        // 提取所有节点
        let mut all_nodes: HashSet<Uuid> = HashSet::new();
        for edge in &edges {
            all_nodes.insert(edge.source_id);
            all_nodes.insert(edge.target_id);
        }
        let node_ids: Vec<Uuid> = all_nodes.into_iter().collect();

        tracing::info!(
            "Starting enhanced community detection for user {} with {} nodes, {} edges",
            user_id,
            node_ids.len(),
            edges.len()
        );

        // 对于大图，使用批量优化版本
        if node_ids.len() > 1000 {
            let batch_detector = BatchCommunityDetector::new(self.graph.clone(), config);
            batch_detector
                .detect_communities_for_user(user_id, &node_ids)
                .await
        } else {
            // 小图直接使用增强检测器
            let detector = EnhancedCommunityDetector::new(config);
            tokio::task::spawn_blocking(move || detector.detect(&edges)).await?
        }
    }

    /// 两阶段社区检测（先快速粗分，再精细优化）
    ///
    /// 适合超大图（> 10000 节点）
    pub async fn detect_communities_two_phase(
        &self,
        user_id: &str,
        config: crate::community::DetectionConfig,
    ) -> Result<crate::community::CommunityResult> {
        use crate::community::BatchCommunityDetector;

        let edges = self.graph.get_all_edges_for_user(user_id).await?;

        if edges.is_empty() {
            return Ok(crate::community::CommunityResult {
                node_to_community: HashMap::new(),
                community_to_nodes: HashMap::new(),
                modularity: 0.0,
                num_communities: 0,
            });
        }

        let mut all_nodes: HashSet<Uuid> = HashSet::new();
        for edge in &edges {
            all_nodes.insert(edge.source_id);
            all_nodes.insert(edge.target_id);
        }
        let node_ids: Vec<Uuid> = all_nodes.into_iter().collect();

        let batch_detector = BatchCommunityDetector::new(self.graph.clone(), config);

        batch_detector.two_phase_detection(user_id, &node_ids).await
    }

    /// 处理社区并生成 L2 摘要（使用增强算法）
    pub async fn process_communities_enhanced(
        &self,
        user_id: &str,
        config: crate::community::DetectionConfig,
    ) -> Result<()> {
        let result = self.detect_communities_enhanced(user_id, config).await?;

        tracing::info!(
            "Detected {} communities with modularity {:.4} for user {}",
            result.num_communities,
            result.modularity,
            user_id
        );

        // 为每个社区生成 L2 摘要
        for (_comm_id, members) in result.community_to_nodes {
            let member_ids: Vec<String> = members.iter().map(|id| id.to_string()).collect();
            let units = self.fetch_units(user_id, member_ids.clone()).await?;

            if units.is_empty() {
                continue;
            }

            let texts: Vec<String> = units.iter().map(|u| u.content.clone()).collect();
            let insight = self.arbitrator.summarize_community(texts).await?;

            let mut l2_unit = MemoryUnit::new(
                None,
                user_id.to_string(),
                None,
                Uuid::new_v4(),
                memorose_common::MemoryType::Factual,
                insight.summary,
                None,
            );
            l2_unit.level = 2;
            l2_unit.keywords.push(insight.name.clone());
            l2_unit.keywords.extend(insight.keywords);
            l2_unit.references = members.clone();
            let l2_id = l2_unit.id;
            let uid2 = user_id.to_string();
            let post_publish_edges = members
                .iter()
                .map(|member_id| {
                    GraphEdge::new(
                        uid2.clone(),
                        l2_id,
                        *member_id,
                        RelationType::DerivedFrom,
                        1.0,
                    )
                })
                .collect::<Vec<_>>();
            self.enqueue_materialization_jobs(vec![PendingMaterializationJob::new(
                l2_unit,
                post_publish_edges,
                None,
            )])?;

            tracing::info!(
                "Created L2 Insight '{}' from {} members for user {}",
                insight.name,
                units.len(),
                user_id
            );
        }

        Ok(())
    }
}
