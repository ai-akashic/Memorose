// Graph Query Builder - 借鉴 lance-graph 的声明式理念
// 但使用纯 Rust API，无需 Cypher 解析

use crate::storage::graph::GraphStore;
use memorose_common::{GraphEdge, RelationType};
use uuid::Uuid;
use anyhow::Result;
use std::collections::HashSet;

/// 图遍历配置
#[derive(Debug, Clone)]
pub struct TraversalSpec {
    pub relation_types: Vec<RelationType>,
    pub direction: TraversalDirection,
    pub min_hops: usize,
    pub max_hops: usize,
    pub weight_threshold: Option<f32>,
}

#[derive(Debug, Clone, Copy)]
pub enum TraversalDirection {
    Outgoing,
    Incoming,
    Both,
}

/// 图查询构建器
pub struct GraphQueryBuilder {
    user_id: String,
    start_nodes: Vec<Uuid>,
    traversals: Vec<TraversalSpec>,
    limit: Option<usize>,
}

impl GraphQueryBuilder {
    pub fn new(user_id: String) -> Self {
        Self {
            user_id,
            start_nodes: Vec::new(),
            traversals: Vec::new(),
            limit: None,
        }
    }

    pub fn start_from(mut self, node_ids: Vec<Uuid>) -> Self {
        self.start_nodes = node_ids;
        self
    }

    pub fn traverse(self, relation_type: RelationType) -> TraversalBuilder {
        TraversalBuilder {
            query: self,
            spec: TraversalSpec {
                relation_types: vec![relation_type],
                direction: TraversalDirection::Outgoing,
                min_hops: 1,
                max_hops: 1,
                weight_threshold: None,
            },
        }
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// 执行优化后的查询计划
    pub async fn execute(self, graph: &GraphStore) -> Result<Vec<Uuid>> {
        let planner = QueryPlanner::new(graph);
        planner.execute_plan(self).await
    }
}

/// 遍历配置构建器
pub struct TraversalBuilder {
    query: GraphQueryBuilder,
    spec: TraversalSpec,
}

impl TraversalBuilder {
    pub fn max_hops(mut self, n: usize) -> Self {
        self.spec.max_hops = n;
        self
    }

    pub fn min_weight(mut self, threshold: f32) -> Self {
        self.spec.weight_threshold = Some(threshold);
        self
    }

    pub fn bidirectional(mut self) -> Self {
        self.spec.direction = TraversalDirection::Both;
        self
    }

    pub fn build(mut self) -> GraphQueryBuilder {
        self.query.traversals.push(self.spec);
        self.query
    }
}

/// 查询优化器和执行器
struct QueryPlanner<'a> {
    graph: &'a GraphStore,
}

impl<'a> QueryPlanner<'a> {
    fn new(graph: &'a GraphStore) -> Self {
        Self { graph }
    }

    /// 核心优化：将多跳查询转换为批量操作
    async fn execute_plan(&self, query: GraphQueryBuilder) -> Result<Vec<Uuid>> {
        if query.traversals.is_empty() {
            return Ok(query.start_nodes);
        }

        // 策略：批量查询代替循环
        let mut current_frontier = query.start_nodes.clone();
        let mut all_visited = HashSet::new();

        for traversal in &query.traversals {
            let mut next_frontier = HashSet::new();

            // 关键优化：批量获取所有边，而非逐个查询
            let edges = self.batch_get_edges(
                &query.user_id,
                &current_frontier,
                &traversal.relation_types,
                traversal.direction,
            ).await?;

            // 过滤和收集目标节点
            for edge in edges {
                if let Some(threshold) = traversal.weight_threshold {
                    if edge.weight < threshold {
                        continue;
                    }
                }

                let target = match traversal.direction {
                    TraversalDirection::Outgoing => edge.target_id,
                    TraversalDirection::Incoming => edge.source_id,
                    TraversalDirection::Both => {
                        if current_frontier.contains(&edge.source_id) {
                            edge.target_id
                        } else {
                            edge.source_id
                        }
                    }
                };

                next_frontier.insert(target);
                all_visited.insert(target);
            }

            current_frontier = next_frontier.into_iter().collect();
        }

        let mut results: Vec<Uuid> = all_visited.into_iter().collect();
        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// 批量边查询 - 消除 N+1 问题
    async fn batch_get_edges(
        &self,
        user_id: &str,
        source_nodes: &[Uuid],
        relation_types: &[RelationType],
        direction: TraversalDirection,
    ) -> Result<Vec<GraphEdge>> {
        // 优化：使用 IN 查询而非多次单独查询
        let mut all_edges = Vec::new();

        match direction {
            TraversalDirection::Outgoing => {
                let edges_map = self.graph.batch_get_outgoing_edges(user_id, source_nodes).await?;
                for (_, edges) in edges_map {
                    all_edges.extend(edges.into_iter().filter(|e| {
                        relation_types.is_empty() || relation_types.contains(&e.relation)
                    }));
                }
            }
            TraversalDirection::Incoming => {
                let edges_map = self.graph.batch_get_incoming_edges(user_id, source_nodes).await?;
                for (_, edges) in edges_map {
                    all_edges.extend(edges.into_iter().filter(|e| {
                        relation_types.is_empty() || relation_types.contains(&e.relation)
                    }));
                }
            }
            TraversalDirection::Both => {
                let out_map = self.graph.batch_get_outgoing_edges(user_id, source_nodes).await?;
                let in_map = self.graph.batch_get_incoming_edges(user_id, source_nodes).await?;
                
                let mut combined = out_map.into_values().flatten().collect::<Vec<_>>();
                combined.extend(in_map.into_values().flatten());
                
                all_edges.extend(combined.into_iter().filter(|e| {
                    relation_types.is_empty() || relation_types.contains(&e.relation)
                }));
            }
        }

        Ok(all_edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_query_builder_api() {
        // 演示 API 用法
        let user_id = "test_user".to_string();
        let start = Uuid::new_v4();

        // 构建查询：找到 2 跳内的高权重相关节点
        let _query = GraphQueryBuilder::new(user_id)
            .start_from(vec![start])
            .traverse(RelationType::RelatedTo)
                .max_hops(2)
                .min_weight(0.7)
                .build()
            .limit(10);

        // 执行: let results = query.execute(&graph).await?;
    }
}
