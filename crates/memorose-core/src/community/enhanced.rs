// 增强版社区检测 - 支持多种算法和批量优化
//
// 算法：
// 1. Label Propagation Algorithm (LPA) - 快速但质量一般
// 2. Weighted LPA - 考虑边权重的 LPA
// 3. Louvain - 模块度优化，质量更高
//
// 优化：
// - 利用 BatchExecutor 处理大图
// - 增量式更新
// - 模块度评估

use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use memorose_common::GraphEdge;
use rand::seq::SliceRandom;
use rand::thread_rng;
use anyhow::Result;

/// 社区检测算法类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    /// 标签传播算法（快速，适合大图）
    LabelPropagation,
    /// 加权标签传播（考虑边权重）
    WeightedLPA,
    /// Louvain 算法（高质量，适合中等规模图）
    Louvain,
}

/// 社区检测配置
#[derive(Debug, Clone)]
pub struct DetectionConfig {
    pub algorithm: Algorithm,
    pub max_iterations: usize,
    pub min_community_size: usize,
    pub resolution: f32,  // Louvain 的分辨率参数
}

impl Default for DetectionConfig {
    fn default() -> Self {
        Self {
            algorithm: Algorithm::Louvain,
            max_iterations: 100,
            min_community_size: 3,
            resolution: 1.0,
        }
    }
}

/// 社区检测结果
#[derive(Debug)]
pub struct CommunityResult {
    /// 节点到社区的映射
    pub node_to_community: HashMap<Uuid, Uuid>,
    /// 社区到节点的映射
    pub community_to_nodes: HashMap<Uuid, Vec<Uuid>>,
    /// 模块度得分（质量指标）
    pub modularity: f64,
    /// 检测到的社区数量
    pub num_communities: usize,
}

/// 增强版社区检测器
pub struct EnhancedCommunityDetector {
    config: DetectionConfig,
}

impl EnhancedCommunityDetector {
    pub fn new(config: DetectionConfig) -> Self {
        Self { config }
    }

    /// 执行社区检测
    pub fn detect(&self, edges: &[GraphEdge]) -> Result<CommunityResult> {
        let communities = match self.config.algorithm {
            Algorithm::LabelPropagation => self.label_propagation(edges, false)?,
            Algorithm::WeightedLPA => self.label_propagation(edges, true)?,
            Algorithm::Louvain => self.louvain(edges)?,
        };

        let modularity = self.calculate_modularity(edges, &communities);
        let community_groups = self.group_by_community(&communities);
        let num_communities = community_groups.len();

        Ok(CommunityResult {
            node_to_community: communities,
            community_to_nodes: community_groups,
            modularity,
            num_communities,
        })
    }

    /// 标签传播算法（支持加权）
    fn label_propagation(&self, edges: &[GraphEdge], weighted: bool) -> Result<HashMap<Uuid, Uuid>> {
        let mut communities: HashMap<Uuid, Uuid> = HashMap::new();
        let mut adjacency: HashMap<Uuid, Vec<(Uuid, f32)>> = HashMap::new();
        let mut all_nodes: HashSet<Uuid> = HashSet::new();

        // 构建邻接表
        for edge in edges {
            let weight = if weighted { edge.weight } else { 1.0 };
            adjacency.entry(edge.source_id).or_default().push((edge.target_id, weight));
            adjacency.entry(edge.target_id).or_default().push((edge.source_id, weight));
            all_nodes.insert(edge.source_id);
            all_nodes.insert(edge.target_id);
        }

        // 初始化：每个节点是自己的社区
        for node in &all_nodes {
            communities.insert(*node, *node);
        }

        let mut nodes_vec: Vec<Uuid> = all_nodes.into_iter().collect();
        let mut rng = thread_rng();

        for _ in 0..self.config.max_iterations {
            nodes_vec.shuffle(&mut rng);
            let mut changed = false;

            for &node in &nodes_vec {
                if let Some(neighbors) = adjacency.get(&node) {
                    let mut label_weights: HashMap<Uuid, f32> = HashMap::new();

                    for (neighbor, weight) in neighbors {
                        if let Some(label) = communities.get(neighbor) {
                            *label_weights.entry(*label).or_default() += weight;
                        }
                    }

                    // 找到权重最大的标签
                    if let Some((best_label, _)) = label_weights.iter()
                        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap()) {
                        if let Some(current_label) = communities.get(&node) {
                            if current_label != best_label {
                                communities.insert(node, *best_label);
                                changed = true;
                            }
                        }
                    }
                }
            }

            if !changed {
                break;
            }
        }

        Ok(communities)
    }

    /// Louvain 算法实现
    fn louvain(&self, edges: &[GraphEdge]) -> Result<HashMap<Uuid, Uuid>> {
        let mut graph = LouvainGraph::from_edges(edges);
        let mut best_communities = HashMap::new();
        let mut best_modularity = -1.0;

        for _ in 0..self.config.max_iterations {
            // Phase 1: 模块度优化
            let improved = graph.optimize_modularity(self.config.resolution);

            let current_modularity = graph.compute_modularity(self.config.resolution);
            if current_modularity > best_modularity {
                best_modularity = current_modularity;
                best_communities = graph.get_communities();
            }

            if !improved {
                break;
            }

            // Phase 2: 图聚合
            graph = graph.aggregate();
        }

        Ok(best_communities)
    }

    /// 计算模块度（质量指标）
    fn calculate_modularity(&self, edges: &[GraphEdge], communities: &HashMap<Uuid, Uuid>) -> f64 {
        if edges.is_empty() {
            return 0.0;
        }

        let total_weight: f64 = edges.iter().map(|e| e.weight as f64).sum();
        let m2 = total_weight * 2.0;

        // 计算每个节点的度（权重和）
        let mut degrees: HashMap<Uuid, f64> = HashMap::new();
        for edge in edges {
            *degrees.entry(edge.source_id).or_default() += edge.weight as f64;
            *degrees.entry(edge.target_id).or_default() += edge.weight as f64;
        }

        let mut modularity = 0.0;
        for edge in edges {
            if let (Some(comm_u), Some(comm_v)) = (
                communities.get(&edge.source_id),
                communities.get(&edge.target_id),
            ) {
                if comm_u == comm_v {
                    let degree_u = degrees.get(&edge.source_id).unwrap_or(&0.0);
                    let degree_v = degrees.get(&edge.target_id).unwrap_or(&0.0);
                    let expected = (degree_u * degree_v) / m2;
                    modularity += edge.weight as f64 - expected;
                }
            }
        }

        modularity / total_weight
    }

    /// 按社区分组节点
    fn group_by_community(&self, communities: &HashMap<Uuid, Uuid>) -> HashMap<Uuid, Vec<Uuid>> {
        let mut groups: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        for (node, community) in communities {
            groups.entry(*community).or_default().push(*node);
        }

        // 过滤小社区
        groups.retain(|_, members| members.len() >= self.config.min_community_size);
        groups
    }
}

/// Louvain 算法的图表示
struct LouvainGraph {
    nodes: Vec<Uuid>,
    #[allow(dead_code)]
    node_to_idx: HashMap<Uuid, usize>,
    communities: Vec<usize>,  // 每个节点的社区 ID
    edges: Vec<(usize, usize, f32)>,  // (source_idx, target_idx, weight)
    degrees: Vec<f64>,  // 每个节点的度
    total_weight: f64,
}

impl LouvainGraph {
    fn from_edges(edges: &[GraphEdge]) -> Self {
        let mut nodes_set: HashSet<Uuid> = HashSet::new();
        for edge in edges {
            nodes_set.insert(edge.source_id);
            nodes_set.insert(edge.target_id);
        }

        let nodes: Vec<Uuid> = nodes_set.into_iter().collect();
        let node_to_idx: HashMap<Uuid, usize> = nodes.iter()
            .enumerate()
            .map(|(idx, &node)| (node, idx))
            .collect();

        let mut graph_edges = Vec::new();
        let mut degrees = vec![0.0; nodes.len()];
        let mut total_weight = 0.0;

        for edge in edges {
            let src_idx = node_to_idx[&edge.source_id];
            let tgt_idx = node_to_idx[&edge.target_id];
            let weight = edge.weight as f64;

            graph_edges.push((src_idx, tgt_idx, edge.weight));
            degrees[src_idx] += weight;
            degrees[tgt_idx] += weight;
            total_weight += weight;
        }

        let communities = (0..nodes.len()).collect();

        Self {
            nodes,
            node_to_idx,
            communities,
            edges: graph_edges,
            degrees,
            total_weight,
        }
    }

    fn optimize_modularity(&mut self, resolution: f32) -> bool {
        let mut improved = false;
        let m2 = self.total_weight * 2.0;

        for node_idx in 0..self.nodes.len() {
            let current_comm = self.communities[node_idx];

            // 计算移动到邻居社区的增益
            let mut neighbor_comms: HashMap<usize, f64> = HashMap::new();
            for (src, tgt, weight) in &self.edges {
                if *src == node_idx {
                    let neighbor_comm = self.communities[*tgt];
                    *neighbor_comms.entry(neighbor_comm).or_default() += *weight as f64;
                } else if *tgt == node_idx {
                    let neighbor_comm = self.communities[*src];
                    *neighbor_comms.entry(neighbor_comm).or_default() += *weight as f64;
                }
            }

            // 找到最佳社区
            let mut best_comm = current_comm;
            let mut best_gain = 0.0;

            for (comm, edge_weight) in neighbor_comms {
                if comm == current_comm {
                    continue;
                }

                let gain = self.modularity_gain(node_idx, comm, edge_weight, m2, resolution);
                if gain > best_gain {
                    best_gain = gain;
                    best_comm = comm;
                }
            }

            if best_comm != current_comm {
                self.communities[node_idx] = best_comm;
                improved = true;
            }
        }

        improved
    }

    fn modularity_gain(&self, node_idx: usize, target_comm: usize, edge_weight: f64, m2: f64, resolution: f32) -> f64 {
        let node_degree = self.degrees[node_idx];

        // 计算目标社区的总度
        let comm_degree: f64 = self.communities.iter()
            .enumerate()
            .filter(|(_, &c)| c == target_comm)
            .map(|(idx, _)| self.degrees[idx])
            .sum();

        let gain = edge_weight - (resolution as f64 * node_degree * comm_degree / m2);
        gain
    }

    fn compute_modularity(&self, resolution: f32) -> f64 {
        let m2 = self.total_weight * 2.0;
        let mut modularity = 0.0;

        for (src, tgt, weight) in &self.edges {
            if self.communities[*src] == self.communities[*tgt] {
                let expected = (self.degrees[*src] * self.degrees[*tgt]) / m2;
                modularity += *weight as f64 - resolution as f64 * expected;
            }
        }

        modularity / self.total_weight
    }

    fn aggregate(&self) -> Self {
        // 将每个社区聚合成单个超节点
        let mut comm_set: HashSet<usize> = self.communities.iter().cloned().collect();
        let comm_list: Vec<usize> = comm_set.drain().collect();

        // 创建新的节点列表（每个社区一个节点）
        let new_nodes: Vec<Uuid> = comm_list.iter().map(|_| Uuid::new_v4()).collect();
        let new_node_to_idx: HashMap<Uuid, usize> = new_nodes.iter()
            .enumerate()
            .map(|(idx, &node)| (node, idx))
            .collect();

        // 映射：旧社区 ID -> 新节点索引
        let comm_to_new_idx: HashMap<usize, usize> = comm_list.iter()
            .enumerate()
            .map(|(idx, &comm)| (comm, idx))
            .collect();

        // 聚合边
        let mut edge_weights: HashMap<(usize, usize), f32> = HashMap::new();
        for (src, tgt, weight) in &self.edges {
            let src_comm = self.communities[*src];
            let tgt_comm = self.communities[*tgt];
            let new_src = comm_to_new_idx[&src_comm];
            let new_tgt = comm_to_new_idx[&tgt_comm];

            if new_src != new_tgt {  // 忽略自环
                *edge_weights.entry((new_src.min(new_tgt), new_src.max(new_tgt))).or_default() += weight;
            }
        }

        let new_edges: Vec<(usize, usize, f32)> = edge_weights.into_iter()
            .map(|((src, tgt), weight)| (src, tgt, weight))
            .collect();

        let mut new_degrees = vec![0.0; new_nodes.len()];
        let mut new_total_weight = 0.0;
        for (src, tgt, weight) in &new_edges {
            new_degrees[*src] += *weight as f64;
            new_degrees[*tgt] += *weight as f64;
            new_total_weight += *weight as f64;
        }

        let num_new_nodes = new_nodes.len();

        Self {
            nodes: new_nodes,
            node_to_idx: new_node_to_idx,
            communities: (0..num_new_nodes).collect(),
            edges: new_edges,
            degrees: new_degrees,
            total_weight: new_total_weight,
        }
    }

    fn get_communities(&self) -> HashMap<Uuid, Uuid> {
        let mut result = HashMap::new();

        // 找到每个社区的代表节点
        let mut comm_representatives: HashMap<usize, Uuid> = HashMap::new();
        for (idx, &comm) in self.communities.iter().enumerate() {
            comm_representatives.entry(comm).or_insert(self.nodes[idx]);
        }

        for (idx, &comm) in self.communities.iter().enumerate() {
            let node = self.nodes[idx];
            let representative = comm_representatives[&comm];
            result.insert(node, representative);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memorose_common::RelationType;

    #[test]
    fn test_weighted_lpa() {
        let edges = vec![
            GraphEdge::new("user1".to_string(), Uuid::new_v4(), Uuid::new_v4(), RelationType::RelatedTo, 0.9),
            GraphEdge::new("user1".to_string(), Uuid::new_v4(), Uuid::new_v4(), RelationType::RelatedTo, 0.1),
        ];

        let config = DetectionConfig {
            algorithm: Algorithm::WeightedLPA,
            ..Default::default()
        };

        let detector = EnhancedCommunityDetector::new(config);
        let result = detector.detect(&edges).unwrap();

        assert!(result.modularity >= -1.0 && result.modularity <= 1.0);
    }

    #[test]
    fn test_louvain() {
        let node_a = Uuid::new_v4();
        let node_b = Uuid::new_v4();
        let node_c = Uuid::new_v4();
        let node_d = Uuid::new_v4();

        let edges = vec![
            GraphEdge::new("user1".to_string(), node_a, node_b, RelationType::RelatedTo, 0.9),
            GraphEdge::new("user1".to_string(), node_b, node_c, RelationType::RelatedTo, 0.8),
            GraphEdge::new("user1".to_string(), node_c, node_d, RelationType::RelatedTo, 0.1),
        ];

        let config = DetectionConfig {
            algorithm: Algorithm::Louvain,
            min_community_size: 2,
            ..Default::default()
        };

        let detector = EnhancedCommunityDetector::new(config);
        let result = detector.detect(&edges).unwrap();

        assert!(result.num_communities >= 1);
        println!("Louvain found {} communities with modularity {}",
            result.num_communities, result.modularity);
    }
}
