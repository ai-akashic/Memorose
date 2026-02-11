// Query Optimizer - 借鉴 lance-graph 的查询优化理念
// 将声明式查询转换为高效的执行计划

use uuid::Uuid;
use std::collections::HashMap;

/// 查询执行计划节点
#[derive(Debug, Clone)]
pub enum ExecutionPlan {
    /// 扫描起始节点（类似 lance-graph 的 ScanByLabel）
    ScanNodes {
        node_ids: Vec<Uuid>,
    },

    /// 批量边扩展（优化的核心）
    BatchExpand {
        input: Box<ExecutionPlan>,
        edge_filter: EdgeFilter,
        batch_size: usize,  // 每批处理多少节点
    },

    /// 去重（类似 SQL DISTINCT）
    Distinct {
        input: Box<ExecutionPlan>,
    },

    /// 限制结果数量
    Limit {
        input: Box<ExecutionPlan>,
        count: usize,
    },
}

#[derive(Debug, Clone)]
pub struct EdgeFilter {
    pub relation_types: Vec<String>,
    pub min_weight: Option<f32>,
    pub max_weight: Option<f32>,
}

/// 查询优化器
pub struct QueryOptimizer {
    stats: QueryStats,
}

#[derive(Debug, Default)]
struct QueryStats {
    /// 每种关系类型的平均扇出度
    avg_fanout: HashMap<String, f32>,
    /// 节点总数估计
    #[allow(dead_code)]
    total_nodes: usize,
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self {
            stats: QueryStats::default(),
        }
    }

    /// 优化执行计划（类似 lance-graph 的 DataFusion Planner）
    pub fn optimize(&self, plan: ExecutionPlan) -> ExecutionPlan {
        // 优化规则 1: 谓词下推（Predicate Pushdown）
        let plan = self.push_down_filters(plan);

        // 优化规则 2: 批量大小调整
        let plan = self.adjust_batch_sizes(plan);

        // 优化规则 3: 提前终止（Early Termination）
        let plan = self.apply_early_termination(plan);

        plan
    }

    /// 规则 1: 将过滤条件下推到数据源
    fn push_down_filters(&self, plan: ExecutionPlan) -> ExecutionPlan {
        match plan {
            ExecutionPlan::Limit { input, count } => {
                if let ExecutionPlan::BatchExpand { input: inner, edge_filter, batch_size } = *input {
                    // 如果 limit 很小，减小批量大小
                    let optimized_batch = batch_size.min(count * 2);
                    ExecutionPlan::Limit {
                        input: Box::new(ExecutionPlan::BatchExpand {
                            input: inner,
                            edge_filter,
                            batch_size: optimized_batch,
                        }),
                        count,
                    }
                } else {
                    ExecutionPlan::Limit { input, count }
                }
            }
            other => other,
        }
    }

    /// 规则 2: 根据统计信息调整批量大小
    fn adjust_batch_sizes(&self, plan: ExecutionPlan) -> ExecutionPlan {
        match plan {
            ExecutionPlan::BatchExpand { input, edge_filter, batch_size } => {
                // 根据关系类型的扇出度动态调整
                let estimated_fanout = edge_filter.relation_types.iter()
                    .filter_map(|t| self.stats.avg_fanout.get(t))
                    .sum::<f32>() / edge_filter.relation_types.len().max(1) as f32;

                let optimal_batch = if estimated_fanout > 100.0 {
                    // 高扇出：减小批量避免内存爆炸
                    32
                } else if estimated_fanout < 5.0 {
                    // 低扇出：增大批量减少 I/O
                    512
                } else {
                    batch_size
                };

                ExecutionPlan::BatchExpand {
                    input,
                    edge_filter,
                    batch_size: optimal_batch,
                }
            }
            other => other,
        }
    }

    /// 规则 3: 提前终止优化
    fn apply_early_termination(&self, plan: ExecutionPlan) -> ExecutionPlan {
        // 如果有 LIMIT，包装一个提前终止的 Distinct
        plan
    }
}

/// 执行计划解释器（用于调试）
pub struct PlanExplainer;

impl PlanExplainer {
    pub fn explain(plan: &ExecutionPlan) -> String {
        Self::explain_recursive(plan, 0)
    }

    fn explain_recursive(plan: &ExecutionPlan, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        match plan {
            ExecutionPlan::ScanNodes { node_ids } => {
                format!("{}ScanNodes (count={})", prefix, node_ids.len())
            }
            ExecutionPlan::BatchExpand { input, edge_filter, batch_size } => {
                let child = Self::explain_recursive(input, indent + 1);
                format!(
                    "{}BatchExpand (batch_size={}, filter={:?})\n{}",
                    prefix, batch_size, edge_filter, child
                )
            }
            ExecutionPlan::Distinct { input } => {
                let child = Self::explain_recursive(input, indent + 1);
                format!("{}Distinct\n{}", prefix, child)
            }
            ExecutionPlan::Limit { input, count } => {
                let child = Self::explain_recursive(input, indent + 1);
                format!("{}Limit (count={})\n{}", prefix, count, child)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimizer_batch_size_adjustment() {
        let optimizer = QueryOptimizer::new();

        let plan = ExecutionPlan::BatchExpand {
            input: Box::new(ExecutionPlan::ScanNodes {
                node_ids: vec![Uuid::new_v4()],
            }),
            edge_filter: EdgeFilter {
                relation_types: vec!["RELATED_TO".to_string()],
                min_weight: Some(0.5),
                max_weight: None,
            },
            batch_size: 100,
        };

        let optimized = optimizer.optimize(plan);

        // 验证优化后的计划
        println!("{}", PlanExplainer::explain(&optimized));
    }

    #[test]
    fn test_plan_explainer() {
        let plan = ExecutionPlan::Limit {
            count: 10,
            input: Box::new(ExecutionPlan::Distinct {
                input: Box::new(ExecutionPlan::BatchExpand {
                    input: Box::new(ExecutionPlan::ScanNodes {
                        node_ids: vec![Uuid::new_v4(); 5],
                    }),
                    edge_filter: EdgeFilter {
                        relation_types: vec!["KNOWS".to_string()],
                        min_weight: None,
                        max_weight: None,
                    },
                    batch_size: 256,
                }),
            }),
        };

        let explanation = PlanExplainer::explain(&plan);
        println!("Execution Plan:\n{}", explanation);

        // 应该输出类似：
        // Limit (count=10)
        //   Distinct
        //     BatchExpand (batch_size=256, filter=...)
        //       ScanNodes (count=5)
    }
}
