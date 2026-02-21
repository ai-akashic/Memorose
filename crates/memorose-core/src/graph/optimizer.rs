// Query Optimizer - Borrowing lance-graph's query optimization concepts
// Converts declarative queries into efficient execution plans

use uuid::Uuid;
use std::collections::HashMap;

/// Query execution plan node
#[derive(Debug, Clone)]
pub enum ExecutionPlan {
    /// Scan starting nodes (similar to ScanByLabel in lance-graph)
    ScanNodes {
        node_ids: Vec<Uuid>,
    },

    /// Batch edge expansion (core of optimization)
    BatchExpand {
        input: Box<ExecutionPlan>,
        edge_filter: EdgeFilter,
        batch_size: usize,  // How many nodes to process per batch
    },

    /// Deduplication (similar to SQL DISTINCT)
    Distinct {
        input: Box<ExecutionPlan>,
    },

    /// Limit the number of results
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

/// Query Optimizer
pub struct QueryOptimizer {
    stats: QueryStats,
}

#[derive(Debug, Default)]
struct QueryStats {
    /// Average fanout degree per relation type
    avg_fanout: HashMap<String, f32>,
    /// Estimated total node count
    #[allow(dead_code)]
    total_nodes: usize,
}

impl QueryOptimizer {
    pub fn new() -> Self {
        Self {
            stats: QueryStats::default(),
        }
    }

    /// Optimize execution plan (similar to DataFusion Planner in lance-graph)
    pub fn optimize(&self, plan: ExecutionPlan) -> ExecutionPlan {
        // Optimization Rule 1: Predicate Pushdown
        let plan = self.push_down_filters(plan);

        // Optimization Rule 2: Batch Size Adjustment
        let plan = self.adjust_batch_sizes(plan);

        // Optimization Rule 3: Early Termination
        let plan = self.apply_early_termination(plan);

        plan
    }

    /// Rule 1: Push down filter conditions to the data source
    fn push_down_filters(&self, plan: ExecutionPlan) -> ExecutionPlan {
        match plan {
            ExecutionPlan::Limit { input, count } => {
                if let ExecutionPlan::BatchExpand { input: inner, edge_filter, batch_size } = *input {
                    // If limit is small, reduce the batch size
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

    /// Rule 2: Adjust batch size based on statistics
    fn adjust_batch_sizes(&self, plan: ExecutionPlan) -> ExecutionPlan {
        match plan {
            ExecutionPlan::BatchExpand { input, edge_filter, batch_size } => {
                // Dynamically adjust based on relation type fanout degree
                let estimated_fanout = edge_filter.relation_types.iter()
                    .filter_map(|t| self.stats.avg_fanout.get(t))
                    .sum::<f32>() / edge_filter.relation_types.len().max(1) as f32;

                let optimal_batch = if estimated_fanout > 100.0 {
                    // High fanout: decrease batch size to prevent memory explosion
                    32
                } else if estimated_fanout < 5.0 {
                    // Low fanout: increase batch size to reduce I/O
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

    /// Rule 3: Early Termination optimization
    fn apply_early_termination(&self, plan: ExecutionPlan) -> ExecutionPlan {
        // If there is a LIMIT, wrap an early terminating Distinct
        plan
    }
}

/// Execution Plan Explainer (for debugging)
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

        // Validate optimized plan
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

        // Should output something like:
        // Limit (count=10)
        //   Distinct
        //     BatchExpand (batch_size=256, filter=...)
        //       ScanNodes (count=5)
    }
}