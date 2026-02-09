// Community Detection Module
//
// Provides multiple community detection algorithms:
// - Basic LPA (legacy, backward compatible)
// - Enhanced algorithms (LPA, Weighted LPA, Louvain)
// - Batch-optimized for large graphs

mod basic;
mod enhanced;
mod batch;

pub use basic::CommunityDetector;  // 保持向后兼容
pub use enhanced::{
    EnhancedCommunityDetector,
    Algorithm,
    DetectionConfig,
    CommunityResult,
};
pub use batch::BatchCommunityDetector;
