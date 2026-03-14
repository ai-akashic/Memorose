// Community Detection Module
//
// Provides multiple community detection algorithms:
// - Basic LPA (legacy, backward compatible)
// - Enhanced algorithms (LPA, Weighted LPA, Louvain)
// - Batch-optimized for large graphs

mod basic;
mod batch;
mod enhanced;

pub use basic::CommunityDetector; // 保持向后兼容
pub use batch::BatchCommunityDetector;
pub use enhanced::{Algorithm, CommunityResult, DetectionConfig, EnhancedCommunityDetector};
