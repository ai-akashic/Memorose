pub mod types;

mod auth;
mod stats;
mod memories;
mod search;
mod graph;
mod forget;
mod corrections;
mod organizations;
mod config;
mod chat;
mod agents;

// Re-export all public handler functions so main.rs paths don't change
pub use auth::{login, change_password};
pub use stats::{cluster_status, stats};
pub use memories::{list_memories, get_memory};
pub use search::search;
pub use graph::graph_data;
pub use forget::{forget_preview, forget_execute};
pub use corrections::{
    semantic_memory_preview, user_semantic_memory_preview,
    semantic_memory_execute, user_semantic_memory_execute,
    apply_manual_correction,
    list_rac_reviews, approve_rac_review, reject_rac_review,
};
pub use organizations::{
    list_organizations, create_organization,
    list_api_keys, create_api_key, revoke_api_key,
    list_organization_knowledge, get_organization_knowledge,
    get_organization_knowledge_metrics,
};
pub use config::get_config;
pub use chat::chat;
pub use agents::list_agents;
