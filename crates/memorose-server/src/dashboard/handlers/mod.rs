pub mod types;

mod agents;
mod auth;
mod chat;
mod config;
mod corrections;
mod forget;
mod graph;
mod memories;
mod organizations;
mod search;
mod stats;

// Re-export all public handler functions so main.rs paths don't change
pub use agents::list_agents;
pub use auth::{change_password, login};
pub use chat::chat;
pub use config::get_config;
pub use corrections::{
    apply_manual_correction, approve_rac_review, list_rac_reviews, reject_rac_review,
    semantic_memory_execute, semantic_memory_preview, user_semantic_memory_execute,
    user_semantic_memory_preview,
};
pub use forget::{forget_execute, forget_preview};
pub use graph::graph_data;
pub use memories::{get_memory, list_memories};
pub use organizations::{
    create_api_key, create_organization, get_organization_knowledge,
    get_organization_knowledge_metrics, list_api_keys, list_organization_knowledge,
    list_organizations, revoke_api_key,
};
pub use search::search;
pub use stats::{cluster_status, stats};
