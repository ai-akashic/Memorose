use axum::{extract::State, Json};
use std::sync::Arc;

// ── Config ────────────────────────────────────────────────────────

pub async fn get_config(State(state): State<Arc<crate::AppState>>) -> Json<serde_json::Value> {
    let config = &state.config;

    let mut result = serde_json::json!({
        "raft": {
            "node_id": config.raft.node_id,
            "raft_addr": config.raft.raft_addr,
            "heartbeat_interval_ms": config.raft.heartbeat_interval_ms,
            "election_timeout_min_ms": config.raft.election_timeout_min_ms,
            "election_timeout_max_ms": config.raft.election_timeout_max_ms,
            "snapshot_logs": config.raft.snapshot_logs,
        },
        "worker": {
            "llm_concurrency": config.worker.llm_concurrency,
            "consolidation_interval_ms": config.worker.consolidation_interval_ms,
            "community_interval_ms": config.worker.community_interval_ms,
            "insight_interval_ms": config.worker.insight_interval_ms,
        },
        "llm": {
            "provider": format!("{:?}", config.llm.provider),
            "model": config.llm.model,
            "embedding_model": config.llm.embedding_model,
        },
        "storage": {
            "root_dir": config.storage.root_dir,
            "index_commit_interval_ms": config.storage.index_commit_interval_ms,
        },
    });

    if config.is_sharded() {
        result["sharding"] = serde_json::json!({
            "enabled": true,
            "shard_count": config.shard_count(),
            "physical_node_id": config.physical_node_id(),
        });
    }

    Json(result)
}
