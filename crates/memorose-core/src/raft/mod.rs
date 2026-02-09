use std::sync::Arc;
use openraft::{Config, Raft, SnapshotPolicy};
use self::types::MemoroseTypeConfig;

pub mod types;
pub mod storage;
pub mod network;

pub type MemoroseRaft = Raft<MemoroseTypeConfig>;

pub async fn start_raft_node(node_id: u64, engine: crate::MemoroseEngine, config: memorose_common::config::AppConfig) -> Result<MemoroseRaft, openraft::error::Fatal<u64>> {
    tracing::info!("Starting Raft node {} with snapshot_logs={}", node_id, config.raft.snapshot_logs);
    let raft_config = Config {
        heartbeat_interval: config.raft.heartbeat_interval_ms,
        election_timeout_min: config.raft.election_timeout_min_ms,
        election_timeout_max: config.raft.election_timeout_max_ms,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(config.raft.snapshot_logs),
        ..Default::default()
    };
    
    let raft_config = Arc::new(raft_config);
    let storage = storage::MemoroseRaftStorage::new(engine);
    let (log_store, state_machine) = openraft::storage::Adaptor::new(storage);
    let network = network::MemoroseNetworkFactory::default();

    Raft::new(node_id, raft_config, network, log_store, state_machine).await
}
