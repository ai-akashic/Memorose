use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;

use memorose_common::config::AppConfig;
use memorose_common::sharding::{encode_raft_node_id, raft_addr_for_shard, user_id_to_shard};
use memorose_core::raft::network::run_raft_server;
use memorose_core::raft::start_raft_node;
use memorose_core::raft::MemoroseRaft;
use memorose_core::{BackgroundWorker, MemoroseEngine};
use openraft::BasicNode;

pub struct ShardState {
    pub engine: MemoroseEngine,
    pub raft: MemoroseRaft,
}

pub struct ShardManager {
    shards: HashMap<u32, ShardState>,
    shard_count: u32,
    physical_node_id: u32,
}

impl ShardManager {
    /// Create a multi-shard manager from sharding config.
    pub async fn new(config: &AppConfig) -> anyhow::Result<Self> {
        let sharding = config.sharding.as_ref()
            .expect("ShardManager::new called without sharding config");
        let shard_count = sharding.shard_count.max(1);
        let physical_node_id = sharding.physical_node_id;
        let base_dir = &config.storage.root_dir;

        // Find this node's raft_base_port from the sharding node list
        let this_node = sharding.nodes.iter()
            .find(|n| n.id == physical_node_id)
            .expect("Physical node ID not found in sharding.nodes");
        let raft_base_port = this_node.raft_base_port;
        let raft_host_raw = this_node.http_addr.split(':').next().unwrap_or("127.0.0.1");
        let raft_host = if raft_host_raw == "0.0.0.0" { "127.0.0.1" } else { raft_host_raw };

        let mut shards = HashMap::new();

        for shard_id in 0..shard_count {
            let shard_dir = format!("{}/shard_{}", base_dir, shard_id);
            let raft_node_id = encode_raft_node_id(shard_id, physical_node_id);
            let raft_addr_str = raft_addr_for_shard(raft_host, raft_base_port, shard_id);

            tracing::info!(
                "Initializing shard {} (raft_node_id={}, raft_addr={})",
                shard_id, raft_node_id, raft_addr_str
            );

            let engine = MemoroseEngine::new(
                &shard_dir,
                config.storage.index_commit_interval_ms,
                config.worker.enable_auto_planner,
                config.worker.enable_task_reflection,
            ).await?;

            // Override raft config for this shard
            let mut shard_config = config.clone();
            shard_config.raft.node_id = raft_node_id;
            shard_config.raft.raft_addr = raft_addr_str.clone();

            let raft = start_raft_node(raft_node_id, engine.clone(), shard_config.clone()).await
                .map_err(|e| anyhow::anyhow!("Failed to start raft for shard {}: {:?}", shard_id, e))?;

            // Start background worker for this shard
            let mut worker = BackgroundWorker::with_config(engine.clone(), shard_config);
            worker.set_raft(raft.clone());
            tokio::spawn(async move {
                worker.run().await;
            });

            // Start raft gRPC server for this shard
            let raft_addr: SocketAddr = raft_addr_str.parse()?;
            let raft_for_server = raft.clone();
            tokio::spawn(async move {
                tracing::info!("Raft gRPC server for shard {} listening on {}", shard_id, raft_addr);
                if let Err(e) = run_raft_server(raft_addr, raft_for_server).await {
                    tracing::error!("Raft server error for shard {}: {:?}", shard_id, e);
                }
            });

            shards.insert(shard_id, ShardState { engine, raft });
        }

        Ok(Self { shards, shard_count, physical_node_id })
    }

    /// Create a single-shard manager (backward compatible, no sharding config needed).
    pub async fn new_single_shard(config: &AppConfig) -> anyhow::Result<Self> {
        let data_dir = &config.storage.root_dir;
        let node_id = config.raft.node_id;
        let raft_addr_str = config.raft.raft_addr.clone();

        let engine = MemoroseEngine::new(
            data_dir,
            config.storage.index_commit_interval_ms,
            config.worker.enable_auto_planner,
            config.worker.enable_task_reflection,
        ).await?;

        let raft = start_raft_node(node_id, engine.clone(), config.clone()).await
            .map_err(|e| anyhow::anyhow!("Failed to start raft: {:?}", e))?;

        // Start background worker
        let mut worker = BackgroundWorker::with_config(engine.clone(), config.clone());
        worker.set_raft(raft.clone());
        tokio::spawn(async move {
            worker.run().await;
        });

        // Start raft gRPC server
        let raft_addr: SocketAddr = raft_addr_str.parse()?;
        let raft_for_server = raft.clone();
        tokio::spawn(async move {
            tracing::info!("Raft gRPC server listening on {}", raft_addr);
            if let Err(e) = run_raft_server(raft_addr, raft_for_server).await {
                tracing::error!("Raft server error: {:?}", e);
            }
        });

        let mut shards = HashMap::new();
        shards.insert(0, ShardState { engine, raft });

        Ok(Self {
            shards,
            shard_count: 1,
            physical_node_id: node_id as u32,
        })
    }

    /// Route a user_id to the appropriate shard.
    pub fn shard_for_user(&self, user_id: &str) -> &ShardState {
        let shard_id = user_id_to_shard(user_id, self.shard_count);
        self.shards.get(&shard_id)
            .expect("shard_for_user: shard missing from map")
    }

    /// Get a specific shard by ID.
    pub fn shard(&self, shard_id: u32) -> Option<&ShardState> {
        self.shards.get(&shard_id)
    }

    /// Iterate over all shards.
    pub fn all_shards(&self) -> impl Iterator<Item = (u32, &ShardState)> {
        self.shards.iter().map(|(&id, state)| (id, state))
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    pub fn physical_node_id(&self) -> u32 {
        self.physical_node_id
    }

    /// Initialize all Raft groups (for cluster bootstrap).
    /// Idempotent: returns success if already initialized.
    pub async fn initialize_all(&self, config: &AppConfig) -> Vec<serde_json::Value> {
        let mut results = Vec::new();
        let sharding = config.sharding.as_ref();

        for (&shard_id, shard) in &self.shards {
            // Check if already initialized (has logs or membership)
            let metrics = shard.raft.metrics().borrow().clone();
            if metrics.last_log_index.unwrap_or(0) > 0
                || metrics.membership_config.membership().voter_ids().count() > 0
            {
                tracing::info!("Shard {} already initialized, skipping", shard_id);
                results.push(serde_json::json!({
                    "shard_id": shard_id, "status": "already_initialized"
                }));
                continue;
            }

            let raft_node_id = if self.shard_count > 1 {
                encode_raft_node_id(shard_id, self.physical_node_id)
            } else {
                config.raft.node_id
            };

            let raft_addr = if let Some(sc) = sharding {
                let node = sc.nodes.iter().find(|n| n.id == self.physical_node_id);
                node.map(|n| {
                    let host = n.http_addr.split(':').next().unwrap_or("127.0.0.1");
                    raft_addr_for_shard(host, n.raft_base_port, shard_id)
                }).unwrap_or_else(|| config.raft.raft_addr.clone())
            } else {
                config.raft.raft_addr.clone()
            };

            let mut nodes = BTreeMap::new();
            nodes.insert(raft_node_id, BasicNode { addr: raft_addr });

            match shard.raft.initialize(nodes).await {
                Ok(_) => {
                    tracing::info!("Initialized raft for shard {} (node_id={})", shard_id, raft_node_id);
                    results.push(serde_json::json!({
                        "shard_id": shard_id, "status": "initialized"
                    }));
                }
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    if err_str.contains("NotAllowed") {
                        tracing::info!("Shard {} already initialized (NotAllowed), treating as success", shard_id);
                        results.push(serde_json::json!({
                            "shard_id": shard_id, "status": "already_initialized"
                        }));
                    } else {
                        tracing::warn!("Failed to initialize shard {}: {:?}", shard_id, e);
                        results.push(serde_json::json!({
                            "shard_id": shard_id, "error": format!("{:?}", e)
                        }));
                    }
                }
            }
        }
        results
    }

    /// Add a joining node to all Raft groups.
    pub async fn join_all(&self, joining_physical_node_id: u32, config: &AppConfig) -> Vec<serde_json::Value> {
        let mut results = Vec::new();
        let sharding = config.sharding.as_ref();

        for (&shard_id, shard) in &self.shards {
            let joining_raft_id = if self.shard_count > 1 {
                encode_raft_node_id(shard_id, joining_physical_node_id)
            } else {
                joining_physical_node_id as u64
            };

            let joining_addr = if let Some(sc) = sharding {
                let node = sc.nodes.iter().find(|n| n.id == joining_physical_node_id);
                node.map(|n| {
                    let host = n.http_addr.split(':').next().unwrap_or("127.0.0.1");
                    raft_addr_for_shard(host, n.raft_base_port, shard_id)
                }).unwrap_or_default()
            } else {
                // Single-shard mode: expect address to be passed separately
                String::new()
            };

            if joining_addr.is_empty() {
                results.push(serde_json::json!({
                    "shard_id": shard_id,
                    "error": "Cannot resolve joining node address"
                }));
                continue;
            }

            let node = BasicNode { addr: joining_addr };

            // Add as learner
            match shard.raft.add_learner(joining_raft_id, node, true).await {
                Ok(_) => {}
                Err(e) => {
                    results.push(serde_json::json!({
                        "shard_id": shard_id,
                        "error": format!("add_learner failed: {:?}", e)
                    }));
                    continue;
                }
            }

            tokio::task::yield_now().await;

            // Promote to voter
            let metrics = shard.raft.metrics().borrow().clone();
            let mut members: BTreeSet<u64> = metrics.membership_config.membership().voter_ids().collect();
            members.insert(joining_raft_id);

            match shard.raft.change_membership(members, false).await {
                Ok(_) => {
                    results.push(serde_json::json!({
                        "shard_id": shard_id,
                        "status": "joined",
                        "raft_node_id": joining_raft_id
                    }));
                }
                Err(e) => {
                    results.push(serde_json::json!({
                        "shard_id": shard_id,
                        "error": format!("change_membership failed: {:?}", e)
                    }));
                }
            }
        }
        results
    }

    /// Remove a node from all Raft groups.
    pub async fn leave_all(&self, leaving_physical_node_id: u32) -> Vec<serde_json::Value> {
        let mut results = Vec::new();

        for (&shard_id, shard) in &self.shards {
            let leaving_raft_id = if self.shard_count > 1 {
                encode_raft_node_id(shard_id, leaving_physical_node_id)
            } else {
                leaving_physical_node_id as u64
            };

            let metrics = shard.raft.metrics().borrow().clone();
            let mut members: BTreeSet<u64> = metrics.membership_config.membership().voter_ids().collect();

            if !members.remove(&leaving_raft_id) {
                results.push(serde_json::json!({
                    "shard_id": shard_id,
                    "error": "Node not found in cluster"
                }));
                continue;
            }

            match shard.raft.change_membership(members, false).await {
                Ok(_) => {
                    results.push(serde_json::json!({
                        "shard_id": shard_id,
                        "status": "left",
                        "raft_node_id": leaving_raft_id
                    }));
                }
                Err(e) => {
                    results.push(serde_json::json!({
                        "shard_id": shard_id,
                        "error": format!("{:?}", e)
                    }));
                }
            }
        }
        results
    }

    /// Gracefully shut down all Raft groups.
    pub async fn shutdown_all(&self) {
        for (&shard_id, shard) in &self.shards {
            if let Err(e) = shard.engine.graph().flush().await {
                tracing::error!("Graph flush error for shard {}: {:?}", shard_id, e);
            }
            if let Err(e) = shard.raft.shutdown().await {
                tracing::error!("Raft shutdown error for shard {}: {:?}", shard_id, e);
            }
        }
    }
}
