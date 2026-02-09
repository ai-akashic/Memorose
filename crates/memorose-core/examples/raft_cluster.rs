use memorose_core::raft::start_raft_node;
use memorose_core::raft::network::run_raft_server;
use memorose_core::MemoroseEngine;
use memorose_common::config::AppConfig;
use memorose_common::{Event, EventContent};
use openraft::BasicNode;
use std::collections::BTreeMap;
use tempfile::tempdir;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set RUST_LOG=info to see Raft state changes
    tracing_subscriber::fmt::init();

    // 1. Setup 3 nodes configs
    let node_ids = vec![1, 2, 3];
    let mut node_configs = BTreeMap::new();
    
    for id in &node_ids {
        let port = 5000 + id;
        node_configs.insert(*id, BasicNode { addr: format!("127.0.0.1:{}", port) });
    }

    println!("🚀 Starting Memorose 3-node Raft Cluster...");

    let mut nodes = Vec::new();
    let config = AppConfig::load().unwrap_or_default();

    for id in node_ids {
        let dir = tempdir()?;
        let engine = MemoroseEngine::new(dir.path(), 1000, true, true).await?;
        
        let mut node_config = config.clone();
        node_config.raft.node_id = id;
        node_config.raft.raft_addr = format!("127.0.0.1:{}", 5000 + id);

        let raft = start_raft_node(id, engine.clone(), node_config).await?;
        
        let addr = format!("127.0.0.1:{}", 5000 + id).parse()?;
        let raft_server = raft.clone();
        
        // Start gRPC server in background
        tokio::spawn(async move {
            if let Err(e) = run_raft_server(addr, raft_server).await {
                eprintln!("Node {} server error: {:?}", id, e);
            }
        });

        nodes.push((id, raft, engine, dir));
    }

    // 2. Initialize the cluster
    println!("📡 Initializing cluster with all 3 nodes...");
    nodes[0].1.initialize(node_configs.clone()).await?;

    // 3. Wait for leader election
    println!("⏳ Waiting for leader election...");
    sleep(Duration::from_secs(5)).await;

    // 4. Verify Leader
    let leader_raft = &nodes[0].1;
    let metrics = leader_raft.metrics().borrow().clone();
    println!("📊 Current Leader Node ID: {:?}", metrics.current_leader);

    if let Some(leader_id) = metrics.current_leader {
        println!("✅ Cluster is UP. Leader is Node {}", leader_id);
        
        // 5. Test Consensus: Write through Leader
        println!("📝 Writing event to Leader...");
        let event = Event::new("example_user".into(), "example_app".into(), Uuid::new_v4(), EventContent::Text("Distributed Consensus Test".into()));
        let event_id = event.id;
        
        // Must find the leader's raft handle
        let leader_handle = nodes.iter().find(|(id, _, _, _)| *id == leader_id).map(|(_, r, _, _)| r).unwrap();
        leader_handle.client_write(memorose_core::raft::types::ClientRequest::IngestEvent(event)).await?;

        println!("⏳ Waiting for log replication...");
        sleep(Duration::from_secs(2)).await;

        // 6. Verify Data on ALL nodes (State Machine Replication)
        for (id, _, engine, _) in &nodes {
            let saved = engine.get_event("example_user", &event_id.to_string()).await?;
            if saved.is_some() {
                println!("✅ Node {} has the replicated data.", id);
            } else {
                println!("❌ Node {} is MISSING data!", id);
                return Err(anyhow::anyhow!("Replication failed on node {}", id));
            }
        }
    } else {
        println!("❌ Cluster election failed or timed out.");
        return Err(anyhow::anyhow!("Election failed"));
    }

    println!("🎉 Raft Cluster Consistency Verified!");

    Ok(())
}
