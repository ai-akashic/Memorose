use memorose_core::MemoroseEngine;
use anyhow::Result;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    // Modify path if your data is elsewhere
    let data_path = PathBuf::from("data"); 
    
    println!("🕵️‍♀️ Inspecting Memorose State at {:?}...", data_path); 
    
    let engine = MemoroseEngine::new(&data_path, 1000, true, true).await?;
    let user_id = "example_user";
    
    // 1. Check Pending Events (L0)
    let pending = engine.fetch_pending_events().await?;
    println!("\n[L0 WAL] Pending Events: {}", pending.len());
    for event in pending.iter().take(5) {
        println!("  - ID: {}, Stream: {}, Content (prefix): {:.50}...",
            event.id, event.stream_id, format!("{:?}", event.content));
    }

    // 2. Check L1 Memories (LanceDB/RocksDB Metadata)
    // We scan RocksDB 'unit:' prefix which is the source of truth for metadata.
    // We can't count LanceDB easily without scan query, but unit: metadata is sufficient proxy.
    // Engine exposes fetch_recent_l1_units for a quick check.
    
    println!("\n[L1 Memory] Checking recent units...");
    let units = engine.fetch_recent_l1_units(user_id, 100).await?;
    println!("  - Found {} units.", units.len());
    for unit in units.iter().take(5) {
         println!("    * Unit ID: {}, Level: {}, Content: {:.50}...", unit.id, unit.level, unit.content);
    }

    Ok(())
}
