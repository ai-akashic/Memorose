use memorose_core::{MemoroseEngine, BackgroundWorker, Event, EventContent};
use uuid::Uuid;
use anyhow::Result;
use std::path::PathBuf;
use std::fs;
use std::time::{Instant, Duration};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    let data_dir = PathBuf::from("./data_example_frequency");
    if data_dir.exists() { fs::remove_dir_all(&data_dir)?; }
    
    let engine = MemoroseEngine::new_with_default_threshold(&data_dir, 1000, true, true).await?;
    let engine_clone = engine.clone();
    let user_id = "example_user".to_string();
    let app_id = "example_app".to_string();
    
    // Start worker (Consolidation Cycle)
    tokio::spawn(async move {
        let worker = BackgroundWorker::new(engine_clone);
        worker.run().await;
    });

    println!("🧪 --- Memorose Multi-Frequency Update Concept ---");
    println!("Goal: Demonstrate fast ingestion (L0) vs. slow consolidation (L1/L2)");

    let stream_id = Uuid::new_v4();
    let n_events = 100;

    // 1. L0 Burst Write
    println!("\n🚀 Starting L0 Burst Write ({} events)...", n_events);
    let start = Instant::now();
    for i in 0..n_events {
        let event = Event::new(user_id.clone(), None, app_id.clone(), stream_id, EventContent::Text(format!("Log entry #{}", i)));
        engine.ingest_event(event).await?;
    }
    let duration = start.elapsed();
    println!("✅ L0 Write Complete. Time: {:.2?}, Throughput: {:.0} ops/sec", duration, n_events as f64 / duration.as_secs_f64());

    // 2. Observe L1 Lag
    println!("\n👀 Observing L1 Propagation (Simulating Async Processing)...");
    for _ in 0..5 {
        let l1_count = engine.fetch_recent_l1_units(&user_id, 1000).await?.len();
        let pending = engine.fetch_pending_events().await?.len();
        
        println!("   Status: L0 Pending: {:<3} | L1 Consolidated: {:<3}", pending, l1_count);
        
        if l1_count > 0 {
            break; 
        }
        sleep(Duration::from_millis(500)).await;
    }

    // 3. Force Wait for more
    sleep(Duration::from_secs(2)).await;
    let l1_final = engine.fetch_recent_l1_units(&user_id, 1000).await?.len();
    println!("   Final:  L0 Pending: {:<3} | L1 Consolidated: {:<3}", 
             engine.fetch_pending_events().await?.len(), l1_final);

    println!("\n📊 Performance Metrics (Concept):");
    println!("   - L0 (WAL): Microsecond latency, High throughput");
    println!("   - L1 (Vector): Second-level latency, Batch optimized");
    println!("   - L2 (Graph): Minute-level latency, Periodic");

    println!("\n💡 Potential Use Cases:");
    println!("   - Real-time gaming (record everything, summarize later)");
    println!("   - High-frequency trading logs -> Daily strategy insights");

    fs::remove_dir_all(&data_dir)?;
    Ok(())
}
