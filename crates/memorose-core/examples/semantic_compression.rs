use memorose_core::{MemoroseEngine, BackgroundWorker, Event, EventContent};
use uuid::Uuid;
use anyhow::Result;
use std::path::PathBuf;
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    let data_dir = PathBuf::from("./data_example_compression");
    if data_dir.exists() { fs::remove_dir_all(&data_dir)?; }
    
    // Initialize Engine & Worker
    let engine = MemoroseEngine::new(&data_dir, 1000, true, true).await?;
    let engine_clone = engine.clone();
    
    // Note: This requires an API Key to actually work with LLM. 
    // If not present, the worker falls back to raw text, which we can still observe.
    tokio::spawn(async move {
        let worker = BackgroundWorker::new(engine_clone);
        worker.run().await;
    });

    println!("🧪 --- Memorose Semantic Compression Concept ---");
    println!("Goal: Convert high-volume L0 events into concise L1 memories");
    
    // Simulate a verbose meeting log
    let conversation = vec![
        "Alice: Hey Bob, did you see the server logs?",
        "Bob: Yeah, lots of 500 errors on the payment gateway.",
        "Alice: Is it the database connection?",
        "Bob: Looks like the connection pool is exhausted.",
        "Alice: Okay, let's restart the service and increase pool size to 100.",
        "Bob: Doing it now... Done. Errors stopped.",
        "Alice: Great, let's monitor it for an hour.",
    ];
    
    let stream_id = Uuid::new_v4();
    let user_id = "example_user".to_string();
    let app_id = "example_app".to_string();

    println!("\n📥 Ingesting {} verbose events...", conversation.len());
    let start_size: usize = conversation.iter().map(|s| s.len()).sum();
    
    for line in conversation {
        let event = Event::new(user_id.clone(), app_id.clone(), stream_id, EventContent::Text(line.to_string()));
        engine.ingest_event(event).await?;
    }

    println!("⏳ Waiting for Background Consolidation (5s)...");
    sleep(Duration::from_secs(5)).await;

    // Fetch L1 Memories
    let l1_memories = engine.fetch_recent_l1_units(&user_id, 10).await?;
    
    println!("\n📦 L1 Consolidated Memory (Semantic Compression):");
    if l1_memories.is_empty() {
        println!("   (No L1 memories generated. Check API Key or Wait Time)");
    } else {
        let mut end_size = 0;
        for unit in &l1_memories {
            println!("   - \"{}\"", unit.content);
            end_size += unit.content.len();
        }
        
        // Metrics
        println!("\n📊 Performance Metrics:");
        println!("   - Original Size: {} chars", start_size);
        println!("   - Compressed Size: {} chars", end_size);
        if start_size > 0 {
            println!("   - Compression Ratio: {:.1}x", start_size as f64 / end_size as f64);
        }
    }

    println!("\n💡 Potential Use Cases:");
    println!("   - Meeting transcripts -> Action items");
    println!("   - Customer support chat -> Ticket summaries");
    println!("   - Debug logs -> Root cause events");

    fs::remove_dir_all(&data_dir)?;
    Ok(())
}
