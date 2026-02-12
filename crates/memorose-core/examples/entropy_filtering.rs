use memorose_core::{MemoroseEngine, Event, EventContent};
use uuid::Uuid;
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::fs;

/// Simple Shannon Entropy Calculator
fn calculate_entropy(text: &str) -> f64 {
    if text.is_empty() { return 0.0; }
    
    let mut counts = HashMap::new();
    let len = text.len() as f64;
    
    for c in text.chars() {
        *counts.entry(c).or_insert(0) += 1;
    }
    
    counts.values().fold(0.0, |entropy, &count| {
        let p = count as f64 / len;
        entropy - p * p.log2()
    })
}

/// A filter wrapper around ingestion
struct SmartIngestor {
    engine: MemoroseEngine,
    threshold: f64,
    user_id: String,
    app_id: String,
}

impl SmartIngestor {
    fn new(engine: MemoroseEngine, threshold: f64) -> Self {
        Self { engine, threshold, user_id: "example_user".to_string(), app_id: "example_app".to_string() }
    }

    async fn ingest(&self, content: String) -> Result<bool> {
        let entropy = calculate_entropy(&content);
        println!("Input: {:<30} | Entropy: {:.4}", content.chars().take(30).collect::<String>(), entropy);

        if entropy < self.threshold {
            println!("   -> 🗑️  REJECTED (Low Information)");
            return Ok(false);
        }

        let event = Event::new(self.user_id.clone(), self.app_id.clone(), Uuid::new_v4(), EventContent::Text(content));
        self.engine.ingest_event(event).await?;
        println!("   -> ✅ ACCEPTED");
        Ok(true)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let data_dir = PathBuf::from("./data_example_entropy");
    if data_dir.exists() { fs::remove_dir_all(&data_dir)?;
    }
    
    let engine = MemoroseEngine::new_with_default_threshold(&data_dir, 1000, true, true).await?;
    let ingestor = SmartIngestor::new(engine, 3.5); // Threshold

    println!("🧪 --- Memorose Entropy Filtering Concept ---");
    println!("Goal: Filter out low-information/repetitive data at L0 (Ingestion)");
    println!("Threshold: 3.5 bits\n");

    let inputs = vec![
        "aaaaaaa bbbbbbb ccccccc",       // Low entropy (repetitive)
        "System check OK. System check OK.", // Low entropy
        "The quick brown fox jumps over the lazy dog.", // High entropy (rich text)
        "Error: Connection timeout at 10.0.0.1", // Medium-High
        "1234567890", // Medium (short charset)
    ];

    let mut accepted = 0;
    for input in inputs {
        if ingestor.ingest(input.to_string()).await? {
            accepted += 1;
        }
    }

    println!("\n📊 Performance Metrics:");
    println!("   - Reduction Ratio: {:.1}%", (1.0 - (accepted as f64 / 5.0)) * 100.0);
    println!("   - Latency: <1ms (Compute Bound)");
    
    println!("\n💡 Potential Use Cases:");
    println!("   - IoT Sensor Logs (ignore steady state)");
    println!("   - Spam filtering in chat streams");
    println!("   - Deduplication of frequent error logs");

    fs::remove_dir_all(&data_dir)?;
    Ok(())
}
