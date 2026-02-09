use memorose_core::MemoroseEngine;
use memorose_common::MemoryUnit;
use uuid::Uuid;
use anyhow::Result;
use std::path::PathBuf;
use std::fs;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Setup a temporary directory for the example
    let data_dir = PathBuf::from("./data_example_basic");
    if data_dir.exists() {
        fs::remove_dir_all(&data_dir)?;
    }
    fs::create_dir_all(&data_dir)?;

    println!("🚀 Starting Memorose Basic Operations Example...");
    println!("📂 Data directory: {:?}", data_dir);

    // 2. Initialize the Engine
    let engine = MemoroseEngine::new(&data_dir, 1000, true, true).await?;
    let stream_id = Uuid::new_v4();

    // 3. Store some memories (L1) with simulated embeddings
    // Vector dim: 384 (assuming a small model)
    // We simulate 3 memories: A and B are similar (Topic 1), C is different (Topic 2)
    
    // Memory A: "I like coding in Rust"
    let mut vec_a = vec![0.0; 384]; 
    vec_a[0] = 1.0; vec_a[1] = 0.5; // Simple signature for Topic 1
    
    // Memory B: "Rust is a safe systems language"
    let mut vec_b = vec![0.0; 384];
    vec_b[0] = 0.9; vec_b[1] = 0.6; // Similar to A
    
    // Memory C: "The weather is sunny today"
    let mut vec_c = vec![0.0; 384];
    vec_c[10] = 1.0; // Orthogonal to A/B

    let user_id = "example_user".to_string();
    let app_id = "example_app".to_string();

    let unit_a = MemoryUnit::new(user_id.clone(), app_id.clone(), stream_id, "I like coding in Rust".into(), Some(vec_a.clone()));
    let unit_b = MemoryUnit::new(user_id.clone(), app_id.clone(), stream_id, "Rust is a safe systems language".into(), Some(vec_b.clone()));
    let unit_c = MemoryUnit::new(user_id.clone(), app_id.clone(), stream_id, "The weather is sunny today".into(), Some(vec_c.clone()));

    println!("\n📥 Storing 3 memory units...");
    engine.store_memory_unit(unit_a.clone()).await?;
    engine.store_memory_unit(unit_b.clone()).await?;
    engine.store_memory_unit(unit_c.clone()).await?;
    println!("✅ Stored.");

    // 4. Vector Search (Simulate retrieving "Rust" related memories)
    println!("\n🔍 Searching for similar vectors (Query: Topic 1)...");
    let results = engine.search_similar(&user_id, Some(app_id.as_str()), &vec_a, 5, None).await?;
    for (unit, score) in results {
        println!("   - Found: \"{}\" (Score: {:.4})", unit.content, score);
    }

    // 5. Full-text Search
    println!("\n🔎 Full-text Search (Query: 'weather')...");
    let text_results = engine.search_text(&user_id, Some(app_id.as_str()), "weather", 5, false, None).await?;
    for unit in text_results {
        println!("   - Found: \"{}\"", unit.content);
    }

    // 6. Graph Inspection (Check if A and B were automatically linked)
    // Note: Auto-linking happens in store_memory_unit if similarity > 0.7
    println!("\n🕸️  Inspecting Graph Connections...");
    let graph = engine.graph();

    // Check edges from A (should link to B if B was inserted after A? No, storing B checks for existing.
    // Since we stored A then B, B's insertion triggers search. If A is found, B links to A.
    // So we check edges from B.
    let edges_from_b = graph.get_outgoing_edges("example_user", unit_b.id).await?;

    if edges_from_b.is_empty() {
        println!("   (No edges found from B yet. Auto-linking relies on vector index visibility. LanceDB might need a moment or consistent index state.)");
    } else {
        for edge in edges_from_b {
            println!("   - Edge: B -> {} (Type: {:?}, Weight: {:.2})", edge.target_id, edge.relation, edge.weight);
            if edge.target_id == unit_a.id {
                println!("     🎉 Correctly linked to Memory A!");
            }
        }
    }

    // 7. Cleanup
    println!("\n🧹 Cleaning up...");
    fs::remove_dir_all(&data_dir)?;
    println!("✅ Done.");

    Ok(())
}
