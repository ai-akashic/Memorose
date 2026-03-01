use memorose_core::{MemoroseEngine, GraphEdge, RelationType};
use memorose_common::MemoryUnit;
use uuid::Uuid;
use anyhow::Result;
use std::path::PathBuf;
use std::fs;

#[tokio::main]
async fn main() -> Result<()> {
    let data_dir = PathBuf::from("./data_example_graph");
    if data_dir.exists() { fs::remove_dir_all(&data_dir)?; }    
    let engine = MemoroseEngine::new_with_default_threshold(&data_dir, 1000, true, true).await?;
    let stream_id = Uuid::new_v4();
    let user_id = "example_user".to_string();
    let app_id = "example_app".to_string();

    println!("🧪 --- Memorose Graph Structure Storage Concept ---");
    println!("Goal: Build and query a knowledge graph of memories");

    // 1. Create Nodes (Memories)
    // Concept: A simple knowledge graph about "Programming Languages"
    let nodes = vec![
        ("Rust", vec![1.0, 0.0, 0.0]),
        ("Systems Programming", vec![0.9, 0.1, 0.0]),
        ("Memory Safety", vec![0.8, 0.2, 0.0]),
        ("Python", vec![0.0, 1.0, 0.0]),
        ("Data Science", vec![0.1, 0.9, 0.0]),
    ];

    let mut units = Vec::new();
    println!("\n🏗️  Building Nodes...");
    for (content, vec_data) in nodes {
        let mut embedding = vec![0.0; 384];
        for (i, val) in vec_data.iter().enumerate() { embedding[i] = *val; }        
        let unit = MemoryUnit::new(user_id.clone(), None, app_id.clone(), stream_id, memorose_common::MemoryType::Factual, content.to_string(), Some(embedding));
        units.push(unit.clone());
        engine.store_memory_unit(unit).await?;
    }
    
    // 2. Create Edges (Relationships)
    // Rust -> Systems Programming (RelatedTo)
    // Rust -> Memory Safety (DerivedFrom - conceptually)
    // Python -> Data Science (RelatedTo)
    println!("🔗 Building Edges...");
    let edges_def = vec![
        (0, 1, RelationType::RelatedTo),
        (0, 2, RelationType::RelatedTo),
        (3, 4, RelationType::RelatedTo),
        (0, 3, RelationType::RelatedTo), // Maybe weak relation?
    ];

    let graph = engine.graph();
    for (src_idx, tgt_idx, rel) in edges_def {
        let src = units[src_idx].id;
        let tgt = units[tgt_idx].id;
        let edge = GraphEdge::new(user_id.clone(), src, tgt, rel, 1.0);

        graph.add_edge(&edge).await?;
    }

    // 3. Traversal / Query
    println!("\n🕸️  Graph Traversal (Start Node: 'Rust')...");
    let start_node = units[0].id;

    let outgoing = graph.get_outgoing_edges("example_user", start_node).await?;

    println!("   Node 'Rust' connects to:");
    for edge in outgoing {
        // Find target content
        let target_unit = units.iter().find(|u| u.id == edge.target_id).unwrap();
        println!("     -> [{:?}] {}", edge.relation, target_unit.content);
    }

    // 4. Community Detection (Mock logic if not fully exposed, but engine has it)
    // The engine library exports CommunityDetector.
    // Let's see if we can use it.
    use memorose_core::CommunityDetector;
    let all_edges = graph.get_all_edges_for_user("example_user").await?;
    
    let communities = CommunityDetector::detect_communities(&all_edges);
    println!("\n🏘️  Community Detection:");
    println!("   Found {} communities/clusters.", communities.values().collect::<std::collections::HashSet<_>>().len());
    
    for (node_id, comm_id) in &communities {
        let unit = units.iter().find(|u| u.id == *node_id).unwrap();
        println!("   - [{}] is in Cluster {}", unit.content, comm_id);
    }

    println!("\n📊 Performance Metrics:");
    println!("   - Graph Storage: Adjacency List in RocksDB");
    println!("   - Traversal: O(k) where k is degree of node");
    
    println!("\n💡 Potential Use Cases:");
    println!("   - Knowledge Graph RAG (GraphRAG)");
    println!("   - Concept Drift Detection");
    println!("   - User Interest Clustering");

    fs::remove_dir_all(&data_dir)?;
    Ok(())
}
