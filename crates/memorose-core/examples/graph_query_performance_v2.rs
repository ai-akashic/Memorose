// 更新的性能测试 - 验证批量查询优化

use memorose_core::MemoroseEngine;
use memorose_common::{MemoryUnit, GraphEdge, RelationType};
use uuid::Uuid;
use std::time::Instant;
use anyhow::Result;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("\n🎯 Updated Performance Benchmark - With Real Batch API");
    println!("=======================================================\n");

    // 初始化引擎
    let data_dir = PathBuf::from("./data_perf_test_v2");
    if data_dir.exists() { std::fs::remove_dir_all(&data_dir)?; }
    let engine = MemoroseEngine::new_with_default_threshold(&data_dir, 1000, true, true).await?;

    let user_id = "perf_test_user";
    let app_id = "perf_test_app";
    let stream_id = Uuid::new_v4();

    // 构建更大的测试图：1000 节点
    println!("📊 Building test graph (1000 nodes, ~5000 edges)...");
    let nodes = build_test_graph(&engine, user_id, app_id, stream_id, 1000, 5).await?;
    println!("✅ Graph built\n");

    // === 测试 1: 批量查询性能 ===
    println!("🔍 Test 1: Batch Query Performance (50 nodes)");
    println!("----------------------------------------------");
    test_batch_query_improved(&engine, user_id, &nodes).await?;
    println!();

    // === 测试 2: 缓存性能 ===
    println!("🔍 Test 2: Cache Performance");
    println!("-----------------------------");
    test_cache_performance(&engine, user_id, &nodes).await?;
    println!();

    // === 测试 3: 多跳遍历 ===
    println!("🔍 Test 3: Multi-hop Traversal (3 hops)");
    println!("----------------------------------------");
    test_multi_hop(&engine, user_id, &nodes).await?;
    println!();

    // 清理
    std::fs::remove_dir_all(&data_dir)?;

    println!("✅ All tests completed!");
    Ok(())
}

async fn build_test_graph(
    engine: &MemoroseEngine,
    user_id: &str,
    app_id: &str,
    stream_id: Uuid,
    num_nodes: usize,
    avg_edges_per_node: usize,
) -> Result<Vec<Uuid>> {
    let mut node_ids = Vec::new();

    // 创建节点
    for i in 0..num_nodes {
        let content = format!("Node {}", i);
        let embedding = vec![i as f32 / num_nodes as f32; 384];
        let unit = MemoryUnit::new(
            user_id.to_string(),
            app_id.to_string(),
            stream_id,
            content,
            Some(embedding),
        );
        node_ids.push(unit.id);
        engine.store_memory_unit(unit).await?;
    }

    // 创建边
    let graph = engine.graph();
    for i in 0..num_nodes {
        for j in 0..avg_edges_per_node {
            let target_idx = (i + j + 1) % num_nodes;
            let edge = GraphEdge::new(
                user_id.to_string(),
                node_ids[i],
                node_ids[target_idx],
                RelationType::RelatedTo,
                0.5 + (j as f32 * 0.1),
            );
            graph.add_edge(&edge).await?;
        }
    }

    graph.flush().await?;
    Ok(node_ids)
}

async fn test_batch_query_improved(
    engine: &MemoroseEngine,
    user_id: &str,
    nodes: &[Uuid],
) -> Result<()> {
    let query_nodes = &nodes[0..50];  // 查询前 50 个节点

    // ❌ 传统方式：逐个查询
    let start = Instant::now();
    let mut total_edges = 0;
    for node_id in query_nodes {
        let edges = engine.graph().get_outgoing_edges(user_id, *node_id).await?;
        total_edges += edges.len();
    }
    let traditional_duration = start.elapsed();

    println!("  ❌ Traditional (50 sequential queries):");
    println!("     Time: {:?}", traditional_duration);
    println!("     Total edges: {}", total_edges);

    // ✅ 优化方式：批量查询（真正的 SQL IN）
    let start = Instant::now();
    let edges_map = engine.batch_get_neighbors(user_id, query_nodes).await?;
    let optimized_total: usize = edges_map.values().map(|v| v.len()).sum();
    let optimized_duration = start.elapsed();

    println!("  ✅ Optimized (1 batch query with SQL IN):");
    println!("     Time: {:?}", optimized_duration);
    println!("     Total edges: {}", optimized_total);

    let speedup = traditional_duration.as_micros() as f64 / optimized_duration.as_micros() as f64;
    println!("  🚀 Speedup: {:.2}x", speedup);

    Ok(())
}

async fn test_cache_performance(
    engine: &MemoroseEngine,
    user_id: &str,
    nodes: &[Uuid],
) -> Result<()> {
    let test_node = nodes[0];

    // 首次查询（未缓存）
    let start = Instant::now();
    let _edges = engine.get_neighbors_cached(user_id, test_node).await?;
    let first_query = start.elapsed();

    println!("  First query (cache miss): {:?}", first_query);

    // 重复查询 10 次（已缓存）
    let start = Instant::now();
    for _ in 0..10 {
        let _edges = engine.get_neighbors_cached(user_id, test_node).await?;
    }
    let cached_total = start.elapsed();
    let cached_avg = cached_total / 10;

    println!("  Cached queries (10x avg): {:?}", cached_avg);

    let speedup = first_query.as_micros() as f64 / cached_avg.as_micros() as f64;
    println!("  🚀 Speedup: {:.2}x", speedup);

    // 显示缓存统计
    let stats = engine.query_cache_stats().await;
    println!("  📊 Cache stats: {} entries", stats.edge_cache_size);

    Ok(())
}

async fn test_multi_hop(
    engine: &MemoroseEngine,
    user_id: &str,
    nodes: &[Uuid],
) -> Result<()> {
    let start_node = nodes[0];

    // 使用批量优化的多跳遍历
    let start = Instant::now();
    let related_nodes = engine.multi_hop_traverse(
        user_id,
        vec![start_node],
        3,  // 3 跳
        Some(0.5),  // 最小权重
    ).await?;
    let duration = start.elapsed();

    println!("  ✅ Batch-optimized 3-hop traversal:");
    println!("     Time: {:?}", duration);
    println!("     Nodes found: {}", related_nodes.len());

    Ok(())
}
