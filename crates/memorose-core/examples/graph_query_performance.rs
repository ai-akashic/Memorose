// 性能对比示例：传统实现 vs 优化后的图查询

use memorose_core::MemoroseEngine;
use memorose_core::graph::{BatchExecutor, QueryCache, CacheConfig, CacheKey};
use memorose_common::{MemoryUnit, GraphEdge, RelationType};
use uuid::Uuid;
use std::time::Instant;
use anyhow::Result;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("🚀 Graph Query Performance Comparison");
    println!("=====================================\n");

    // 初始化引擎
    let data_dir = PathBuf::from("./data_perf_test");
    if data_dir.exists() { std::fs::remove_dir_all(&data_dir)?; }
    let engine = MemoroseEngine::new(&data_dir, 1000, true, true).await?;

    let user_id = "perf_test_user";
    let app_id = "perf_test_app";
    let stream_id = Uuid::new_v4();

    // 构建测试图：100 个节点，平均每个节点 5 条边
    println!("📊 Building test graph (100 nodes, ~500 edges)...");
    let nodes = build_test_graph(&engine, user_id, app_id, stream_id, 100, 5).await?;
    println!("✅ Graph built\n");

    // === 测试 1: 单节点邻居查询 ===
    println!("🔍 Test 1: Single-hop neighbor query");
    println!("-------------------------------------");

    test_single_hop_query(&engine, user_id, &nodes).await?;
    println!();

    // === 测试 2: 多跳遍历 ===
    println!("🔍 Test 2: Multi-hop traversal (2 hops)");
    println!("----------------------------------------");

    test_multi_hop_traversal(&engine, user_id, &nodes).await?;
    println!();

    // === 测试 3: 批量查询 ===
    println!("🔍 Test 3: Batch query (20 nodes)");
    println!("----------------------------------");

    test_batch_query(&engine, user_id, &nodes).await?;
    println!();

    // === 测试 4: 缓存效果 ===
    println!("🔍 Test 4: Cache performance");
    println!("-----------------------------");

    test_cache_performance(&engine, user_id, &nodes).await?;
    println!();

    // 清理
    std::fs::remove_dir_all(&data_dir)?;

    println!("✅ All tests completed!");
    Ok(())
}

/// 构建测试图
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

    // 创建边（随机连接）
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

    graph.flush().await?;  // 确保数据写入
    Ok(node_ids)
}

/// 测试 1: 单节点邻居查询
async fn test_single_hop_query(
    engine: &MemoroseEngine,
    user_id: &str,
    nodes: &[Uuid],
) -> Result<()> {
    let graph = engine.graph();
    let test_node = nodes[0];

    // 传统方式
    let start = Instant::now();
    let edges = graph.get_outgoing_edges(user_id, test_node).await?;
    let duration = start.elapsed();

    println!("  Traditional query: {:?} ({} edges)", duration, edges.len());
    println!("  ✅ Fast path - already optimized");

    Ok(())
}

/// 测试 2: 多跳遍历对比
async fn test_multi_hop_traversal(
    engine: &MemoroseEngine,
    user_id: &str,
    nodes: &[Uuid],
) -> Result<()> {
    let graph = engine.graph();
    let start_node = nodes[0];

    // ❌ 传统方式：手动循环
    let start = Instant::now();
    let mut visited = std::collections::HashSet::new();
    visited.insert(start_node);

    // Hop 1
    let hop1_edges = graph.get_outgoing_edges(user_id, start_node).await?;
    for edge in &hop1_edges {
        visited.insert(edge.target_id);
    }

    // Hop 2
    for edge in hop1_edges {
        let hop2_edges = graph.get_outgoing_edges(user_id, edge.target_id).await?;
        for e2 in hop2_edges {
            visited.insert(e2.target_id);
        }
    }
    let traditional_duration = start.elapsed();
    let traditional_count = visited.len();

    println!("  ❌ Traditional (sequential):");
    println!("     Time: {:?}", traditional_duration);
    println!("     Nodes found: {}", traditional_count);

    // ✅ 优化方式：批量执行
    let executor = BatchExecutor::new(graph.clone());
    let start = Instant::now();
    let optimized_nodes = executor.batch_multi_hop_traverse(
        user_id,
        vec![start_node],
        2,
        None,
    ).await?;
    let optimized_duration = start.elapsed();

    println!("  ✅ Optimized (batched):");
    println!("     Time: {:?}", optimized_duration);
    println!("     Nodes found: {}", optimized_nodes.len());

    let speedup = traditional_duration.as_micros() as f64 / optimized_duration.as_micros() as f64;
    println!("  🚀 Speedup: {:.2}x", speedup);

    Ok(())
}

/// 测试 3: 批量查询对比
async fn test_batch_query(
    engine: &MemoroseEngine,
    user_id: &str,
    nodes: &[Uuid],
) -> Result<()> {
    let graph = engine.graph();
    let query_nodes = &nodes[0..20];  // 查询前 20 个节点

    // ❌ 传统方式：逐个查询
    let start = Instant::now();
    let mut total_edges = 0;
    for node_id in query_nodes {
        let edges = graph.get_outgoing_edges(user_id, *node_id).await?;
        total_edges += edges.len();
    }
    let traditional_duration = start.elapsed();

    println!("  ❌ Traditional (20 sequential queries):");
    println!("     Time: {:?}", traditional_duration);
    println!("     Total edges: {}", total_edges);

    // ✅ 优化方式：批量查询
    let executor = BatchExecutor::new(graph.clone());
    let start = Instant::now();
    let edges_map = executor.batch_get_outgoing_edges(user_id, query_nodes).await?;
    let optimized_total: usize = edges_map.values().map(|v| v.len()).sum();
    let optimized_duration = start.elapsed();

    println!("  ✅ Optimized (1 batch query):");
    println!("     Time: {:?}", optimized_duration);
    println!("     Total edges: {}", optimized_total);

    let speedup = traditional_duration.as_micros() as f64 / optimized_duration.as_micros() as f64;
    println!("  🚀 Speedup: {:.2}x", speedup);

    Ok(())
}

/// 测试 4: 缓存效果
async fn test_cache_performance(
    engine: &MemoroseEngine,
    user_id: &str,
    nodes: &[Uuid],
) -> Result<()> {
    use memorose_core::graph::cache::Direction;

    let cache = QueryCache::new(CacheConfig::default());
    let graph = engine.graph();
    let test_node = nodes[0];

    // 首次查询（未缓存）
    let key = CacheKey::OneHopNeighbors {
        user_id: user_id.to_string(),
        node_id: test_node,
        direction: Direction::Outgoing,
    };

    let start = Instant::now();
    let edges = if let Some(cached) = cache.get_edges(&key).await {
        cached
    } else {
        let edges = graph.get_outgoing_edges(user_id, test_node).await?;
        cache.put_edges(key.clone(), edges.clone()).await;
        edges
    };
    let first_query = start.elapsed();

    println!("  First query (cache miss): {:?}", first_query);

    // 重复查询（已缓存）
    let start = Instant::now();
    let _cached_edges = cache.get_edges(&key).await.unwrap();
    let cached_query = start.elapsed();

    println!("  Second query (cache hit): {:?}", cached_query);

    let speedup = first_query.as_micros() as f64 / cached_query.as_micros() as f64;
    println!("  🚀 Speedup: {:.2}x", speedup);

    Ok(())
}
