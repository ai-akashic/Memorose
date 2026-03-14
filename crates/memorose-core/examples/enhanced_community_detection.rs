// 社区检测增强功能演示
//
// 展示：
// 1. 多种算法对比（LPA vs Weighted LPA vs Louvain）
// 2. 模块度评估
// 3. 批量优化的性能

use anyhow::Result;
use memorose_common::{GraphEdge, MemoryUnit, RelationType};
use memorose_core::MemoroseEngine;
use std::path::PathBuf;
use std::time::Instant;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("\n🎯 Enhanced Community Detection Demo");
    println!("=====================================\n");

    // 初始化引擎
    let data_dir = PathBuf::from("./data_community_demo");
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir)?;
    }
    let engine = MemoroseEngine::new_with_default_threshold(&data_dir, 1000, false, false).await?;

    let user_id = "demo_user";
    let app_id = "demo_app";
    let stream_id = Uuid::new_v4();

    // 构建测试图：3个明显的社区
    println!("📊 Building test graph (3 communities, ~150 nodes, ~600 edges)...");
    build_test_communities(&engine, user_id, app_id, stream_id).await?;
    println!("✅ Graph built\n");

    // === 测试 1: 基础 LPA ===
    println!("🔍 Test 1: Basic Label Propagation");
    println!("-----------------------------------");
    test_algorithm(
        &engine,
        user_id,
        memorose_core::community::Algorithm::LabelPropagation,
        "Basic LPA",
    )
    .await?;
    println!();

    // === 测试 2: 加权 LPA ===
    println!("🔍 Test 2: Weighted Label Propagation");
    println!("--------------------------------------");
    test_algorithm(
        &engine,
        user_id,
        memorose_core::community::Algorithm::WeightedLPA,
        "Weighted LPA",
    )
    .await?;
    println!();

    // === 测试 3: Louvain ===
    println!("🔍 Test 3: Louvain Algorithm");
    println!("-----------------------------");
    test_algorithm(
        &engine,
        user_id,
        memorose_core::community::Algorithm::Louvain,
        "Louvain",
    )
    .await?;
    println!();

    // === 测试 4: 两阶段检测（大图优化）===
    println!("🔍 Test 4: Two-Phase Detection (for large graphs)");
    println!("--------------------------------------------------");
    test_two_phase(&engine, user_id).await?;
    println!();

    // 清理
    std::fs::remove_dir_all(&data_dir)?;

    println!("✅ All tests completed!");
    println!("\n📊 Summary:");
    println!("  • Louvain typically provides the highest modularity");
    println!("  • Weighted LPA respects edge weights better than basic LPA");
    println!("  • Two-phase detection is recommended for graphs > 10,000 nodes");

    Ok(())
}

/// 构建测试社区图
async fn build_test_communities(
    engine: &MemoroseEngine,
    user_id: &str,
    app_id: &str,
    stream_id: Uuid,
) -> Result<()> {
    // 创建 3 个社区，每个社区 50 个节点
    let mut all_nodes = Vec::new();

    for comm in 0..3 {
        let mut community_nodes = Vec::new();

        // 创建社区内的节点
        for i in 0..50 {
            let content = format!("Community {} - Node {}", comm, i);
            let embedding = vec![(comm * 50 + i) as f32 / 150.0; 384];
            let unit = MemoryUnit::new(
                None,
                user_id.to_string(),
                None,
                app_id.to_string(),
                stream_id,
                memorose_common::MemoryType::Factual,
                content,
                Some(embedding),
            );
            community_nodes.push(unit.id);
            engine.store_memory_unit(unit).await?;
        }

        // 社区内部：强连接（权重 0.8-1.0）
        let graph = engine.graph();
        for i in 0..community_nodes.len() {
            for j in (i + 1)..community_nodes.len() {
                // 每个节点连接约 20% 的社区内节点
                if (i * 13 + j * 7) % 5 == 0 {
                    let weight = 0.8 + ((i + j) % 3) as f32 * 0.1;
                    let edge = GraphEdge::new(
                        user_id.to_string(),
                        community_nodes[i],
                        community_nodes[j],
                        RelationType::RelatedTo,
                        weight,
                    );
                    graph.add_edge(&edge).await?;
                }
            }
        }

        all_nodes.push(community_nodes);
    }

    // 社区之间：弱连接（权重 0.1-0.3）
    let graph = engine.graph();
    for i in 0..3 {
        for j in (i + 1)..3 {
            // 少量跨社区连接
            for k in 0..5 {
                let node1 = all_nodes[i][k * 10];
                let node2 = all_nodes[j][k * 10];
                let weight = 0.1 + (k as f32 * 0.04);
                let edge = GraphEdge::new(
                    user_id.to_string(),
                    node1,
                    node2,
                    RelationType::RelatedTo,
                    weight,
                );
                graph.add_edge(&edge).await?;
            }
        }
    }

    graph.flush().await?;
    Ok(())
}

/// 测试单个算法
async fn test_algorithm(
    engine: &MemoroseEngine,
    user_id: &str,
    algorithm: memorose_core::community::Algorithm,
    name: &str,
) -> Result<()> {
    let config = memorose_core::community::DetectionConfig {
        algorithm,
        max_iterations: 100,
        min_community_size: 3,
        resolution: 1.0,
    };

    let start = Instant::now();
    let result = engine.detect_communities_enhanced(user_id, config).await?;
    let duration = start.elapsed();

    println!("  Algorithm: {}", name);
    println!("  Time: {:?}", duration);
    println!("  Communities found: {}", result.num_communities);
    println!("  Modularity: {:.4}", result.modularity);

    // 显示社区大小分布
    let mut sizes: Vec<usize> = result
        .community_to_nodes
        .values()
        .map(|members| members.len())
        .collect();
    sizes.sort_by(|a, b| b.cmp(a));

    println!("  Community sizes: {:?}", &sizes[..sizes.len().min(10)]);

    Ok(())
}

/// 测试两阶段检测
async fn test_two_phase(engine: &MemoroseEngine, user_id: &str) -> Result<()> {
    let config = memorose_core::community::DetectionConfig {
        algorithm: memorose_core::community::Algorithm::Louvain,
        max_iterations: 50,
        min_community_size: 3,
        resolution: 1.0,
    };

    let start = Instant::now();
    let result = engine.detect_communities_two_phase(user_id, config).await?;
    let duration = start.elapsed();

    println!("  Algorithm: Two-Phase (LPA + Louvain)");
    println!("  Time: {:?}", duration);
    println!("  Communities found: {}", result.num_communities);
    println!("  Modularity: {:.4}", result.modularity);

    let mut sizes: Vec<usize> = result
        .community_to_nodes
        .values()
        .map(|members| members.len())
        .collect();
    sizes.sort_by(|a, b| b.cmp(a));

    println!("  Community sizes: {:?}", &sizes[..sizes.len().min(10)]);

    Ok(())
}
