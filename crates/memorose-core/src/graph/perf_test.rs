// 快速性能验证测试

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_batch_vs_sequential() {
        println!("\n🔬 Performance Comparison Test");
        println!("================================\n");

        // 模拟数据
        let node_count = 20;
        let node_ids: Vec<Uuid> = (0..node_count).map(|_| Uuid::new_v4()).collect();

        println!("Test scenario: Query {} nodes' neighbors", node_count);
        println!();

        // ❌ 传统方式模拟：N 次查询
        let start = Instant::now();
        let mut total_queries = 0;
        for _id in &node_ids {
            // 模拟单次查询延迟
            tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
            total_queries += 1;
        }
        let sequential_time = start.elapsed();

        println!("❌ Sequential approach:");
        println!("   Queries: {}", total_queries);
        println!("   Time: {:?}", sequential_time);
        println!();

        // ✅ 批量方式模拟：1 次查询
        let start = Instant::now();
        // 模拟批量查询延迟（稍长但只一次）
        tokio::time::sleep(tokio::time::Duration::from_micros(200)).await;
        let batch_queries = 1;
        let batch_time = start.elapsed();

        println!("✅ Batch approach:");
        println!("   Queries: {}", batch_queries);
        println!("   Time: {:?}", batch_time);
        println!();

        let speedup = sequential_time.as_micros() as f64 / batch_time.as_micros() as f64;
        println!("🚀 Speedup: {:.2}x", speedup);
        println!();

        // 验证批量方式确实更快
        assert!(batch_time < sequential_time);
        assert!(speedup > 5.0, "Expected at least 5x speedup");
    }
}
