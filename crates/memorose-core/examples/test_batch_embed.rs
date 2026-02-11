// Test batch embedding functionality
use memorose_core::{GeminiClient, LLMClient};

#[tokio::main]
async fn main() {
    let api_key = std::env::var("GOOGLE_API_KEY")
        .expect("GOOGLE_API_KEY not set. Run with: GOOGLE_API_KEY=... cargo run --example test_batch_embed");
    let client = GeminiClient::new(
        api_key,
        "gemini-3-flash-preview".to_string(),
        "gemini-embedding-001".to_string(),
    );

    println!("=== Testing Batch Embedding ===\n");

    let texts = vec![
        "Hello world".to_string(),
        "This is a test".to_string(),
        "Batch embedding is faster".to_string(),
        "Four test messages".to_string(),
        "Five embeddings total".to_string(),
    ];

    println!("📝 Texts to embed: {:?}\n", texts);

    // Test batch embedding
    println!("⏱️  Testing batch embedding...");
    let start = std::time::Instant::now();
    let batch_result = client.embed_batch(texts.clone()).await;
    let batch_time = start.elapsed();

    match batch_result {
        Ok(embeddings) => {
            println!("✅ Batch embedding succeeded!");
            println!("   - Time: {:?}", batch_time);
            println!("   - Count: {} embeddings", embeddings.len());
            println!("   - Dimensions: {}", embeddings[0].len());
            println!("   - First embedding (first 5 values): {:?}\n", &embeddings[0][..5]);
        }
        Err(e) => {
            println!("❌ Batch embedding failed: {}", e);
            return;
        }
    }

    // Test individual embedding for comparison
    println!("⏱️  Testing individual embeddings (for comparison)...");
    let start = std::time::Instant::now();
    let mut individual_results = Vec::new();
    for text in &texts {
        match client.embed(text).await {
            Ok(emb) => individual_results.push(emb),
            Err(e) => {
                println!("❌ Individual embedding failed: {}", e);
                return;
            }
        }
    }
    let individual_time = start.elapsed();

    println!("✅ Individual embeddings succeeded!");
    println!("   - Time: {:?}", individual_time);
    println!("   - Count: {} embeddings\n", individual_results.len());

    // Performance comparison
    let speedup = individual_time.as_secs_f64() / batch_time.as_secs_f64();
    println!("📊 Performance Comparison:");
    println!("   - Batch time:      {:?}", batch_time);
    println!("   - Individual time: {:?}", individual_time);
    println!("   - Speedup:         {:.2}x faster! 🚀", speedup);
}
