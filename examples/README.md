# Memorose Examples

This directory contains examples of how to interact with Memorose using different languages and interfaces.

## 🐍 Python Examples (`/python`)
These scripts use the `requests` library to interact with the Memorose REST API.

*   **`infinite_memory_agent.py`**: 🌟 **Start Here!** A highly interactive, colorful terminal demo simulating an AI agent with infinite memory. Demonstrates L0 (Working Memory) saves and L1/L2 context recalls.
*   **`http_client.py`**: A smart client demonstrating automatic leader redirection and basic CRUD operations.
*   **`multimodal_test.py`**: Demonstrates image-to-text (Vision) capabilities.
*   **`stt_test.py`**: Demonstrates speech-to-text (STT) capabilities.
*   **`bench_level_1.py`**: Basic performance benchmarking for L1 memories.

**Run the Cool Demo:**
```bash
cd python
python3 infinite_memory_agent.py
```

## 🦀 Rust Examples (`/crates/memorose-core/examples`)
These are internal examples that use the `memorose-core` crate directly.

*   **`raft_cluster.rs`**: Demonstrates setting up a distributed Raft cluster.
*   **`graph_analysis.rs`**: Demonstrates knowledge graph traversal and community detection.
*   **`basic_operations.rs`**: Standard KV and Vector operations.

**Run:**
```bash
cd crates/memorose-core
cargo run --example raft_cluster
```
