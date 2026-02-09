# Memorose Examples

This directory contains examples of how to interact with Memorose using different languages and interfaces.

## 🐍 Python Examples (`/python`)
These scripts use the `requests` library to interact with the Memorose REST API.

*   **`http_client.py`**: A smart client demonstrating automatic leader redirection and basic CRUD operations.
*   **`multimodal_test.py`**: Demonstrates image-to-text (Vision) capabilities.
*   **`stt_test.py`**: Demonstrates speech-to-text (STT) capabilities.
*   **`bench_level_1.py`**: Basic performance benchmarking for L1 memories.

**Run:**
```bash
cd examples/python
python3 http_client.py
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
