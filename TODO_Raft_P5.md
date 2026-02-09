# Raft Phase 5: Snapshotting & Hardening TODO

- [x] Task 1: Implement Raft Snapshotting
  - [x] Implement `build_snapshot` in `storage.rs` (Export RocksDB/LanceDB state)
  - [x] Implement `install_snapshot` in `storage.rs` (Restore from binary stream)
- [x] Task 2: Smart Client Implementation
  - [x] Update `http_client.py` to handle `status: redirect` and 503 Not Leader
- [x] Task 3: Cluster Management API
  - [x] Implement `POST /v1/cluster/join` to add nodes dynamically
