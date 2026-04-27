# Vector Index Degraded Startup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Memorose start and continue serving primary data when LanceDB is disabled or unavailable.

**Architecture:** Add vector-index configuration and status types, make `MemoroseEngine` hold an optional `VectorStore`, and update all vector read/write/maintenance call sites to degrade gracefully. RocksDB remains required; LanceDB becomes optional for Phase 1.

**Tech Stack:** Rust, Tokio, config crate, LanceDB, RocksDB, existing Memorose engine tests.

---

### Task 1: Add Vector Config

**Files:**
- Modify: `crates/memorose-common/src/config.rs`
- Test: `crates/memorose-common/src/config.rs`

- [ ] **Step 1: Write failing tests**

Add tests proving vector indexing is enabled by default and can be disabled via environment variables.

- [ ] **Step 2: Run tests**

Run: `cargo test -p memorose-common vector_config -- --nocapture`

- [ ] **Step 3: Implement config**

Add `VectorConfig` to `AppConfig` with defaults:

```rust
enabled = true
degrade_on_startup_failure = true
startup_timeout_secs = 10
io_core_reservation = Some(0)
cpu_threads = Some(1)
io_threads = Some(1)
```

- [ ] **Step 4: Run tests again**

Run: `cargo test -p memorose-common vector_config -- --nocapture`

### Task 2: Make Vector Store Optional

**Files:**
- Modify: `crates/memorose-core/src/engine/mod.rs`
- Modify: `crates/memorose-core/src/engine/search.rs`
- Modify: `crates/memorose-core/src/engine/memory_crud.rs`
- Modify: `crates/memorose-core/src/engine/organization.rs`
- Modify: `crates/memorose-core/src/engine/forgetting.rs`
- Test: `crates/memorose-core/src/engine/tests.rs`

- [ ] **Step 1: Write failing tests**

Add tests proving an engine can start with vector disabled, ingest a memory with an embedding, and run text fallback without LanceDB.

- [ ] **Step 2: Run tests**

Run: `cargo test -p memorose-core vector_disabled -- --nocapture`

- [ ] **Step 3: Implement optional vector handling**

Store vector status in the engine, skip vector writes/deletes/compaction when unavailable, and return empty vector hits from vector search paths.

- [ ] **Step 4: Run tests again**

Run: `cargo test -p memorose-core vector_disabled -- --nocapture`

### Task 3: Verify Phase 1 Build

**Files:**
- All modified Rust files

- [ ] **Step 1: Run focused tests**

Run:

```bash
cargo test -p memorose-common vector_config -- --nocapture
cargo test -p memorose-core vector_disabled -- --nocapture
```

- [ ] **Step 2: Run compile check**

Run: `cargo check -p memorose-server`

- [ ] **Step 3: Update docs if behavior changed**

If needed, update `docs/lancedb-index-resilience-design.md` with exact Phase 1 config names.

