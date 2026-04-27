# LanceDB Index Resilience Design

Date: 2026-04-27

## Background

The current production incident showed that a data directory can contain a very small
primary store and a very large vector index:

```text
70M   node-1/rocksdb
140K  node-1/tantivy
15G   node-1/lancedb
```

An empty data directory starts correctly, while the old data directory is killed by
the container runtime with:

```text
OOMKilled=true
ExitCode=137
```

The only visible application log before the kill is emitted by Lance:

```text
Number of CPUs is less than or equal to the number of IO core reservations...
```

This means the current failure is not a Rust panic and is not directly explained by
background forgetting. The process is being killed by the kernel or container runtime
because memory usage exceeds the available limit while opening or initializing the
existing LanceDB data.

The larger design issue is that LanceDB is currently treated as a required startup
dependency even though it is a derived index. It backs both vector search and graph
relationships in the current codebase. When the derived Lance-backed index becomes
large, corrupt, fragmented, or expensive to open, it can prevent the whole service
from starting even though the primary RocksDB data is still available.

## Goals

1. RocksDB remains the source of truth for memory units and system state.
2. LanceDB is treated as a derived vector/graph index that can be disabled, rebuilt,
   or replaced without losing primary data.
3. Tantivy remains a derived text index and follows the same resilience principle.
4. A bad or oversized LanceDB directory must not prevent the API server from starting.
5. Rebuild, repair, and compaction workflows must be explicit, resumable, and bounded
   in memory.
6. Vector index storage must avoid duplicating large MemoryUnit fields that can be
   fetched from RocksDB by ID.
7. Startup must avoid heavy full-data scans and must have clear degraded-mode telemetry.

## Non-Goals

1. Do not change the logical memory model.
2. Do not make LanceDB the source of truth.
3. Do not delete user data automatically during startup.
4. Do not require a larger server as the primary fix.
5. Do not implement a full data migration framework before adding emergency recovery.

## Current Design Problems

### LanceDB is a hard startup dependency

`MemoroseEngine::new_with_storage_config` opens LanceDB during engine initialization. If
this step is slow, memory-heavy, or fails, server startup fails.

Current behavior:

```text
open RocksDB
open LanceDB
open graph store on LanceDB connection
open Tantivy
start server
```

Desired behavior:

```text
open RocksDB
try open LanceDB with timeout and safety settings
if LanceDB fails, mark vector index degraded
open Tantivy with the same resilience pattern
start server
repair or rebuild derived indexes in the background or via explicit command
```

### LanceDB compaction must be real

The original `VectorStore::compact_files` implementation only logged that compaction
was skipped. The worker called the compaction cycle, but the call did not reclaim space
or reduce fragmentation.

After upgrading to `lancedb = "=0.27.2"`, `compact_files` delegates to
`Table::optimize(OptimizeAction::All)`, which runs file compaction, safe old-version
pruning, and index optimization. This makes routine maintenance real, while explicit
`vector-rebuild` remains the recovery path for very large or corrupted indexes.

### Deletes do not guarantee physical reclamation

Vector delete calls remove matching IDs logically, but columnar/versioned stores often
retain old fragments, versions, or index files until compaction or cleanup occurs.
Without a real vacuum or rebuild path, disk growth is expected.

### LanceDB stores too much duplicated metadata

The current vector table stores fields such as `content`, `user_id`, `namespace_key`,
`domain`, timestamps, and the vector. Some metadata is needed for filtering, but large
or complex fields should not be duplicated when RocksDB already stores the full
`MemoryUnit`.

### No repair or rebuild entry point

The current operational workaround is to rename `lancedb` manually. The product should
provide a supported flow:

```bash
memorose-server repair vector-rebuild --data-dir /app/data
memorose-server repair vector-status --data-dir /app/data
memorose repair vector-disable --data-dir /app/data
```

### Background tasks can still create memory spikes

The forgetting path has a full `decoded_units.collect::<Vec<_>>()` in pruning. There is
also a worker path that scans `u:` keys to discover users. These are separate from the
confirmed startup OOM, but they follow the same anti-pattern: full materialization when
bounded or streaming processing is safer.

## Recommended Architecture

### Storage roles

```text
RocksDB
  Role: primary durable source of truth
  Contains: events, memory units, system state, indexes needed for recovery
  Startup: required

LanceDB
  Role: derived vector search index
  Contains: minimal vector rows for semantic retrieval
  Startup: optional/degradable

Tantivy
  Role: derived text search index
  Contains: text index for keyword and hybrid retrieval
  Startup: optional/degradable
```

### Engine representation

Introduce explicit index status instead of assuming all indexes are available.

```rust
pub enum DerivedIndexStatus {
    Available,
    DisabledByConfig,
    Degraded { reason: String },
    Rebuilding { progress: Option<RebuildProgress> },
}

pub enum VectorIndex {
    Available(VectorStore),
    Unavailable { status: DerivedIndexStatus },
}
```

The engine should expose:

```rust
engine.vector_status() -> DerivedIndexStatus
engine.text_status() -> DerivedIndexStatus
engine.health_report() -> StorageHealthReport
```

### Startup behavior

Startup should follow this sequence:

1. Load config.
2. Open RocksDB.
3. Try to open LanceDB if the Lance-backed vector/graph index is enabled.
4. Apply a startup timeout to LanceDB initialization.
5. If LanceDB fails or times out and degraded startup is enabled, continue with
   vector search and graph expansion unavailable.
6. Open Tantivy using the same degraded-index pattern.
7. Start API routes.
8. Start background workers only after storage status is known.

If RocksDB fails to open, startup should fail. If LanceDB fails to open, startup should
not fail when `degrade_on_startup_failure` is enabled.

### Config

Add a new vector config section:

```toml
[vector]
enabled = true
degrade_on_startup_failure = true
startup_timeout_secs = 10
rebuild_on_missing = false
rebuild_batch_size = 128
max_index_size_gb = 5
io_threads = 1
cpu_threads = 1
io_core_reservation = 0
schema_version = 2
```

Environment variables:

```bash
MEMOROSE__VECTOR__ENABLED=false
MEMOROSE__VECTOR__DEGRADE_ON_STARTUP_FAILURE=true
MEMOROSE__VECTOR__STARTUP_TIMEOUT_SECS=10
MEMOROSE__VECTOR__REBUILD_ON_MISSING=false
MEMOROSE__VECTOR__REBUILD_BATCH_SIZE=128
MEMOROSE__VECTOR__MAX_INDEX_SIZE_GB=5
MEMOROSE__VECTOR__SCHEMA_VERSION=2
MEMOROSE__VECTOR__IO_THREADS=1
MEMOROSE__VECTOR__CPU_THREADS=1
MEMOROSE__VECTOR__IO_CORE_RESERVATION=0
```

At process startup, map the vector runtime settings to Lance environment variables
before opening LanceDB:

```text
LANCE_IO_CORE_RESERVATION
LANCE_CPU_THREADS
LANCE_IO_THREADS
```

## Slim Vector Schema

LanceDB should keep only fields needed to search, filter, and resolve rows back to the
primary store.

Recommended schema:

```text
id                 string, required
user_id            string, required
org_id             string, nullable
agent_id           string, nullable
domain             string, required
namespace_key      string, required
level              uint8, required
transaction_time   timestamp, required
valid_time         timestamp, nullable
vector             fixed_size_list<float32>, required
```

Fields to remove from LanceDB:

```text
content
keywords
references
assets
extracted_facts
task_metadata
full MemoryUnit JSON
```

Search flow after slimming:

```text
1. LanceDB vector search returns top K ids plus lightweight filter metadata.
2. Engine performs RocksDB multi_get for those ids.
3. Engine applies final visibility, bitemporal, and policy checks.
4. Engine returns full MemoryUnit results.
```

This adds a RocksDB lookup after vector search. For normal `top_k <= 100`, this cost is
small compared with the stability and disk-size benefits.

## Rebuild Workflow

Add a repair command:

```bash
memorose-server repair vector-rebuild --data-dir /app/data
```

Implementation behavior:

1. Refuse to run if another rebuild lock exists, unless `--force` is supplied.
2. Open RocksDB as the source of truth.
3. Create a fresh `lancedb.rebuilding` directory.
4. Scan memory units in bounded batches.
5. Only index units where `embedding.is_some()`.
6. Write batches to the new LanceDB table.
7. Store rebuild progress in system metadata.
8. Validate row count and schema version.
9. Atomically replace directories:

```text
lancedb -> lancedb.backup.TIMESTAMP
lancedb.rebuilding -> lancedb
```

10. Keep the previous LanceDB backup until an operator removes it.

The rebuild command must not load all units or embeddings into memory at once.

Recommended defaults:

```text
batch size: 128
max in-memory decoded units: 128
progress checkpoint interval: every 1,000 units
```

## Rebuild and Optimize

With LanceDB 0.27.2, routine maintenance should use `Table::optimize` through
`VectorStore::compact_files`. Rebuild remains the supported recovery and schema
migration strategy because it can reconstruct LanceDB from RocksDB without opening an
oversized or corrupted old index.

Trigger conditions:

```text
lancedb size > vector.max_index_size_gb
lancedb size / rocksdb size > configured ratio
fragment count above threshold
schema version mismatch
operator explicitly runs repair vector-rebuild
```

Automatic rebuild should be disabled by default for production until the flow is proven.
The first version should provide explicit repair commands and status endpoints.

## Degraded Mode Behavior

When the Lance-backed vector/graph index is unavailable:

1. Ingest still stores events and MemoryUnit data in RocksDB.
2. Text index continues if available.
3. Retrieval falls back to text search and recent-memory retrieval.
4. API health reports vector status as degraded.
5. Dashboard shows a clear warning instead of failing.
6. Background vector writes are skipped or queued depending on config.

Search behavior:

```text
Available vector index:
  vector search + text search + graph/context expansion

Unavailable vector index:
  text search + recent L1 + RocksDB fallback
```

In degraded mode, semantic search quality is reduced, but the service remains usable.

## Operational Commands

Recommended command surface:

```bash
memorose-server repair vector-status --data-dir /app/data
memorose-server repair vector-rebuild --data-dir /app/data
memorose repair vector-disable --data-dir /app/data
memorose repair vector-enable --data-dir /app/data
memorose repair text-rebuild --data-dir /app/data
```

Phase 2 implements the first two commands on the existing `memorose-server` binary.
`vector-status` intentionally does not open LanceDB by default, because opening the
oversized index can reproduce the startup OOM. Use `--open-lancedb` only when the
operator wants row-count verification and the machine has enough memory:

```bash
memorose-server repair vector-status --data-dir /app/data
memorose-server repair vector-status --data-dir /app/data --open-lancedb
```

`vector-rebuild` reads MemoryUnit records from RocksDB in bounded batches, indexes only
units with embeddings, writes a fresh `lancedb.rebuilding.TIMESTAMP`, moves the old
`lancedb` to `lancedb.backup.TIMESTAMP`, and then moves the rebuilt directory into
place:

```bash
memorose-server repair vector-rebuild --data-dir /app/data
memorose-server repair vector-rebuild --data-dir /app/data --batch-size 128
```

`vector-status` should report:

```text
status
expected schema version
actual schema version when --open-lancedb is used
schema status and columns when --open-lancedb is used
directory size
configured max index size
whether directory size exceeds the configured max index size
table row count
RocksDB units with embeddings
last rebuild time
last rebuild error
fragment count when available
recommended action
```

## HTTP Health and Dashboard

Add storage index health to the existing health/status surfaces:

```json
{
  "storage": {
    "rocksdb": { "status": "available" },
    "vector": {
      "status": "degraded",
      "reason": "startup timeout opening LanceDB",
      "can_rebuild": true
    },
    "text": { "status": "available" }
  }
}
```

Dashboard should show:

```text
Vector search degraded. Primary memory data is safe. Rebuild vector index to restore semantic retrieval.
```

## Background Task Safety

The same design principle should be applied to maintenance jobs:

1. No full-user or full-database decoded `Vec<MemoryUnit>` unless the operation has a
   hard cap.
2. Use two-pass or streaming scans for pruning.
3. Use key-only scans when only user IDs are needed.
4. Add batch size and memory budget config for rebuild, pruning, and maintenance tasks.
5. Avoid running expensive maintenance immediately on startup.

Specific follow-ups:

1. Replace `prune_memories` full decode collection with bounded two-pass processing.
2. Replace `run_l3_task_cycle` full `kv.scan(b"u:")` value loading with an active-user
   marker scan or key-only scan.
3. Add `KvStore::scan_keys` and streaming iterator helpers.

## Migration Strategy

### Phase 1: Emergency resilience

Deliverables:

1. `MEMOROSE__VECTOR__ENABLED=false`.
2. LanceDB startup failure becomes degraded mode when configured.
3. Search and ingest handle missing vector index.
4. Health endpoint reports vector status.

This phase prevents old or oversized LanceDB directories from blocking service startup.

### Phase 2: Rebuild and repair

Deliverables:

1. `memorose-server repair vector-status`.
2. `memorose-server repair vector-rebuild`.
3. Atomic directory replacement.
4. Bounded batch rebuild from RocksDB.
5. Operational docs for restoring service from an oversized LanceDB directory.

This phase makes LanceDB disposable and recoverable.

### Phase 3: Slim schema

Deliverables:

1. New vector schema version.
2. Rebuild writes slim rows.
3. Search performs vector lookup followed by RocksDB `multi_get`.
4. Old schema detection recommends rebuild.
5. Optional compatibility read path during transition.

Implementation status:

1. The active LanceDB `memories` table schema is slimmed to:
   `id`, `user_id`, `org_id`, `agent_id`, `domain`, `namespace_key`, `level`,
   `transaction_time`, `valid_time`, and `vector`.
2. `content`, `stream_id`, and `memory_type` are no longer stored in LanceDB. Full
   `MemoryUnit` data remains in RocksDB and is fetched by ID after vector search.
3. Procedural search no longer requires a LanceDB `memory_type` filter; it applies the
   procedural filter after resolving rows from RocksDB.
4. The repair rebuild command writes the slim schema because it uses the same
   `VectorStore` table creation path.
5. The active schema version is `2`. `vector-status --open-lancedb` reports the
   detected schema version/status and recommends `vector-rebuild` when the table is
   not current.

This phase reduces long-term disk growth and startup pressure.

### Phase 4: Maintenance hardening

Deliverables:

1. Rebuild-as-compaction policy.
2. Size and ratio monitoring.
3. Background maintenance rate limits.
4. Pruning and user-discovery scan fixes.

Implementation status:

1. `prune_memories` no longer fully materializes all decoded user memories. It scans
   RocksDB in bounded pages, first collecting referenced L1 IDs, then deleting pruned
   units in bounded batches.
2. L3 task worker user discovery now scans RocksDB keys only in bounded pages instead of
   loading all `u:` values.
3. `VectorStore::compact_files` now uses LanceDB 0.27.2 `Table::optimize` to run
   compaction, safe old-version pruning, and index optimization.
4. Size/ratio monitoring beyond the configured max-index-size threshold is still
   pending.

This phase prevents the same class of problem from returning.

## Testing Plan

### Unit tests

1. Engine starts when vector indexing is disabled.
2. Engine starts in degraded mode when LanceDB open fails.
3. Ingest succeeds without vector index.
4. Vector search falls back when vector index is unavailable.
5. Slim vector rows can be resolved back to RocksDB MemoryUnit records.
6. Rebuild batches do not exceed configured batch size.

### Integration tests

1. Create memory units with embeddings, rebuild vector index, search successfully.
2. Rename/delete LanceDB directory, start service, verify degraded mode.
3. Rebuild from RocksDB and verify vector status returns to available.
4. Simulate schema mismatch and verify rebuild recommendation.
5. Verify old LanceDB backup is preserved after rebuild.

### Operational tests

1. Start with empty data directory.
2. Start with old data directory and disabled vector index.
3. Start with intentionally invalid LanceDB directory.
4. Run rebuild on a large fixture under constrained memory.
5. Verify dashboard and health endpoints surface the degraded status clearly.

## Rollback Plan

1. Phase 1 is mostly additive. If degraded mode causes issues, set
   `MEMOROSE__VECTOR__ENABLED=true` and `MEMOROSE__VECTOR__DEGRADE_ON_STARTUP_FAILURE=false`
   to restore strict behavior.
2. Rebuild does not delete the old LanceDB directory. It moves it to
   `lancedb.backup.TIMESTAMP`, so rollback is a directory rename.
3. Slim schema rollout should be gated by schema version. If a problem is found, keep
   using the previous LanceDB backup and disable slim-schema rebuild.

## Success Criteria

1. A data directory with a broken or oversized LanceDB can still start the API server.
2. Operators can restore vector search without manually deleting data.
3. LanceDB size no longer grows without an explicit repair or compaction path.
4. Vector index failure is visible in health checks and dashboard.
5. Primary memory data remains accessible as long as RocksDB is healthy.

## Immediate Production Runbook

For the current incident:

0. Stop the running Memorose container before running repair commands. RocksDB is opened
   exclusively, and replacing LanceDB while the server is running is not supported.

1. Check the primary data and LanceDB size without opening LanceDB:

```bash
sudo docker run --rm \
  -v /root/memorose_data/node-1:/app/data \
  dylan2024/memorose:latest \
  /app/memorose-server repair vector-status --data-dir /app/data
```

2. Rebuild LanceDB from RocksDB without opening the old LanceDB:

```bash
sudo docker run --rm \
  -v /root/memorose_data/node-1:/app/data \
  dylan2024/memorose:latest \
  /app/memorose-server repair vector-rebuild --data-dir /app/data --batch-size 128
```

3. If the deployed image does not have repair commands yet, preserve the old LanceDB
   directory manually:

```bash
sudo mv /root/memorose_data/node-1/lancedb \
  /root/memorose_data/node-1/lancedb.disabled
```

4. Start with vector disabled or constrained Lance settings:

```bash
sudo docker run --rm -p 3000:3000 -p 3100:3100 \
  -v /root/memorose_data/node-1:/app/data \
  -e GOOGLE_API_KEY=xxx \
  -e MEMOROSE__LLM__MODEL=gemini-3.1-flash-lite-preview \
  -e MEMOROSE__LLM__EMBEDDING_MODEL=gemini-embedding-2-preview \
  -e MEMOROSE__VECTOR__ENABLED=false \
  -e LANCE_IO_CORE_RESERVATION=0 \
  -e LANCE_CPU_THREADS=1 \
  -e LANCE_IO_THREADS=1 \
  dylan2024/memorose:latest
```

5. Keep `rocksdb` unchanged.
