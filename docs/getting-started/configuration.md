---
title: Configuration
---

# Configuration

Memorose reads configuration from `config.toml` plus environment variables. The current config surface is broader than the earlier docs versions and should be treated as infrastructure configuration, not just storage tuning.

## Main Files

- `config.toml`: runtime and storage behavior
- `.env`: provider keys and environment-specific overrides
- `.env.example`: current env variable reference
- `config.example.toml`: current config shape reference

## Current Top-Level Sections

```toml
[server]
[database]
[database.rocksdb]
[database.lance]
[database.tantivy]
[consolidation]
[consolidation.llm]
[forgetting]
[graph]
[raft]
[cache]
[multimodal]
[security]
[telemetry]
[development]
```

## Minimal Example

```toml
[server]
host = "0.0.0.0"
port = 3000

[database]
data_dir = "./data"
wal_sync_mode = "normal"

[consolidation]
enabled = true
interval_secs = 10
batch_size = 100
entropy_threshold = 2.5
similarity_threshold = 0.7

[consolidation.llm]
provider = "gemini"
model = "gemini-3.1-flash-lite-preview"
embedding_model = "gemini-embedding-2-preview"
max_retries = 3
timeout_secs = 30
temperature = 0.3

[forgetting]
enabled = true
decay_half_life_days = 30
min_importance = 0.1
prune_interval_secs = 3600

[graph]
auto_link_threshold = 0.7
max_edges_per_node = 100
enable_page_rank = true
enable_community_detection = true

[raft]
node_id = "node1"
raft_addr = "127.0.0.1:5001"
```

## Environment Variables

Common environment variables from `.env.example`:

```bash
LLM_PROVIDER=gemini
GOOGLE_API_KEY=your_google_api_key_here
LLM_MODEL=gemini-3.1-flash-lite-preview
EMBEDDING_MODEL=gemini-embedding-2-preview
MEMOROSE_WORKER__CONSOLIDATION_INTERVAL_MS=1000
MEMOROSE_WORKER__LLM_CONCURRENCY=5
MEMOROSE_WORKER__AUTO_LINK_SIMILARITY_THRESHOLD=0.6
```

Use `DASHBOARD_ADMIN_PASSWORD` to replace the default dashboard password.

## What Each Section Controls

- `[server]`: bind address, port, worker count
- `[database]`: on-disk layout and engine-specific tuning
- `[consolidation]`: L0 to L1 extraction cadence and acceptance thresholds
- `[forgetting]`: decay and pruning of low-value memory
- `[graph]`: auto-linking and community detection
- `[raft]`: clustering, election timing, membership, snapshots
- `[cache]`: cached query behavior
- `[multimodal]`: image, audio, and video processing toggles
- `[security]`: auth and rate limiting
- `[telemetry]`: logs, metrics, tracing
- `[development]`: mock LLMs, debug settings, test determinism

## Guidance

- Use `config.example.toml` as the canonical template.
- Use `.env.example` as the canonical provider and worker override reference.
- Avoid older docs examples that mention `[storage]`, `[dashboard]`, or `/health`; those reflect a previous API/config model.
