<div align="center">
  <br />
  <a href="https://memorose.io">
    <img src=".github/assets/logo-512.png" alt="Memorose" width="160" />
  </a>
  <h1>Memorose</h1>
  <p><b>The open-source memory layer for AI agents.</b></p>
  <p>Give your agents persistent, structured memory that learns, connects, and forgets — just like humans do.</p>
  <br />
  <p>
    <a href="https://memorose.io/docs"><b>Documentation</b></a> &nbsp;&bull;&nbsp;
    <a href="https://memorose.io"><b>Website</b></a> &nbsp;&bull;&nbsp;
    <a href="https://github.com/ai-akashic/Memorose/issues"><b>Issues</b></a> &nbsp;&bull;&nbsp;
    <a href="https://discord.gg/memorose"><b>Discord</b></a>
  </p>
  <p>
    <a href="https://github.com/ai-akashic/Memorose/stargazers"><img src="https://img.shields.io/github/stars/ai-akashic/Memorose?style=flat&color=yellow" alt="Stars" /></a>
    <a href="https://github.com/ai-akashic/Memorose/releases"><img src="https://img.shields.io/github/v/release/ai-akashic/Memorose?style=flat&color=blue" alt="Release" /></a>
    <a href="https://github.com/ai-akashic/Memorose/blob/main/LICENSE"><img src="https://img.shields.io/github/license/ai-akashic/Memorose?style=flat" alt="License" /></a>
    <img src="https://img.shields.io/badge/language-Rust-orange?style=flat&logo=rust" alt="Rust" />
    <a href="https://github.com/ai-akashic/Memorose/commits/main"><img src="https://img.shields.io/github/commit-activity/m/ai-akashic/Memorose?style=flat&color=green" alt="Commits" /></a>
  </p>
  <br />
</div>

<!-- GIF demo placeholder — replace with actual recording -->
<!-- <p align="center"><img src=".github/assets/demo.gif" alt="Memorose Demo" width="720" /></p> -->

---

## The Problem

AI agents are **goldfish**. Every session starts from zero. RAG retrieves text chunks, but it doesn't understand what matters, what changed, or what to forget. Your agents deserve a brain, not a filing cabinet.

**Memorose** is a cognitive memory engine that:

- **Compresses** verbose conversations into dense factual/procedural memories
- **Connects** related memories into a traversable knowledge graph
- **Reflects** on interaction patterns and generates higher-order insights
- **Forgets** stale information through importance decay — just like humans
- **Embeds** text, images, audio, and video natively in a unified vector space

One binary. Sub-10ms retrieval. Zero Python dependencies.

---

<details>
<summary><b>Table of Contents</b></summary>

- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Multi-Dimensional Memory](#multi-dimensional-memory)
- [Six Cognitive Operations](#six-cognitive-operations)
- [Native Multimodal Embedding](#native-multimodal-embedding)
- [Feature Comparison](#feature-comparison)
- [Performance](#performance)
- [Architecture](#architecture)
- [Dashboard](#dashboard)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

</details>

---

## Quick Start

### Option 1: Docker (recommended)

```bash
docker run -d -p 3000:3000 \
  -e GOOGLE_API_KEY=your_key \
  -e MEMOROSE__LLM__MODEL=gemini-2.0-flash \
  -e MEMOROSE__LLM__EMBEDDING_MODEL=gemini-embedding-2-preview \
  akashic/memorose:latest
```

### Option 2: Build from source

```bash
git clone https://github.com/ai-akashic/Memorose.git
cd Memorose
cargo build --release
./target/release/memorose-server
```

### Store a memory

```bash
export STREAM=$(uuidgen)

# Your agent observes something
curl -s -X POST http://localhost:3000/v1/users/dylan/apps/assistant/streams/$STREAM/events \
  -H "Content-Type: application/json" \
  -d '{"content": "I prefer Rust over Python. I hate unnecessary meetings. My dog is named Rosie."}'
```

### Retrieve with context

```bash
# Later, in a new session — the agent remembers
curl -s -X POST http://localhost:3000/v1/users/dylan/apps/assistant/streams/$STREAM/retrieve \
  -H "Content-Type: application/json" \
  -d '{"query": "What should I keep in mind when working with Dylan?"}'
```

```json
{
  "results": [
    ["Dylan prefers Rust, dislikes unnecessary meetings, has a dog named Rosie", 0.94]
  ]
}
```

### Cross-modal retrieval

```bash
# Find memories related to an image — no text conversion needed
curl -s -X POST http://localhost:3000/v1/users/dylan/apps/assistant/streams/$STREAM/retrieve \
  -H "Content-Type: application/json" \
  -d '{"query": "what is this?", "image": "'$(base64 -i photo.jpg)'"}'
```

---

## How It Works

Memorose processes memories through a 4-tier cognitive pipeline, modeled after human memory consolidation:

```
  Event (text/image/audio/video/json)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  L0  Working Memory                                 │
│  Raw event log. Append-only. Zero processing.       │
│  ► RocksDB                                          │
└──────────────────────┬──────────────────────────────┘
                       │  Background workers (async)
                       ▼
┌─────────────────────────────────────────────────────┐
│  L1  Episodic Memory                                │
│  Compressed summaries. Vectorized. Auto-linked.     │
│  ► RocksDB + LanceDB + Tantivy                     │
│                                                     │
│  Operations: Compress ─► Embed ─► Associate         │
└──────────────────────┬──────────────────────────────┘
                       │  Community detection + LLM synthesis
                       ▼
┌─────────────────────────────────────────────────────┐
│  L2  Semantic Memory                                │
│  Abstract knowledge clusters. Cross-session insight.│
│  ► Knowledge Graph                                  │
│                                                     │
│  Operations: Insight ─► Reflect                     │
└──────────────────────┬──────────────────────────────┘
                       │  Goal decomposition
                       ▼
┌─────────────────────────────────────────────────────┐
│  L3  Goal Memory                                    │
│  Hierarchical task trees. Progress tracking.        │
│  ► RocksDB                                          │
└─────────────────────────────────────────────────────┘

  ↕ Forgetting runs continuously across all tiers:
    importance decay + threshold pruning + deduplication
```

---

## Multi-Dimensional Memory

Every memory is indexed across four orthogonal dimensions:

```
Organization (org_id)          ← Multi-tenant SaaS isolation
  └─ Application (app_id)     ← Per-app memory separation
       ├─ User (user_id)      ← Factual: preferences, facts, profile
       └─ Agent (agent_id)    ← Procedural: tool usage, strategies, reflections
```

| Dimension | What it captures | Example |
|-----------|-----------------|---------|
| **Organization** | Tenant boundary for SaaS platforms | `org: acme-corp` |
| **Application** | Per-product memory separation | `app: coding-assistant` vs `app: support-bot` |
| **User** | Facts, preferences, personal context | _"Dylan prefers Rust and hates meetings"_ |
| **Agent** | Execution trajectories, learned strategies, tool patterns | _"API X fails on large payloads — use streaming instead"_ |

Query any combination: _"What has agent-X learned about user-Y within app-Z?"_

---

## Six Cognitive Operations

| | Operation | What it does | When it runs |
|-|-----------|-------------|--------------|
| 1 | **Align** | Map multimodal input (text, image, audio, video) to structured events | On ingest |
| 2 | **Compress** | LLM-extract high-density facts from verbose conversations | L0 → L1 consolidation |
| 3 | **Associate** | Auto-link semantically similar memories via cosine similarity | Post-embedding |
| 4 | **Insight** | Community detection (Louvain/LPA) + LLM synthesis of abstract knowledge | Periodic L2 cycle |
| 5 | **Reflect** | Per-session retrospective: what happened, what was learned | Post-session |
| 6 | **Forget** | Importance decay + threshold pruning + semantic deduplication | Continuous background |

---

## Native Multimodal Embedding

Memorose embeds images, audio, and video **natively** via Gemini Embedding 2 — no text conversion, no information loss.

| Provider | Text | Image | Audio | Video | Dim |
|----------|------|-------|-------|-------|-----|
| **Gemini** | Native | Native | Native | Native | 3072 (MRL: 1536/768) |
| **OpenAI** | Native | Fallback* | Fallback* | Fallback* | Model-dependent |

_*Fallback: multimodal content is described via vision/transcription, then text-embedded._

This enables **true cross-modal retrieval**: search with text, find matching images. Search with an image, find related conversations.

---

## Feature Comparison

| Feature | Memorose | Mem0 | Zep | ChromaDB |
|---------|:--------:|:----:|:---:|:--------:|
| Open Source | **Yes** | Partial | Yes | Yes |
| Self-Hosted | **Yes** | No | Yes | Yes |
| Hybrid Search (Vector + BM25) | **Yes** | No | Yes | No |
| Knowledge Graph | **Yes** | Yes | No | No |
| Native Multimodal Embedding | **Yes** | No | No | No |
| Active Forgetting | **Yes** | No | No | No |
| Raft Replication | **Yes** | No | No | No |
| Bitemporal Queries | **Yes** | No | No | No |
| Built-in Dashboard | **Yes** | Yes | No | No |
| Language | Rust | Python | Go | Python |
| Latency (p99) | **<10ms** | ~50ms | ~30ms | ~20ms |

---

## Performance

Benchmarked on a single 8-core node with 1M stored memories:

| Metric | Value |
|--------|-------|
| **Search Latency** | <8ms p99 (hybrid vector + BM25) |
| **Write Throughput** | 50K ops/sec sustained |
| **Memory Footprint** | ~120 MB baseline |
| **Cold Start** | <200ms to first query |

---

## Architecture

```
                        ┌─────────────────────┐
                        │  HTTP API  (Axum)    │
                        │  /v1/users/…         │
                        └─────────┬───────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    │       Shard Manager        │
                    │    (hash-based routing)    │
                    └────┬────────┬────────┬─────┘
                         │        │        │
                    ┌────▼──┐ ┌───▼──┐ ┌───▼───┐
                    │Shard 0│ │Shard1│ │Shard N│
                    │       │ │      │ │       │
                    │Engine │ │Engine│ │Engine │
                    │ +Raft │ │+Raft │ │ +Raft │
                    │+Worker│ │+Wrkr │ │+Worker│
                    └───┬───┘ └──────┘ └───────┘
                        │
          ┌─────────────┼──────────────┐
          │             │              │
     ┌────▼────┐  ┌─────▼─────┐  ┌────▼────┐
     │ RocksDB │  │  LanceDB  │  │ Tantivy │
     │  (KV)   │  │ (Vector)  │  │ (Text)  │
     └─────────┘  └───────────┘  └─────────┘
```

**Key design decisions:**
- **Rust-native**: No GC pauses, predictable latency, single binary deployment
- **Embedded storage**: RocksDB + LanceDB + Tantivy run in-process — no external dependencies
- **Sharded Raft**: Each shard has its own consensus group, preventing leader bottleneck
- **Pluggable LLM**: Gemini, OpenAI, or any OpenAI-compatible endpoint
- **Pluggable reranker**: Built-in weighted RRF or external HTTP reranker

---

## Dashboard

Memorose ships with a built-in web dashboard at `http://localhost:3000/dashboard`:

- **Memory Browser** — search, filter by user/agent/app, inspect memories
- **Knowledge Graph** — interactive visualization of memory relationships
- **Agent Metrics** — per-agent activity and memory statistics
- **App Stats** — per-application memory distribution
- **Playground** — live query testing with real-time results
- **Cluster Health** — multi-node Raft status monitoring
- **Settings** — runtime configuration management

<!-- Screenshot placeholder -->
<!-- <p align="center"><img src=".github/assets/dashboard.png" alt="Dashboard" width="720" /></p> -->

---

## Configuration

Configure via `config.toml`, environment variables (`MEMOROSE__` prefix), or legacy env vars:

```toml
[llm]
provider = "Gemini"                          # "Gemini" | "OpenAI"
google_api_key = "..."
model = "gemini-2.0-flash"
embedding_model = "gemini-embedding-2-preview"
embedding_dim = 3072                         # native dim for Gemini Embedding 2
# embedding_output_dim = 1536               # optional: MRL truncation (auto L2-normalized)
# embedding_task_type = "RETRIEVAL_DOCUMENT" # optional: task type hint

[storage]
root_dir = "./data"

[worker]
llm_concurrency = 5          # parallel LLM calls
decay_interval_secs = 60     # how often importance decays
decay_factor = 0.9            # multiplier per decay cycle
prune_threshold = 0.1         # memories below this are pruned
auto_link_similarity_threshold = 0.6

[raft]
node_id = 1
raft_addr = "127.0.0.1:5001"
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/v1/users/:uid/apps/:aid/streams/:sid/events` | Ingest event (text, image, audio, video, json) |
| `POST` | `/v1/users/:uid/apps/:aid/streams/:sid/retrieve` | Hybrid search with optional cross-modal query |
| `GET` | `/v1/users/:uid/tasks/tree` | Get all goal/task hierarchies |
| `GET` | `/v1/users/:uid/tasks/ready` | Get auto-executable tasks |
| `PUT` | `/v1/users/:uid/tasks/:tid/status` | Update task status |
| `POST` | `/v1/users/:uid/graph/edges` | Add graph edge |
| `GET` | `/v1/status/pending` | Pending event count |
| `POST` | `/v1/cluster/initialize` | Initialize Raft cluster |
| `POST` | `/v1/cluster/join` | Join node to cluster |
| `DELETE` | `/v1/cluster/nodes/:nid` | Remove node from cluster |

<details>
<summary><b>Retrieve request body</b></summary>

```json
{
  "query": "string (required)",
  "agent_id": "string (optional — filter by agent)",
  "image": "base64 (optional — cross-modal image search)",
  "audio": "base64 (optional — cross-modal audio search)",
  "video": "base64 (optional — cross-modal video search)",
  "enable_arbitration": false,
  "min_score": 0.0,
  "graph_depth": 1,
  "start_time": "ISO8601 (optional — valid time filter)",
  "end_time": "ISO8601 (optional)",
  "as_of": "ISO8601 (optional — bitemporal point-in-time query)",
  "include_vector": false
}
```

</details>

---

## Roadmap

- [ ] Python & TypeScript SDKs
- [ ] Streaming event ingestion (WebSocket / SSE)
- [ ] Multi-modal dashboard playground (upload images/audio for cross-modal search)
- [ ] Helm chart for Kubernetes deployment
- [ ] Plugin system for custom memory processors
- [ ] Benchmarking suite with reproducible scripts

---

## Contributing

We welcome contributions of all kinds.

```bash
# Fork, clone, then:
cargo test -p memorose-core
cargo run -p memorose-server
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

[Apache License 2.0](LICENSE)

---

<div align="center">
  <sub>Built with Rust. Designed for agents that remember.</sub>
  <br /><br />
  <a href="https://github.com/ai-akashic/Memorose">
    <img src="https://img.shields.io/github/stars/ai-akashic/Memorose?style=social" alt="Star on GitHub" />
  </a>
</div>
