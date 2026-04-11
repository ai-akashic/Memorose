---
title: Storage Engines
---

# Storage Engines

Memorose combines several engines because no single storage model is enough for the full runtime.

## RocksDB

Used for:

- local durable key-value state
- event and memory bookkeeping
- task persistence
- graph edge support structures
- organization knowledge metadata

## Lance

Used for:

- embedding storage
- vector similarity retrieval
- efficient ANN-style search over memory units

## Tantivy

Used for:

- full-text retrieval
- exact and keyword-heavy lookup
- hybrid scoring alongside vector results

## Why Hybrid Storage Exists

Memorose needs to support:

- append-heavy ingest
- structured task state
- semantic retrieval
- keyword retrieval
- graph expansion
- projected shared knowledge

That is why the project is a runtime with multiple cooperating storage paths rather than a single database wrapper.
