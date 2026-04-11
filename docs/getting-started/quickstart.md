---
title: Quickstart
---

# Quickstart

This page follows the current `memorose-server` API directly. It is the shortest path from a running node to a real retrieval round-trip.

## What You Need

- a running Memorose server on `http://127.0.0.1:3000`
- dashboard credentials
- `jq`
- `uuidgen`

> Note:
> In the current single-node default setup, Memorose auto-initializes Raft at startup. You do **not** need to call `/v1/cluster/initialize` before sending writes unless you intentionally disabled `raft.auto_initialize`.

## The Happy Path

1. log in and get a bearer token
2. create a stream
3. ingest one event
4. retrieve against that stream
5. check whether consolidation backlog is clear

> Tip:
> If you only want to validate the API surface, this page is enough. If you want the full model behind it, continue with [Architecture Overview](../architecture/overview).

## 1. Set Base Variables

```bash
export BASE_URL="http://127.0.0.1:3000"
export USER_ID="dylan"
export ORG_ID="default"
```

## 2. Log In And Get A Token

`/v1` routes accept a dashboard JWT bearer token.

```bash
TOKEN=$(curl -s -X POST "$BASE_URL/v1/dashboard/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "admin",
    "password": "admin"
  }' | jq -r '.token')
```

## 3. Create A Stream ID

```bash
STREAM_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
```

## 4. Ingest An Event

```bash
curl -s -X POST "$BASE_URL/v1/users/$USER_ID/streams/$STREAM_ID/events" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "org_id": "default",
    "content": "Dylan prefers Rust for systems work and wants concise status updates.",
    "content_type": "text"
  }'
```

Expected response shape:

```json
{
  "status": "accepted",
  "event_id": "..."
}
```

## 5. Retrieve Memory

```bash
curl -s -X POST "$BASE_URL/v1/users/$USER_ID/streams/$STREAM_ID/retrieve" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "org_id": "default",
    "query": "What does Dylan prefer for systems work?",
    "limit": 5,
    "graph_depth": 1
  }'
```

Expected response shape:

```json
{
  "stream_id": "...",
  "query": "What does Dylan prefer for systems work?",
  "results": [
    {
      "unit": {
        "id": "...",
        "memory_type": "Fact",
        "content": "...",
        "keywords": ["rust", "systems"],
        "level": 1
      },
      "score": 0.9
    }
  ],
  "query_time_ms": 12
}
```

Important response fields:

- `results[].unit.level`: where the retrieved memory sits in the hierarchy
- `results[].unit.memory_type`: what kind of memory unit matched
- `score`: final ranking score after retrieval fusion
- `query_time_ms`: observed server-side latency for the retrieval call

## 6. Check Consolidation Backlog

```bash
curl -s "$BASE_URL/v1/status/pending" \
  -H "Authorization: Bearer $TOKEN"
```

When `"ready": true`, the current pending-event backlog is clear.

## Where To Go Next

- [Configuration](./configuration)
- [Basic Operations](../guides/basic-operations)
- [Cluster API](../api/cluster)
- [Tasks And Goals](../guides/tasks-and-goals)
- [REST API](../api/rest-api)
