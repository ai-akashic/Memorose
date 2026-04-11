---
title: Basic Operations
---

# Basic Operations

This guide shows the current minimal flow for the real server API.

## Authenticate

```bash
TOKEN=$(curl -s -X POST http://127.0.0.1:3000/v1/dashboard/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}' | jq -r '.token')
```

## Create A Stream

```bash
STREAM_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
```

## Ingest Text

```bash
curl -s -X POST "http://127.0.0.1:3000/v1/users/user_123/streams/$STREAM_ID/events" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "org_id": "default",
    "content": "User writes Rust code with async patterns",
    "content_type": "text"
  }'
```

## Retrieve

```bash
curl -s -X POST "http://127.0.0.1:3000/v1/users/user_123/streams/$STREAM_ID/retrieve" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "org_id": "default",
    "query": "What language and style does the user prefer?",
    "limit": 5,
    "graph_depth": 1
  }'
```

## Add A Manual Graph Edge

```bash
curl -s -X POST "http://127.0.0.1:3000/v1/users/user_123/graph/edges" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source_id": "11111111-1111-1111-1111-111111111111",
    "target_id": "22222222-2222-2222-2222-222222222222",
    "relation": "Supports",
    "weight": 0.8
  }'
```

## Preview A Semantic Memory Change

Use this when the user intent is “update this memory” or “forget that fact” rather than a hard delete.

```bash
curl -s -X POST "http://127.0.0.1:3000/v1/users/user_123/memories/semantic/preview" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "instruction": "I moved from Shanghai to Beijing and my email changed from old@example.com to new@example.com",
    "org_id": "default",
    "mode": "update",
    "forget_mode": "soft",
    "limit": 10
  }'
```

The response returns a plan plus affected memory candidates. Execute only after review.

## Execute A Reviewed Semantic Plan

```bash
curl -s -X POST "http://127.0.0.1:3000/v1/users/user_123/memories/semantic/execute" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "plan_id": "<plan-id>",
    "org_id": "default",
    "confirm": true
  }'
```

## Hard Delete A Memory Unit

Use hard delete only when you want direct irreversible removal of one memory unit.

```bash
curl -s -X DELETE "http://127.0.0.1:3000/v1/users/user_123/memories/<memory-id>" \
  -H "Authorization: Bearer $TOKEN"
```

## Inspect Pending Work

```bash
curl -s http://127.0.0.1:3000/v1/status/pending \
  -H "Authorization: Bearer $TOKEN"
```

## Use The Dashboard

- UI: `http://127.0.0.1:3100/dashboard`
- API redirect: `http://127.0.0.1:3000/dashboard`

The dashboard is useful for memory inspection, graph exploration, organization knowledge, and cluster health.
