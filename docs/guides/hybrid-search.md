---
title: Hybrid Search
---

# Hybrid Search

Memorose retrieval combines multiple signals rather than relying on one search path.

## What Happens During Retrieval

1. The query is embedded
2. Vector retrieval runs against stored memory units
3. Text retrieval runs against Tantivy
4. Optional graph expansion uses `graph_depth`
5. Shared organization knowledge can be included
6. Results are merged into one ranked response

## API Example

```bash
curl -s -X POST "http://127.0.0.1:3000/v1/users/user_123/streams/$STREAM_ID/retrieve" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "org_id": "default",
    "agent_id": "coding-assistant",
    "query": "How should I help this user with Rust async work?",
    "limit": 10,
    "enable_arbitration": false,
    "min_score": 0.2,
    "graph_depth": 2
  }'
```

## Useful Filters

- `org_id`: include organization-scoped shared knowledge
- `agent_id`: bias toward one agent's procedural memory
- `start_time` and `end_time`: valid-time filtering
- `as_of`: point-in-time transaction filtering
- `graph_depth`: expand from directly matched memory into related graph context

## When Hybrid Beats Single-Mode Search

- Preference and profile recall
- Mixed factual and procedural lookup
- Queries that need both exact terms and semantic similarity
- Retrieval that benefits from graph neighbors or organization knowledge
