---
title: Knowledge Graph
---

# Knowledge Graph

Memorose uses graph relationships to connect memory units, tasks, and higher-level insights.

## Two Ways The Graph Grows

### Automatic Linking

Background processing can create graph structure from similarity, consolidation, and community analysis.

### Manual Linking

You can explicitly add edges through the API:

```bash
curl -s -X POST "http://127.0.0.1:3000/v1/users/user_123/graph/edges" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source_id": "11111111-1111-1111-1111-111111111111",
    "target_id": "22222222-2222-2222-2222-222222222222",
    "relation": "RelatedTo",
    "weight": 0.75
  }'
```

Supported relation values come from the core relation model, including:

- `Next`
- `RelatedTo`
- `Contradicts`
- `Supports`
- `Abstracts`
- `DerivedFrom`
- `CausedBy`
- `EvolvedTo`
- `IsSubTaskOf`
- `Blocks`
- `Accomplishes`

## Retrieval Integration

Retrieval accepts `graph_depth`, so graph structure can influence the final result set instead of living in a disconnected subsystem.
