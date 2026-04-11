---
title: Retrieve API
---

# Retrieve API

## POST `/v1/users/:user_id/streams/:stream_id/retrieve`

Run hybrid retrieval for one user and stream.

## What This Route Is For

This is the main recall route for the runtime. It merges multiple retrieval signals instead of relying on a single search path.

Signals can include:

- semantic similarity
- text retrieval
- graph expansion
- time filtering
- organization knowledge
- multimodal query input

## Request

```http
POST /v1/users/dylan/streams/11111111-1111-1111-1111-111111111111/retrieve
Authorization: Bearer <token>
Content-Type: application/json

{
  "org_id": "default",
  "agent_id": "coding-assistant",
  "query": "What does Dylan prefer for systems work?",
  "limit": 10,
  "enable_arbitration": false,
  "min_score": 0.2,
  "graph_depth": 1,
  "start_time": "2026-03-01T00:00:00Z",
  "end_time": "2026-03-31T23:59:59Z",
  "as_of": "2026-03-25T12:00:00Z"
}
```

## Request Fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `query` | string | Yes | Retrieval query text |
| `limit` | number | No | Max results, capped server-side |
| `enable_arbitration` | boolean | No | Enable arbitrated retrieval mode |
| `min_score` | number | No | Minimum score cutoff |
| `graph_depth` | number | No | Graph expansion depth |
| `start_time` | ISO8601 | No | Valid-time lower bound |
| `end_time` | ISO8601 | No | Valid-time upper bound |
| `as_of` | ISO8601 | No | Transaction-time point-in-time filter |
| `org_id` | string | No | Include organization knowledge scope |
| `agent_id` | string | No | Filter toward one agent's procedural memory |
| `image` | string | No | Base64 image for cross-modal retrieval |
| `audio` | string | No | Base64 audio for cross-modal retrieval |
| `video` | string | No | Base64 video for cross-modal retrieval |

## Response

```json
{
  "stream_id": "11111111-1111-1111-1111-111111111111",
  "query": "What does Dylan prefer for systems work?",
  "results": [
    {
      "unit": {
        "id": "9db1d859-0a32-4c33-8b94-8cab9f8e0d16",
        "memory_type": "Fact",
        "content": "Dylan prefers Rust for systems work.",
        "keywords": ["rust", "systems"],
        "level": 1
      },
      "score": 0.94
    }
  ],
  "query_time_ms": 8
}
```

## Response Fields

| Field | Meaning |
| --- | --- |
| `stream_id` | The stream used for this retrieval request |
| `query` | The final text query that was embedded and searched |
| `results[].unit` | The matched memory unit view |
| `results[].score` | Final ranking score after retrieval fusion |
| `query_time_ms` | Server-side retrieval latency |

## Practical Notes

- Query text is always required, even for multimodal retrieval.
- Shared organization knowledge can enter the result set when `org_id` is provided.
- `graph_depth` lets retrieval expand through graph-linked neighbors instead of staying on direct matches only.
- `agent_id` is useful when you want to bias toward one agent's procedural memory.

## Related Pages

- [Hybrid Search Guide](../guides/hybrid-search)
- [Organization Knowledge API](./organization-knowledge)
- [Graph API](./graph)
