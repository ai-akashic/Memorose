---
title: Multimodal Ingest
---

# Multimodal Ingest

Memorose can ingest and retrieve more than plain text.

## Ingest Content Types

Current ingest accepts:

- `text`
- `image`
- `audio`
- `video`
- `json`

The request body uses `content` plus `content_type`.

## Example: JSON Event

```bash
curl -s -X POST "http://127.0.0.1:3000/v1/users/user_123/streams/$STREAM_ID/events" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "org_id": "default",
    "content_type": "json",
    "content": "{\"source\":\"calendar\",\"title\":\"Team sync\",\"time\":\"2026-03-25T09:00:00Z\"}"
  }'
```

## Multimodal Retrieval

Retrieval can embed a text query together with base64-encoded media:

- `image`
- `audio`
- `video`

```bash
curl -s -X POST "http://127.0.0.1:3000/v1/users/user_123/streams/$STREAM_ID/retrieve" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Find related UI references for this screenshot",
    "image": "<base64-image>",
    "limit": 5
  }'
```

The query text is still required. Media acts as additional retrieval signal.
