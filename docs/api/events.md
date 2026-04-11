---
title: Events API
---

# Events API

## POST `/v1/users/:user_id/streams/:stream_id/events`

Store a raw event in the L0 event stream.

## What This Route Is For

Use this route when you need to append source material into Memorose before it becomes durable memory.

Typical uses:

- chat turns
- tool outputs
- structured JSON payloads
- image, audio, or video references
- task-related execution events

## Request

```http
POST /v1/users/dylan/streams/11111111-1111-1111-1111-111111111111/events
Authorization: Bearer <token>
Content-Type: application/json

{
  "org_id": "default",
  "content": "Dylan prefers dark terminals and concise updates.",
  "content_type": "text",
  "level": 1,
  "parent_id": "22222222-2222-2222-2222-222222222222",
  "task_status": "InProgress",
  "task_progress": 0.6
}
```

## Request Fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `content` | string | Yes | Event payload |
| `content_type` | string | No | `text`, `image`, `audio`, `video`, or `json` |
| `org_id` | string | No | Organization scope |
| `level` | number | No | Target level hint stored in metadata |
| `parent_id` | string | No | Parent object reference stored in metadata |
| `task_status` | string | No | Task status hint stored in metadata |
| `task_progress` | number | No | Task progress hint stored in metadata |

## Response

```json
{
  "status": "accepted",
  "event_id": "9db1d859-0a32-4c33-8b94-8cab9f8e0d16"
}
```

## Practical Notes

- `stream_id` is a UUID path parameter.
- For `content_type: "json"`, `content` must be a JSON string payload.
- The route appends to `L0`; durable `L1` or `L2` memory appears later through consolidation and reflection.
- Writes go to the current leader. Non-leader nodes forward writes when possible.

## Related Pages

- [Quickstart](../getting-started/quickstart)
- [Retrieve API](./retrieve)
- [Multimodal Ingest Guide](../guides/multimodal-ingest)
