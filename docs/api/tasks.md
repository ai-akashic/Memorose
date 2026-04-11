---
title: Tasks API
---

# Tasks API

Memorose exposes `L3` as a task and goal execution surface rather than hiding it behind internal workers.

## What These Routes Are For

Use the Tasks API when you need to:

- inspect task trees for one stream
- inspect all goal trees for one user
- pull ready-to-run work
- update task state after execution

## Endpoints

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/users/:user_id/streams/:stream_id/tasks/tree` | Return the goal tree for one stream |
| `GET` | `/v1/users/:user_id/tasks/tree` | Return all goal trees for one user |
| `GET` | `/v1/users/:user_id/tasks/ready` | Return ready-to-run tasks whose dependencies are satisfied |
| `PUT` | `/v1/users/:user_id/tasks/:task_id/status` | Update one task |

## Status Update Request

```http
PUT /v1/users/user_123/tasks/11111111-1111-1111-1111-111111111111/status
Authorization: Bearer <token>
Content-Type: application/json

{
  "status": "Completed",
  "progress": 1.0,
  "result_summary": "Released the synchronized bilingual docs set."
}
```

## Request Fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `status` | enum | Yes | New task status |
| `progress` | number | No | Progress value from `0.0` to `1.0` |
| `result_summary` | string | No | Human-readable summary of the outcome |

## Response

The update route returns the updated `L3Task` object.

## Practical Notes

- `status` follows the `TaskStatus` enum in `memorose-common`.
- Completed tasks can sediment their outcome back into the event stream.
- `ready` tasks are not just pending tasks; they are pending tasks whose dependencies are already satisfied.

## Related Pages

- [Tasks And Goals Guide](../guides/tasks-and-goals)
- [REST API](./rest-api)
