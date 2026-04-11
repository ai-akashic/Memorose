---
title: Tasks And Goals
---

# Tasks And Goals

Memorose exposes `L3` as goals and task memory: an execution-oriented task and planning system.

## Core Endpoints

- `GET /v1/users/:user_id/streams/:stream_id/tasks/tree`
- `GET /v1/users/:user_id/tasks/tree`
- `GET /v1/users/:user_id/tasks/ready`
- `PUT /v1/users/:user_id/tasks/:task_id/status`

## What The API Returns

Task-tree routes return goal roots plus nested `L3Task` children. Ready-task routes return tasks whose dependencies are satisfied and whose status is still pending.

## Update Task Status

```bash
curl -s -X PUT "http://127.0.0.1:3000/v1/users/user_123/tasks/11111111-1111-1111-1111-111111111111/status" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "Completed",
    "progress": 1.0,
    "result_summary": "Migrated the API docs to the current route model."
  }'
```

When a task completes, Memorose can sediment the outcome back into the event stream.

## Why This Matters

This is what makes `L3` different from a generic note store. You can track execution, not just remembered facts.
