---
title: REST API
---

# REST API Reference

Base API URL:

```text
http://127.0.0.1:3000
```

## Authentication

Current `/v1` routes accept either:

- `Authorization: Bearer <dashboard-jwt>`
- `x-api-key: <api-key>`

For local development, the fastest path is:

```http
POST /v1/dashboard/auth/login
```

> Note:
> Older docs examples that use `/api/v1/*`, `/v1/memories`, `/v1/search`, or `/health` are from an older product model and should not be used for current integrations.

## Route Groups

### Public Login

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/v1/dashboard/auth/login` | Get dashboard JWT token |

### Memory Runtime

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/v1/users/:user_id/streams/:stream_id/events` | Ingest an event |
| `POST` | `/v1/users/:user_id/streams/:stream_id/retrieve` | Hybrid retrieval |
| `POST` | `/v1/users/:user_id/graph/edges` | Add graph edge |
| `GET` | `/v1/status/pending` | Pending event backlog |

### Memory Maintenance

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `DELETE` | `/v1/users/:user_id/memories/:id` | Hard-delete one memory unit |
| `POST` | `/v1/users/:user_id/memories/semantic/preview` | Preview semantic update or forgetting plan |
| `POST` | `/v1/users/:user_id/memories/semantic/execute` | Execute a reviewed semantic plan |

### Tasks And Goals

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/users/:user_id/streams/:stream_id/tasks/tree` | Task tree for one stream |
| `GET` | `/v1/users/:user_id/tasks/tree` | All task trees for one user |
| `GET` | `/v1/users/:user_id/tasks/ready` | Ready-to-run tasks |
| `PUT` | `/v1/users/:user_id/tasks/:task_id/status` | Update task status |

### Organization Knowledge

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/organizations/:org_id/knowledge` | List organization knowledge |
| `GET` | `/v1/organizations/:org_id/knowledge/:id` | Fetch one knowledge record |
| `GET` | `/v1/organizations/:org_id/knowledge/metrics` | Organization knowledge metrics |

### Cluster Operations

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/v1/cluster/initialize` | Manually initialize cluster |
| `POST` | `/v1/cluster/join` | Join node to cluster |
| `DELETE` | `/v1/cluster/nodes/:node_id` | Remove node from cluster |

### Dashboard API

Dashboard routes are nested under `/v1/dashboard` and back the operational UI.

Important routes include:

- `/v1/dashboard/stats`
- `/v1/dashboard/memories`
- `/v1/dashboard/memories/:id`
- `/v1/dashboard/graph`
- `/v1/dashboard/search`
- `/v1/dashboard/forget/preview`
- `/v1/dashboard/forget/execute`
- `/v1/dashboard/corrections/semantic/preview`
- `/v1/dashboard/corrections/semantic/execute`
- `/v1/dashboard/corrections/manual`
- `/v1/dashboard/corrections/reviews`
- `/v1/dashboard/agents`
- `/v1/dashboard/cluster/status`
- `/v1/dashboard/organizations`

## Integration Notes

- `stream_id` is a UUID path parameter in the current model.
- For single-node default deployments, Raft auto-bootstrap is enabled by default.
- The docs treat `README.md`, `config.example.toml`, `.env.example`, and `crates/memorose-server/src/main.rs` as the source of truth.
- If you are building your own client, start from [Quickstart](../getting-started/quickstart) and then fan out into the dedicated endpoint pages.
