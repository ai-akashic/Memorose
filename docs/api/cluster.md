---
title: Cluster API
---

# Cluster API

Memorose exposes a small operational API for Raft cluster management.

## Endpoints

### POST `/v1/cluster/initialize`

Manually initialize the local cluster or all shard groups.

### POST `/v1/cluster/join`

Join a node.

Request body:

```json
{
  "node_id": 2,
  "address": "127.0.0.1:5002"
}
```

### DELETE `/v1/cluster/nodes/:node_id`

Remove a node from the cluster.

## Notes

- In the current single-node default setup, Raft auto-bootstrap is enabled. A fresh node usually becomes writable without a manual `initialize` call.
- Use `POST /v1/cluster/initialize` when you intentionally disabled `raft.auto_initialize`, or when you are handling explicit multi-node bootstrap flows yourself.
- In multi-node deployments, set `raft.bootstrap_seed_node_id` so only the chosen seed node auto-initializes.
- In single-node mode, join and leave operate on the local raft group.
- In sharded mode, these calls fan out across shard groups.
- Use the dashboard cluster views for operator-facing status inspection.
