---
title: Distributed Deployment
---

# Distributed Deployment

Memorose supports replicated deployment with Raft and can be started locally in clustered form through the project scripts.

## Recommended Local Cluster

```bash
./scripts/start_cluster.sh start --clean --build
```

Typical local shape:

- backend nodes on `3000`, `3001`, `3002`
- dashboard UI on `3100`

## Cluster Management Endpoints

- `POST /v1/cluster/initialize`
- `POST /v1/cluster/join`
- `DELETE /v1/cluster/nodes/:node_id`

## Join Payload

```json
{
  "node_id": 2,
  "address": "127.0.0.1:5002"
}
```

## Example Initialization

```bash
curl -s -X POST http://127.0.0.1:3000/v1/cluster/initialize \
  -H "Authorization: Bearer $TOKEN"
```

## Example Join

```bash
curl -s -X POST http://127.0.0.1:3000/v1/cluster/join \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "node_id": 2,
    "address": "127.0.0.1:5002"
  }'
```

## Operational Notes

- In single-node mode, join and leave operate on the local raft group.
- In sharded mode, cluster operations fan out across shard groups.
- Leader forwarding is built into write paths, but operational cluster control still expects a healthy Raft topology.
