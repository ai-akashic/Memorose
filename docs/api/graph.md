---
title: Graph API
---

# Graph API

## POST `/v1/users/:user_id/graph/edges`

Create a graph edge between two memory objects.

## Request

```http
POST /v1/users/user_123/graph/edges
Authorization: Bearer <token>
Content-Type: application/json

{
  "source_id": "11111111-1111-1111-1111-111111111111",
  "target_id": "22222222-2222-2222-2222-222222222222",
  "relation": "RelatedTo",
  "weight": 0.8
}
```

## Fields

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `source_id` | UUID | Yes | Source memory or task node |
| `target_id` | UUID | Yes | Target memory or task node |
| `relation` | string | Yes | Relation type |
| `weight` | number | No | Optional edge weight |

## Response

```json
{
  "status": "accepted"
}
```

## Supported Relation Types

`Next`, `RelatedTo`, `Contradicts`, `Supports`, `Abstracts`, `DerivedFrom`, `CausedBy`, `EvolvedTo`, `IsSubTaskOf`, `Blocks`, `Accomplishes`
