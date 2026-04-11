---
title: Organization Knowledge API
---

# Organization Knowledge API

Organization knowledge is the shared memory read model for one organization.

## What These Routes Are For

Use this API when memory should be reusable across multiple users or agents inside the same `org_id`.

This layer is meant for:

- shared policies
- terminology
- reusable workflows
- generalized best practices
- organization-level insight rollups

## Endpoints

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/organizations/:org_id/knowledge` | List organization knowledge entries |
| `GET` | `/v1/organizations/:org_id/knowledge/:id` | Fetch one knowledge entry by UUID |
| `GET` | `/v1/organizations/:org_id/knowledge/metrics` | Fetch organization knowledge metrics and rollups |

## List Query Parameters

| Parameter | Meaning |
| --- | --- |
| `q` | Free-text match against content or keywords |
| `contributor` | Filter by contributing user |
| `source_type` | Filter by source memory type |
| `sort` | Ordering mode such as `published_desc`, `contributions_desc`, `active_desc`, or `topic_asc` |

## Example

```http
GET /v1/organizations/default/knowledge?q=rust&sort=published_desc
Authorization: Bearer <token>
```

## Practical Notes

- This API is also used by the dashboard.
- Treat organization knowledge as curated shared memory, not as raw user-event storage.
- Use it when personal or agent memory needs to evolve into reusable organizational knowledge.

## Related Pages

- [Organization Knowledge Guide](../guides/organization-knowledge)
- [REST API](./rest-api)
- [Dashboard Guide](../guides/dashboard)
