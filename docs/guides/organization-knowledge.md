---
title: Organization Knowledge
---

# Organization Knowledge

Organization knowledge is the shared memory layer for reusable knowledge inside one `org_id`.

## Endpoints

- `GET /v1/organizations/:org_id/knowledge`
- `GET /v1/organizations/:org_id/knowledge/:id`
- `GET /v1/organizations/:org_id/knowledge/metrics`

## Query Parameters

The list route currently supports:

- `q`: search text
- `contributor`: filter by contributing user
- `source_type`: filter by source memory type
- `sort`: one of `published_desc`, `contributions_desc`, `active_desc`, `topic_asc`

## Example

```bash
curl -s "http://127.0.0.1:3000/v1/organizations/default/knowledge?q=rust&sort=published_desc" \
  -H "Authorization: Bearer $TOKEN"
```

## Mental Model

Treat this as an evolved read model, not a dump of all organization events. It should hold reusable knowledge that multiple users and agents can benefit from.
