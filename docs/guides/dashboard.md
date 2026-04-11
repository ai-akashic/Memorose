---
title: Dashboard
---

# Dashboard

Memorose ships with a separate dashboard UI plus authenticated dashboard API routes under `/v1/dashboard`.

## URLs

- Local dashboard UI: `http://127.0.0.1:3100/dashboard`
- Backend API: `http://127.0.0.1:3000/v1/dashboard`
- Backend redirect: `http://127.0.0.1:3000/dashboard`

## Login

Public login endpoint:

```http
POST /v1/dashboard/auth/login
```

Default credentials are `admin` / `admin` unless you set `DASHBOARD_ADMIN_PASSWORD`.

> **Note on Docker Deployments:**
> If you are deploying the Dashboard via a custom `docker-compose.yml` or `docker run`, you **must** set the `DASHBOARD_API_ORIGIN` environment variable for the Dashboard container to point to the backend API (e.g., `DASHBOARD_API_ORIGIN=http://memorose:3000`). If omitted, the Dashboard will attempt to proxy login requests to `127.0.0.1` inside its own container, resulting in a `connect ECONNREFUSED 127.0.0.1:3000` error.

## Main Features

- memory browser
- graph explorer
- search playground
- semantic forget / update preview flows
- correction review queue
- cluster status
- agent metrics
- organization management
- organization knowledge views
- runtime config inspection

## Important API Routes

- `POST /v1/dashboard/auth/login`
- `POST /v1/dashboard/auth/password`
- `GET /v1/dashboard/stats`
- `GET /v1/dashboard/memories`
- `GET /v1/dashboard/memories/:id`
- `GET /v1/dashboard/graph`
- `POST /v1/dashboard/search`
- `POST /v1/dashboard/chat`
- `POST /v1/dashboard/forget/preview`
- `POST /v1/dashboard/forget/execute`
- `POST /v1/dashboard/corrections/semantic/preview`
- `POST /v1/dashboard/corrections/semantic/execute`
- `POST /v1/dashboard/corrections/manual`
- `GET /v1/dashboard/corrections/reviews`
- `GET /v1/dashboard/config`
- `GET /v1/dashboard/agents`
- `GET /v1/dashboard/cluster/status`
- `GET /v1/dashboard/organizations`
- `POST /v1/dashboard/organizations`

The dashboard API is not a separate product line. It is the operational UI for the same runtime.
