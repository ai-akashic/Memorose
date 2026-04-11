---
title: Installation
---

# Installation

Memorose is typically run as a Rust service plus a separate dashboard UI.

## Prerequisites

- Rust toolchain
- An LLM provider key for consolidation and embedding
- Optional: `jq` for the command-line examples in this docs set

## Clone The Repository

```bash
git clone https://github.com/ai-akashic/Memorose.git
cd Memorose
```

## Environment Setup

Copy the example environment file and fill in your provider settings.

```bash
cp .env.example .env
```

At minimum, configure:

```bash
LLM_PROVIDER=gemini
GOOGLE_API_KEY=your_google_api_key_here
LLM_MODEL=gemini-3.1-flash-lite-preview
EMBEDDING_MODEL=gemini-embedding-2-preview
```

If you want to avoid the default dashboard password warning, also set:

```bash
DASHBOARD_ADMIN_PASSWORD=change-me
```

## Configuration

Start from the shipped example:

```bash
cp config.example.toml config.toml
```

The current config shape is organized around:

- `[server]`
- `[database]`
- `[consolidation]`
- `[forgetting]`
- `[graph]`
- `[raft]`
- `[cache]`
- `[multimodal]`
- `[security]`
- `[telemetry]`
- `[development]`

## Start The Stack

Recommended local startup:

```bash
./scripts/start_cluster.sh start --clean --build
```

This starts backend nodes on `3000` and related cluster ports, and the dashboard UI on `3100`.

## Manual Server Startup

If you only want the API server:

```bash
cargo run --release -p memorose-server
```

## Verify The Installation

Check the root endpoint:

```bash
curl http://127.0.0.1:3000/
```

Expected response:

```text
Memorose is running.
```

## Dashboard

- Dashboard UI: `http://127.0.0.1:3100/dashboard`
- API redirect: `http://127.0.0.1:3000/dashboard`
- Default login: `admin` / `admin`

Change the password on first use, or set `DASHBOARD_ADMIN_PASSWORD` before startup.

> **Note on Dashboard in Docker:** 
> If you deploy the Dashboard using your own `docker-compose.yml` or `docker run`, you **must** set the `DASHBOARD_API_ORIGIN` environment variable on the Dashboard container to point to your backend service (e.g., `DASHBOARD_API_ORIGIN=http://memorose:3000`). Otherwise, API requests won't reach the backend and you will encounter `connect ECONNREFUSED 127.0.0.1:3000` errors during login.
