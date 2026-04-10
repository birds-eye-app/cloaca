---
name: infra
description: How to deploy, manage services, and work with Render infrastructure for the birds-eye-app project.
---

# Infrastructure

## Services on Render

All services run in the **Oregon** region under the "Birds Eye" project in David Meadows's workspace.

| Service | Type | Repo | Branch | Deploy trigger |
|---------|------|------|--------|----------------|
| **cloaca** | Web service (Docker) | `birds-eye-app/cloaca` | `main` | checksPass |
| **beak-v2** | Static site | `birds-eye-app/beak-v2` | `main` | commit |

- **cloaca** is the main API server (FastAPI). Piper (Discord bot) runs inside cloaca as an asyncio background task.
- **beak-v2** is the frontend.

To find service IDs, run: `render services --confirm -o json` or check memory.

## Deploying from a branch

To deploy a specific branch/commit to a service without merging to main:

```bash
# Get the commit SHA
git rev-parse HEAD

# Deploy it (the commit must be pushed to the remote first)
render deploys create <service-id> --commit <sha> --confirm -o json

# Check deploy status
render deploys list <service-id> --confirm -o json

# View logs from a specific time
render logs -r <service-id> --start "<ISO timestamp>" -o text --confirm
```

## Render disk

Cloaca has a 1GB persistent disk mounted at `/var/data`. This holds the `ebd_nyc.db` DuckDB database used by piper for local bird observation queries. To upload files to it, use `scp` via Render SSH (see memory for the exact command).

## Environment variables

Managed per-service in the Render dashboard. Key ones for cloaca+piper:
- `ANTHROPIC_API_KEY` -- Claude API access for piper
- `PIPER_DISCORD_BOT_TOKEN` -- Discord bot token
- `EBIRD_MCP_URL` -- eBird MCP server endpoint
- `PIPER_DUCK_DB_PATH` -- path to DuckDB on the Render disk (`/var/data/ebd_nyc.db`)
- `DUCK_DB_PATH` -- path to cloaca's own DuckDB for hotspot queries
