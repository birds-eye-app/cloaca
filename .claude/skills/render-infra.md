---
name: render-infra
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

## Blueprint (render.yaml)

`render.yaml` defines services, databases, env var references, and the pre-deploy command (`uv run alembic upgrade head`).

**Changes to `render.yaml` are NOT automatically applied.** After merging render.yaml changes, manually sync the blueprint in the Render dashboard:

1. Go to the "Birds Eye" project in the Render dashboard
2. Settings → Blueprint → Sync
3. Review the proposed changes before applying

Validate locally before pushing:
```bash
render blueprints validate --confirm -o text
```

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

To monitor a deploy until it finishes, use a Monitor with a poll loop:

```bash
while true; do
  deploy_status=$(render deploys list <service-id> --output json 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['status'])" 2>/dev/null)
  echo "Deploy: $deploy_status"
  case "$deploy_status" in
    live|deactivated|build_failed|update_failed|canceled)
      echo "Deploy finished: $deploy_status"; break ;;
  esac
  sleep 30
done
```

After a deploy goes live, verify the health check and tail logs:

```bash
# Health check
curl -s https://cloaca.onrender.com/v1/health

# Tail recent logs (last 5 minutes)
render logs -r <service-id> --start "$(date -u -v-5M +%Y-%m-%dT%H:%M:%SZ)" -o text --confirm
```

## Database (Postgres)

Piper's state is stored in a managed Render Postgres instance (`piper-state-db`, basic-256mb plan). Migrations are managed by Alembic and run automatically via the pre-deploy command on each deploy.

To query production Postgres (get the database ID from memory or `render postgres list`):
```bash
render psql <postgres-id> -c "SELECT ..." -o text
```

## Render disk

Cloaca has a 1GB persistent disk mounted at `/var/data`. This holds:
- `ebd_nyc.db` — read-only eBird observation database used by piper via MCP

To upload files to the disk, use `scp` via Render SSH (see memory for the exact command).

## Environment variables

Managed per-service in the Render dashboard. Key ones for cloaca+piper:
- `ANTHROPIC_API_KEY` -- Claude API access for piper
- `PIPER_DISCORD_BOT_TOKEN` -- Discord bot token
- `EBIRD_MCP_URL` -- eBird MCP server endpoint
- `PIPER_DUCK_DB_PATH` -- path to the eBird observation database on the Render disk (`/var/data/ebd_nyc.db`), used by piper via MCP
- `PIPER_POSTGRES_DB_URL` -- Postgres connection string for piper state (year lifers tracking), managed Render Postgres DB
- `DUCK_DB_PATH` -- path to cloaca's own DuckDB for hotspot queries (separate database from piper's)
