#!/bin/sh
# Container entrypoint. On Render, `preDeployCommand` runs Alembic before the new instance starts;
# plain Docker hosts (the patch-town box) have no such hook, so they set MIGRATE_ON_START=1 and the
# container migrates itself before serving. Gated by env so Render's zero-downtime deploys (which
# briefly run two instances) don't race concurrent `alembic upgrade head` runs.
set -e

if [ "${MIGRATE_ON_START:-0}" = "1" ]; then
  echo "entrypoint: running alembic upgrade head (MIGRATE_ON_START=1)"
  uv run alembic upgrade head
fi

exec uv run fastapi run src/cloaca/main.py
