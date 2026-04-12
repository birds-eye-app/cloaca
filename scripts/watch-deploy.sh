#!/usr/bin/env bash
# Watch a Render deploy for a specific commit until it reaches a terminal state.
# Usage: scripts/watch-deploy.sh <SERVICE_ID> <COMMIT_SHA> [HEALTH_URL]
#   HEALTH_URL defaults to https://cloaca.onrender.com/v1/health
#
# Waits for a deploy matching the given commit SHA to appear, then
# tracks it to completion. This ensures we're watching the right
# deploy, not a stale previous one.
#
# Designed to be run via Claude Code's Monitor tool so each
# status line becomes a notification.

set -euo pipefail

service_id="${1:?Usage: watch-deploy.sh <SERVICE_ID> <COMMIT_SHA> [HEALTH_URL]}"
commit_sha="${2:?Usage: watch-deploy.sh <SERVICE_ID> <COMMIT_SHA> [HEALTH_URL]}"
health_url="${3:-https://cloaca.onrender.com/v1/health}"

# Allow matching on short SHAs
sha_len=${#commit_sha}

echo "Waiting for deploy of ${commit_sha:0:12} on service ${service_id}..."

while true; do
  result=$(render deploys list "$service_id" --output json --confirm 2>/dev/null \
    | python3 -c "
import json, sys
sha = '${commit_sha}'
sha_len = ${sha_len}
for d in json.load(sys.stdin):
    if d.get('commit', {}).get('id', '')[:sha_len] == sha:
        print(d['id'], d['status'])
        sys.exit(0)
print('not_found not_found')
" 2>/dev/null) || result="error error"

  deploy_id="${result%% *}"
  deploy_status="${result#* }"

  if [ "$deploy_id" = "not_found" ]; then
    echo "No deploy yet for ${commit_sha:0:12}, waiting..."
    sleep 30
    continue
  fi

  echo "Deploy ${deploy_id}: ${deploy_status}"

  case "$deploy_status" in
    live)
      echo "DEPLOY SUCCEEDED"
      health=$(curl -sf "$health_url" 2>/dev/null) || health="unreachable"
      echo "Health: $health"
      exit 0
      ;;
    deactivated|build_failed|update_failed|canceled)
      echo "DEPLOY FAILED: $deploy_status"
      exit 1
      ;;
  esac

  sleep 30
done
