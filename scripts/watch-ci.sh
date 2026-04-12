#!/usr/bin/env bash
# Watch CI checks on a GitHub PR until they all complete.
# Usage: scripts/watch-ci.sh [PR_NUMBER]
#   PR_NUMBER defaults to the PR for the current branch.
#
# Designed to be run via Claude Code's Monitor tool so each
# status line becomes a notification.

set -euo pipefail

pr="${1:-}"
if [ -z "$pr" ]; then
  pr=$(gh pr view --json number -q .number 2>/dev/null) || {
    echo "ERROR: No PR number given and no PR found for current branch"
    exit 1
  }
fi

echo "Watching CI for PR #${pr}..."

while true; do
  # Get check status summary
  output=$(gh pr checks "$pr" 2>&1) || true

  # Count statuses
  total=$(echo "$output" | grep -cE '(pass|fail|pending)' || true)
  passed=$(echo "$output" | grep -c 'pass' || true)
  failed=$(echo "$output" | grep -c 'fail' || true)
  pending=$(echo "$output" | grep -c 'pending' || true)

  echo "PR #${pr} checks: ${passed} passed, ${failed} failed, ${pending} pending (${total} total)"

  if [ "$failed" -gt 0 ]; then
    echo "CI FAILED"
    echo "$output"
    exit 1
  fi

  if [ "$pending" -eq 0 ] && [ "$total" -gt 0 ]; then
    echo "CI PASSED — all ${total} checks green"
    exit 0
  fi

  sleep 30
done
