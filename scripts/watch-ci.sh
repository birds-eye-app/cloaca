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

echo "CI #${pr}: watching..."

while true; do
  # Get check status summary
  output=$(gh pr checks "$pr" 2>&1) || true

  # Count statuses
  total=$(echo "$output" | grep -cE '(pass|fail|pending)' || true)
  passed=$(echo "$output" | grep -c 'pass' || true)
  failed=$(echo "$output" | grep -c 'fail' || true)
  pending=$(echo "$output" | grep -c 'pending' || true)

  if [ "$failed" -gt 0 ]; then
    echo "CI #${pr}: FAILED (${passed}/${total} passed, ${failed} failed)"
    echo "$output"
    exit 1
  fi

  if [ "$pending" -eq 0 ] && [ "$total" -gt 0 ]; then
    echo "CI #${pr}: ALL GREEN (${total}/${total} passed)"
    exit 0
  fi

  echo "CI #${pr}: ${passed}/${total} passed, ${pending} pending"
  sleep 30
done
