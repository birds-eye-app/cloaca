---
name: get-it-out-the-door
description: Full workflow for committing changes, opening a PR, watching CI, merging, and monitoring the deploy to Render.
---

# Committing, Merging, and Deploying Changes

Use this skill when the user asks to ship, merge, deploy, or go through the full commit-to-deploy workflow.

## Workflow

### 1. Commit

- Run `git status`, `git diff`, and `git log --oneline -5` to understand the change set
- Create a feature branch (never push directly to main)
- Write a descriptive commit message: summary line + bullet points for what changed and why
- Stage specific files (avoid `git add -A`)

### 2. Review

Run two review agents **in parallel** before pushing:

- **PR code review agent** — logic correctness, edge cases, test coverage gaps
- **Security review agent** — SQL injection, secrets, input validation, OWASP top 10, DoS vectors

Address any actionable findings before proceeding.

### 3. Documentation check

Verify that `CLAUDE.md` and any relevant docs reflect the changes (new tables, changed schedules, new env vars, etc.).

### 4. Push and open PR

- Push the branch: `git push -u origin <branch-name>`
- Open the PR with `gh pr create` including a summary, change list, and test plan

### 5. Watch CI

Use a **Monitor** to stream CI status:

```bash
Monitor(
  description="CI checks on PR #<N>",
  command="scripts/watch-ci.sh <PR_NUMBER>",
  timeout_ms=300000,
  persistent=false
)
```

The script polls `gh pr checks` every 30 seconds and reports pass/fail/pending counts. It exits 0 when all checks pass, or exits 1 if any check fails.

### 6. Merge

Once CI passes:

```bash
gh pr merge <PR_NUMBER> --squash --delete-branch
```

### 7. Watch deploy

After merging to main, Render auto-deploys. Get the merge commit SHA from `git rev-parse origin/main`, then use a **Monitor** to stream deploy status:

```bash
Monitor(
  description="Render deploy for cloaca",
  command="scripts/watch-deploy.sh <SERVICE_ID> <COMMIT_SHA>",
  timeout_ms=600000,
  persistent=false
)
```

The script waits for a deploy matching the specific commit SHA to appear, then tracks it to completion. It exits 0 when the deploy goes live (and verifies the health check), or exits 1 on failure.

### 8. Post-deploy verification

After the deploy monitor reports success:

- Check health: `curl -s https://cloaca.onrender.com/v1/health`
- Tail logs: `render logs -r <SERVICE_ID> --start "$(date -u -v-5M +%Y-%m-%dT%H:%M:%SZ)" -o text --confirm`
- If the change includes a DB migration, verify it ran: `uv run alembic -c alembic.ini history`

## Important notes

- Always use PRs, never push directly to main
- Always specify branch name on `git push`
- Look up the Render service ID from memory (reference_render_service_ids.md) — do not hardcode it in code or commits
- `status` is a reserved variable in zsh — use `deploy_status` instead in shell scripts
- Render suspend/resume is via API, not CLI
