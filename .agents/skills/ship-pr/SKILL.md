---
name: ship-pr
description: Use when finishing implementation work in this repository and preparing it for human review through a GitHub pull request.
metadata:
  opencode/slash: "true"
---

# Deliver changes through a pull request

Use this workflow after completing repository implementation work. The repository policy in `AGENTS.md` requires completed changes to be delivered in a PR.

## Workflow

1. Inspect `git status`, the current branch, and the intended base branch. Preserve pre-existing user changes; stage only files belonging to this change.
2. If work is on the base branch, create a feature branch before preparing the delivery. Never deliver implementation changes directly on the base branch.
3. Run the relevant offline validation:
   - `uv run python -m unittest discover -s tests`
   - `uv run ruff check ingest_data`
   - `uv run ty check ingest_data`
4. If Ruff reports auto-fixable style or import issues, run `uv run ruff check --fix ingest_data`, inspect the resulting diff, and rerun Ruff. Do not accept unrelated formatting or behavior changes without review.
5. Review the final diff, stage only intended files, and create a focused commit. Do not use force-push.
6. Push the feature branch and open a PR with `gh pr create`. Include a concise summary, validation results, and known risks or follow-ups. If a PR already exists for the branch, update that PR instead of creating a duplicate.
7. Report the PR URL, the validation commands and their results, and the current CI status. The PR still requires human review and approval.

## GitHub CLI unavailable

Check access with `gh auth status`. If `gh` is not installed or authenticated, do not claim a PR was opened. Report the blocker and provide the pushed branch name and the PR title/body details so the user can complete or authorize the GitHub step.

## Scope

This skill covers delivery of implementation work in this repository. Live integration pipelines are not offline tests and must not be run as part of this workflow.
