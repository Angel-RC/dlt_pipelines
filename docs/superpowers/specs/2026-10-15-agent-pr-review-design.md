# Agent PR and review workflow

## Goal

Standardize how coding agents deliver repository changes: changes are published on a branch, proposed through a GitHub pull request, checked by CI, and reviewed by a person before merge.

## Chosen approach

Use four layers, each with a distinct responsibility:

1. **`AGENTS.md` — always-on repository policy.** State briefly that agents must not deliver changes directly on the base branch. For completed implementation work, agents should run the relevant offline checks, publish a feature branch, open a PR with `gh`, and report its URL. If GitHub CLI access or authentication is unavailable, agents must report that blocker rather than claim the PR was opened.
2. **`.agents/skills/ship-pr/SKILL.md` — repeatable procedure.** Describe the detailed sequence for checking repository state, creating/using a feature branch, validating changes, committing and pushing, creating a PR, and reporting the result. The skill supports consistency and discoverability; it does not replace the always-on policy in `AGENTS.md`.
3. **`.github/workflows/ci.yml` — deterministic validation.** Trigger on PR opened, reopened, or synchronized. Set up Python 3.13 and `uv`, install from the committed lockfile, run the offline `unittest` suite, Ruff, and ty. Do not run live ingestion pipelines because they access external services and write local DuckDB data.
4. **`.github/pull_request_template.md` — reviewer context.** Prompt the author to summarize the change, list validation performed, and flag risks or follow-ups. Include a reviewer checklist that the CI result is green and the code change has been reviewed.

Before enabling the workflow as a gate, meet this baseline prerequisite: reconcile `uv.lock` with `pyproject.toml` and resolve the existing Ruff and ty diagnostics in the integrations. When Ruff reports safe, auto-fixable style/import issues, agents should try `uv run ruff check --fix ingest_data`, review its diff, and then rerun checks. Keep any remaining rule exceptions narrow and justified. Add the CI workflow only after the required checks pass locally.

CI is a validation gate, not an AI code reviewer. A person reviews the PR. Repository administrators must configure base-branch protection in GitHub to require the CI check and at least one human approval; that enforcement setting lives in GitHub, not in repository files.

## PR lifecycle

1. The agent inspects repository status and starts work on a feature branch, preserving any pre-existing user changes.
2. After implementation, the agent runs offline tests, lint, and type checks; it does not invoke live ingestion pipelines as a test.
3. The agent commits and pushes the change, then opens a PR with `gh` and supplies a concise summary and validation results.
4. GitHub Actions runs or reruns CI when the PR is opened or updated. The agent reports the PR URL and validation status.
5. A human reviews the changes and CI results. GitHub branch protection prevents merging until the required checks and approval are satisfied.

## Scope and constraints

- Do not add an AI review service, API secrets, or automated AI comments.
- Do not run supermarket or Klaviyo pipelines in CI.
- Keep the main instructions in `AGENTS.md` short; keep procedural detail in the dedicated skill.
- Use the existing project checks: `uv run python -m unittest discover -s tests`, `uv run ruff check ingest_data`, and `uv run ty check ingest_data`.
- Before enabling those checks as PR gates, reconcile the lockfile and fix the existing Ruff/ty baseline failures. Prefer Ruff's safe auto-fixes for style/import diagnostics, reviewing every resulting change.
- The existing skill mechanism in `.agents/skills/` is already used by this repository, so the delivery procedure follows the local convention.

## Acceptance criteria

- An agent reading `AGENTS.md` knows that implementation work should be delivered as a PR and knows where to find the procedure.
- The delivery skill covers branch, checks, commit/push, PR creation, and reporting, including the case where GitHub access is unavailable.
- A PR workflow runs the offline test, lint, and type-check commands on Python 3.13 using the locked dependencies.
- `uv sync --locked`, offline tests, Ruff, and ty all pass before the workflow is enabled as a required check.
- The PR template requests a useful summary and validation details for human review.
- The repository documentation identifies the GitHub branch-protection settings required to enforce approval and CI before merge.
