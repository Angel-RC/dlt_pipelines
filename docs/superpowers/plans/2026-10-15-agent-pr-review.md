# Agent PR, CI, and baseline readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Standardize agent delivery through feature branches and pull requests, with offline CI checks and human review before merge.

**Architecture:** Put the mandatory delivery rule in root `AGENTS.md` and the reusable step-by-step procedure in `.agents/skills/ship-pr/SKILL.md`. Add a pull-request workflow for tests, lint, and type checking, a PR template for author/reviewer context, and README guidance for the GitHub branch-protection settings that enforce review.

**Tech Stack:** Markdown, GitHub Actions, Python 3.13, `uv`, `unittest`, Ruff, ty.

**Spec:** `docs/superpowers/specs/2026-10-15-agent-pr-review-design.md`

## Global Constraints

- Set up Python 3.13 and `uv` in CI.
- Use `uv run python -m unittest discover -s tests`, `uv run ruff check ingest_data`, and `uv run ty check ingest_data`.
- Do not run supermarket or Klaviyo ingestion pipelines in CI.
- Do not add an AI review service, API secrets, or automated AI comments.
- Require a human approval and passing CI through GitHub base-branch protection settings.
- Make the locked dependency environment, tests, Ruff, and ty pass before enabling CI checks as a gate.
- Use Ruff auto-fixes for safe style/import diagnostics and review the complete resulting diff.

---

### Task 1: Repair the existing validation baseline

**Files:**
- Modify: `uv.lock`
- Modify: `ingest_data/consum/pipeline.py`
- Modify: `ingest_data/klaviyo/pipeline.py`
- Modify: `ingest_data/klaviyo/source.py`
- Modify: `ingest_data/mercadona/pipeline.py`
- Modify: `ingest_data/mercadona/source.py`
- Modify: `ingest_data/trustpilot/pipeline.py`
- Modify: `ingest_data/trustpilot/source.py`
- Modify: `ingest_data/utils.py`
- Create: `tests/test_klaviyo_source.py`
- Create: `tests/test_trustpilot_source.py`

**Interfaces:**
- The lockfile matches `pyproject.toml` and `uv sync --locked` succeeds.
- Existing integrations pass Ruff and ty without changing their intended ingestion behavior.
- Klaviyo reports a clear configuration error when no API key is available.
- Trustpilot build-id extraction returns a parsed ID and raises a clear `ValueError` when the response has no build ID.

- [ ] **Step 1: Add a failing offline test for a missing Trustpilot build ID**

Create `tests/test_trustpilot_source.py` with a `unittest.TestCase` and tests for valid and missing build IDs. Patch `ingest_data.trustpilot.source.requests.get` with `unittest.mock.patch` and a `types.SimpleNamespace(text=...)`; do not make network requests. The missing-ID case should assert that `get_build_id()` raises `ValueError` mentioning `buildId`. Create `tests/test_klaviyo_source.py` with a test that clears `os.environ` using `patch.dict`, calls `klaviyo_source(api_key=None)`, and asserts a `ValueError` mentioning `KLAVIYO_API_KEY`.

- [ ] **Step 2: Run the focused test and confirm the expected failure**

Run:

```bash
uv run python -m unittest discover -s tests -p test_trustpilot_source.py
uv run python -m unittest discover -s tests -p test_klaviyo_source.py
```

Expected: the valid-ID test passes; the missing-ID test fails with `AttributeError` instead of the expected `ValueError`; and the missing-key test fails with dlt's `ConfigFieldMissingException` instead of the expected `ValueError`.

- [ ] **Step 3: Reconcile and verify the lockfile**

Run:

```bash
uv lock
uv sync --locked
```

Expected: `uv.lock` reflects the current project dependency declarations and the locked sync completes without changing `pyproject.toml`.

- [ ] **Step 4: Apply Ruff's safe automatic fixes**

Run: `uv run ruff check --fix ingest_data`

Review the entire diff before accepting it. Keep the changes limited to fixes Ruff reports, including import order, unused imports, deprecated typing aliases, and other safe automatic rewrites. Rerun `uv run ruff check ingest_data` to identify remaining diagnostics.

- [ ] **Step 5: Resolve remaining Ruff and ty diagnostics**

For the remaining Ruff `BLE001` in `ingest_data/klaviyo/pipeline.py`, preserve per-resource best-effort error handling and add a narrow `# noqa: BLE001` with an adjacent explanation if no project-specific exception type accurately represents all pipeline failures. In `ingest_data/klaviyo/source.py`, resolve the optional argument from `KLAVIYO_API_KEY` at call time, raise a clear `ValueError` if it remains missing, and annotate the decorated source's return as `DltSource`. In `ingest_data/mercadona/source.py`, make the response callback type reflect the actual non-optional response it handles. In `ingest_data/trustpilot/source.py`, check the regex match before accessing its group and raise the tested `ValueError` when absent.

- [ ] **Step 6: Run the focused Trustpilot test and the complete offline suite**

Run:

```bash
uv run python -m unittest discover -s tests -p test_trustpilot_source.py
uv run python -m unittest discover -s tests -p test_klaviyo_source.py
uv run python -m unittest discover -s tests
uv run ruff check ingest_data
uv run ty check ingest_data
uv sync --locked
```

Expected: all commands pass. Review `git diff -- uv.lock` and confirm it only reconciles dependencies with `pyproject.toml`; review all auto-fixes and behavior changes before continuing.

### Task 2: Add always-on agent policy and delivery skill

**Files:**
- Modify: `AGENTS.md`
- Create: `.agents/skills/ship-pr/SKILL.md`

**Interfaces:**
- The `AGENTS.md` policy directs agents to the `ship-pr` skill after implementation work is complete.
- The skill describes the exact branch, validation, commit, push, PR creation, and reporting procedure.

- [ ] **Step 1: Add the mandatory delivery rule to `AGENTS.md`**

Append a section with this policy, matching the existing concise bullet style:

```markdown
## Delivering changes

- Preserve pre-existing user changes and do implementation work on a feature branch, not directly on the base branch.
- After implementation, run relevant offline tests, lint, and type checks; do not run live ingestion pipelines as tests.
- If Ruff reports auto-fixable style or import issues, try `uv run ruff check --fix ingest_data`, review its diff, and rerun Ruff.
- Deliver completed implementation work through a GitHub PR: commit the changes, push the feature branch, and open the PR with `gh`. Include a summary, validation results, and known risks or follow-ups.
- Follow `.agents/skills/ship-pr/SKILL.md` for the delivery procedure. If `gh` or GitHub authentication is unavailable, report that blocker and do not claim a PR was opened.
```

- [ ] **Step 2: Create `.agents/skills/ship-pr/SKILL.md`**

Use the repository's existing `.agents/skills/pipe/SKILL.md` frontmatter convention. Start with this metadata and purpose:

```markdown
---
name: ship-pr
description: Use when finishing implementation work in this repository and preparing it for human review through a GitHub pull request.
metadata:
  opencode/slash: "true"
---

# Deliver changes through a pull request
```

Add an ordered workflow that instructs the agent to inspect `git status` and the current branch, preserve pre-existing changes, create a feature branch if needed, run the relevant offline checks, try `uv run ruff check --fix ingest_data` if Ruff reports auto-fixable style/import diagnostics, review the diff, rerun Ruff, commit only intended changes, push the feature branch, open a PR with `gh pr create` including summary/tests/risks, and report the PR URL and current CI state. Explicitly forbid force-pushing and state that if `gh` or authentication is unavailable, the agent must report the blocker without claiming the PR was created. Keep the procedure consistent with the `AGENTS.md` policy and the project's commands in `justfile`/`AGENTS.md`.

- [ ] **Step 3: Verify the policy and skill are consistent**

Run: `git diff --check`

Expected: no whitespace errors. Read both files and confirm that `AGENTS.md` makes the PR workflow mandatory, the skill gives the complete sequence, and neither suggests bypassing human review or running live ingestion pipelines as tests.

### Task 3: Add pull-request CI

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Workflow runs for `pull_request` events `opened`, `reopened`, and `synchronize`.
- The CI job checks the code with the project's existing offline test, lint, and type-check commands.

- [ ] **Step 1: Create a minimal read-only pull-request workflow**

Create `.github/workflows/ci.yml` with this workflow structure. Use the listed major versions for the setup actions, Python 3.13, read-only token permissions, and no secrets:

```yaml
name: CI

"on":
  pull_request:
    types: [opened, reopened, synchronize]

permissions:
  contents: read

jobs:
  checks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
      - uses: astral-sh/setup-uv@v6
      - run: uv sync --locked
      - run: uv run python -m unittest discover -s tests
      - run: uv run ruff check ingest_data
      - run: uv run ty check ingest_data
```

Do not add a `push` trigger or execute pipeline entrypoints; validation is tied to the PR lifecycle and remains safe for PRs from forks.

- [ ] **Step 2: Validate the CI commands locally**

Run:

```bash
uv sync --locked
uv run python -m unittest discover -s tests
uv run ruff check ingest_data
uv run ty check ingest_data
```

Expected: dependency sync completes and all three checks pass. If a check fails, diagnose and report it rather than changing unrelated application code as part of this workflow setup.

- [ ] **Step 3: Review the workflow trigger and permissions**

Run: `git diff --check`

Expected: no whitespace errors. Inspect `.github/workflows/ci.yml` and confirm it only requests read access, runs for the specified PR events, and never invokes a live integration pipeline.

### Task 4: Add PR review template and repository setup guidance

**Files:**
- Create: `.github/pull_request_template.md`
- Modify: `README.md`

**Interfaces:**
- The PR template prompts authors for change summary, validation, and risks/follow-ups, and reminds reviewers to inspect code and CI.
- README documents the one-time GitHub branch-protection settings corresponding to the CI job and human approval requirement.

- [ ] **Step 1: Create the pull-request template**

Use this concise template, keeping author-provided validation separate from the reviewer checklist:

```markdown
## Summary

<!-- What changed and why? -->

## Validation

- [ ] `uv run python -m unittest discover -s tests`
- [ ] `uv run ruff check ingest_data`
- [ ] `uv run ty check ingest_data`
- [ ] Other checks (describe below)

## Risks or follow-ups

<!-- Note risks, limitations, or follow-up work; write "None" if there are none. -->

## Reviewer checklist

- [ ] Reviewed the diff and behavior
- [ ] Confirmed required CI checks pass
- [ ] Recorded human review / approval
```

- [ ] **Step 2: Document required GitHub branch protection in `README.md`**

Add a short `## Pull request checks and review` section explaining that CI runs offline unit tests, Ruff, and ty on PRs. Tell a repository administrator to protect the base branch in GitHub settings, require the `CI / checks` status check, and require at least one approving human review before merge. State that branch protection is a one-time hosted-repository setting and is not enforced by the workflow file alone.

- [ ] **Step 3: Review documentation and final diff**

Run: `git diff --check`

Expected: no whitespace errors. Read the PR template and README section and confirm their instructions match the `AGENTS.md` policy, the `ship-pr` skill, and the CI job.

### Task 5: Final verification and GitHub handoff

**Files:**
- Verify: `AGENTS.md`
- Verify: `.agents/skills/ship-pr/SKILL.md`
- Verify: `.github/workflows/ci.yml`
- Verify: `.github/pull_request_template.md`
- Verify: `README.md`

**Interfaces:**
- All repository files agree on the same branch → checks → PR → human review lifecycle.

- [ ] **Step 1: Run the complete offline verification suite**

Run:

```bash
uv run python -m unittest discover -s tests
uv run ruff check ingest_data
uv run ty check ingest_data
git diff --check
```

Expected: all commands pass.

- [ ] **Step 2: Confirm GitHub setup requirements for the repository owner**

Summarize that the base branch must require the CI status check and one approving human review. Do not claim these hosted settings have been configured from repository files.

- [ ] **Step 3: Report the resulting changes and verification**

List the changed files, checks run and their results, and the GitHub branch-protection setup still required. If the changes are being delivered as a PR, report its URL and the current CI state.
