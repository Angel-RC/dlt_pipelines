# Repository guidance

- Use `uv sync` to install dependencies; the project requires Python 3.12+ and `.python-version` pins 3.13.
- Use Ruff for linting and ty for type checking: `just lint`, `just typecheck`, or both with `just check`.
- Use `just --list` to see shortcuts; `just` must be installed separately from the Python dependencies.
- Each integration has a runnable entrypoint: `uv run ingest_data/{mercadona,consum,trustpilot,carrefour,klaviyo}/pipeline.py` (run one source at a time).
- Run offline tests with `uv run python -m unittest discover -s tests`; live pipeline entrypoints write DuckDB files under ignored `data/` and are not tests.
- Mercadona, Consum, Trustpilot, and Carrefour pipelines use `ingest_data/utils.py::run_pipeline`, which accepts an optional resource subset. Klaviyo has a separate runner; it reads `KLAVIYO_API_KEY` from the environment and skips the `products` resource.
- Carrefour reads its public sitemap and product pages with ordinary HTTP requests; the site may return 403, which the pipeline reports without attempting to bypass.

## Delivering changes

- Preserve pre-existing user changes and do implementation work on a feature branch, not directly on the base branch.
- After implementation, run relevant offline tests, lint, and type checks; do not run live ingestion pipelines as tests.
- If Ruff reports auto-fixable style or import issues, try `uv run ruff check --fix ingest_data`, review its diff, and rerun Ruff.
- Deliver completed implementation work through a GitHub PR: commit the changes, push the feature branch, and open the PR with `gh`. Include a summary, validation results, and known risks or follow-ups.
- Follow `.agents/skills/ship-pr/SKILL.md` for the delivery procedure. If `gh` or GitHub authentication is unavailable, report that blocker and do not claim a PR was opened.
