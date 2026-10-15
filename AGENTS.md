# Repository guidance

- Use `uv sync` to install dependencies; the project requires Python 3.12+ and `.python-version` pins 3.13.
- Use Ruff for linting and ty for type checking: `just lint`, `just typecheck`, or both with `just check`.
- Use `just --list` to see shortcuts; `just` must be installed separately from the Python dependencies.
- Each integration has a runnable entrypoint: `uv run ingest_data/{mercadona,consum,trustpilot,carrefour,klaviyo}/pipeline.py` (run one source at a time).
- Run offline tests with `uv run python -m unittest discover -s tests`; live pipeline entrypoints write DuckDB files under ignored `data/` and are not tests.
- Mercadona, Consum, Trustpilot, and Carrefour pipelines use `ingest_data/utils.py::run_pipeline`, which accepts an optional resource subset. Klaviyo has a separate runner; it reads `KLAVIYO_API_KEY` from the environment and skips the `products` resource.
- Carrefour reads its public sitemap and product pages with ordinary HTTP requests; the site may return 403, which the pipeline reports without attempting to bypass.
