---
name: pipe
description: Use when adding a supermarket or retail catalog integration to this repository, including products, categories, prices, API feeds, sitemaps, or public product pages.
metadata:
  opencode/slash: "true"
---

# Add a retail catalog integration

Use this workflow for supermarkets and other retailers whose data is a product catalog: products, categories, and prices. When invoked as `/pipe <name>`, use the supplied name as the store slug and display name; ask if it is missing or ambiguous.

## Workflow

1. Read `AGENTS.md`, `README.md`, `justfile`, and the closest existing `source.py`, `pipeline.py`, and offline tests. Use `consum` as the REST API pattern, `carrefour` for sitemap/public-page extraction, and `mercadona` for a category-driven REST API. Adapt to the new source instead of copying an unrelated extraction pattern.
2. Establish the source contract from official API documentation or the retailer's public catalog: endpoint/page discovery, authentication, pagination, region/store selection, response shape, rate limits, and a stable product identifier. Do not invent these details. Ask the user for unresolved information that affects access, completeness, or product identity.
3. Add `ingest_data/<slug>/source.py` and `pipeline.py`. Use dlt and DuckDB, the shared `ingest_data.utils.run_pipeline` runner when it fits, `data/<slug>.duckdb`, and dataset `<slug>_raw`. Create category and product resources only when the source provides them. Keep extraction and destination setup separate.
4. For product history, use SCD2 with a verified, stable source product ID as the primary key, following the existing catalog integrations. If the source has no verified stable ID, stop and ask rather than substituting a name, row number, or guessed key. The current SCD2 version is queryable with `_dlt_valid_to IS NULL`.
5. Use ordinary, documented API or HTTP access. Keep credentials out of source files and use the project's established configuration pattern. If access is denied (including HTTP 403), report the restriction and ask for an authorized feed or endpoint; do not evade it with proxies, browser fingerprinting, CAPTCHA workarounds, or similar methods.
6. Add offline tests with mocked responses for parsing, pagination, identifiers, and declared resources. Update `justfile` and `README.md` with the new entrypoint and integration.
7. Run `uv run python -m unittest discover -s tests`, `uv run ruff check ingest_data`, and `uv run ty check ingest_data` (or `just check` when `just` is installed). Do not run a live pipeline as a test: pipeline entrypoints contact real sources and write DuckDB files under `data/`. Run one only when the user explicitly asks for a live ingestion.
8. Summarize the files and resources added, the source/access method, validation performed, and any unresolved blockers. Distinguish offline checks from live ingestion results.

## Scope

This workflow covers retail product catalogs, categories, and prices. For orders, reviews, loyalty data, or other non-catalog datasets, confirm the scope before using this workflow.
