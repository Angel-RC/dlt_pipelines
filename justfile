default:
    @just --list

sync:
    uv sync

lint:
    uv run ruff check ingest_data

typecheck:
    uv run ty check ingest_data

check:
    just lint
    just typecheck

mercadona:
    uv run ingest_data/mercadona/pipeline.py

consum:
    uv run ingest_data/consum/pipeline.py

trustpilot:
    uv run ingest_data/trustpilot/pipeline.py

carrefour:
    uv run ingest_data/carrefour/pipeline.py

klaviyo:
    uv run ingest_data/klaviyo/pipeline.py
