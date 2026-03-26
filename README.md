# Ingesta Supermercados

Pipeline de ingesta de datos de supermercados usando [dlt](https://dlthub.com/) y DuckDB.

## Descripción

Extrae, normaliza y carga en DuckDB el catálogo de productos y categorías de distintos supermercados, permitiendo hacer seguimiento histórico de precios.

## Supermercados

| Supermercado | Datos ingestados |
|---|---|
| Mercadona | Categorías, historial de productos (scd2) |
| Consum | Categorías, productos |

## Estructura

```
ingest_data/
├── mercadona/
│   ├── source.py    # Definición de la fuente REST API
│   └── pipeline.py  # Pipeline dlt
├── consum/
│   ├── source.py
│   └── pipeline.py
└── klaviyo/
    ├── source.py
    └── pipeline.py

data/
├── mercadona.duckdb
└── consum.duckdb
```

## Instalación

```bash
uv sync
```

## Uso

```bash
# Mercadona
uv run ingest_data/mercadona/pipeline.py

# Consum
uv run ingest_data/consum/pipeline.py
```

## Tecnologías

- **[dlt](https://dlthub.com/)**: extracción y carga de datos desde APIs REST
- **[DuckDB](https://duckdb.org/)**: base de datos destino
- **[uv](https://docs.astral.sh/uv/)**: gestión de dependencias
