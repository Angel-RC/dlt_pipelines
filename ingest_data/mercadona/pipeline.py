from loguru import logger
import dlt
from dlt.destinations import duckdb
from ingest_data.mercadona.source import mercadona_source
from ingest_data.utils import run_pipeline


def load_mercadona() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mercadona",
        destination=duckdb(credentials="data/mercadona.duckdb"),
        dataset_name="mercadona_raw",
        progress=dlt.progress.tqdm()
    )

    run_pipeline(pipeline, mercadona_source)


if __name__ == "__main__":
    load_mercadona()
