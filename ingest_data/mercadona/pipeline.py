from loguru import logger
import dlt
from dlt.destinations import duckdb
from ingest_data.mercadona.source import mercadona_source


def load_mercadona() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="mercadona",
        destination=duckdb(credentials="data/mercadona.duckdb"),
        dataset_name="mercadona_raw",
        progress=dlt.progress.tqdm()
    )

    resource_list = mercadona_source().resources.keys()

    for resource in resource_list:
        logger.info(f"Loading data for resource: {resource} ...")
        pipeline.run(mercadona_source().with_resources(resource))


if __name__ == "__main__":
    load_mercadona()
