from typing import Any, Optional
import json
from loguru import logger
import dlt
from dlt.destinations import duckdb
from ingest_data.consum.source import consum_source

def load_consum(resource_list = None) -> None:
    pipeline = dlt.pipeline(
        pipeline_name = "consum",
        destination   = duckdb(credentials="data/consum.duckdb"),
        dataset_name  = "consum_raw",
        progress      = dlt.progress.tqdm()
    )

    resource_list = resource_list or consum_source().resources.keys()

    for resource in resource_list:
        logger.info(f"Loading data for resource: {resource} ...")
        pipeline.run(consum_source().with_resources(resource))

if __name__ == "__main__":
    load_consum()
