from typing import Any, Optional
import json
from loguru import logger
import dlt
from ingest_data.klaviyo.source import klaviyo_source

def load_klaviyo(resource_list = None) -> None:
    pipeline = dlt.pipeline(
        pipeline_name = "klaviyo",
        destination   = "duckdb",
        dataset_name  = "klaviyo_raw",
        progress      = dlt.progress.tqdm()
    )

    resource_list = resource_list or klaviyo_source().resources.keys()

    for resource in resource_list:
        if resource != "products":
            try:
                logger.info(f"Loading data for resource: {resource} ...")
                pipeline.run(klaviyo_source().with_resources(resource))
            except Exception as e:
                logger.error(f"Loading data for resource: {resource} ...")
                logger.error(e)


if __name__ == "__main__":
    load_klaviyo()
