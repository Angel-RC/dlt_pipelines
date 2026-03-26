import dlt
from dlt.destinations import duckdb
from ingest_data.trustpilot.source import source_trustpilot
from ingest_data.utils import run_pipeline


def load_trustpilot(resources=None) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="trustpilot",
        destination=duckdb(credentials="data/trustpilot.duckdb"),
        dataset_name="trustpilot_raw",
        progress=dlt.progress.tqdm()
    )

    run_pipeline(pipeline, source_trustpilot, resources)


if __name__ == "__main__":
    load_trustpilot()
