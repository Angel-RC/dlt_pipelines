import dlt
from dlt.destinations import duckdb
from ingest_data.consum.source import consum_source
from ingest_data.utils import run_pipeline


def load_consum(resources=None) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="consum",
        destination=duckdb(credentials="data/consum.duckdb"),
        dataset_name="consum_raw",
        progress=dlt.progress.tqdm()
    )

    run_pipeline(pipeline, consum_source, resources)


if __name__ == "__main__":
    load_consum()
