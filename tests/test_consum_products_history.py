import tempfile
import unittest
from pathlib import Path

import dlt
import duckdb
from dlt.destinations import duckdb as duckdb_destination

from ingest_data.consum import source as consum_source_module


class ConsumProductsHistoryTest(unittest.TestCase):
    def test_products_resource_loads_scd2_history_with_current_record(self) -> None:
        source_resource = consum_source_module.consum_source().resources["products"]
        schema = source_resource.compute_table_schema(
            {"id": "p-1", "price": 1.5}
        )
        self.assertEqual(schema["name"], "products_history")
        self.assertEqual(schema["x-merge-strategy"], "scd2")
        primary_keys = [
            name
            for name, column in schema["columns"].items()
            if column.get("primary_key")
        ]
        self.assertEqual(primary_keys, ["id"])

        records = [{"id": "p-1", "price": 1.5}]
        extraction_count = 0

        @dlt.resource(
            name=source_resource.name,
            table_name=schema["name"],
            primary_key=primary_keys,
            write_disposition={
                "disposition": schema["write_disposition"],
                "strategy": schema["x-merge-strategy"],
            },
        )
        def product_records():
            nonlocal extraction_count
            extraction_count += 1
            yield from records

        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "consum.duckdb"
            pipeline = dlt.pipeline(
                pipeline_name="consum_products_history_test",
                destination=duckdb_destination(credentials=str(database_path)),
                dataset_name="consum_raw",
            )

            pipeline.run(product_records)
            records[:] = [{"id": "p-1", "price": 1.75}]
            pipeline.run(product_records)

            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                history = connection.execute(
                    """
                    SELECT id, price, _dlt_valid_from, _dlt_valid_to
                    FROM consum_raw.products_history
                    ORDER BY _dlt_valid_from
                    """
                ).fetchall()
                current = connection.execute(
                    """
                    SELECT id, price
                    FROM consum_raw.products_history
                    WHERE _dlt_valid_to IS NULL
                    """
                ).fetchall()
                products_table = connection.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'consum_raw' AND table_name = 'products'
                    """
                ).fetchall()
            finally:
                connection.close()

        self.assertEqual(extraction_count, 2)
        self.assertEqual(
            [(row[0], row[1]) for row in history],
            [("p-1", 1.5), ("p-1", 1.75)],
        )
        self.assertIsNotNone(history[0][3])
        self.assertIsNone(history[1][3])
        self.assertEqual(current, [("p-1", 1.75)])
        self.assertEqual(products_table, [])


if __name__ == "__main__":
    unittest.main()
