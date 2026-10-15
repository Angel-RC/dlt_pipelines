import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ingest_data.trustpilot.source import get_build_id


class TrustpilotBuildIdTest(unittest.TestCase):
    def test_get_build_id_extracts_build_id_from_response(self) -> None:
        response = SimpleNamespace(text='{"buildId": "build-123"}')
        with patch(
            "ingest_data.trustpilot.source.requests.get", return_value=response
        ):
            self.assertEqual(get_build_id("https://example.test"), "build-123")

    def test_get_build_id_raises_value_error_when_missing(self) -> None:
        response = SimpleNamespace(text="<html>no build data</html>")
        with patch(
            "ingest_data.trustpilot.source.requests.get", return_value=response
        ):
            with self.assertRaisesRegex(ValueError, "buildId"):
                get_build_id("https://example.test")


if __name__ == "__main__":
    unittest.main()
