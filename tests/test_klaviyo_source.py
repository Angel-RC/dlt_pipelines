import os
import unittest
from unittest.mock import patch

from ingest_data.klaviyo.source import klaviyo_source


class KlaviyoSourceTest(unittest.TestCase):
    def test_source_requires_api_key_when_environment_and_argument_are_empty(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "KLAVIYO_API_KEY"):
                klaviyo_source(api_key=None)


if __name__ == "__main__":
    unittest.main()
