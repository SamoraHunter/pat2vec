import unittest
from unittest.mock import patch
import pandas as pd
import numpy as np

from pat2vec.util.elasticsearch_methods import (
    handle_inconsistent_dtypes,
    get_guess_datetime_column,
    ingest_data_to_elasticsearch,
)


class TestElasticsearchMethods(unittest.TestCase):

    def test_handle_inconsistent_dtypes(self):
        """Test casting columns to the majority data type."""
        df = pd.DataFrame(
            {
                "mixed_ints": [1, 2, 3, "4"],
                "mixed_dates": ["2023-01-01", "2023-01-02", "2023-01-03", np.nan],
            }
        )

        # Silent TQDM during tests
        with patch(
            "pat2vec.util.elasticsearch_methods.tqdm", side_effect=lambda x, **kwargs: x
        ):
            result = handle_inconsistent_dtypes(df)

        self.assertTrue(
            pd.api.types.is_integer_dtype(result["mixed_ints"])
            or pd.api.types.is_numeric_dtype(result["mixed_ints"])
        )
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["mixed_dates"]))

    def test_guess_datetime_column(self):
        """Test identifying the column most likely to contain dates."""
        df = pd.DataFrame(
            {
                "not_dates": ["A", "B", "C"],
                "mostly_dates": ["2020-01-01", "2021-02-02", "invalid"],
            }
        )

        with patch(
            "pat2vec.util.elasticsearch_methods.tqdm", side_effect=lambda x, **kwargs: x
        ):
            col = get_guess_datetime_column(df)

        self.assertEqual(col, "mostly_dates")

    @patch("pat2vec.util.elasticsearch_methods.Elasticsearch")
    def test_ingest_data_safety_block_remote_host(self, mock_es_cls):
        """Verify that ingestion is blocked for non-local/test hosts."""
        df = pd.DataFrame({"data": [1]})

        # Mocking credentials to look like a remote prod server
        with patch(
            "pat2vec.util.elasticsearch_methods.host_name", "prod-cluster.internal"
        ):
            with self.assertRaises(ConnectionError) as cm:
                ingest_data_to_elasticsearch(df, "test_index")
            self.assertIn("denied", str(cm.exception))

    @patch("pat2vec.util.elasticsearch_methods.Elasticsearch")
    def test_ingest_data_safety_block_unauthorized_user(self, mock_es_cls):
        """Verify that ingestion is blocked for non-test users."""
        df = pd.DataFrame({"data": [1]})

        with patch("pat2vec.util.elasticsearch_methods.host_name", "localhost"):
            with patch("pat2vec.util.elasticsearch_methods.username", "admin_user"):
                with self.assertRaises(ConnectionError):
                    ingest_data_to_elasticsearch(df, "test_index")


if __name__ == "__main__":
    unittest.main()
