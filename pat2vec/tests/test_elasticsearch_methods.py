import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np

from pat2vec.util.elasticsearch_methods import (
    handle_inconsistent_dtypes,
    get_guess_datetime_column,
    guess_datetime_columns,
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

    def test_guess_datetime_columns(self):
        """Test identifying all columns likely to contain dates."""
        df = pd.DataFrame(
            {
                "dates_1": ["2020-01-01", "2020-01-02", "A"],
                "dates_2": ["2021-01-01", "B", "2021-01-03"],
                "no_dates": ["X", "Y", "Z"],
            }
        )

        with patch(
            "pat2vec.util.elasticsearch_methods.tqdm", side_effect=lambda x, **kwargs: x
        ):
            cols = guess_datetime_columns(df, threshold=0.5)

        self.assertCountEqual(cols, ["dates_1", "dates_2"])

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

    @patch("pat2vec.util.elasticsearch_methods.Elasticsearch")
    @patch("pat2vec.util.elasticsearch_methods.helpers.streaming_bulk")
    def test_ingest_data_to_elasticsearch_success(
        self, mock_streaming_bulk, mock_es_cls
    ):
        """Test successful data ingestion into Elasticsearch."""
        mock_es_instance = MagicMock()
        mock_es_cls.return_value = mock_es_instance
        mock_es_instance.ping.return_value = True
        mock_es_instance.indices.exists.return_value = (
            False  # Index does not exist initially
        )

        # Simulate successful bulk ingestion
        mock_streaming_bulk.return_value = [
            (True, {"index": {"_id": "1"}}),
            (True, {"index": {"_id": "2"}}),
        ]

        df = pd.DataFrame({"id": [1, 2], "data": ["a", "b"]})
        index_name = "test_index"

        with patch("pat2vec.util.elasticsearch_methods.host_name", "localhost"):
            with patch("pat2vec.util.elasticsearch_methods.username", "elastic"):
                result = ingest_data_to_elasticsearch(df, index_name)

        mock_es_instance.indices.create.assert_called_once()
        mock_streaming_bulk.assert_called_once()
        self.assertEqual(result["success"], 2)
        self.assertEqual(result["failed"], 0)

    @patch("pat2vec.util.elasticsearch_methods.Elasticsearch")
    @patch("pat2vec.util.elasticsearch_methods.helpers.streaming_bulk")
    def test_ingest_data_to_elasticsearch_replace_index(
        self, mock_streaming_bulk, mock_es_cls
    ):
        """Test data ingestion with replace_index=True."""
        mock_es_instance = MagicMock()
        mock_es_cls.return_value = mock_es_instance
        mock_es_instance.ping.return_value = True

        # Index exists initially, then does not exist after deletion
        mock_es_instance.indices.exists.side_effect = [True, False]

        mock_streaming_bulk.return_value = [(True, {})]

        df = pd.DataFrame({"id": [1]})
        index_name = "test_index"

        with patch("pat2vec.util.elasticsearch_methods.host_name", "localhost"):
            with patch("pat2vec.util.elasticsearch_methods.username", "elastic"):
                ingest_data_to_elasticsearch(df, index_name, replace_index=True)

        mock_es_instance.indices.delete.assert_called_once_with(index=index_name)
        mock_es_instance.indices.create.assert_called_once()  # Should recreate after deleting

    @patch("pat2vec.util.elasticsearch_methods.Elasticsearch")
    @patch("pat2vec.util.elasticsearch_methods.helpers.streaming_bulk")
    def test_ingest_data_to_elasticsearch_bulk_error(
        self, mock_streaming_bulk, mock_es_cls
    ):
        """Test error handling during bulk ingestion."""
        mock_es_instance = MagicMock()
        mock_es_cls.return_value = mock_es_instance
        mock_es_instance.ping.return_value = True
        mock_es_instance.indices.exists.return_value = False

        # Simulate partial failure in bulk ingestion
        mock_streaming_bulk.return_value = [
            (True, {}),
            (
                False,
                {
                    "index": {
                        "error": {
                            "type": "mapper_parsing_exception",
                            "reason": "failed to parse field [data]",
                        }
                    }
                },
            ),
        ]

        df = pd.DataFrame({"id": [1, 2], "data": ["a", "b"]})
        index_name = "test_index"

        with patch("pat2vec.util.elasticsearch_methods.host_name", "localhost"):
            with patch("pat2vec.util.elasticsearch_methods.username", "elastic"):
                result = ingest_data_to_elasticsearch(df, index_name)

        self.assertEqual(result["success"], 1)
        self.assertEqual(result["failed"], 1)


if __name__ == "__main__":
    unittest.main()
