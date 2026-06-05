import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from pat2vec.pat2vec_search.cogstack_search_methods import (
    check_patients_existence,
    list_chunker,
    set_index_safe_wrapper,
    initialize_cogstack_client,
)


class TestCogstackSearchMethods(unittest.TestCase):
    """Unit tests for the CogStack search utility module."""

    def test_list_chunker(self):
        """Test splitting a list into chunks of 10,000."""
        big_list = list(range(25000))
        chunks = list_chunker(big_list)
        self.assertEqual(len(chunks), 3)
        self.assertEqual(len(chunks[0]), 10000)
        self.assertEqual(len(chunks[2]), 5000)

    def test_set_index_safe_wrapper_failure(self):
        """Test graceful failure when the 'id' column is missing."""
        df = pd.DataFrame({"not_id": [1, 2]})
        # Should return original and not raise exception
        result = set_index_safe_wrapper(df.copy())
        self.assertIsNone(result.index.name)

    @patch("pat2vec.pat2vec_search.cogstack_search_methods.initialize_cogstack_client")
    def test_check_patients_existence_basic(self, mock_initialize):
        """Test patient existence check using terms aggregation."""
        # Setup mocks
        mock_cs = MagicMock()
        mock_initialize.return_value = mock_cs

        # Mock aggregation response
        mock_response = {
            "aggregations": {
                "existing_ids": {"buckets": [{"key": "P1"}, {"key": "P2"}]}
            }
        }
        mock_cs.elastic.search.return_value = mock_response

        patient_ids = ["P1", "P2", "P3"]
        result = check_patients_existence(
            patient_ids, index_name="test_index", id_field="pid"
        )

        # P1 and P2 found, P3 missing
        self.assertCountEqual(result, ["P1", "P2"])

    @patch("pat2vec.pat2vec_search.cogstack_search_methods.initialize_cogstack_client")
    def test_check_patients_existence_fielddata_fallback(self, mock_initialize):
        """Test fallback to search-based check when fielddata is disabled (aggregation fails)."""
        mock_cs = MagicMock()
        mock_initialize.return_value = mock_cs

        # First call fails with fielddata error
        mock_cs.elastic.search.side_effect = [
            Exception("fielddata=true"),  # Simplified error message for mocking
            {"hits": {"hits": [{"_source": {"pid": "P1"}}]}},
        ]

        patient_ids = ["P1"]
        result = check_patients_existence(patient_ids, id_field="pid")

        self.assertCountEqual(result, ["P1"])
        self.assertEqual(mock_cs.elastic.search.call_count, 2)

    @patch("importlib.util.spec_from_file_location")
    def test_initialize_cogstack_client_from_config_path(self, mock_spec):
        """Test initializing the client using a specific credentials path in config."""
        mock_config = MagicMock()
        mock_config.credentials_path = "/fake/path/creds.py"

        with patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.CogStack"
        ) as mock_cogstack:
            initialize_cogstack_client(mock_config)
            mock_cogstack.assert_called_once()
