import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pat2vec.util.retrieve_data import retrieve_patient_data


class TestRetrieveData(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_config.storage_backend = "file"
        self.mock_config.pre_bloods_batch_path = "/data/bloods"
        self.patient_id = "P123"

    @patch("pandas.read_csv")
    def test_retrieve_patient_data_file_success(self, mock_read_csv):
        """Test successful CSV retrieval for a known data type."""
        mock_df = pd.DataFrame({"val": [1, 2]})
        mock_read_csv.return_value = mock_df

        result = retrieve_patient_data(self.patient_id, "bloods", self.mock_config)

        self.assertEqual(len(result), 2)
        mock_read_csv.assert_called_with("/data/bloods/P123.csv")

    def test_retrieve_patient_data_invalid_type(self):
        """Ensure an empty DataFrame is returned for an unsupported data type."""
        result = retrieve_patient_data(
            self.patient_id, "unsupported_type", self.mock_config
        )
        self.assertTrue(result.empty)

    @patch("pat2vec.util.retrieve_data.get_df_from_db")
    def test_retrieve_patient_data_database(self, mock_get_db):
        """Test retrieval via the database backend."""
        self.mock_config.storage_backend = "database"
        mock_df = pd.DataFrame({"val": [10]})
        mock_get_db.return_value = mock_df

        result = retrieve_patient_data(self.patient_id, "epr_docs", self.mock_config)

        self.assertEqual(len(result), 1)
        mock_get_db.assert_called_once_with(
            self.mock_config,
            "raw_data",
            "raw_epr_docs",
            patient_ids=[self.patient_id],
            patient_id_column="client_idcode",
        )

    @patch("pandas.read_csv", side_effect=FileNotFoundError)
    def test_retrieve_patient_data_file_missing(self, mock_read_csv):
        """Ensure an empty DataFrame is returned if the file is missing."""
        result = retrieve_patient_data(self.patient_id, "bloods", self.mock_config)
        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
