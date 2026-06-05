import unittest
import os
import shutil
import tempfile

import pandas as pd
from unittest.mock import MagicMock, patch
from pat2vec.util.retrieve_data import retrieve_patient_data, DATA_TYPE_CONFIG


class TestRetrieveData(unittest.TestCase):
    """Unit tests for the data retrieval utility module."""

    def setUp(self):
        """Set up temporary directory and mock configuration."""
        self.test_dir = tempfile.mkdtemp()
        self.config_obj = MagicMock()
        self.config_obj.verbosity = 0

        # Setup for file-based tests
        self.config_obj.storage_backend = "file"
        self.file_path_attr = "pre_document_batch_path"
        self.config_obj.pre_document_batch_path = os.path.join(
            self.test_dir, "epr_docs"
        )
        os.makedirs(self.config_obj.pre_document_batch_path)

        self.patient_id = "P1"
        self.sample_df = pd.DataFrame(
            {"client_idcode": [self.patient_id], "data": [123]}
        )
        self.sample_df.to_csv(
            os.path.join(
                self.config_obj.pre_document_batch_path, f"{self.patient_id}.csv"
            ),
            index=False,
        )

        # Setup for database-based tests
        self.config_obj.db_engine = MagicMock()

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    def test_retrieve_patient_data_file_success(self):
        """Test successful retrieval of patient data from a file."""
        result_df = retrieve_patient_data(self.patient_id, "epr_docs", self.config_obj)
        self.assertFalse(result_df.empty)
        self.assertEqual(result_df.iloc[0]["client_idcode"], self.patient_id)
        self.assertEqual(result_df.iloc[0]["data"], 123)

    def test_retrieve_patient_data_file_not_found(self):
        """Test retrieval from a file that does not exist."""
        non_existent_patient = "P999"
        result_df = retrieve_patient_data(
            non_existent_patient, "epr_docs", self.config_obj
        )
        self.assertTrue(result_df.empty)

    def test_retrieve_patient_data_file_missing_config_attr(self):
        """Test retrieval when the config object lacks the required path attribute."""
        del self.config_obj.pre_document_batch_path  # Simulate missing attribute
        result_df = retrieve_patient_data(self.patient_id, "epr_docs", self.config_obj)
        self.assertTrue(result_df.empty)

    @patch("pat2vec.util.retrieve_data.get_df_from_db")
    def test_retrieve_patient_data_db_success(self, mock_get_df_from_db):
        """Test successful retrieval of patient data from the database."""
        self.config_obj.storage_backend = "database"
        mock_get_df_from_db.return_value = self.sample_df.copy()

        result_df = retrieve_patient_data(self.patient_id, "epr_docs", self.config_obj)
        self.assertFalse(result_df.empty)
        self.assertEqual(result_df.iloc[0]["client_idcode"], self.patient_id)
        mock_get_df_from_db.assert_called_once_with(
            self.config_obj,
            DATA_TYPE_CONFIG["epr_docs"]["db_schema"],
            DATA_TYPE_CONFIG["epr_docs"]["db_table"],
            patient_ids=[self.patient_id],
            patient_id_column=DATA_TYPE_CONFIG["epr_docs"]["id_column"],
        )

    @patch("pat2vec.util.retrieve_data.get_df_from_db")
    def test_retrieve_patient_data_db_not_found(self, mock_get_df_from_db):
        """Test retrieval from the database when no data is found."""
        self.config_obj.storage_backend = "database"
        mock_get_df_from_db.return_value = pd.DataFrame()  # Simulate no data

        result_df = retrieve_patient_data(self.patient_id, "epr_docs", self.config_obj)
        self.assertTrue(result_df.empty)

    def test_retrieve_patient_data_unknown_type(self):
        """Test retrieval with an unknown data type."""
        with self.assertLogs("pat2vec.util.retrieve_data", level="ERROR") as cm:
            result_df = retrieve_patient_data(
                self.patient_id, "unknown_type", self.config_obj
            )
            self.assertTrue(result_df.empty)
            self.assertIn("Unknown data type: 'unknown_type'", cm.output[0])

    def test_retrieve_patient_data_file_empty_csv(self):
        """Test retrieval from an empty CSV file."""
        empty_df_path = os.path.join(
            self.config_obj.pre_document_batch_path, "P_empty.csv"
        )
        pd.DataFrame().to_csv(empty_df_path, index=False)
        result_df = retrieve_patient_data("P_empty", "epr_docs", self.config_obj)
        self.assertTrue(result_df.empty)

    @patch(
        "pat2vec.util.retrieve_data.pd.read_csv",
        side_effect=Exception("Simulated read error"),
    )
    def test_retrieve_patient_data_file_read_error(self, mock_read_csv):
        """Test retrieval from a file that causes a read error."""
        with self.assertLogs("pat2vec.util.retrieve_data", level="ERROR") as cm:
            result_df = retrieve_patient_data(
                self.patient_id, "epr_docs", self.config_obj
            )
            self.assertTrue(result_df.empty)
            self.assertIn("Error reading file", cm.output[0])

    def test_retrieve_patient_data_db_missing_db_engine(self):
        """Test database retrieval when db_engine is not configured."""
        self.config_obj.storage_backend = "database"
        del self.config_obj.db_engine  # Simulate missing db_engine
        result_df = retrieve_patient_data(self.patient_id, "epr_docs", self.config_obj)
        self.assertTrue(result_df.empty)


if __name__ == "__main__":
    unittest.main()
