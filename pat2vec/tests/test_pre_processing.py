import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pat2vec.util.pre_processing import get_all_patient_list


class TestPreProcessing(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_config.verbosity = 0
        # Ensure mock attributes are None to allow fall-through logic in tests
        self.mock_config.all_patient_list = None
        self.mock_config.all_patient_list_path = None
        self.mock_config.all_patient_list_column = None
        self.mock_config.pre_document_batch_path = None

    @patch("pandas.read_csv")
    @patch("os.path.exists")
    def test_get_all_patient_list_from_csv(self, mock_exists, mock_read_csv):
        """Test extracting patient IDs from a source CSV file."""
        mock_exists.return_value = True
        mock_read_csv.return_value = pd.DataFrame({"client_idcode": ["P1", "P2", "P3"]})

        self.mock_config.all_patient_list_path = "patients.csv"
        self.mock_config.all_patient_list_column = "client_idcode"

        result = get_all_patient_list(self.mock_config)
        self.assertEqual(result, ["P1", "P2", "P3"])

    def test_get_all_patient_list_direct_config(self):
        """Test when the list is already provided in the config object."""
        self.mock_config.all_patient_list = ["P100", "P200"]
        self.mock_config.all_patient_list_path = None

        result = get_all_patient_list(self.mock_config)
        self.assertEqual(result, ["P100", "P200"])

    @patch("os.path.isdir")
    @patch("os.listdir")
    def test_get_all_patient_list_from_directory(self, mock_listdir, mock_isdir):
        """Test identifying patients based on filenames in a directory."""
        mock_isdir.return_value = True
        self.mock_config.pre_document_batch_path = "/data/batch"
        mock_listdir.return_value = ["P1.csv", "P2.csv", "metadata.txt"]

        result = get_all_patient_list(self.mock_config)
        # Should extract 'P1' and 'P2' from .csv files
        self.assertIn("P1", result)
        self.assertIn("P2", result)
        self.assertNotIn("metadata", result)
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
