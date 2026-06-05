import unittest
import pandas as pd
import os
import tempfile
import shutil
from unittest.mock import MagicMock, patch
from datetime import datetime
from pat2vec.util.pre_processing import (
    draw_document_samples,
    demo_to_latest,
    calculate_age_append,
    get_all_patient_list,
)


class TestPreProcessing(unittest.TestCase):
    """Unit tests for the pre-processing utility module."""

    def setUp(self):
        """Set up temporary directory."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    def test_calculate_age_append(self):
        """Test age calculation and column appending."""
        df = pd.DataFrame(
            {"client_idcode": ["P1", "P2"], "client_dob": ["1990-01-01", "2000-01-01"]}
        )

        # Mock datetime.now() to ensure a fixed reference point for age calculation
        with patch("pat2vec.util.pre_processing.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 1)
            mock_datetime.strptime.side_effect = datetime.strptime
            mock_datetime.combine.side_effect = datetime.combine

            result = calculate_age_append(df.copy())

        self.assertIn("age", result.columns)
        self.assertEqual(result.iloc[0]["age"], 34)
        self.assertEqual(result.iloc[1]["age"], 24)

    def test_demo_to_latest(self):
        """Test keeping only the most recent demographic entry per patient."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P1", "P2"],
                "updatetime": ["2023-01-01", "2023-02-01", "2023-01-15"],
                "note": ["old", "new", "only"],
            }
        )
        result = demo_to_latest(df)
        self.assertEqual(len(result), 2)
        # Verify P1 got the latest record
        p1_res = result[result["client_idcode"] == "P1"].iloc[0]
        self.assertEqual(p1_res["note"], "new")

    def test_draw_document_samples(self):
        """Test stratified sampling by search term."""
        df = pd.DataFrame(
            {"search_term": ["A", "A", "A", "B", "B"], "content": [1, 2, 3, 4, 5]}
        )

        # Sample 2 from each group. Group A has 3, B has 2.
        result = draw_document_samples(df, n=2)
        self.assertEqual(len(result), 4)
        self.assertEqual(result["search_term"].value_counts()["A"], 2)
        self.assertEqual(result["search_term"].value_counts()["B"], 2)

    def test_get_all_patient_list_logic(self):
        """Test the priority-based patient list retrieval."""
        config = MagicMock()

        # 1. From direct attribute
        config.all_patient_list = ["P1"]
        self.assertEqual(get_all_patient_list(config), ["P1"])

        # 2. From CSV file
        config.all_patient_list = None
        config.all_patient_list_path = os.path.join(self.test_dir, "pats.csv")
        config.all_patient_list_column = "client_id"
        pd.DataFrame({"client_id": ["P2", "P3"]}).to_csv(
            config.all_patient_list_path, index=False
        )
        self.assertEqual(get_all_patient_list(config), ["P2", "P3"])

        # 3. From directory scan
        config.all_patient_list_path = None
        config.pre_document_batch_path = os.path.join(self.test_dir, "batches")
        os.makedirs(config.pre_document_batch_path)
        open(os.path.join(config.pre_document_batch_path, "P4.csv"), "w").close()
        open(os.path.join(config.pre_document_batch_path, "P5.csv"), "w").close()

        # Using assertCountEqual because file system order is non-deterministic
        self.assertCountEqual(get_all_patient_list(config), ["P4", "P5"])


if __name__ == "__main__":
    unittest.main()
