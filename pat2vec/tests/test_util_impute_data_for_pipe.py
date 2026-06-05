import unittest
import pandas as pd
import numpy as np
import os
import shutil
import tempfile
import pickle
from unittest.mock import patch
from pat2vec.util.impute_data_for_pipe import (
    mean_impute_dataframe,
    save_missing_percentage,
)


class TestImputeDataForPipe(unittest.TestCase):
    """Unit tests for data imputation and missing value analysis utilities."""

    def setUp(self):
        """Set up a temporary directory for test outputs."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up the temporary directory."""
        shutil.rmtree(self.test_dir)

    def test_mean_impute_dataframe_basic(self):
        """Test basic mean imputation with a single target variable."""
        df = pd.DataFrame(
            {
                "feature1": [1, 2, np.nan, 4],
                "feature2": [5, np.nan, 7, 8],
                "target": [0, 1, 0, 1],
            }
        )
        imputed_df = mean_impute_dataframe(df.copy(), y_vars="target", random_state=42)

        self.assertFalse(imputed_df["feature1"].isnull().any())
        self.assertFalse(imputed_df["feature2"].isnull().any())
        self.assertEqual(len(imputed_df), len(df))

        # Verify that the imputed values are close to the mean of the training set
        # Due to random splits, we can't assert exact mean, but can check for non-NaN
        # and reasonable values.
        self.assertTrue(imputed_df["feature1"].iloc[2] > 0)
        self.assertTrue(imputed_df["feature2"].iloc[1] > 0)

    def test_mean_impute_dataframe_multiple_y_vars(self):
        """Test mean imputation with multiple target variables."""
        df = pd.DataFrame(
            {
                "feature1": [1, np.nan, 3],
                "target1": [0, 1, 0],
                "target2": ["A", "B", "A"],
            }
        )
        imputed_df = mean_impute_dataframe(
            df.copy(), y_vars=["target1", "target2"], random_state=42
        )

        self.assertFalse(imputed_df["feature1"].isnull().any())
        self.assertEqual(len(imputed_df), len(df))
        self.assertIn("target1", imputed_df.columns)
        self.assertIn("target2", imputed_df.columns)

    def test_mean_impute_dataframe_empty_columns(self):
        """Test handling of columns that are entirely NaN."""
        df = pd.DataFrame(
            {
                "feature1": [1, 2, 3],
                "empty_col": [np.nan, np.nan, np.nan],
                "target": [0, 1, 0],
            }
        )
        imputed_df = mean_impute_dataframe(df.copy(), y_vars="target", random_state=42)

        self.assertFalse(imputed_df["feature1"].isnull().any())
        self.assertFalse(imputed_df["empty_col"].isnull().any())
        # Completely empty columns should be imputed with 0
        self.assertTrue((imputed_df["empty_col"] == 0).all())

    def test_mean_impute_dataframe_no_numeric_columns(self):
        """Test behavior when no numeric columns are present for imputation."""
        df = pd.DataFrame({"feature_str": ["a", "b", "c"], "target": [0, 1, 0]})
        imputed_df = mean_impute_dataframe(df.copy(), y_vars="target", random_state=42)

        self.assertFalse(imputed_df.isnull().any().any())  # No NaNs should remain
        self.assertEqual(len(imputed_df), len(df))
        self.assertIn("feature_str", imputed_df.columns)

    @patch("pat2vec.util.impute_data_for_pipe.logger")
    def test_mean_impute_dataframe_no_nans_warning(self, mock_logger):
        """Test that no warning is logged if no NaNs are present after imputation."""
        df = pd.DataFrame({"feature1": [1, 2, 3], "target": [0, 1, 0]})
        mean_impute_dataframe(df.copy(), y_vars="target", random_state=42)
        mock_logger.warning.assert_not_called()

    def test_save_missing_percentage_basic(self):
        """Test basic functionality of saving missing percentages."""
        df = pd.DataFrame(
            {"col1": [1, 2, np.nan], "col2": [4, 5, 6], "col3": [np.nan, np.nan, 9]}
        )
        output_file = os.path.join(self.test_dir, "test_missing.pkl")
        missing_percentages = save_missing_percentage(df, output_file)

        self.assertTrue(os.path.exists(output_file))
        with open(output_file, "rb") as f:
            loaded_data = pickle.load(f)

        self.assertAlmostEqual(loaded_data["col1"], 33.33333, places=5)
        self.assertEqual(loaded_data["col2"], 0.0)
        self.assertAlmostEqual(loaded_data["col3"], 66.66667, places=5)
        self.assertEqual(missing_percentages["col2"], 0.0)

    def test_save_missing_percentage_empty_df(self):
        """Test saving missing percentages for an empty DataFrame."""
        df = pd.DataFrame(columns=["colA", "colB"])
        output_file = os.path.join(self.test_dir, "empty_missing.pkl")
        missing_percentages = save_missing_percentage(df, output_file)

        self.assertTrue(os.path.exists(output_file))
        with open(output_file, "rb") as f:
            loaded_data = pickle.load(f)

        self.assertEqual(len(loaded_data), 2)
        self.assertEqual(loaded_data["colA"], 100.0)
        self.assertEqual(missing_percentages["colB"], 100.0)


if __name__ == "__main__":
    unittest.main()
