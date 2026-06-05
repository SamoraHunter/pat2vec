import unittest
import pandas as pd
import numpy as np
import os
import shutil
import tempfile
import pickle
from datetime import datetime
from pat2vec.util.post_processing_dataframe import (
    extract_datetime_to_column,
    extract_datetime_from_binary_columns,
    extract_datetime_from_binary_columns_chunk_reader,
    drop_columns_with_all_nan,
    save_missing_values_pickle,
    convert_true_to_float,
    impute_datetime,
    impute_dataframe,
    missing_percentage_df,
    aggregate_dataframe_mean,
)


class TestPostProcessingDataframe(unittest.TestCase):
    """Unit tests for the dataframe post-processing and imputation utilities."""

    def setUp(self):
        """Set up temporary directory."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    def test_extract_datetime_to_column_basic(self):
        """Test extracting datetime from binary columns using vectorization."""
        df = pd.DataFrame(
            {
                "val": [1, 2],
                "(2023, 01, 01)_date_time_stamp": [1, 0],
                "(2023, 01, 02)_date_time_stamp": [0, 1],
            }
        )
        result = extract_datetime_to_column(df.copy(), drop=True)
        self.assertIn("extracted_datetime_stamp", result.columns)
        self.assertEqual(
            result["extracted_datetime_stamp"].iloc[0], pd.Timestamp("2023-01-01")
        )
        self.assertEqual(
            result["extracted_datetime_stamp"].iloc[1], pd.Timestamp("2023-01-02")
        )
        # Verify that original columns are dropped
        self.assertNotIn("(2023, 01, 01)_date_time_stamp", result.columns)

    def test_extract_datetime_from_binary_columns_iterative(self):
        """Test the iterative extraction method with NaT handling."""
        df = pd.DataFrame(
            {
                "(2023, 05, 10)_date_time_stamp": [1, 0, 0],
                "(2023, 05, 11)_date_time_stamp": [0, 1, 0],
            }
        )
        result = extract_datetime_from_binary_columns(df.copy())
        self.assertIn("datetime", result.columns)
        self.assertEqual(result["datetime"].iloc[0], datetime(2023, 5, 10))
        self.assertTrue(pd.isna(result["datetime"].iloc[2]))  # Third row is all 0s

    def test_extract_datetime_chunk_reader(self):
        """Test chunked CSV reading and processing."""
        csv_path = os.path.join(self.test_dir, "chunks.csv")
        df = pd.DataFrame(
            {"id": [1, 2, 3, 4], "(2023, 01, 01)_date_time_stamp": [1, 1, 0, 0]}
        )
        df.to_csv(csv_path, index=False)

        # Read in chunks of 2
        result = extract_datetime_from_binary_columns_chunk_reader(
            csv_path, chunk_size=2
        )
        self.assertEqual(len(result), 4)
        self.assertIn("datetime", result.columns)
        self.assertEqual(result["datetime"].iloc[0], datetime(2023, 1, 1))

    def test_drop_columns_with_all_nan(self):
        """Test dropping completely empty columns."""
        df = pd.DataFrame(
            {"useful": [1, 2], "empty": [np.nan, np.nan], "mixed": [1, np.nan]}
        )
        res_df, dropped = drop_columns_with_all_nan(df.copy())
        self.assertNotIn("empty", res_df.columns)
        self.assertIn("mixed", res_df.columns)
        self.assertListEqual(dropped.tolist(), ["empty"])

    def test_impute_datetime_temporal(self):
        """Test forward and backward filling of timestamps within patient groups."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P1", "P1"],
                "datetime": [
                    pd.Timestamp("2023-01-01"),
                    pd.NaT,
                    pd.Timestamp("2023-01-03"),
                ],
                "data": [10, 20, 30],
            }
        )
        # This function sorts the DF, ffills/bfills non-patient columns based on group
        result = impute_datetime(
            df.copy(), forward=True, backward=True, mean_impute=False
        )
        # The NaT at index 1 should be filled by 2023-01-01
        self.assertEqual(result.iloc[1]["datetime"], pd.Timestamp("2023-01-01"))

    def test_impute_dataframe_numeric(self):
        """Test numeric imputation with grouping and global mean fallback."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P1", "P2"],
                "datetime": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-01"]),
                "val": [100.0, np.nan, np.nan],
            }
        )
        # Forward fill for P1: index 1 gets 100.0.
        # P2 is isolated: ffill does nothing. mean_impute fills it with overall mean.
        # Mean calculation: (100+100)/2 = 100.
        result = impute_dataframe(df.copy(), verbose=False)
        self.assertEqual(result.iloc[1]["val"], 100.0)
        self.assertEqual(result.iloc[2]["val"], 100.0)

    def test_aggregate_dataframe_mean(self):
        """Test aggregation of numeric columns while picking 'first' for strings."""
        df = pd.DataFrame(
            {"client_idcode": ["P1", "P1"], "score": [10, 20], "category": ["A", "B"]}
        )
        result = aggregate_dataframe_mean(df)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["score"], 15.0)
        self.assertEqual(result.iloc[0]["category"], "A")

    def test_save_missing_values_pickle(self):
        """Test calculation and export of missing value percentages."""
        df = pd.DataFrame({"A": [1, np.nan, np.nan], "B": [1, 2, 3]})
        out_path = os.path.join(self.test_dir, "data.csv")
        save_missing_values_pickle(df, out_path)

        expected_pickle = os.path.join(self.test_dir, "data_missing_dict.pickle")
        self.assertTrue(os.path.exists(expected_pickle))
        with open(expected_pickle, "rb") as f:
            stats = pickle.load(f)
        self.assertAlmostEqual(stats["A"], 66.6666, places=1)
        self.assertEqual(stats["B"], 0.0)

    def test_convert_true_to_float(self):
        """Test conversion of boolean-like strings to floating point numbers."""
        df = pd.DataFrame({"census_white": ["True", "False", np.nan]})
        result = convert_true_to_float(df.copy(), columns=["census_white"])
        self.assertEqual(result["census_white"].iloc[0], 1.0)
        self.assertEqual(result["census_white"].iloc[1], 0.0)
        self.assertTrue(pd.isna(result["census_white"].iloc[2]))

    def test_missing_percentage_df(self):
        """Test calculation of missing percentage per column."""
        df = pd.DataFrame(
            {"A": [1, np.nan, 3], "B": [np.nan, np.nan, np.nan], "C": [1, 2, 3]}
        )
        result = missing_percentage_df(df)
        self.assertEqual(len(result), 3)
        self.assertAlmostEqual(
            result[result["Column"] == "A"]["MissingPercentage"].iloc[0],
            33.3333,
            places=3,
        )
        self.assertEqual(
            result[result["Column"] == "B"]["MissingPercentage"].iloc[0], 100.0
        )


if __name__ == "__main__":
    unittest.main()
