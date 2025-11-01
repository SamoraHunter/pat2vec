import unittest
import pandas as pd

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp


class TestFilterDataFrameByTimestamp(unittest.TestCase):
    """Unit tests for the filter_dataframe_by_timestamp function."""

    def setUp(self):
        """Set up a sample DataFrame for testing."""
        data = {
            "timestamp": [
                "2023-01-15 10:00:00",  # Before range
                "2023-02-01 00:00:00",  # Start of range
                "2023-02-20 12:30:00",  # Within range
                "2023-03-31 23:59:59",  # End of range
                "2023-04-05 08:00:00",  # After range
                "not a date",  # Invalid date
                None,  # Missing value
                "2023-02-15 05:00:00+01:00",  # With timezone (becomes 2023-02-15 04:00:00 UTC)
            ],
            "value": [1, 2, 3, 4, 5, 6, 7, 8],
        }
        self.df = pd.DataFrame(data)

    def test_basic_filtering(self):
        """Test basic filtering within a date range."""
        filtered = filter_dataframe_by_timestamp(
            self.df.copy(),
            start_year=2023,
            start_month=2,
            start_day=1,
            end_year=2023,
            end_month=3,
            end_day=31,
            timestamp_string="timestamp",
        )
        # Expected: 2023-02-01, 2023-02-20, 2023-03-31, 2023-02-15 (from timezone conversion)
        self.assertEqual(len(filtered), 4)
        self.assertCountEqual(filtered["value"].tolist(), [2, 3, 4, 8])

    def test_inclusive_boundaries(self):
        """Test if the start and end boundaries are inclusive."""
        df = pd.DataFrame(
            {
                "timestamp": ["2023-02-01 00:00:00", "2023-03-31 23:59:59"],
                "value": [1, 2],
            }
        )
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=2023,
            start_month=2,
            start_day=1,
            end_year=2023,
            end_month=3,
            end_day=31,
            timestamp_string="timestamp",
        )
        self.assertEqual(len(filtered), 2)

    def test_empty_result_for_no_match(self):
        """Test a date range that should yield no results."""
        filtered = filter_dataframe_by_timestamp(
            self.df.copy(),
            start_year=2024,
            start_month=1,
            start_day=1,
            end_year=2024,
            end_month=12,
            end_day=31,
            timestamp_string="timestamp",
        )
        self.assertTrue(filtered.empty)

    def test_swapped_start_and_end_dates(self):
        """Test if the function handles swapped start and end dates correctly."""
        filtered = filter_dataframe_by_timestamp(
            self.df.copy(),
            start_year=2023,
            start_month=3,
            start_day=31,
            end_year=2023,
            end_month=2,
            end_day=1,
            timestamp_string="timestamp",
        )
        # When dates are swapped (start=2023-03-31, end=2023-02-01),
        # function should swap them to (start=2023-02-01, end=2023-03-31)
        # Should get same results as basic filtering: values 2, 3, 4, 8
        self.assertEqual(len(filtered), 4)
        self.assertCountEqual(filtered["value"].tolist(), [2, 3, 4, 8])

    def test_handling_of_invalid_and_null_dates(self):
        """Test how the function handles non-date strings and None values."""
        # The function should automatically exclude NaN values from filtering
        filtered = filter_dataframe_by_timestamp(
            self.df.copy(),
            start_year=2023,
            start_month=1,
            start_day=1,
            end_year=2023,
            end_month=12,
            end_day=31,
            timestamp_string="timestamp",
        )
        # Should contain 5 valid dates from the original 8 rows within 2023
        # Valid timestamps in 2023: 2023-01-15, 2023-02-01, 2023-02-20, 2023-03-31, 2023-04-05, 2023-02-15+01:00
        # All 6 are within 2023, but let's verify the actual data
        expected_values = [1, 2, 3, 4, 5, 8]  # All valid 2023 timestamps
        self.assertEqual(len(filtered), 6)
        self.assertCountEqual(filtered["value"].tolist(), expected_values)
        self.assertNotIn(6, filtered["value"].tolist())  # 'not a date'
        self.assertNotIn(7, filtered["value"].tolist())  # None

    def test_with_timezone(self):
        """Test filtering with timezone-aware timestamps."""
        # The function converts everything to UTC.
        # 2023-02-15 05:00:00+01:00 becomes 2023-02-15 04:00:00 UTC.
        # This should be inside the range when filtering for 2023-02-15.
        filtered = filter_dataframe_by_timestamp(
            self.df.copy(),
            start_year=2023,
            start_month=2,
            start_day=15,
            end_year=2023,
            end_month=2,
            end_day=15,
            timestamp_string="timestamp",
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered["value"].iloc[0], 8)

    def test_dropna_parameter_false(self):
        """Test the dropna parameter when False (default)."""
        df_with_na_ts = pd.DataFrame(
            {"timestamp": [None, "2023-02-15 12:00:00"], "value": [9, 10]}
        )
        filtered = filter_dataframe_by_timestamp(
            df_with_na_ts,
            start_year=2023,
            start_month=2,
            start_day=1,
            end_year=2023,
            end_month=3,
            end_day=31,
            timestamp_string="timestamp",
            dropna=False,
        )
        # With dropna=False, NaN timestamps are still excluded from results
        # due to the filtering logic, but they remain in the intermediate DataFrame
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered["value"].iloc[0], 10)

    def test_dropna_parameter_true(self):
        """Test the dropna parameter when True."""
        df_with_na_ts = pd.DataFrame(
            {"timestamp": [None, "2023-02-15 12:00:00"], "value": [9, 10]}
        )
        filtered = filter_dataframe_by_timestamp(
            df_with_na_ts,
            start_year=2023,
            start_month=2,
            start_day=1,
            end_year=2023,
            end_month=3,
            end_day=31,
            timestamp_string="timestamp",
            dropna=True,
        )
        # With dropna=True, NaN timestamps are explicitly dropped before filtering
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered["value"].iloc[0], 10)

    def test_empty_dataframe(self):
        """Test with an empty DataFrame."""
        empty_df = pd.DataFrame({"timestamp": [], "value": []})
        filtered = filter_dataframe_by_timestamp(
            empty_df,
            start_year=2023,
            start_month=1,
            start_day=1,
            end_year=2023,
            end_month=12,
            end_day=31,
            timestamp_string="timestamp",
        )
        self.assertTrue(filtered.empty)

    def test_missing_timestamp_column(self):
        """Test when the timestamp column is missing."""
        with self.assertRaises(KeyError):
            filter_dataframe_by_timestamp(
                self.df.copy(),
                start_year=2023,
                start_month=1,
                start_day=1,
                end_year=2023,
                end_month=12,
                end_day=31,
                timestamp_string="non_existent_column",
            )

    def test_original_dataframe_is_not_modified(self):
        """Test that the original DataFrame is not modified."""
        original_df = self.df.copy()
        original_timestamp_col = original_df["timestamp"].copy()

        filter_dataframe_by_timestamp(
            original_df,
            start_year=2023,
            start_month=2,
            start_day=1,
            end_year=2023,
            end_month=3,
            end_day=31,
            timestamp_string="timestamp",
        )

        # Check that the original DataFrame's timestamp column is unchanged
        pd.testing.assert_series_equal(original_df["timestamp"], original_timestamp_col)


if __name__ == "__main__":
    unittest.main()
