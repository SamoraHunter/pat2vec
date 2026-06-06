import unittest
import pandas as pd
from datetime import datetime
import pytz

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp


class TestFilterDataFrameByTimestampExtended(unittest.TestCase):
    """
    Extended unit tests for the filter_dataframe_by_timestamp function,
    covering additional edge cases and input types.
    """

    def test_string_date_components(self):
        """Test if the function handles string inputs for date components."""
        df = pd.DataFrame({"timestamp": ["2023-02-15 12:00:00"], "value": [1]})
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year="2023",
            start_month="2",
            start_day="1",
            end_year="2023",
            end_month="2",
            end_day="28",
            timestamp_string="timestamp",
        )
        self.assertEqual(len(filtered), 1)

    def test_with_preexisting_datetime_column_naive(self):
        """Test filtering when the timestamp column is already a naive datetime object."""
        df = pd.DataFrame(
            {
                "ts_col": [
                    datetime(2023, 1, 10),  # before
                    datetime(2023, 2, 15),  # in
                    datetime(2023, 4, 1),  # after
                ],
                "value": [1, 2, 3],
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
            timestamp_string="ts_col",
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered["value"].iloc[0], 2)

    def test_with_preexisting_datetime_column_aware(self):
        """Test filtering when the timestamp column is already a timezone-aware datetime object."""
        utc_tz = pytz.UTC
        df = pd.DataFrame(
            {
                "ts_col": [
                    utc_tz.localize(datetime(2023, 1, 10)),  # before
                    utc_tz.localize(datetime(2023, 2, 15)),  # in
                    utc_tz.localize(datetime(2023, 4, 1)),  # after
                ],
                "value": [1, 2, 3],
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
            timestamp_string="ts_col",
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered["value"].iloc[0], 2)

    def test_with_mixed_timezones_in_column(self):
        """Test filtering with a column containing mixed timezone information."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2023-02-10 10:00:00+00:00",  # 10:00 UTC
                    "2023-02-10 12:00:00+02:00",  # 10:00 UTC
                    "2023-02-10 08:00:00-02:00",  # 10:00 UTC
                    "2023-02-11 01:00:00+00:00",  # outside range
                ],
                "value": [1, 2, 3, 4],
            }
        )
        # Filter for Feb 10th UTC
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=2023,
            start_month=2,
            start_day=10,
            end_year=2023,
            end_month=2,
            end_day=10,
            timestamp_string="timestamp",
        )
        self.assertEqual(len(filtered), 3)
        self.assertCountEqual(filtered["value"].tolist(), [1, 2, 3])

    def test_dataframe_unmodified(self):
        """Test that the original DataFrame is not modified in place."""
        original_df = pd.DataFrame({"timestamp": ["2023-01-15 10:00:00"], "value": [1]})
        original_df_copy = original_df.copy()

        filter_dataframe_by_timestamp(
            original_df,
            start_year=2023,
            start_month=1,
            start_day=1,
            end_year=2023,
            end_month=12,
            end_day=31,
            timestamp_string="timestamp",
        )
        pd.testing.assert_frame_equal(original_df, original_df_copy)

    def test_leap_year_boundary(self):
        """Test filtering around a leap day."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2024-02-28 23:59:59",  # in
                    "2024-02-29 12:00:00",  # in (leap day)
                    "2024-03-01 00:00:00",  # in
                    "2024-03-02 10:00:00",  # out
                ],
                "value": [1, 2, 3, 4],
            }
        )
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=2024,
            start_month=2,
            start_day=28,
            end_year=2024,
            end_month=3,
            end_day=1,
            timestamp_string="timestamp",
        )
        self.assertEqual(len(filtered), 3)
        self.assertCountEqual(filtered["value"].tolist(), [1, 2, 3])

    def test_column_with_only_invalid_dates(self):
        """Test that an empty DataFrame is returned if the timestamp column has no valid dates."""
        df = pd.DataFrame(
            {"timestamp": ["not a date", "invalid", None], "value": [1, 2, 3]}
        )
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=2023,
            start_month=1,
            start_day=1,
            end_year=2023,
            end_month=12,
            end_day=31,
            timestamp_string="timestamp",
        )
        self.assertTrue(filtered.empty)

    def test_filter_single_day_with_multiple_entries(self):
        """Test filtering for a single day with multiple entries on that day."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2023-07-20 08:00:00",
                    "2023-07-20 12:00:00",
                    "2023-07-20 18:00:00",
                    "2023-07-19 23:59:59",  # Day before
                    "2023-07-21 00:00:01",  # Day after
                ],
                "value": [10, 20, 30, 40, 50],
            }
        )
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=2023,
            start_month=7,
            start_day=20,
            end_year=2023,
            end_month=7,
            end_day=20,
            timestamp_string="timestamp",
        )
        self.assertEqual(len(filtered), 3)
        self.assertCountEqual(filtered["value"].tolist(), [10, 20, 30])

    def test_invalid_timestamp_column_name(self):
        """Test filtering with a timestamp_string that does not exist in the DataFrame."""
        df = pd.DataFrame({"correct_timestamp": ["2023-01-01 00:00:00"], "value": [1]})
        with self.assertRaises(KeyError):
            filter_dataframe_by_timestamp(
                df,
                start_year=2023,
                start_month=1,
                start_day=1,
                end_year=2023,
                end_month=1,
                end_day=1,
                timestamp_string="non_existent_column",
            )

    def test_no_date_range_specified(self):
        """Test that the function returns the DataFrame with invalid dates removed if no date range is specified."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2023-01-01 10:00:00",
                    "invalid-date",
                    "2023-03-15 12:00:00",
                ],
                "value": [1, 2, 3],
            }
        )
        filtered = filter_dataframe_by_timestamp(
            df,
            timestamp_string="timestamp",
            start_year=None,
            start_month=None,
            start_day=None,
            end_year=None,
            end_month=None,
            end_day=None,
        )
        self.assertEqual(len(filtered), 2)
        self.assertCountEqual(filtered["value"].tolist(), [1, 3])
        self.assertNotIn("invalid-date", filtered["timestamp"].tolist())

    def test_only_start_date_specified(self):
        """Test filtering with only a start date."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2023-01-01 10:00:00",
                    "2023-02-01 12:00:00",
                    "2023-03-01 14:00:00",
                ],
                "value": [1, 2, 3],
            }
        )
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=2023,
            start_month=2,
            start_day=1,
            end_year=None,
            end_month=None,
            end_day=None,
            timestamp_string="timestamp",
        )
        self.assertEqual(len(filtered), 2)
        self.assertCountEqual(filtered["value"].tolist(), [2, 3])

    def test_only_end_date_specified(self):
        """Test filtering with only an end date."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2023-01-01 10:00:00",
                    "2023-02-01 12:00:00",
                    "2023-03-01 14:00:00",
                ],
                "value": [1, 2, 3],
            }
        )
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=None,
            start_month=None,
            start_day=None,
            end_year=2023,
            end_month=2,
            end_day=1,
            timestamp_string="timestamp",
        )
        self.assertEqual(len(filtered), 2)
        self.assertCountEqual(filtered["value"].tolist(), [1, 2])

    def test_dataframe_with_some_invalid_dates(self):
        """Test filtering a DataFrame where some dates are invalid and should be coerced."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2023-01-01",
                    "not-a-date",
                    "2023-02-15",
                    "another-bad-date",
                    "2023-03-30",
                ],
                "value": [1, 2, 3, 4, 5],
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
        self.assertCountEqual(filtered["value"].tolist(), [3, 5])

    def test_empty_dataframe_after_invalid_date_coercion(self):
        """Test that an empty DataFrame with original columns is returned if all dates are invalid."""
        df = pd.DataFrame({"timestamp": ["invalid", "bad-date"], "value": [1, 2]})
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=2023,
            start_month=1,
            start_day=1,
            end_year=2023,
            end_month=12,
            end_day=31,
            timestamp_string="timestamp",
        )
        self.assertTrue(filtered.empty)
        self.assertListEqual(list(filtered.columns), list(df.columns))

    def test_dropna_true_behavior(self):
        """Test that dropna=True explicitly removes NaT/None from results."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2023-01-01",
                    None,
                    "2023-01-02",
                    pd.NaT,
                ],
                "value": [1, 2, 3, 4],
            }
        )
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=2023,
            start_month=1,
            start_day=1,
            end_year=2023,
            end_month=1,
            end_day=31,
            timestamp_string="timestamp",
            dropna=True,
        )
        self.assertEqual(len(filtered), 2)
        self.assertCountEqual(filtered["value"].tolist(), [1, 3])

    def test_invalid_types_in_column(self):
        """Test filtering when column contains non-string, non-datetime types."""
        df = pd.DataFrame(
            {"timestamp": [123, True, datetime(2023, 1, 1)], "value": [1, 2, 3]}
        )
        # Function uses pd.to_datetime(..., errors='coerce')
        filtered = filter_dataframe_by_timestamp(
            df,
            start_year=2023,
            start_month=1,
            start_day=1,
            end_year=2023,
            end_month=1,
            end_day=1,
            timestamp_string="timestamp",
        )
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered["value"].iloc[0], 3)


if __name__ == "__main__":
    unittest.main()
