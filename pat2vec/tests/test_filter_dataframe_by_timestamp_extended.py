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


if __name__ == "__main__":
    unittest.main()
