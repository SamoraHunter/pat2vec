import unittest
from unittest.mock import MagicMock
from datetime import datetime
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

from pat2vec.util.generate_date_list import generate_date_list


class TestGenerateDateList(unittest.TestCase):
    """Test suite for the generate_date_list function."""

    def setUp(self):
        """Set up a mock config object for the tests."""
        self.mock_config = MagicMock()
        self.mock_config.lookback = False
        self.mock_config.global_start_year = "2020"
        self.mock_config.global_start_month = "1"
        self.mock_config.global_start_day = "1"
        self.mock_config.global_end_year = "2022"
        self.mock_config.global_end_month = "12"
        self.mock_config.global_end_day = "31"
        self.mock_config.verbosity = 0

    def test_missing_config_obj(self):
        """Test that a ValueError is raised if config_obj is missing."""
        with self.assertRaisesRegex(ValueError, "A valid config_obj must be provided."):
            generate_date_list(datetime(2021, 1, 1), 0, 0, 5, config_obj=None)

    def test_forward_generation_daily(self):
        """Test basic forward date generation with a daily interval."""
        start_date = datetime(2021, 1, 1)
        result = generate_date_list(
            start_date, years=0, months=0, days=4, config_obj=self.mock_config
        )
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0], (2021, 1, 1))
        self.assertEqual(result[-1], (2021, 1, 5))

    def test_forward_generation_monthly(self):
        """Test forward date generation with a monthly interval."""
        start_date = datetime(2021, 1, 15)
        result = generate_date_list(
            start_date,
            years=0,
            months=2,
            days=0,
            time_window_interval_delta=relativedelta(months=1),
            config_obj=self.mock_config,
        )
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], (2021, 1, 15))
        self.assertEqual(result[1], (2021, 2, 15))
        self.assertEqual(result[2], (2021, 3, 15))

    def test_lookback_generation_daily(self):
        """Test basic lookback date generation with a daily interval."""
        self.mock_config.lookback = True
        start_date = datetime(2021, 1, 5)
        result = generate_date_list(
            start_date, years=0, months=0, days=4, config_obj=self.mock_config
        )
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0], (2021, 1, 1))
        self.assertEqual(result[-1], (2021, 1, 5))

    def test_clamping_to_global_start_date(self):
        """Test that the date list is clamped to the global start date."""
        self.mock_config.global_start_year = "2021"
        self.mock_config.global_start_month = "1"
        self.mock_config.global_start_day = "3"
        start_date = datetime(2021, 1, 1)
        result = generate_date_list(
            start_date, years=0, months=0, days=4, config_obj=self.mock_config
        )
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], (2021, 1, 3))
        self.assertEqual(result[-1], (2021, 1, 5))

    def test_clamping_to_global_end_date(self):
        """Test that the date list is clamped to the global end date."""
        self.mock_config.global_end_year = "2021"
        self.mock_config.global_end_month = "1"
        self.mock_config.global_end_day = "3"
        start_date = datetime(2021, 1, 1)
        result = generate_date_list(
            start_date, years=0, months=0, days=4, config_obj=self.mock_config
        )
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], (2021, 1, 1))
        self.assertEqual(result[-1], (2021, 1, 3))

    def test_clamping_to_global_start_and_end(self):
        """Test clamping to both global start and end dates."""
        self.mock_config.global_start_year = "2021"
        self.mock_config.global_start_month = "2"
        self.mock_config.global_start_day = "1"
        self.mock_config.global_end_year = "2021"
        self.mock_config.global_end_month = "3"
        self.mock_config.global_end_day = "1"
        start_date = datetime(2021, 1, 1)
        result = generate_date_list(
            start_date,
            years=0,
            months=3,
            days=0,
            time_window_interval_delta=relativedelta(months=1),
            config_obj=self.mock_config,
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], (2021, 2, 1))
        self.assertEqual(result[-1], (2021, 3, 1))

    def test_range_completely_outside_global_bounds(self):
        """Test that an empty list is returned if the range is outside global bounds."""
        start_date = datetime(2019, 1, 1)
        result = generate_date_list(
            start_date, years=0, months=0, days=4, config_obj=self.mock_config
        )
        self.assertEqual(len(result), 0)

    def test_invalid_final_range_returns_empty(self):
        """Test that an empty list is returned if the final range is invalid."""
        self.mock_config.global_start_year = "2021"
        self.mock_config.global_start_month = "1"
        self.mock_config.global_start_day = "10"
        start_date = datetime(2021, 1, 1)
        result = generate_date_list(
            start_date, years=0, months=0, days=4, config_obj=self.mock_config
        )
        self.assertEqual(len(result), 0)

    def test_zero_interval_delta_raises_error(self):
        """Test that a zero time_window_interval_delta raises a ValueError."""
        with self.assertRaisesRegex(
            ValueError, "time_window_interval_delta must be greater than zero"
        ):
            generate_date_list(
                datetime(2021, 1, 1),
                0,
                0,
                5,
                time_window_interval_delta=relativedelta(),
                config_obj=self.mock_config,
            )

    def test_negative_interval_delta_raises_error(self):
        """Test that a negative time_window_interval_delta raises a ValueError."""
        with self.assertRaisesRegex(
            ValueError, "time_window_interval_delta must be positive"
        ):
            generate_date_list(
                datetime(2021, 1, 1),
                0,
                0,
                5,
                time_window_interval_delta=relativedelta(days=-1),
                config_obj=self.mock_config,
            )

    def test_leap_year_handling(self):
        """Test that leap years are handled correctly."""
        self.mock_config.global_start_year = "2020"
        start_date = datetime(2020, 2, 27)
        result = generate_date_list(
            start_date, years=0, months=0, days=3, config_obj=self.mock_config
        )
        self.assertEqual(len(result), 4)
        self.assertIn((2020, 2, 29), result)
        self.assertEqual(result[-1], (2020, 3, 1))

    def test_timezone_aware_inputs(self):
        """Test that timezone-aware inputs are handled correctly."""
        utc_tz = ZoneInfo("UTC")
        est_tz = ZoneInfo("America/New_York")

        # Global dates are naive, start_date is aware
        start_date = datetime(2021, 1, 1, 5, 0, 0, tzinfo=est_tz)  # 10:00 UTC
        self.mock_config.global_start_day = "1"
        self.mock_config.global_end_day = "2"

        result = generate_date_list(
            start_date, years=0, months=0, days=1, config_obj=self.mock_config
        )
        # The range is [2021-01-01 10:00 UTC, 2021-01-02 10:00 UTC]
        # Global range is [2021-01-01 00:00 UTC, 2021-01-02 23:59 UTC]
        # Final range is the same as calculated range.
        # Dates generated should be 2021-01-01 and 2021-01-02
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], (2021, 1, 1))
        self.assertEqual(result[1], (2021, 1, 2))

    def test_end_of_month_logic(self):
        """Test date generation across month ends."""
        start_date = datetime(2021, 1, 30)
        result = generate_date_list(
            start_date, years=0, months=0, days=3, config_obj=self.mock_config
        )
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0], (2021, 1, 30))
        self.assertEqual(result[1], (2021, 1, 31))
        self.assertEqual(result[2], (2021, 2, 1))
        self.assertEqual(result[3], (2021, 2, 2))


if __name__ == "__main__":
    unittest.main(verbosity=2)
