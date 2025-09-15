import unittest
import datetime
from dateutil.relativedelta import relativedelta

from pat2vec.util.get_start_end_year_month import get_start_end_year_month


class MockConfig:
    """A simple mock class to hold the time_window_interval_delta."""

    def __init__(self, delta):
        self.time_window_interval_delta = delta


class TestGetStartEndYearMonth(unittest.TestCase):
    """Unit tests for the get_start_end_year_month function."""

    def test_basic_positive_delta(self):
        """Test a simple case with a positive time delta of a few days."""
        delta = relativedelta(days=10)
        config = MockConfig(delta)
        target_date_range = (2023, 2, 15)

        (
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
        ) = get_start_end_year_month(target_date_range, config_obj=config)

        self.assertEqual(start_year, 2023)
        self.assertEqual(start_month, 2)
        self.assertEqual(start_day, 15)
        self.assertEqual(end_year, 2023)
        self.assertEqual(end_month, 2)
        self.assertEqual(end_day, 25)

    def test_delta_crossing_month_boundary(self):
        """Test a delta that crosses into the next month."""
        delta = relativedelta(days=5)
        config = MockConfig(delta)
        target_date_range = (2023, 1, 30)

        (
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
        ) = get_start_end_year_month(target_date_range, config_obj=config)

        self.assertEqual(start_year, 2023)
        self.assertEqual(start_month, 1)
        self.assertEqual(start_day, 30)
        self.assertEqual(end_year, 2023)
        self.assertEqual(end_month, 2)
        self.assertEqual(end_day, 4)

    def test_delta_crossing_year_boundary(self):
        """Test a delta that crosses into the next year."""
        delta = relativedelta(months=1)
        config = MockConfig(delta)
        target_date_range = (2023, 12, 15)

        (
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
        ) = get_start_end_year_month(target_date_range, config_obj=config)

        self.assertEqual(start_year, 2023)
        self.assertEqual(start_month, 12)
        self.assertEqual(start_day, 15)
        self.assertEqual(end_year, 2024)
        self.assertEqual(end_month, 1)
        self.assertEqual(end_day, 15)

    def test_leap_year_handling(self):
        """Test a delta that crosses a leap day."""
        delta = relativedelta(days=5)
        config = MockConfig(delta)
        target_date_range = (2024, 2, 27)  # 2024 is a leap year

        (
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
        ) = get_start_end_year_month(target_date_range, config_obj=config)

        self.assertEqual(start_year, 2024)
        self.assertEqual(start_month, 2)
        self.assertEqual(start_day, 27)
        self.assertEqual(end_year, 2024)
        self.assertEqual(end_month, 3)
        self.assertEqual(end_day, 3)

    def test_no_config_obj_raises_error(self):
        """Test that a ValueError is raised if config_obj is None."""
        target_date_range = (2023, 1, 1)
        with self.assertRaisesRegex(ValueError, "config_obj cannot be None"):
            get_start_end_year_month(target_date_range, config_obj=None)

    def test_zero_delta(self):
        """Test that a zero delta results in the same start and end date."""
        delta = relativedelta(days=0)
        config = MockConfig(delta)
        target_date_range = (2023, 5, 10)

        (
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
        ) = get_start_end_year_month(target_date_range, config_obj=config)

        self.assertEqual(start_year, 2023)
        self.assertEqual(start_month, 5)
        self.assertEqual(start_day, 10)
        self.assertEqual(end_year, 2023)
        self.assertEqual(end_month, 5)
        self.assertEqual(end_day, 10)

    def test_negative_delta(self):
        """Test that a negative delta correctly calculates an end date before the start date."""
        delta = relativedelta(days=-5)
        config = MockConfig(delta)
        target_date_range = (2023, 3, 10)

        (
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
        ) = get_start_end_year_month(target_date_range, config_obj=config)

        self.assertEqual(start_year, 2023)
        self.assertEqual(start_month, 3)
        self.assertEqual(start_day, 10)
        self.assertEqual(end_year, 2023)
        self.assertEqual(end_month, 3)
        self.assertEqual(end_day, 5)

    def test_complex_delta_from_end_of_month(self):
        """Test a complex delta of months and days from the end of a month."""
        delta = relativedelta(months=1, days=5)
        config = MockConfig(delta)
        target_date_range = (2023, 1, 30)  # Start date
        # End date should be 2023-01-30 + 1 month -> 2023-02-28, then + 5 days -> 2023-03-05

        _, _, end_year, end_month, _, end_day = get_start_end_year_month(
            target_date_range, config_obj=config
        )

        self.assertEqual(end_year, 2023)
        self.assertEqual(end_month, 3)
        self.assertEqual(end_day, 5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
