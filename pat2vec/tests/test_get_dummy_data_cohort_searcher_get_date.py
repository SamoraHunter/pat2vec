import unittest
from datetime import datetime
import calendar
from pat2vec.util.get_dummy_data_cohort_searcher import (
    create_random_date_from_globals,
)


class TestCreateRandomDateFromGlobals(unittest.TestCase):
    """Unit tests for the create_random_date_from_globals function."""

    def test_date_within_range(self):
        """Test that the generated date is within the specified range."""
        start_year, start_month = 2023, 1
        end_year, end_month = 2023, 3

        start_dt = datetime(start_year, start_month, 1)
        _, num_days_in_end_month = calendar.monthrange(end_year, end_month)
        end_dt = datetime(end_year, end_month, num_days_in_end_month, 23, 59, 59)

        random_date = create_random_date_from_globals(
            start_year, start_month, end_year, end_month
        )

        self.assertIsInstance(random_date, datetime)
        self.assertTrue(start_dt <= random_date <= end_dt)

    def test_same_start_and_end_month(self):
        """Test when the start and end month/year are the same."""
        start_year, start_month = 2024, 2  # February 2024 (leap year)
        end_year, end_month = 2024, 2

        start_dt = datetime(start_year, start_month, 1)
        _, num_days_in_end_month = calendar.monthrange(end_year, end_month)
        end_dt = datetime(end_year, end_month, num_days_in_end_month, 23, 59, 59)

        self.assertEqual(num_days_in_end_month, 29)  # Check leap year

        random_date = create_random_date_from_globals(
            start_year, start_month, end_year, end_month
        )

        self.assertIsInstance(random_date, datetime)
        self.assertTrue(start_dt <= random_date <= end_dt)
        self.assertEqual(random_date.year, 2024)
        self.assertEqual(random_date.month, 2)

    def test_invalid_range_end_before_start(self):
        """Test that an invalid range (end before start) returns the start datetime."""
        start_year, start_month = 2023, 5
        end_year, end_month = 2023, 3

        start_dt = datetime(start_year, start_month, 1)

        result_date = create_random_date_from_globals(
            start_year, start_month, end_year, end_month
        )

        self.assertEqual(result_date, start_dt)

    def test_range_across_years(self):
        """Test a date range that spans across multiple years."""
        start_year, start_month = 2022, 11
        end_year, end_month = 2023, 2

        start_dt = datetime(start_year, start_month, 1)
        _, num_days_in_end_month = calendar.monthrange(end_year, end_month)
        end_dt = datetime(end_year, end_month, num_days_in_end_month, 23, 59, 59)

        for _ in range(10):  # run a few times to increase chance of catching errors
            random_date = create_random_date_from_globals(
                start_year, start_month, end_year, end_month
            )
            self.assertTrue(start_dt <= random_date <= end_dt)


if __name__ == "__main__":
    unittest.main()
