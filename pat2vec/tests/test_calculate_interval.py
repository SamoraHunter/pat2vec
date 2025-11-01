import unittest
from datetime import datetime
from dateutil.relativedelta import relativedelta

from pat2vec.util.calculate_interval import calculate_interval


# PYTHONPATH=. pytest pat2vec/tests
class TestCalculateInterval(unittest.TestCase):
    """Unit tests for the calculate_interval function."""

    def test_exact_multiple_intervals(self):
        """Test with a total delta that is an exact multiple of the interval delta."""
        start_date = datetime(2020, 1, 1)
        total_delta = relativedelta(months=12)
        interval_delta = relativedelta(months=2)
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 6)

    def test_non_exact_multiple_intervals(self):
        """Test with a total delta that is not an exact multiple, expecting floor behavior."""
        start_date = datetime(2020, 1, 1)
        total_delta = relativedelta(months=13)
        interval_delta = relativedelta(months=2)
        # 6 intervals fit perfectly (12 months). The 7th interval would end at 14 months, which is > 13.
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 6)

    def test_total_delta_smaller_than_interval(self):
        """Test when the total duration is less than one interval."""
        start_date = datetime(2020, 1, 1)
        total_delta = relativedelta(days=5)
        interval_delta = relativedelta(days=10)
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 0)

    def test_total_delta_equals_interval(self):
        """Test when the total duration is exactly one interval."""
        start_date = datetime(2020, 1, 1)
        total_delta = relativedelta(months=1)
        interval_delta = relativedelta(months=1)
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 1)

    def test_zero_total_delta(self):
        """Test with a zero-length total duration."""
        start_date = datetime(2023, 1, 1)
        total_delta = relativedelta(days=0)
        interval_delta = relativedelta(days=1)
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 0)

    def test_negative_total_delta(self):
        """Test with a negative total duration, which should result in 0 intervals."""
        start_date = datetime(2023, 2, 1)
        total_delta = relativedelta(days=-10)
        interval_delta = relativedelta(days=1)
        # end_date will be before start_date, so the loop should not run.
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 0)

    def test_zero_interval_delta_raises_error(self):
        """Test that a zero-duration interval raises a ValueError."""
        start_date = datetime(2023, 1, 1)
        total_delta = relativedelta(days=10)
        interval_delta = relativedelta(days=0)
        with self.assertRaisesRegex(
            ValueError, "The time interval delta must be a positive duration."
        ):
            calculate_interval(start_date, total_delta, interval_delta)

    def test_negative_interval_delta_raises_error(self):
        """Test that a negative-duration interval raises a ValueError."""
        start_date = datetime(2023, 1, 1)
        total_delta = relativedelta(days=10)
        interval_delta = relativedelta(days=-1)
        with self.assertRaisesRegex(
            ValueError, "The time interval delta must be a positive duration."
        ):
            calculate_interval(start_date, total_delta, interval_delta)

    def test_leap_year_handling(self):
        """Test a one-year duration over a leap year."""
        start_date = datetime(2020, 2, 1)  # 2020 is a leap year
        total_delta = relativedelta(years=1)  # ends 2021-02-01
        interval_delta = relativedelta(months=1)
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 12)

    def test_non_leap_year_handling(self):
        """Test a one-year duration over a non-leap year."""
        start_date = datetime(2021, 2, 1)
        total_delta = relativedelta(years=1)
        interval_delta = relativedelta(months=1)
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 12)

    def test_complex_deltas(self):
        """Test with complex, mixed-unit relativedeltas."""
        start_date = datetime(2020, 1, 1)
        total_delta = relativedelta(years=2, months=3, days=5)  # End date: 2022-04-06
        interval_delta = relativedelta(months=7)
        # Intervals end at:
        # 1: 2020-08-01
        # 2: 2021-03-01
        # 3: 2021-10-01
        # 4: 2022-05-01 (This is > 2022-04-06, so it's excluded)
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 3)

    def test_end_of_month_logic(self):
        """Test that relativedelta handles month ends correctly."""
        start_date = datetime(2020, 1, 31)
        total_delta = relativedelta(months=4)  # End date: 2020-05-31
        interval_delta = relativedelta(months=1)
        # Intervals end at:
        # 1: 2020-02-29 (leap year)
        # 2: 2020-03-31
        # 3: 2020-04-30
        # 4: 2020-05-31
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 4)

    def test_total_delta_just_shy_of_multiple(self):
        """Test when the total delta is one day less than an exact multiple."""
        start_date = datetime(2020, 1, 1)
        # Total duration is 6 months minus 1 day. End date: 2020-06-30
        total_delta = relativedelta(months=6, days=-1)
        interval_delta = relativedelta(months=2)
        # Intervals end at: 2020-03-01, 2020-05-01.
        # The 3rd interval would end at 2020-07-01, which is after the end date.
        result = calculate_interval(start_date, total_delta, interval_delta)
        self.assertEqual(result, 2)

    def test_weekly_interval_over_different_month_lengths(self):
        """Test a weekly interval over months with different numbers of days."""
        # January (31 days)
        start_jan = datetime(2023, 1, 1)
        total_delta_month = relativedelta(months=1)
        interval_delta_week = relativedelta(weeks=1)
        # 4 full weeks fit in Jan (1, 8, 15, 22, 29). The 5th week starts on Feb 5.
        result_jan = calculate_interval(
            start_jan, total_delta_month, interval_delta_week
        )
        self.assertEqual(result_jan, 4)


if __name__ == "__main__":
    unittest.main(verbosity=2)
