import unittest
from datetime import datetime
from dateutil.relativedelta import relativedelta

from pat2vec.util.calculate_interval import calculate_interval


class TestCalculateInterval(unittest.TestCase):
    """Unit tests for the calculate_interval function."""

    def test_simple_days_interval(self):
        """Test with a simple interval of days and m=1."""
        start_date = datetime(2023, 1, 1)
        time_delta = relativedelta(days=10)
        result = calculate_interval(start_date, time_delta, m=1)
        self.assertEqual(result, 10)

    def test_interval_with_m_divisor(self):
        """Test with m > 1 that is a perfect divisor of the interval."""
        start_date = datetime(2023, 1, 1)
        time_delta = relativedelta(days=30)
        result = calculate_interval(start_date, time_delta, m=5)
        self.assertEqual(result, 6)

    def test_interval_with_m_not_divisor(self):
        """Test with m > 1 that is not a perfect divisor, checking integer division."""
        start_date = datetime(2023, 1, 1)
        time_delta = relativedelta(days=32)
        result = calculate_interval(start_date, time_delta, m=5)
        self.assertEqual(result, 6)  # 32 // 5 = 6

    def test_monthly_interval(self):
        """Test with a monthly interval."""
        start_date = datetime(2023, 1, 15)
        time_delta = relativedelta(months=2)  # Jan 15 to Mar 15 is 59 days
        result = calculate_interval(start_date, time_delta, m=1)
        self.assertEqual(result, 59)

    def test_yearly_interval_leap_year(self):
        """Test with a yearly interval that includes a leap year."""
        start_date = datetime(2020, 1, 1)  # 2020 is a leap year
        time_delta = relativedelta(years=1)
        result = calculate_interval(start_date, time_delta, m=1)
        self.assertEqual(result, 366)

    def test_yearly_interval_non_leap_year(self):
        """Test with a yearly interval that does not include a leap year."""
        start_date = datetime(2021, 1, 1)
        time_delta = relativedelta(years=1)
        result = calculate_interval(start_date, time_delta, m=1)
        self.assertEqual(result, 365)

    def test_mixed_delta_with_leap_year(self):
        """Test a mixed relativedelta that spans across a leap day."""
        start_date = datetime(2020, 1, 1)  # 2020 is a leap year
        time_delta = relativedelta(months=6, days=15)
        result = calculate_interval(start_date, time_delta, m=1)
        self.assertEqual(result, 197)

    def test_zero_time_delta(self):
        """Test with a zero time delta."""
        start_date = datetime(2023, 1, 1)
        time_delta = relativedelta(days=0)
        result = calculate_interval(start_date, time_delta, m=1)
        self.assertEqual(result, 0)

    def test_negative_time_delta(self):
        """Test with a negative time delta."""
        start_date = datetime(2023, 2, 1)
        time_delta = relativedelta(days=-10)
        result = calculate_interval(start_date, time_delta, m=1)
        self.assertEqual(result, -10)

    def test_division_by_zero(self):
        """Test that m=0 raises a ZeroDivisionError."""
        start_date = datetime(2023, 1, 1)
        time_delta = relativedelta(days=10)
        with self.assertRaises(ZeroDivisionError):
            calculate_interval(start_date, time_delta, m=0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
