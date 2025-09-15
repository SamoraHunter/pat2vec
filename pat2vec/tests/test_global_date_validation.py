import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import sys

# Mock the imports that might not be available in test environment
# sys.modules["pat2vec.util.current_pat_batch_path_methods"] = MagicMock()
# sys.modules["pat2vec.util.methods_get"] = MagicMock()
# sys.modules["paramiko"] = MagicMock()
# sys.modules["IPython.display"] = MagicMock()

from pat2vec.util.config_pat2vec import (
    validate_and_fix_global_dates,
    update_global_start_date,
)


class TestGlobalDateValidation(unittest.TestCase):
    """Test suite for global date validation functions."""

    def test_global_date_validation_correct_order(self):
        """Test that global dates in correct order remain unchanged."""
        mock_config = MagicMock()
        mock_config.global_start_year = "2020"
        mock_config.global_start_month = "01"
        mock_config.global_start_day = "01"
        mock_config.global_end_year = "2021"
        mock_config.global_end_month = "01"
        mock_config.global_end_day = "01"

        result = validate_and_fix_global_dates(mock_config)

        # Should remain unchanged
        self.assertEqual(result.global_start_year, "2020")
        self.assertEqual(result.global_end_year, "2021")

    def test_global_date_validation_wrong_order(self):
        """Test that global dates in wrong order get swapped."""
        mock_config = MagicMock()
        mock_config.global_start_year = "2021"
        mock_config.global_start_month = "01"
        mock_config.global_start_day = "01"
        mock_config.global_end_year = "2020"
        mock_config.global_end_month = "01"
        mock_config.global_end_day = "01"
        mock_config.global_start_date = datetime(2021, 1, 1)
        mock_config.global_end_date = datetime(2020, 1, 1)

        with patch("builtins.print"):  # Suppress print output
            result = validate_and_fix_global_dates(mock_config)

        # Should be swapped
        self.assertEqual(result.global_start_year, "2020")
        self.assertEqual(result.global_end_year, "2021")
        self.assertEqual(result.global_start_date, datetime(2020, 1, 1))
        self.assertEqual(result.global_end_date, datetime(2021, 1, 1))

    def test_update_global_start_date_lookback_true(self):
        """Test that update_global_start_date does nothing when lookback=True."""
        mock_config = MagicMock()
        mock_config.lookback = True
        mock_config.global_start_year = "2020"

        later_date = datetime(2021, 6, 1)
        result = update_global_start_date(mock_config, later_date)

        # Should return unchanged config when lookback=True
        self.assertEqual(result.global_start_year, "2020")

    def test_update_global_start_date_forward_later(self):
        """Test global start date update when provided date is later."""
        mock_config = MagicMock()
        mock_config.lookback = False
        mock_config.global_start_year = "2020"
        mock_config.global_start_month = "1"
        mock_config.global_start_day = "1"
        mock_config.global_start_date = datetime(2020, 1, 1)

        later_date = datetime(2021, 6, 15)

        with patch("builtins.print"):  # Suppress print output
            update_global_start_date(mock_config, later_date)

        # Should update to the later date
        self.assertEqual(mock_config.global_start_year, "2021")
        self.assertEqual(mock_config.global_start_month, "06")
        self.assertEqual(mock_config.global_start_day, "15")
        self.assertEqual(mock_config.global_start_date, later_date)

    def test_update_global_start_date_forward_earlier(self):
        """Test global start date remains unchanged when provided date is earlier."""
        mock_config = MagicMock()
        mock_config.lookback = False
        mock_config.global_start_year = "2021"
        mock_config.global_start_month = "6"
        mock_config.global_start_day = "15"
        mock_config.global_start_date = datetime(2021, 6, 15)

        earlier_date = datetime(2020, 1, 1)
        result = update_global_start_date(mock_config, earlier_date)

        # Should remain unchanged
        self.assertEqual(result.global_start_year, "2021")

    def test_invalid_global_dates_handling(self):
        """Test handling of invalid global date attributes."""
        mock_config = MagicMock()
        mock_config.lookback = False
        mock_config.global_start_year = "invalid"
        mock_config.global_start_month = "also_invalid"
        mock_config.global_start_day = "still_invalid"

        later_date = datetime(2021, 6, 15)

        with patch("builtins.print") as mock_print:
            result = update_global_start_date(mock_config, later_date)

        # Should print warning and return unchanged config
        mock_print.assert_called_with(
            "Warning: Invalid global date attributes in config. Cannot update."
        )
        self.assertEqual(result, mock_config)


if __name__ == "__main__":
    unittest.main(verbosity=2)
