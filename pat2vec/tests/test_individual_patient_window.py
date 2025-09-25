import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

# The class to be tested
from pat2vec.util.config_pat2vec import config_class


class TestIndividualPatientWindow(unittest.TestCase):
    """Unit tests for the individual_patient_window functionality in config_class."""

    def setUp(self):
        """Set up common test fixtures."""
        self.base_start_date = datetime(2020, 1, 1)
        # Mock PathsClass to avoid file system operations during tests
        self.patcher_paths_class = patch("pat2vec.util.config_pat2vec.PathsClass")
        self.mock_paths_class = self.patcher_paths_class.start()
        self.mock_paths_class.return_value = MagicMock()

    def tearDown(self):
        """Clean up after each test."""
        self.patcher_paths_class.stop()

    def test_ipw_false_creates_date_list(self):
        """Test that when individual_patient_window is False, a date_list is created."""
        with patch("pat2vec.util.config_pat2vec.generate_date_list") as mock_generate:
            mock_generate.return_value = [(2020, 1, 1)]
            with patch("builtins.print"):
                config = config_class(
                    start_date=self.base_start_date,
                    individual_patient_window=False,
                    testing=True,
                )

        self.assertFalse(config.individual_patient_window)
        self.assertIsNotNone(config.date_list)
        self.assertEqual(config.n_pat_lines, 1)
        self.assertFalse(
            hasattr(config, "patient_dict") and config.patient_dict is not None
        )

    def test_ipw_true_creates_patient_dict(self):
        """Test that when individual_patient_window is True, a patient_dict is created."""
        mock_df = pd.DataFrame(
            {"patient_id": ["P001", "P002"], "start_date": ["2020-01-01", "2020-02-01"]}
        )

        with patch("builtins.print"):
            config = config_class(
                years=1,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
                lookback=False,
            )

        self.assertTrue(config.individual_patient_window)
        self.assertIsNone(config.date_list)
        self.assertIsNone(config.n_pat_lines)
        self.assertIsNotNone(config.patient_dict)
        self.assertIn("P001", config.patient_dict)
        self.assertIn("P002", config.patient_dict)

        # Check the dates in the patient_dict
        start_p1, end_p1 = config.patient_dict["P001"]
        self.assertEqual(start_p1, pd.Timestamp("2020-01-01", tz="UTC"))
        self.assertEqual(end_p1, pd.Timestamp("2021-01-01", tz="UTC"))

    def test_ipw_with_lookback_true(self):
        """Test lookback=True correctly calculates a past end date."""
        mock_df = pd.DataFrame({"patient_id": ["P001"], "start_date": ["2020-01-01"]})

        with patch("builtins.print"):
            config = config_class(
                years=1,
                days=0,
                lookback=True,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        self.assertIn("P001", config.patient_dict)
        start_date_p1, end_date_p1 = config.patient_dict["P001"]

        # For lookback, the end_date should be before the start_date
        expected_start_p1 = pd.Timestamp("2020-01-01", tz="UTC")
        expected_end_p1 = pd.Timestamp("2019-01-01", tz="UTC")

        self.assertEqual(start_date_p1, expected_start_p1)
        self.assertEqual(end_date_p1, expected_end_p1)
        self.assertTrue(start_date_p1 > end_date_p1)

    def test_ipw_with_preexisting_offset_column(self):
        """Test IPW logic when the offset column already exists in the DataFrame."""
        mock_df = pd.DataFrame(
            {
                "patient_id": ["P001"],
                "start_date": ["2020-01-01"],  # Original date, still needed by the code
                "start_date_offset": ["2021-01-01"],  # This is the start of the window
                "start_date_end_date": ["2022-01-01"],  # This is the end of the window
            }
        )

        with patch(
            "pat2vec.util.config_pat2vec.add_offset_column"
        ) as mock_add_offset, patch("builtins.print"):
            config = config_class(
                years=5,  # This should be ignored
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                lookback=False,
                testing=True,
            )

        # add_offset_column should NOT be called
        mock_add_offset.assert_not_called()

        # The patient_dict should be created from the existing columns
        self.assertIsNotNone(config.patient_dict)
        self.assertIn("P001", config.patient_dict)
        start_p1, end_p1 = config.patient_dict["P001"]
        # Assert that the window is from start_date_offset to start_date_end_date
        self.assertEqual(start_p1, pd.Timestamp("2021-01-01", tz="UTC"))
        self.assertEqual(end_p1, pd.Timestamp("2022-01-01", tz="UTC"))

    def test_ipw_with_invalid_start_dates(self):
        """Test IPW logic handles invalid or missing start dates gracefully."""
        mock_df = pd.DataFrame(
            {
                "patient_id": ["P001", "P002", "P003"],
                "start_date": ["2021-01-01", None, "not a date"],
            }
        )

        with patch("builtins.print"):
            config = config_class(
                years=1,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
                lookback=False,
            )

        # The patient_dict should only contain the valid patient
        self.assertIsNotNone(config.patient_dict)
        self.assertEqual(len(config.patient_dict), 1)
        self.assertIn("P001", config.patient_dict)
        self.assertNotIn("P002", config.patient_dict)
        self.assertNotIn("P003", config.patient_dict)

        start_p1, end_p1 = config.patient_dict["P001"]
        self.assertEqual(start_p1, pd.Timestamp("2021-01-01", tz="UTC"))
        self.assertEqual(end_p1, pd.Timestamp("2022-01-01", tz="UTC"))

    def test_ipw_with_complex_time_delta(self):
        """Test IPW with a complex time delta involving years, months, and days."""
        mock_df = pd.DataFrame({"patient_id": ["P001"], "start_date": ["2020-01-31"]})

        with patch("builtins.print"):
            config = config_class(
                years=1,
                months=2,
                days=5,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
                lookback=False,
            )

        self.assertIn("P001", config.patient_dict)
        start_p1, end_p1 = config.patient_dict["P001"]

        # Expected end date: 2020-01-31 + 1 year, 2 months, 5 days -> 2021-04-05
        expected_start = pd.Timestamp("2020-01-31", tz="UTC")
        expected_end = pd.Timestamp("2021-04-05", tz="UTC")

        self.assertEqual(start_p1, expected_start)
        self.assertEqual(end_p1, expected_end)

    def test_ipw_with_empty_dataframe(self):
        """Test that an empty DataFrame results in an empty patient_dict."""
        mock_df = pd.DataFrame({"patient_id": [], "start_date": []})

        with patch("builtins.print"):
            config = config_class(
                years=1,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        self.assertIsNotNone(config.patient_dict)
        self.assertEqual(len(config.patient_dict), 0)

    def test_ipw_and_prefetch_incompatibility(self):
        """Test that prefetch_pat_batches is disabled when individual_patient_window is True."""
        mock_df = pd.DataFrame({"patient_id": ["P001"], "start_date": ["2020-01-01"]})
        with self.assertLogs('pat2vec.util.config_pat2vec', level='WARNING') as cm:
            config = config_class(
                individual_patient_window=True,
                prefetch_pat_batches=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

            self.assertFalse(config.prefetch_pat_batches)
            self.assertTrue(any("not compatible with 'individual_patient_window'" in log for log in cm.output))
            self.assertTrue(any("Disabling 'prefetch_pat_batches'" in log for log in cm.output))

    def test_ipw_with_missing_column_raises_error(self):
        """Test that a missing start_date or patient_id column raises an error."""
        mock_df_no_date = pd.DataFrame({"patient_id": ["P001"]})
        with self.assertRaises(ValueError) as cm:
            with patch("builtins.print"):
                config_class(
                    years=1,
                    individual_patient_window=True,
                    individual_patient_window_df=mock_df_no_date,
                    individual_patient_window_start_column_name="start_date",
                    individual_patient_id_column_name="patient_id",
                    testing=True,
                )
        self.assertIn("Column 'start_date' does not exist.", str(cm.exception))

        mock_df_no_id = pd.DataFrame({"start_date": ["2020-01-01"]})
        with self.assertRaises(ValueError) as cm:
            with patch("builtins.print"):
                config_class(
                    years=1,
                    individual_patient_window=True,
                    individual_patient_window_df=mock_df_no_id,
                    individual_patient_window_start_column_name="start_date",
                    individual_patient_id_column_name="patient_id",
                    testing=True,
                )
        self.assertIn("Column 'patient_id' does not exist.", str(cm.exception))

    def test_ipw_with_duplicate_patient_ids(self):
        """Test that duplicate patient IDs in the DataFrame use the last entry."""
        mock_df = pd.DataFrame(
            {
                "patient_id": ["P001", "P002", "P001"],
                "start_date": [
                    "2020-01-01",
                    "2020-02-01",
                    "2021-05-05",
                ],  # P001's last entry is 2021-05-05
            }
        )
        with patch("builtins.print"):
            config = config_class(
                years=1,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
                lookback=False,
            )
        self.assertEqual(len(config.patient_dict), 2)  # P001 and P002
        self.assertIn("P001", config.patient_dict)
        start_p1, end_p1 = config.patient_dict["P001"]
        # Should correspond to the last entry for P001
        self.assertEqual(start_p1, pd.Timestamp("2021-05-05", tz="UTC"))
        self.assertEqual(end_p1, pd.Timestamp("2022-05-05", tz="UTC"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
