import unittest
from unittest.mock import patch, Mock
from datetime import datetime
import pandas as pd

from pat2vec.util.config_pat2vec import config_class


class TestIndividualPatientWindow(unittest.TestCase):
    """Test suite for individual patient window functionality."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.base_start_date = datetime(2020, 1, 1)

        # Create patches for commonly used functions
        self.patcher_paths_class = patch(
            "pat2vec.util.current_pat_batch_path_methods.PathsClass"
        )

        # Start only the PathsClass patch globally since it's used in constructor
        self.mock_paths_class = self.patcher_paths_class.start()
        self.mock_paths_class.return_value = Mock()

    def tearDown(self):
        """Clean up patches after each test method."""
        self.patcher_paths_class.stop()

    def test_config_with_individual_patient_window_false(self):
        """Test config initialization with individual_patient_window=False."""
        with patch("builtins.print"):  # Suppress print output
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=False,
                testing=True,
            )

        self.assertFalse(config.individual_patient_window)
        self.assertIsNotNone(config.date_list)
        self.assertIsNotNone(config.n_pat_lines)
        # patient_dict should not exist or be None when individual_patient_window=False
        self.assertFalse(
            hasattr(config, "patient_dict") and config.patient_dict is not None
        )

    # FIX: Patched the functions where they are looked up (in config_pat2vec), not where they are defined.
    @patch("pat2vec.util.config_pat2vec.build_patient_dict")
    @patch("pat2vec.util.config_pat2vec.add_offset_column")
    def test_config_with_individual_patient_window_true(
        self, mock_add_offset, mock_build_dict
    ):
        """Test config initialization with individual_patient_window=True."""
        # Create mock dataframe
        mock_df = pd.DataFrame(
            {"patient_id": ["P001", "P002"], "start_date": ["2020-01-01", "2020-02-01"]}
        )

        # Make sure the returned DataFrame has the expected columns
        mock_df_with_offset = mock_df.copy()
        # The build_patient_dict function expects 'start_date_converted' column
        mock_df_with_offset["start_date_converted"] = pd.to_datetime(
            mock_df["start_date"]
        )
        mock_df_with_offset["start_date_offset"] = pd.to_datetime(
            ["2019-01-01", "2019-02-01"]
        )

        mock_add_offset.return_value = mock_df_with_offset
        mock_build_dict.return_value = {
            "P001": (datetime(2020, 1, 1), datetime(2021, 1, 1)),
            "P002": (datetime(2020, 2, 1), datetime(2021, 2, 1)),
        }

        with patch("builtins.print"):
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        self.assertTrue(config.individual_patient_window)
        self.assertIsNone(config.date_list)
        self.assertIsNone(config.n_pat_lines)
        self.assertIsNotNone(config.patient_dict)

    def test_lookback_with_individual_patient_window(self):
        """Test lookback behavior with individual patient window."""
        mock_df = pd.DataFrame(
            {"patient_id": ["P001", "P002"], "start_date": ["2020-01-01", "2020-02-01"]}
        )

        # FIX: Patched the functions where they are looked up (in config_pat2vec), not where they are defined.
        with patch(
            "pat2vec.util.config_pat2vec.add_offset_column"
        ) as mock_add_offset, patch(
            "pat2vec.util.config_pat2vec.build_patient_dict"
        ) as mock_build_dict, patch(
            "builtins.print"
        ):  # Suppress print output

            mock_df_with_offset = mock_df.copy()
            mock_df_with_offset["start_date_converted"] = pd.to_datetime(
                mock_df["start_date"]
            )
            mock_df_with_offset["start_date_offset"] = pd.to_datetime(
                ["2021-01-01", "2021-02-01"]
            )
            mock_add_offset.return_value = mock_df_with_offset

            original_dict = {
                "P001": (datetime(2020, 1, 1), datetime(2021, 1, 1)),
                "P002": (datetime(2020, 2, 1), datetime(2021, 2, 1)),
            }
            mock_build_dict.return_value = original_dict

            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                lookback=True,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

            # When lookback=True, patient_dict values should be reversed
            for key, value in config.patient_dict.items():
                original_value = original_dict[key]
                expected_reversed = tuple(reversed(original_value))
                self.assertEqual(value, expected_reversed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
