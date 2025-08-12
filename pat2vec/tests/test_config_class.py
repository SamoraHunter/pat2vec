import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import os

# The class to be tested
from pat2vec.util.config_pat2vec import config_class, get_test_options_dict


class TestConfigClass(unittest.TestCase):
    """Unit tests for the config_class."""

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

    def test_default_initialization(self):
        """Test that the class initializes with expected default values."""
        with patch("builtins.print"):  # Suppress print output
            config = config_class(testing=True)

        self.assertEqual(config.proj_name, "new_project")
        self.assertEqual(config.start_date, datetime(1995, 1, 1))
        self.assertEqual(config.years, 0)
        self.assertEqual(config.months, 0)
        self.assertEqual(config.days, 1)
        self.assertFalse(config.remote_dump)
        self.assertEqual(config.verbosity, 3)
        self.assertTrue(config.testing)
        self.assertFalse(config.medcat)
        self.assertTrue(config.lookback)
        self.assertEqual(config.time_window_interval_delta, relativedelta(days=1))
        self.assertIsNotNone(config.main_options)
        self.assertIsNotNone(config.filter_arguments)
        self.assertIsNotNone(config.feature_engineering_arg_dict)
        self.assertEqual(config.global_start_year, "1995")
        self.assertEqual(config.global_end_year, "2023")

    def test_custom_parameter_override(self):
        """Test that custom parameters correctly override the defaults."""
        custom_options = {"demo": False, "bloods": True}
        custom_start_date = datetime(2022, 5, 5)
        with patch("builtins.print"):
            config = config_class(
                proj_name="custom_project",
                start_date=custom_start_date,
                years=5,
                verbosity=0,
                main_options=custom_options,
                testing=True,
            )

        self.assertEqual(config.proj_name, "custom_project")
        self.assertEqual(config.start_date, custom_start_date)
        self.assertEqual(config.years, 5)
        self.assertEqual(config.verbosity, 0)
        self.assertEqual(config.main_options["demo"], False)
        self.assertEqual(config.main_options["bloods"], True)

    def test_testing_mode_updates_options(self):
        """Test that when testing=True, main_options are filtered."""
        # This option is True by default, but False in test_options
        custom_options = {"demo": True, "bmi": True}
        with patch("builtins.print"):
            config = config_class(main_options=custom_options, testing=True)

        # 'demo' is True in test_options, so it should remain True
        self.assertTrue(config.main_options["demo"])
        # 'bmi' is False in test_options, so it should be forced to False
        self.assertFalse(config.main_options["bmi"])
        # The filename should be updated
        self.assertEqual(
            config.treatment_doc_filename,
            os.path.join(os.getcwd(), "test_files", "treatment_docs.csv"),
        )

    @patch("pat2vec.util.config_pat2vec.generate_date_list")
    def test_date_list_generation_for_global_window(self, mock_generate_date_list):
        """Test that date_list is generated when individual_patient_window is False."""
        mock_generate_date_list.return_value = [(2020, 1, 1), (2020, 1, 2)]
        with patch("builtins.print"):
            config = config_class(
                start_date=self.base_start_date,
                individual_patient_window=False,
                testing=True,
            )

        self.assertTrue(mock_generate_date_list.called)
        self.assertEqual(config.date_list, [(2020, 1, 1), (2020, 1, 2)])
        self.assertEqual(config.n_pat_lines, 2)
        self.assertFalse(
            hasattr(config, "patient_dict") and config.patient_dict is not None
        )

    @patch("pat2vec.util.config_pat2vec.build_patient_dict")
    @patch("pat2vec.util.config_pat2vec.add_offset_column")
    def test_patient_dict_generation_for_individual_window(
        self, mock_add_offset, mock_build_dict
    ):
        """Test that patient_dict is generated when individual_patient_window is True."""
        mock_df = pd.DataFrame({"patient_id": ["P001"], "start_date": ["2020-01-01"]})
        mock_add_offset.return_value = mock_df  # Simplified mock
        mock_build_dict.return_value = {
            "P001": (datetime(2020, 1, 1), datetime(2021, 1, 1))
        }

        with patch("builtins.print"):
            config = config_class(
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
                lookback=False,  # Set to False to avoid reversing the patient_dict
            )

        self.assertTrue(mock_add_offset.called)
        self.assertTrue(mock_build_dict.called)
        self.assertIsNotNone(config.patient_dict)
        self.assertEqual(
            config.patient_dict["P001"], (datetime(2020, 1, 1), datetime(2021, 1, 1))
        )
        self.assertIsNone(config.date_list)
        self.assertIsNone(config.n_pat_lines)

    def test_global_date_validation_swap(self):
        """Test that global dates are swapped if in the wrong order."""
        with patch("builtins.print") as mock_print:
            config = config_class(
                global_start_year=2022,
                global_start_month=1,
                global_start_day=1,
                global_end_year=2021,
                global_end_month=1,
                global_end_day=1,
                testing=True,
            )
            self.assertEqual(config.global_start_year, "2021")
            self.assertEqual(config.global_end_year, "2022")
            mock_print.assert_any_call(
                "Swapping dates to ensure Elasticsearch compatibility..."
            )

    @patch("paramiko.SSHClient")
    def test_remote_dump_sftp_setup(self, mock_ssh_client):
        """Test that SFTP client is set up when remote_dump is True."""
        mock_instance = mock_ssh_client.return_value
        mock_instance.open_sftp.return_value = MagicMock()

        with patch("builtins.print"):
            config = config_class(
                remote_dump=True,
                hostname="remote.server",
                username="user",
                password="pw",
                testing=True,
            )

        mock_ssh_client.assert_called_once()
        mock_instance.connect.assert_called_once_with(
            hostname="remote.server", username="user", password="pw"
        )
        mock_instance.open_sftp.assert_called_once()
        self.assertIsNotNone(config.sftp_obj)
