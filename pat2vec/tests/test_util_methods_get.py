import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd
from pat2vec.util.methods_get import (
    convert_timestamp_to_tuple,
    convert_date,
    add_offset_column,
    build_patient_dict,
    list_dir_wrapper,
    dump_results,
    get_free_gpu,
    enum_target_date_vector,
    get_empty_date_vector,
)


class TestUtilMethodsGet(unittest.TestCase):
    def test_convert_timestamp_to_tuple(self):
        ts = "2023-10-26T12:00:00.000+0000"
        self.assertEqual(convert_timestamp_to_tuple(ts), (2023, 10))

    def test_convert_date(self):
        self.assertEqual(convert_date("2023-10-26T12:00:00"), datetime(2023, 10, 26))

    def test_add_offset_column(self):
        df = pd.DataFrame({"start": ["2023-01-01", "25/12/2023", "invalid"]})
        offset = timedelta(days=10)
        # Test flexible parsing and offset application
        result = add_offset_column(df, "start", "end", offset, verbose=0)
        self.assertEqual(result.iloc[0]["end"], datetime(2023, 1, 11))
        self.assertEqual(result.iloc[1]["end"], datetime(2024, 1, 4))
        self.assertTrue(pd.isna(result.iloc[2]["end"]))

    def test_build_patient_dict(self):
        df = pd.DataFrame(
            {
                "pid": ["P1", "P2"],
                "start": [datetime(2023, 1, 1), datetime(2023, 2, 1)],
                "end": [datetime(2023, 1, 10), None],
            }
        )
        with self.assertLogs("pat2vec.util.methods_get", level="WARNING") as cm:
            p_dict = build_patient_dict(df, "pid", "start", "end")
            self.assertEqual(len(p_dict), 1)
            self.assertEqual(
                p_dict["P1"], (datetime(2023, 1, 1), datetime(2023, 1, 10))
            )
            self.assertTrue(any("Ignoring patient P2" in line for line in cm.output))

    @patch("os.path.exists")
    @patch("os.listdir")
    def test_list_dir_wrapper_local(self, mock_listdir, mock_exists):
        config = MagicMock()
        config.remote_dump = False
        mock_exists.return_value = True
        mock_listdir.return_value = ["file1.csv"]

        res = list_dir_wrapper("/tmp", config)
        self.assertEqual(res, ["file1.csv"])

    @patch("paramiko.SSHClient")
    def test_list_dir_wrapper_remote(self, mock_ssh):
        config = MagicMock()
        config.remote_dump = True
        config.share_sftp = False

        # Configure mock_ssh to return a mock sftp_client with a listdir method
        mock_sftp_client = MagicMock()
        mock_sftp_client.listdir.return_value = ["remotefile.csv"]
        mock_ssh.return_value.open_sftp.return_value = mock_sftp_client

        res = list_dir_wrapper("/remote", config)
        self.assertEqual(res, ["remotefile.csv"])

    @patch("pat2vec.util.methods_get.subprocess.check_output")
    def test_get_free_gpu_success(self, mock_check_output):
        """Test successful identification of a GPU with free memory."""
        # Mock output from nvidia-smi
        mock_output = b"memory.used, memory.free\n100 MiB, 8000 MiB\n500 MiB, 4000 MiB"
        mock_check_output.return_value = mock_output

        idx, free_mem = get_free_gpu()

        self.assertEqual(idx, 0)
        self.assertEqual(free_mem.strip(), "8000")

    @patch("pat2vec.util.methods_get.subprocess.check_output")
    def test_get_free_gpu_command_not_found(self, mock_check_output):
        """Test behavior when nvidia-smi is not found."""
        mock_check_output.side_effect = FileNotFoundError()

        idx, free_mem = get_free_gpu()
        self.assertEqual(idx, -1)
        self.assertEqual(free_mem, 0)

    @patch("pat2vec.util.methods_get.get_empty_date_vector")
    def test_enum_target_date_vector(self, mock_get_empty):
        """Test creation of a one-hot encoded date vector."""
        mock_config = MagicMock()
        target_date = (2023, 1, 1)
        col_name = "(2023, 1, 1)_date_time_stamp"

        # Mock empty vector
        mock_df = pd.DataFrame(data=0.0, index=[0], columns=[col_name])
        mock_get_empty.return_value = mock_df

        result = enum_target_date_vector(target_date, "P1", mock_config)

        self.assertEqual(result.at[0, col_name], 1)
        self.assertEqual(result.at[0, "client_idcode"], "P1")

    @patch("pat2vec.util.methods_get.generate_date_list")
    def test_get_empty_date_vector(self, mock_generate):
        """Test generation of an empty date vector template."""
        mock_config = MagicMock()
        mock_config.start_date = datetime(2023, 1, 1)
        mock_config.years = 0
        mock_config.months = 0
        mock_config.days = 1
        mock_config.time_window_interval_delta = timedelta(days=1)

        mock_generate.return_value = [(2023, 1, 1), (2023, 1, 2)]

        result = get_empty_date_vector(mock_config)

        expected_cols = ["(2023, 1, 1)_date_time_stamp", "(2023, 1, 2)_date_time_stamp"]
        self.assertCountEqual(result.columns, expected_cols)
        self.assertEqual(result.iloc[0].sum(), 0.0)

    @patch("pat2vec.util.methods_get.pickle.dump")
    @patch("builtins.open", new_callable=MagicMock)
    def test_dump_results_skips_on_database_backend(self, mock_open, mock_pickle):
        """Verify that dump_results returns immediately if the storage backend is 'database'."""
        config = MagicMock()
        config.storage_backend = "database"
        dump_results({"data": 1}, "/tmp/path", config)
        mock_open.assert_not_called()
