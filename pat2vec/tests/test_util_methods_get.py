import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd
import os
import tempfile
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
    write_csv_wrapper,
    read_csv_wrapper,
    exist_check,
    read_remote,
    filter_stripped_list,
    create_remote_folders,
    create_folders_annot_csv_wrapper,
    sftp_exists,
    create_folders,
    create_local_folders,
    create_folders_for_pat,
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

    def test_write_csv_wrapper_local(self):
        """Test local CSV writing."""
        config = MagicMock()
        config.storage_backend = "file"
        config.remote_dump = False
        df = pd.DataFrame({"a": [1], "b": [2]})
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.csv")
            write_csv_wrapper(path, df, config)
            self.assertTrue(os.path.exists(path))
            read_df = pd.read_csv(path)
            pd.testing.assert_frame_equal(df, read_df)

    def test_read_csv_wrapper_local(self):
        """Test local CSV reading."""
        config = MagicMock()
        config.remote_dump = False
        df = pd.DataFrame({"x": [10]})
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "read_test.csv")
            df.to_csv(path, index=False)
            read_df = read_csv_wrapper(path, config)
            pd.testing.assert_frame_equal(df, read_df)

    def test_exist_check_local(self):
        """Test local existence check."""
        config = MagicMock()
        config.remote_dump = False
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "check.me")
            open(path, "w").close()
            self.assertTrue(exist_check(path, config))
            self.assertFalse(exist_check(path + ".missing", config))

    @patch("pat2vec.util.methods_get.list_dir_wrapper")
    def test_filter_stripped_list_local(self, mock_list_dir):
        """Test local patient list stripping logic."""
        config = MagicMock()
        config.strip_list = True
        config.remote_dump = False
        config.current_pat_lines_path = "/dummy/path/"
        config.n_pat_lines = 2
        config.verbosity = 0

        # P1 has 2 files (>= n_pat_lines), P2 has 1 file (< n_pat_lines)
        mock_list_dir.side_effect = [["f1.csv", "f2.csv"], ["f1.csv"]]

        stripped, stripped_start = filter_stripped_list(["P1", "P2"], config)

        self.assertIn("P1", stripped)
        self.assertEqual(stripped_start, ["P2"])

    def test_create_local_folders(self):
        """Test project directory creation."""
        config = MagicMock()
        config.storage_backend = "file"
        config.proj_name = "test_proj"
        with tempfile.TemporaryDirectory() as tmpdir:
            config.root_path = tmpdir
            create_local_folders(config)
            self.assertTrue(
                os.path.exists(os.path.join(tmpdir, "test_proj", "pat_docs"))
            )
            self.assertTrue(
                os.path.exists(os.path.join(tmpdir, "test_proj", "pat_docs_annot_vecs"))
            )

    def test_create_folders_for_pat(self):
        """Test creation of individual patient directories."""
        config = MagicMock()
        config.storage_backend = "file"
        with tempfile.TemporaryDirectory() as tmpdir:
            config.pre_annotation_path = os.path.join(tmpdir, "annots")
            config.pre_annotation_path_mrc = os.path.join(tmpdir, "mrc")
            config.current_pat_lines_path = os.path.join(tmpdir, "lines")
            config.verbosity = 0
            create_folders_for_pat("P_001", config)
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "annots", "P_001")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "lines", "P_001")))

    @patch("paramiko.SSHClient")
    def test_dump_results_remote(self, mock_ssh_client):
        """Test remote dumping of results."""
        config = MagicMock()
        config.storage_backend = "file"
        config.remote_dump = True
        config.share_sftp = False
        config.hostname = "remote.host"
        config.username = "user"
        config.password = "pass"

        mock_sftp = MagicMock()
        mock_ssh_client.return_value.open_sftp.return_value = mock_sftp

        test_data = {"key": "value"}
        test_path = "/remote/path/data.pkl"

        dump_results(test_data, test_path, config)

        mock_ssh_client.return_value.connect.assert_called_once_with(
            hostname="remote.host", username="user", password="pass"
        )
        mock_sftp.open.assert_called_once_with(test_path, "w")
        mock_sftp.open.return_value.__enter__.return_value.write.assert_called_once()

    @patch("paramiko.SSHClient")
    def test_write_csv_wrapper_remote(self, mock_ssh_client):
        """Test remote CSV writing."""
        config = MagicMock()
        config.storage_backend = "file"
        config.remote_dump = True
        config.share_sftp = False
        config.hostname = "remote.host"
        config.username = "user"
        config.password = "pass"

        mock_sftp = MagicMock()
        mock_ssh_client.return_value.open_sftp.return_value = mock_sftp

        df = pd.DataFrame({"a": [1], "b": [2]})
        test_path = "/remote/path/test.csv"

        write_csv_wrapper(test_path, df, config)

        mock_sftp.open.assert_called_once_with(test_path, "w")
        mock_sftp.open.return_value.__enter__.return_value.write.assert_called()

    @patch("paramiko.SSHClient")
    def test_read_remote_success(self, mock_ssh_client):
        """Test successful remote CSV reading."""
        config = MagicMock()
        config.hostname = "remote.host"
        config.username = "user"
        config.password = "pass"
        config.share_sftp = False

        mock_sftp_file = MagicMock()
        mock_sftp_file.read.return_value = b"col1,col2\n1,a\n2,b"
        mock_sftp_client = MagicMock()
        mock_sftp_client.open.return_value.__enter__.return_value = mock_sftp_file
        mock_ssh_client.return_value.open_sftp.return_value = mock_sftp_client

        df = read_remote("/remote/path/file.csv", config)
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]["col1"], 1)

    @patch("paramiko.SSHClient")
    def test_read_csv_wrapper_remote(self, mock_ssh_client):
        """Test remote CSV reading via wrapper."""
        config = MagicMock()
        config.remote_dump = True
        config.hostname = "remote.host"
        config.username = "user"
        config.password = "pass"
        config.share_sftp = False

        mock_sftp_file = MagicMock()
        mock_sftp_file.read.return_value = b"col1,col2\n1,a\n2,b"
        mock_sftp_client = MagicMock()
        mock_sftp_client.open.return_value.__enter__.return_value = mock_sftp_file
        mock_ssh_client.return_value.open_sftp.return_value = mock_sftp_client

        df = read_csv_wrapper("/remote/path/file.csv", config)
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 2)

    @patch("paramiko.SSHClient")
    def test_create_remote_folders(self, mock_ssh_client):
        """Test remote folder creation."""
        config = MagicMock()
        config.storage_backend = "file"
        config.remote_dump = True
        config.share_sftp = False
        config.hostname = "remote.host"
        config.username = "user"
        config.password = "pass"
        config.root_path = "/remote/root"
        config.proj_name = "test_proj"

        mock_sftp = MagicMock()
        mock_ssh_client.return_value.open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError  # Simulate folders not existing

        create_remote_folders(config)

        self.assertEqual(
            mock_sftp.mkdir.call_count, 3
        )  # pat_docs, pat_docs_annot_vecs, merged_batches

    @patch("pat2vec.util.methods_get.create_local_folders")
    @patch("pat2vec.util.methods_get.create_remote_folders")
    def test_create_folders_annot_csv_wrapper(self, mock_remote, mock_local):
        """Test wrapper for local/remote folder creation."""
        config_local = MagicMock(remote_dump=False)
        create_folders_annot_csv_wrapper(config_local)
        mock_local.assert_called_once_with(config_obj=config_local)
        mock_remote.assert_not_called()

        mock_local.reset_mock()
        mock_remote.reset_mock()

        config_remote = MagicMock(remote_dump=True)
        create_folders_annot_csv_wrapper(config_remote)
        mock_remote.assert_called_once_with(config_obj=config_remote)
        mock_local.assert_not_called()

    @patch("paramiko.SSHClient")
    def test_sftp_exists(self, mock_ssh_client):
        """Test remote file existence check."""
        config = MagicMock(share_sftp=False, hostname="h", username="u", password="p")
        mock_sftp = MagicMock()
        mock_ssh_client.return_value.open_sftp.return_value = mock_sftp

        # Test exists
        mock_sftp.stat.return_value = True
        self.assertTrue(sftp_exists("/remote/file", config))

        # Test not exists
        mock_sftp.stat.side_effect = FileNotFoundError
        self.assertFalse(sftp_exists("/remote/nonexistent", config))

    @patch("pat2vec.util.methods_get.sftp_exists")
    def test_exist_check_remote(self, mock_sftp_exists):
        """Test remote existence check via wrapper."""
        config = MagicMock(remote_dump=True)
        mock_sftp_exists.return_value = True
        self.assertTrue(exist_check("/remote/path", config))
        mock_sftp_exists.assert_called_once_with("/remote/path", config)

    @patch("pat2vec.util.methods_get.sftp_exists", return_value=True)
    @patch("pat2vec.util.methods_get.list_dir_wrapper")
    @patch("paramiko.SSHClient")
    def test_filter_stripped_list_remote(
        self, mock_ssh_client, mock_list_dir, mock_sftp_exists
    ):
        """Test remote patient list stripping logic."""
        config = MagicMock()
        config.strip_list = True
        config.remote_dump = True
        config.current_pat_lines_path = "/remote/path/"
        config.n_pat_lines = 2
        config.verbosity = 0
        config.share_sftp = True
        config.hostname = "h"
        config.username = "u"
        config.password = "p"
        mock_sftp = MagicMock()
        mock_ssh_client.return_value.open_sftp.return_value = mock_sftp
        config.sftp_obj = mock_sftp

        # P1 has 3 files (>= n_pat_lines), P2 has 1 file (< n_pat_lines)
        mock_list_dir.side_effect = [["f1.csv", "f2.csv", "f3.csv"], ["f1.csv"]]

        stripped, stripped_start = filter_stripped_list(["P1", "P2"], config)

        # P1 has 2 files (>= n_pat_lines) -> stripped
        # P2 has 1 file (< n_pat_lines) -> not stripped, remains in stripped_start
        self.assertIn("P1", stripped)
        self.assertIn("P2", stripped_start)
        self.assertNotIn("P2", stripped)

    @patch("pat2vec.util.methods_get.os.makedirs")
    @patch("pat2vec.util.methods_get.sftp_exists", return_value=False)
    @patch("paramiko.SSHClient")
    def test_create_folders_remote(
        self, mock_ssh_client, mock_sftp_exists, mock_makedirs
    ):
        """Test remote folder creation for all patients."""
        config = MagicMock()
        config.storage_backend = "file"
        config.remote_dump = True
        config.share_sftp = False
        config.hostname = "remote.host"
        config.username = "user"
        config.password = "pass"
        config.pre_annotation_path = "/remote/annots"
        config.pre_annotation_path_mrc = "/remote/mrc"
        config.current_pat_lines_path = "/remote/lines"
        config.verbosity = 0
        config.share_sftp = True

        mock_sftp = MagicMock()
        mock_ssh_client.return_value.open_sftp.return_value = mock_sftp
        config.sftp_obj = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError()  # Force creation logic

        all_patient_list = ["P_001", "P_002"]
        create_folders(all_patient_list, config)

        # Expect mkdir to be called for each patient in each path
        self.assertEqual(mock_sftp.mkdir.call_count, len(all_patient_list) * 3)

    @patch("pat2vec.util.methods_get.os.makedirs")
    @patch("pat2vec.util.methods_get.sftp_exists", return_value=False)
    @patch("paramiko.SSHClient")
    def test_create_folders_for_pat_remote(
        self, mock_ssh_client, mock_sftp_exists, mock_makedirs
    ):
        """Test remote folder creation for a single patient."""
        config = MagicMock()
        config.storage_backend = "file"
        config.remote_dump = True
        config.share_sftp = False
        config.hostname = "remote.host"
        config.username = "user"
        config.password = "pass"
        config.pre_annotation_path = "/remote/annots"
        config.pre_annotation_path_mrc = "/remote/mrc"
        config.current_pat_lines_path = "/remote/lines"
        config.verbosity = 0
        config.share_sftp = True

        mock_sftp = MagicMock()
        mock_ssh_client.return_value.open_sftp.return_value = mock_sftp
        config.sftp_obj = mock_sftp
        mock_sftp.stat.side_effect = FileNotFoundError()  # Force creation logic

        create_folders_for_pat("P_001", config)

        # Expect mkdir to be called for each path for the single patient
        self.assertEqual(mock_sftp.mkdir.call_count, 3)
