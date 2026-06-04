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
        config.sftp_obj.listdir.return_value = ["remotefile.csv"]
        res = list_dir_wrapper("/remote", config)
        self.assertEqual(res, ["remotefile.csv"])
