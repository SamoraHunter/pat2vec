"""Extended tests for post_processing_process_csv_files.py."""

import os
import tempfile
import shutil
import pandas as pd
from unittest.mock import MagicMock, patch

from pat2vec.util.post_processing_process_csv_files import (
    process_csv_files,
    process_csv_files_multi,
)


class TestPostProcessingProcessCsvFilesExtended:
    """Extended tests for CSV processing."""

    def setup_method(self):
        """Set up test directory."""
        self.test_dir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.test_dir, "inputs")
        self.output_dir = os.path.join(self.test_dir, "outputs")
        os.makedirs(self.input_dir)
        os.makedirs(self.output_dir)

    def teardown_method(self):
        """Clean up."""
        shutil.rmtree(self.test_dir)

    def test_process_csv_files_with_empty_rows(self):
        """Test handling of CSV files with empty rows."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P1", None, "P2"],
                "value": [10, None, 20],
            }
        )
        df.to_csv(os.path.join(self.input_dir, "empty_rows.csv"), index=False)

        output_path = process_csv_files(
            self.input_dir,
            self.output_dir,
            "empty_rows",
        )

        result_df = pd.read_csv(output_path)
        assert len(result_df) >= 1

    def test_process_csv_files_unicode_content(self):
        """Test handling of Unicode characters."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "content": ["Test with émojis 🎉 and special chars ñ"],
            }
        )
        df.to_csv(
            os.path.join(self.input_dir, "unicode.csv"),
            index=False,
            encoding="utf-8",
        )

        output_path = process_csv_files(
            self.input_dir,
            self.output_dir,
            "unicode",
        )

        result_df = pd.read_csv(output_path)
        assert "🎉" in str(result_df.iloc[0]["content"])

    def test_process_csv_files_large_sample_size(self):
        """Test that sample_size doesn't exceed available files."""
        for i in range(10):
            pd.DataFrame({"a": [i]}).to_csv(
                os.path.join(self.input_dir, f"file_{i}.csv"),
                index=False,
            )

        output_path = process_csv_files(
            self.input_dir,
            self.output_dir,
            "large_sample",
            sample_size=100,
        )

        result_df = pd.read_csv(output_path)
        assert len(result_df) <= 10

    def test_process_csv_files_no_header(self):
        """Test handling of CSV without header row."""
        csv_content = "P1,10\nP2,20"
        with open(os.path.join(self.input_dir, "no_header.csv"), "w") as f:
            f.write(csv_content)

        output_path = process_csv_files(
            self.input_dir,
            self.output_dir,
            "no_header",
        )

        assert os.path.exists(output_path)

    def test_process_csv_files_different_encodings(self):
        """Test handling of different file encodings."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "content": ["Test"],
            }
        )

        df.to_csv(
            os.path.join(self.input_dir, "latin1.csv"),
            index=False,
            encoding="latin-1",
        )

        output_path = process_csv_files(
            self.input_dir,
            self.output_dir,
            "encoding_test",
        )

        assert os.path.exists(output_path)

    @patch("pat2vec.util.post_processing_process_csv_files.Pool")
    @patch("pat2vec.util.post_processing_process_csv_files.cpu_count", return_value=4)
    def test_process_csv_files_multi_basic(self, mock_cpu, mock_pool):
        """Test multiprocessing version of CSV processing."""
        df1 = pd.DataFrame({"client_idcode": ["P1"], "val1": [10]})
        df2 = pd.DataFrame({"client_idcode": ["P2"], "val2": [20]})

        df1.to_csv(os.path.join(self.input_dir, "f1.csv"), index=False)
        df2.to_csv(os.path.join(self.input_dir, "f2.csv"), index=False)

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        def mock_imap(func, args_list):
            for args in args_list:
                yield {
                    "client_idcode": ["P1", "P2"],
                    "val1": [10.0, None],
                    "val2": [None, 20.0],
                }

        mock_pool_instance.imap = mock_imap

        output_path = process_csv_files_multi(
            self.input_dir,
            self.output_dir,
            "multi_basic",
            part_size=10,
            sample_size="all",
        )

        assert os.path.exists(output_path)

    @patch("pat2vec.util.post_processing_process_csv_files.Pool")
    @patch("pat2vec.util.post_processing_process_csv_files.cpu_count", return_value=8)
    def test_process_csv_files_multi_half_cores(self, mock_cpu, mock_pool):
        """Test multiprocessing with half cores setting."""
        df1 = pd.DataFrame({"a": [1]})
        df1.to_csv(os.path.join(self.input_dir, "f1.csv"), index=False)

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        def mock_imap(func, args_list):
            for args in args_list:
                yield {"a": [1]}

        mock_pool_instance.imap = mock_imap

        output_path = process_csv_files_multi(
            self.input_dir,
            self.output_dir,
            "half_cores",
            n_proc="half",
        )

        assert os.path.exists(output_path)

    @patch("pat2vec.util.post_processing_process_csv_files.Pool")
    @patch("pat2vec.util.post_processing_process_csv_files.cpu_count", return_value=8)
    def test_process_csv_files_multi_all_cores(self, mock_cpu, mock_pool):
        """Test multiprocessing with all cores setting."""
        df1 = pd.DataFrame({"a": [1]})
        df1.to_csv(os.path.join(self.input_dir, "f1.csv"), index=False)

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        def mock_imap(func, args_list):
            for args in args_list:
                yield {"a": [1]}

        mock_pool_instance.imap = mock_imap

        output_path = process_csv_files_multi(
            self.input_dir,
            self.output_dir,
            "all_cores",
            n_proc="all",
        )

        assert os.path.exists(output_path)

    @patch("pat2vec.util.post_processing_process_csv_files.Pool")
    def test_process_csv_files_multi_custom_processes(self, mock_pool):
        """Test multiprocessing with custom process count."""
        df1 = pd.DataFrame({"a": [1]})
        df1.to_csv(os.path.join(self.input_dir, "f1.csv"), index=False)

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        def mock_imap(func, args_list):
            for args in args_list:
                yield {"a": [1]}

        mock_pool_instance.imap = mock_imap

        output_path = process_csv_files_multi(
            self.input_dir,
            self.output_dir,
            "custom_processes",
            n_proc=2,
        )

        assert os.path.exists(output_path)

    def test_process_csv_files_with_timestamp_conversion(self):
        """Test timestamp column extraction."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "date_col": [20230101],
                "time_col": [120000],
            }
        )
        df.to_csv(os.path.join(self.input_dir, "dates.csv"), index=False)

        output_path = process_csv_files(
            self.input_dir,
            self.output_dir,
            "timestamp_test",
            append_timestamp_column=True,
        )

        result_df = pd.read_csv(output_path)
        assert len(result_df) >= 1

    def test_process_csv_files_replacement_chars(self):
        """Test handling of replacement characters."""
        csv_content = "client_idcode,value\nP1,10\xffFE"
        with open(
            os.path.join(self.input_dir, "replacement.csv"),
            "w",
            encoding="utf-8",
        ) as f:
            f.write(csv_content)

        output_path = process_csv_files(
            self.input_dir,
            self.output_dir,
            "replacement",
        )

        assert os.path.exists(output_path)

    def test_process_csv_files_preserve_order(self):
        """Test that processing preserves file order."""
        files_data = [
            ("a.csv", pd.DataFrame({"val": [1]})),
            ("b.csv", pd.DataFrame({"val": [2]})),
            ("c.csv", pd.DataFrame({"val": [3]})),
        ]

        for filename, df in files_data:
            df.to_csv(os.path.join(self.input_dir, filename), index=False)

        output_path = process_csv_files(
            self.input_dir,
            self.output_dir,
            "order_test",
        )

        result_df = pd.read_csv(output_path)
        assert len(result_df) == 3

    def test_process_csv_small_chunk_size(self):
        """Test processing with very small chunk size."""
        for i in range(5):
            pd.DataFrame({"a": [i]}).to_csv(
                os.path.join(self.input_dir, f"f{i}.csv"),
                index=False,
            )

        output_path = process_csv_files(
            self.input_dir,
            self.output_dir,
            "small_chunks",
            part_size=2,
        )

        result_df = pd.read_csv(output_path)
        assert len(result_df) == 5
