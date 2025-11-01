import csv
import os
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

# The function to be tested
from pat2vec.util.post_processing_process_csv_files import process_csv_files


class TestProcessCsvFiles(unittest.TestCase):
    """Unit tests for the process_csv_files function."""

    def setUp(self):
        """Set up a temporary directory for test files."""
        self.test_dir = tempfile.TemporaryDirectory()
        self.input_path = os.path.join(self.test_dir.name, "input")
        self.output_path = os.path.join(self.test_dir.name, "output")
        os.makedirs(self.input_path)
        os.makedirs(self.output_path)

    def tearDown(self):
        """Clean up the temporary directory."""
        self.test_dir.cleanup()

    def _create_csv(self, filename, headers, data):
        """Helper function to create a CSV file."""
        filepath = os.path.join(self.input_path, filename)
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(data)
        return filepath

    @patch("pat2vec.util.post_processing_process_csv_files.tqdm", lambda x, **kwargs: x)
    def test_basic_concatenation(self):
        """Test concatenation of CSVs with identical columns."""
        self._create_csv("file1.csv", ["id", "name"], [["1", "Alice"], ["2", "Bob"]])
        self._create_csv(
            "file2.csv", ["id", "name"], [["3", "Charlie"], ["4", "David"]]
        )

        output_file = process_csv_files(self.input_path, self.output_path)

        self.assertTrue(os.path.exists(output_file))
        df = pd.read_csv(output_file)
        self.assertEqual(len(df), 4)
        self.assertEqual(list(df.columns), ["id", "name"])
        # Check that all expected IDs are present, regardless of order
        self.assertEqual(sorted(df["id"].tolist()), [1, 2, 3, 4])

    @patch("pat2vec.util.post_processing_process_csv_files.tqdm", lambda x, **kwargs: x)
    def test_concatenation_with_different_columns(self):
        """Test concatenation of CSVs with different columns."""
        self._create_csv("file1.csv", ["id", "name"], [["1", "Alice"]])
        self._create_csv("file2.csv", ["id", "age"], [["2", "30"]])

        output_file = process_csv_files(self.input_path, self.output_path)
        df = pd.read_csv(output_file)

        self.assertEqual(len(df), 2)
        # The function sorts column names
        self.assertEqual(sorted(list(df.columns)), ["age", "id", "name"])

        # Check values and NaNs (which become empty strings and then read as NaN by pandas)
        row1 = df[df["id"] == 1]
        self.assertEqual(row1["name"].iloc[0], "Alice")
        self.assertTrue(pd.isna(row1["age"].iloc[0]))

        row2 = df[df["id"] == 2]
        self.assertEqual(row2["age"].iloc[0], 30)
        self.assertTrue(pd.isna(row2["name"].iloc[0]))

    @patch("pat2vec.util.post_processing_process_csv_files.tqdm", lambda x, **kwargs: x)
    def test_sample_size_parameter(self):
        """Test that only a sample of files is processed."""
        self._create_csv("file1.csv", ["id"], [["1"]])
        self._create_csv("file2.csv", ["id"], [["2"]])
        self._create_csv("file3.csv", ["id"], [["3"]])

        output_file = process_csv_files(
            self.input_path, self.output_path, sample_size=2
        )
        df = pd.read_csv(output_file)

        self.assertEqual(len(df), 2)
        # The exact files sampled depend on os.walk, but the count should be right.
        self.assertTrue(all(item in [1, 2, 3] for item in df["id"].tolist()))

    @patch("pat2vec.util.post_processing_process_csv_files.tqdm", lambda x, **kwargs: x)
    def test_output_filename_suffix(self):
        """Test that the output filename suffix is correctly applied."""
        self._create_csv("file1.csv", ["id"], [["1"]])

        output_file = process_csv_files(
            self.input_path, self.output_path, output_filename_suffix="custom_suffix"
        )

        expected_filename = "concatenated_data_custom_suffix.csv"
        self.assertEqual(os.path.basename(output_file), expected_filename)
        self.assertTrue(os.path.exists(output_file))

    def test_empty_input_directory_raises_error(self):
        """Test that an empty input directory raises a ValueError."""
        empty_dir = os.path.join(self.test_dir.name, "empty_input")
        os.makedirs(empty_dir)
        with self.assertRaisesRegex(ValueError, "No CSV files found"):
            process_csv_files(empty_dir, self.output_path)

    def test_directory_with_no_csv_files_raises_error(self):
        """Test that a directory with no CSV files raises a ValueError."""
        with open(os.path.join(self.input_path, "test.txt"), "w") as f:
            f.write("hello")
        with self.assertRaisesRegex(ValueError, "No CSV files found"):
            process_csv_files(self.input_path, self.output_path)

    @patch("pat2vec.util.post_processing_process_csv_files.tqdm", lambda x, **kwargs: x)
    def test_backup_file_creation(self):
        """Test that an existing output file is backed up."""
        self._create_csv("file1.csv", ["id"], [["1"]])

        # Create a dummy output file first
        output_file_path = os.path.join(
            self.output_path, "concatenated_data_concatenated_output.csv"
        )
        with open(output_file_path, "w") as f:
            f.write("pre-existing content")

        self.assertTrue(os.path.exists(output_file_path))

        process_csv_files(self.input_path, self.output_path)

        # Check that the new output file exists and is correct
        self.assertTrue(os.path.exists(output_file_path))
        df = pd.read_csv(output_file_path)
        self.assertEqual(len(df), 1)

        # Check that a backup file was created
        backup_found = any("backup" in f for f in os.listdir(self.output_path))
        self.assertTrue(backup_found)

    @patch("pat2vec.util.post_processing_process_csv_files.extract_datetime_to_column")
    @patch("pat2vec.util.post_processing_process_csv_files.tqdm", lambda x, **kwargs: x)
    def test_append_timestamp_column_true(self, mock_extract_dt):
        """Test that extract_datetime_to_column is called when append_timestamp_column is True."""
        self._create_csv("file1.csv", ["id"], [["1"]])

        process_csv_files(
            self.input_path, self.output_path, append_timestamp_column=True
        )

        mock_extract_dt.assert_called_once()

    @patch("pat2vec.util.post_processing_process_csv_files.tqdm", lambda x, **kwargs: x)
    def test_empty_csv_file_handling(self):
        """Test that empty CSV files are skipped with a warning."""
        self._create_csv("file1.csv", ["id"], [["1"]])
        # Create an empty file
        empty_file_path = os.path.join(self.input_path, "empty.csv")
        open(empty_file_path, "w").close()

        with self.assertLogs(
            "pat2vec.util.post_processing_process_csv_files", level="WARNING"
        ) as cm:
            process_csv_files(self.input_path, self.output_path)
            self.assertTrue(
                any(
                    f"Empty file skipped: {empty_file_path}" in log for log in cm.output
                )
            )
