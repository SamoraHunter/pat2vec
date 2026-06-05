import unittest
import os
import shutil
import tempfile
import pandas as pd
from pat2vec.util.post_processing_process_csv_files import (
    process_csv_files,
    process_csv_files_multi,
)


class TestPostProcessingProcessCsvFiles(unittest.TestCase):
    """Unit tests for the CSV processing and concatenation utility module."""

    def setUp(self):
        """Set up temporary directory and sample CSV files."""
        self.test_dir = tempfile.mkdtemp()
        self.input_path = os.path.join(self.test_dir, "input")
        self.output_path = os.path.join(self.test_dir, "output")
        os.makedirs(self.input_path)
        os.makedirs(self.output_path)

        # Create dummy CSV files with varying columns to test union logic
        self.df1 = pd.DataFrame({"client_idcode": ["P1", "P2"], "val1": [10, 20]})
        self.df2 = pd.DataFrame({"client_idcode": ["P3"], "val2": [30]})

        self.df1.to_csv(os.path.join(self.input_path, "file1.csv"), index=False)
        self.df2.to_csv(os.path.join(self.input_path, "file2.csv"), index=False)

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    def test_process_csv_files_concatenation(self):
        """Test sequential concatenation with column union handling."""
        output_file = process_csv_files(
            input_path=self.input_path,
            out_folder=self.output_path,
            output_filename_suffix="test_seq",
        )

        self.assertTrue(os.path.exists(output_file))
        result_df = pd.read_csv(output_file)

        # Verify data merge (3 rows total)
        self.assertEqual(len(result_df), 3)
        # Check for union of columns
        self.assertCountEqual(result_df.columns, ["client_idcode", "val1", "val2"])

        # Verify specific value alignment: P1 has val1=10, P3 has val2=30
        self.assertEqual(
            result_df[result_df["client_idcode"] == "P1"]["val1"].iloc[0], 10.0
        )
        self.assertEqual(
            result_df[result_df["client_idcode"] == "P3"]["val2"].iloc[0], 30.0
        )
        # Verify missing values are handled (read as NaN)
        self.assertTrue(
            pd.isna(result_df[result_df["client_idcode"] == "P3"]["val1"].iloc[0])
        )

    def test_process_csv_files_sample_size(self):
        """Test concatenation with a limited sample of files."""
        output_file = process_csv_files(
            input_path=self.input_path,
            out_folder=self.output_path,
            output_filename_suffix="test_sample",
            sample_size=1,
        )
        result_df = pd.read_csv(output_file)
        # Should only contain data from 1 file (either 1 or 2 rows)
        self.assertIn(len(result_df), [1, 2])

    def test_process_csv_files_timestamp_extraction(self):
        """Test that datetime extraction from binary columns works during processing."""
        # File with binary date column pattern
        df_ts = pd.DataFrame(
            {"client_idcode": ["P4"], "(2023, 02, 20)_date_time_stamp": [1]}
        )
        df_ts.to_csv(os.path.join(self.input_path, "file_ts.csv"), index=False)

        output_file = process_csv_files(
            input_path=self.input_path,
            out_folder=self.output_path,
            output_filename_suffix="test_ts",
            append_timestamp_column=True,
        )

        result_df = pd.read_csv(output_file)
        self.assertIn("extracted_datetime_stamp", result_df.columns)
        # Verify the extracted date matches the column name
        p4_ts = result_df[result_df["client_idcode"] == "P4"][
            "extracted_datetime_stamp"
        ].iloc[0]
        self.assertTrue(p4_ts.startswith("2023-02-20"))

    def test_process_csv_files_multi(self):
        """Test the multiprocessing version of the concatenation utility."""
        output_file = process_csv_files_multi(
            input_path=self.input_path,
            out_folder=self.output_path,
            output_filename_suffix="test_multi",
            n_proc=1,  # Use 1 to simplify execution in test environments
        )
        self.assertTrue(os.path.exists(output_file))
        result_df = pd.read_csv(output_file)
        self.assertEqual(len(result_df), 3)

    def test_process_csv_files_empty_input(self):
        """Test that ValueError is raised when no CSVs are found."""
        empty_dir = os.path.join(self.test_dir, "empty")
        os.makedirs(empty_dir)
        with self.assertRaises(ValueError):
            process_csv_files(input_path=empty_dir)


if __name__ == "__main__":
    unittest.main()
