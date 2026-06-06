import unittest
import os
import shutil
import tempfile
import pandas as pd
from pat2vec.util.post_processing_process_csv_files import process_csv_files


class TestPostProcessingProcessCsvFiles(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.test_dir, "inputs")
        self.output_dir = os.path.join(self.test_dir, "outputs")
        os.makedirs(self.input_dir)
        os.makedirs(self.output_dir)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_process_csv_files_standard(self):
        """Test concatenation of CSVs with different but overlapping columns."""
        df1 = pd.DataFrame({"client_idcode": ["P1"], "val1": [10]})
        df2 = pd.DataFrame({"client_idcode": ["P2"], "val2": [20]})
        df1.to_csv(os.path.join(self.input_dir, "f1.csv"), index=False)
        df2.to_csv(os.path.join(self.input_dir, "f2.csv"), index=False)

        output_path = process_csv_files(self.input_dir, self.output_dir, "standard")

        self.assertTrue(os.path.exists(output_path))
        result_df = pd.read_csv(output_path)
        self.assertEqual(len(result_df), 2)
        # Unique columns: client_idcode, val1, val2
        self.assertCountEqual(result_df.columns, ["client_idcode", "val1", "val2"])

        p1_row = result_df[result_df["client_idcode"] == "P1"].iloc[0]
        self.assertEqual(p1_row["val1"], 10.0)
        self.assertTrue(pd.isna(p1_row["val2"]))

    def test_process_csv_files_sample_logic(self):
        """Test that only a subset of files is processed if sample_size is set."""
        for i in range(5):
            pd.DataFrame({"a": [i]}).to_csv(
                os.path.join(self.input_dir, f"{i}.csv"), index=False
            )

        # Test 'all' string
        output_path = process_csv_files(
            self.input_dir, self.output_dir, "all_str", sample_size="all"
        )
        self.assertEqual(len(pd.read_csv(output_path)), 5)

        # Test integer sample
        output_path = process_csv_files(
            self.input_dir, self.output_dir, "sampled", sample_size=2
        )
        self.assertEqual(len(pd.read_csv(output_path)), 2)

    def test_process_csv_files_backup_existing(self):
        """Verify that existing output files are backed up rather than overwritten immediately."""
        output_file = os.path.join(self.output_dir, "concatenated_data_exists.csv")
        with open(output_file, "w") as f:
            f.write("pre-existing")

        pd.DataFrame({"a": [1]}).to_csv(
            os.path.join(self.input_dir, "f1.csv"), index=False
        )
        process_csv_files(self.input_dir, self.output_dir, "exists")

        filesInOutput = os.listdir(self.output_dir)
        self.assertTrue(any("backup" in f for f in filesInOutput))

    def test_process_csv_files_whitespace_stripping(self):
        """Verify that column names and values are stripped of surrounding whitespace."""
        csv_content = " client_idcode , value \n P1 , 100 "
        csv_path = os.path.join(self.input_dir, "ws.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)

        output_path = process_csv_files(self.input_dir, self.output_dir, "strip")
        result_df = pd.read_csv(output_path)

        self.assertIn("client_idcode", result_df.columns)
        self.assertEqual(result_df.at[0, "client_idcode"], "P1")

    def test_process_csv_files_no_files(self):
        """Verify ValueError is raised when input path contains no CSVs."""
        # Remove any files created in setUp or previous tests
        for f in os.listdir(self.input_dir):
            os.remove(os.path.join(self.input_dir, f))

        with self.assertRaises(ValueError):
            process_csv_files(self.input_dir, self.output_dir, "empty")
