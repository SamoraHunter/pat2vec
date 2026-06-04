import unittest
import os
import pandas as pd
import tempfile
import shutil
from pat2vec.util.post_processing_process_csv_files import process_csv_files


class TestPostProcessingProcessCsvFiles(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.in_dir = os.path.join(self.test_dir, "input")
        self.out_dir = os.path.join(self.test_dir, "output")
        os.makedirs(self.in_dir)

        # Create sample files with different columns
        df1 = pd.DataFrame({"id": [1], "val": ["a"]})
        df2 = pd.DataFrame({"id": [2], "other": ["b"]})
        df1.to_csv(os.path.join(self.in_dir, "f1.csv"), index=False)
        df2.to_csv(os.path.join(self.in_dir, "f2.csv"), index=False)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_process_csv_files_full(self):
        output_file = process_csv_files(self.in_dir, self.out_dir, "test")
        self.assertTrue(os.path.exists(output_file))
        df = pd.read_csv(output_file)
        self.assertEqual(len(df), 2)
        # Verify columns are merged correctly
        self.assertIn("val", df.columns)
        self.assertIn("other", df.columns)

    def test_process_csv_files_backup(self):
        # Create a dummy output to trigger backup
        os.makedirs(self.out_dir, exist_ok=True)
        dummy_out = os.path.join(self.out_dir, "concatenated_data_bk.csv")
        open(dummy_out, "a").close()

        process_csv_files(self.in_dir, self.out_dir, "bk")
        # Check if backup was created (should find a file with _backup in name)
        self.assertTrue(any("_backup" in f for f in os.listdir(self.out_dir)))
