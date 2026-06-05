import unittest
import pandas as pd
import os
import tempfile
import shutil
from pat2vec.util.testing_helpers import read_test_data


class TestTestingHelpers(unittest.TestCase):
    """Unit tests for the testing utility helpers."""

    def setUp(self):
        """Set up a temporary directory."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up the temporary directory."""
        shutil.rmtree(self.test_dir)

    def test_read_test_data_success(self):
        """Test successful reading of CSV test data."""
        path = os.path.join(self.test_dir, "test.csv")
        df = pd.DataFrame({"a": [1], "b": [2]})
        df.to_csv(path, index=False)
        result = read_test_data(path)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)

    def test_read_test_data_not_found(self):
        """Test behavior when the test data file is missing."""
        result = read_test_data("non_existent_path.csv")
        self.assertIsNone(result)

    def test_read_test_data_empty(self):
        """Test behavior when the test data file is empty."""
        path = os.path.join(self.test_dir, "empty.csv")
        pd.DataFrame().to_csv(path, index=False)
        with self.assertLogs("pat2vec.util.testing_helpers", level="WARNING") as cm:
            result = read_test_data(path)
            self.assertTrue(
                any("Test data file is empty" in line for line in cm.output)
            )
        self.assertEqual(len(result), 0)


if __name__ == "__main__":
    unittest.main()
