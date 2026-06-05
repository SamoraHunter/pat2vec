import unittest
import os
import shutil
import tempfile
import pandas as pd
import warnings
from unittest.mock import MagicMock, patch
from pat2vec.util.methods_post_get import (
    retrieve_pat_annotations,
    copy_project_folders_with_substring_match,
    check_csv_integrity,
    check_csv_files_in_directory,
)


class TestMethodsPostGet(unittest.TestCase):
    """Unit tests for utility methods handling post-retrieval data processing and integrity checks."""

    def setUp(self):
        """Set up temporary directory and mock configuration."""
        self.test_dir = tempfile.mkdtemp()
        self.config_obj = MagicMock()
        self.config_obj.proj_name = "test_project"
        self.config_obj.storage_backend = "file"

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    @patch("pat2vec.util.methods_post_get.retrieve_pat_annots_mct_epr")
    def test_retrieve_pat_annotations_proxy(self, mock_retrieve):
        """Verify that retrieve_pat_annotations correctly proxies to retrieve_pat_annots_mct_epr."""
        mock_retrieve.return_value = pd.DataFrame({"client_idcode": ["P1"]})
        result = retrieve_pat_annotations("P1", self.config_obj)
        self.assertEqual(len(result), 1)
        mock_retrieve.assert_called_once_with("P1", self.config_obj, merge_columns=True)

    def test_check_csv_integrity_valid(self):
        """Test integrity check on a valid CSV file."""
        csv_path = os.path.join(self.test_dir, "valid.csv")
        df = pd.DataFrame({"client_idcode": ["P1", "P2"], "data": [1, 2]})
        df.to_csv(csv_path, index=False)
        with warnings.catch_warnings(record=True) as w:
            check_csv_integrity(csv_path, verbosity=0)
            self.assertEqual(len(w), 0)

    def test_check_csv_integrity_missing_id(self):
        """Test integrity check on a CSV with missing critical values."""
        csv_path = os.path.join(self.test_dir, "invalid.csv")
        df = pd.DataFrame({"client_idcode": ["P1", None], "data": [1, 2]})
        df.to_csv(csv_path, index=False)
        with self.assertWarns(UserWarning) as cm:
            check_csv_integrity(csv_path)
        self.assertIn("Column client_idcode contains missing values", str(cm.warning))

    @patch("pat2vec.util.methods_post_get.remove_file_from_paths")
    def test_check_csv_integrity_delete_logic(self, mock_remove):
        """Test that broken files are deleted when delete_broken is True."""
        csv_path = os.path.join(self.test_dir, "broken_file.csv")
        with open(csv_path, "w") as _:
            pass
        check_csv_integrity(csv_path, delete_broken=True, config_obj=self.config_obj)
        mock_remove.assert_called_once_with("broken_file", config_obj=self.config_obj)

    @patch("pat2vec.util.methods_post_get.check_csv_integrity")
    def test_check_csv_files_in_directory_recursion(self, mock_check):
        """Test recursive directory scanning for CSV integrity."""
        os.makedirs(os.path.join(self.test_dir, "outputs"))
        os.makedirs(os.path.join(self.test_dir, "data", "subdir"))
        f1 = os.path.join(self.test_dir, "data", "f1.csv")
        f2 = os.path.join(self.test_dir, "data", "subdir", "f2.csv")
        f_ignored = os.path.join(self.test_dir, "outputs", "f3.csv")
        for f in [f1, f2, f_ignored]:
            open(f, "a").close()
        check_csv_files_in_directory(self.test_dir, ignore_outputs=True)
        self.assertEqual(mock_check.call_count, 2)
        called_paths = [call.args[0] for call in mock_check.call_args_list]
        self.assertIn(f1, called_paths)
        self.assertIn(f2, called_paths)

    @patch("shutil.copytree")
    @patch("os.makedirs")
    @patch("os.path.exists")
    @patch("os.listdir")
    def test_copy_project_folders_versioning(
        self, mock_listdir, mock_exists, mock_makedirs, mock_copytree
    ):
        """Test project folder copying with automated version suffix incrementing."""
        pat2vec_obj = MagicMock()
        pat2vec_obj.config_obj.proj_name = "exp1"
        mock_exists.side_effect = lambda x: x in ["exp1", "exp1_1"]
        mock_listdir.return_value = ["batches_v1", "results"]
        copy_project_folders_with_substring_match(
            pat2vec_obj, substrings_to_match=["batches"]
        )
        mock_makedirs.assert_called_once_with("exp1_2")
        mock_copytree.assert_called_once_with("exp1/batches_v1", "exp1_2/batches_v1")


if __name__ == "__main__":
    unittest.main()
