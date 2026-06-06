import unittest
import os
import shutil
import tempfile
import pandas as pd
import warnings
from unittest.mock import MagicMock, patch, ANY
from pat2vec.util.methods_post_get import (
    check_csv_integrity,
    check_csv_files_in_directory,
    copy_project_folders_with_substring_match,
    retrieve_pat_annotations,
)


class TestMethodsPostGet(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_check_csv_integrity_valid(self):
        """Verify that a clean CSV with required columns passes silently."""
        csv_path = os.path.join(self.test_dir, "valid.csv")
        pd.DataFrame({"client_idcode": ["P1"], "val": [1]}).to_csv(
            csv_path, index=False
        )

        with warnings.catch_warnings(record=True) as w:
            check_csv_integrity(csv_path)
            self.assertEqual(len(w), 0)

    def test_check_csv_integrity_with_nulls(self):
        """Verify warning when non-nullable columns contain null values."""
        csv_path = os.path.join(self.test_dir, "nulls.csv")
        pd.DataFrame({"client_idcode": [None, "P1"]}).to_csv(csv_path, index=False)

        with self.assertWarns(UserWarning):
            check_csv_integrity(csv_path)

    @patch("pat2vec.util.methods_post_get.remove_file_from_paths")
    def test_check_csv_integrity_delete_logic(self, mock_remove):
        """Ensure delete_broken flag triggers removal logic for empty files."""
        csv_path = os.path.join(self.test_dir, "broken_file.csv")
        # Create empty file to trigger EmptyDataError
        open(csv_path, "w").close()

        config = MagicMock()
        with self.assertWarns(UserWarning):
            check_csv_integrity(csv_path, delete_broken=True, config_obj=config)

        mock_remove.assert_called_once()

    def test_check_csv_files_in_directory_filtering(self):
        """Test recursive directory check with path exclusions for output folders."""
        output_dir = os.path.join(self.test_dir, "outputs")
        os.makedirs(output_dir)

        # f1 is in root, f2 is in an 'output' folder
        f1 = os.path.join(self.test_dir, "valid.csv")
        f2 = os.path.join(output_dir, "broken.csv")

        pd.DataFrame({"client_idcode": ["P1"]}).to_csv(f1, index=False)
        pd.DataFrame({"client_idcode": [None]}).to_csv(f2, index=False)

        with warnings.catch_warnings(record=True) as w:
            # ignore_outputs=True should skip f2 entirely
            check_csv_files_in_directory(self.test_dir, ignore_outputs=True)
            self.assertEqual(len(w), 0)

    @patch("shutil.copytree")
    def test_copy_project_folders(self, mock_copy):
        """Test cloning project subfolders into a new versioned directory."""
        proj_name = "my_proj"
        os.makedirs(os.path.join(self.test_dir, proj_name))
        os.makedirs(os.path.join(self.test_dir, proj_name, "batches_v1"))

        pat2vec_obj = MagicMock()
        pat2vec_obj.config_obj.proj_name = proj_name

        # Change current dir to test dir so relative paths in the function work
        original_cwd = os.getcwd()
        os.chdir(self.test_dir)
        try:
            copy_project_folders_with_substring_match(
                pat2vec_obj, substrings_to_match=["batches"]
            )
            # Should have created my_proj_1
            self.assertTrue(os.path.exists("my_proj_1"))
            mock_copy.assert_called_once()
        finally:
            os.chdir(original_cwd)

    @patch("pat2vec.util.methods_post_get.retrieve_pat_annots_mct_epr")
    def test_retrieve_pat_annotations_wrapper(self, mock_retrieve):
        """Verify the wrapper correctly passes parameters to the underlying retrieval logic."""
        retrieve_pat_annotations("P101", config_obj=MagicMock())
        mock_retrieve.assert_called_once_with("P101", ANY, merge_columns=True)
