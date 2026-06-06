import unittest
import os
import shutil
import tempfile
import pandas as pd
from unittest.mock import patch
from pat2vec.util.post_processing_utils import (
    count_files,
    copy_files_and_dirs,
    filter_and_update_csv,
)


class TestPostProcessingUtils(unittest.TestCase):
    """Unit tests for the post-processing utility functions."""

    def setUp(self):
        """Set up temporary directory."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    def test_count_files(self):
        """Test recursive file counting."""
        subdir = os.path.join(self.test_dir, "subdir")
        os.makedirs(subdir)
        with open(os.path.join(self.test_dir, "f1.txt"), "w") as f:
            f.write("test")
        with open(os.path.join(subdir, "f2.txt"), "w") as f:
            f.write("test")

        self.assertEqual(count_files(self.test_dir), 2)

    def test_count_files_empty_dir(self):
        """Test recursive file counting in an empty directory."""
        self.assertEqual(count_files(self.test_dir), 0)

    def test_count_files_only_dirs(self):
        """Test recursive file counting in a directory with only subdirectories."""
        os.makedirs(os.path.join(self.test_dir, "subdir1", "subsubdir1"))
        os.makedirs(os.path.join(self.test_dir, "subdir2"))
        self.assertEqual(count_files(self.test_dir), 0)

    def test_count_files_non_existent_dir(self):
        """Test recursive file counting for a non-existent directory."""
        self.assertEqual(count_files("/non/existent/path"), 0)

    def test_copy_files_and_dirs(self):
        """Test copying specific project subdirectories and loose files with structure preservation."""
        src_root = os.path.join(self.test_dir, "src")
        dest_root = os.path.join(self.test_dir, "dest")
        source_name = "exp1"

        # Create source structure
        outputs_dir = os.path.join(src_root, source_name, "outputs")
        os.makedirs(outputs_dir)
        with open(os.path.join(outputs_dir, "summary.csv"), "w") as f:
            f.write("result")

        loose_file = "control_path.pkl"
        with open(os.path.join(src_root, loose_file), "w") as f:
            f.write("binary_data")

        copy_files_and_dirs(
            src_root,
            source_name,
            dest_root,
            items_to_copy=["outputs"],
            loose_files=[loose_file],
        )

        self.assertTrue(
            os.path.exists(
                os.path.join(dest_root, source_name, "outputs", "summary.csv")
            )
        )
        self.assertTrue(os.path.exists(os.path.join(dest_root, loose_file)))

    def test_copy_files_and_dirs_empty_items_to_copy(self):
        """Test copying with empty items_to_copy list."""
        src_root = os.path.join(self.test_dir, "src")
        dest_root = os.path.join(self.test_dir, "dest")
        source_name = "exp1"
        os.makedirs(os.path.join(src_root, source_name, "outputs"))
        loose_file = "control_path.pkl"
        with open(os.path.join(src_root, loose_file), "w") as f:
            f.write("binary_data")

        copy_files_and_dirs(
            src_root,
            source_name,
            dest_root,
            items_to_copy=[],
            loose_files=[loose_file],
        )
        self.assertFalse(
            os.path.exists(os.path.join(dest_root, source_name, "outputs"))
        )
        self.assertTrue(os.path.exists(os.path.join(dest_root, loose_file)))

    def test_copy_files_and_dirs_non_existent_source(self):
        """Test copying when the source directory does not exist."""
        src_root = "/non/existent/path/at/all"
        dest_root = os.path.join(self.test_dir, "dest")
        source_name = "exp1"

        # Patch both exists and listdir to ensure the error is raised by the filesystem utility
        with (
            patch("os.path.exists", return_value=False),
            patch("os.listdir", side_effect=FileNotFoundError),
        ):
            # Function should handle missing source gracefully and not raise
            copy_files_and_dirs(
                src_root,
                source_name,
                dest_root,
                items_to_copy=["outputs"],
                loose_files=[],
            )

    def test_filter_and_update_csv_after(self):
        """Test filtering rows 'after' a specified date across patient files."""
        target_dir = os.path.join(self.test_dir, "batches")
        os.makedirs(target_dir)

        p1_csv = os.path.join(target_dir, "P1.csv")
        df = pd.DataFrame(
            {
                "updatetime": [
                    "2023-01-01 12:00:00",
                    "2023-02-01 12:00:00",
                    "2023-03-01 12:00:00",
                ],
                "val": [1, 2, 3],
            }
        )
        df.to_csv(p1_csv, index=False)

        ipw_df = pd.DataFrame(
            {"client_idcode": ["P1"], "updatetime": ["2023-01-15 00:00:00"]}
        )

        filter_and_update_csv(target_dir, ipw_df, filter_type="after")

        res_df = pd.read_csv(p1_csv)
        self.assertEqual(len(res_df), 2)
        self.assertCountEqual(res_df["val"].tolist(), [2, 3])
        self.assertTrue(
            (
                pd.to_datetime(res_df["updatetime"], utc=True)
                > pd.to_datetime("2023-01-15", utc=True)
            ).all()
        )


if __name__ == "__main__":
    unittest.main()
