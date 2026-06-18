import unittest
import os
import shutil  # type: ignore
import tempfile
from unittest.mock import MagicMock

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.methods_post_get import copy_project_folders_with_substring_match


class TestProjectVersioningIntegration(unittest.TestCase):
    """
    Integration test for the Project Versioning/Cloning logic using
    `copy_project_folders_with_substring_match`.
    This test verifies that:
    1. A source project structure is correctly created.
    2. The utility copies specified subfolders and loose files to a new versioned directory.
    3. The copied structure and file contents are as expected.
    """

    def setUp(self):
        self.base_test_dir = tempfile.mkdtemp()
        self.source_project_name = "original_project"
        self.source_root_path = os.path.join(
            self.base_test_dir, self.source_project_name
        )

        # Create a dummy config object for the source project
        self.config = config_class(
            storage_backend="file",  # File backend is relevant for this test
            root_path=self.source_root_path,
            proj_name=self.source_project_name,
            testing=True,
            verbosity=0,
        )

        # Create dummy project structure
        os.makedirs(os.path.join(self.source_root_path, "outputs"), exist_ok=True)
        os.makedirs(os.path.join(self.source_root_path, "batches_v1"), exist_ok=True)
        os.makedirs(os.path.join(self.source_root_path, "batches_v2"), exist_ok=True)
        os.makedirs(os.path.join(self.source_root_path, "logs"), exist_ok=True)

        with open(
            os.path.join(self.source_root_path, "outputs", "summary.csv"), "w"
        ) as f:
            f.write("output_data")
        with open(
            os.path.join(self.source_root_path, "batches_v1", "patient_a.csv"), "w"
        ) as f:
            f.write("batch_data_a")
        with open(os.path.join(self.source_root_path, "config.json"), "w") as f:
            f.write("config_json_data")
        with open(os.path.join(self.source_root_path, "README.md"), "w") as f:
            f.write("readme_content")

    def tearDown(self):
        shutil.rmtree(self.base_test_dir)

    def test_copy_project_folders_with_substring_match_lifecycle(self):
        # Simulate the pat2vec_obj that would call this utility
        mock_pat2vec_obj = MagicMock()
        mock_pat2vec_obj.config_obj = self.config

        # Define items to copy: subfolders matching "batches" and specific loose files
        substrings_to_match = ["batches"]
        loose_files = ["config.json", "README.md"]

        # Execute the copy function
        new_project_path = copy_project_folders_with_substring_match(
            mock_pat2vec_obj, substrings_to_match=substrings_to_match + loose_files
        )

        # Verify new project path exists and is named correctly (e.g., original_project_1)
        self.assertTrue(os.path.exists(new_project_path))
        self.assertTrue(
            new_project_path.startswith(
                os.path.join(self.base_test_dir, self.source_project_name + "_")
            )
        )

        # Verify copied subfolders and their contents
        self.assertTrue(
            os.path.exists(
                os.path.join(new_project_path, "batches_v1", "patient_a.csv")
            )
        )
        self.assertTrue(os.path.exists(os.path.join(new_project_path, "batches_v2")))
        self.assertFalse(
            os.path.exists(os.path.join(new_project_path, "outputs"))
        )  # Should not be copied

        # Verify copied loose files
        self.assertTrue(os.path.exists(os.path.join(new_project_path, "config.json")))
        self.assertTrue(os.path.exists(os.path.join(new_project_path, "README.md")))

        # Verify content integrity
        with open(
            os.path.join(new_project_path, "batches_v1", "patient_a.csv"), "r"
        ) as f:
            self.assertEqual(f.read(), "batch_data_a")
        with open(os.path.join(new_project_path, "config.json"), "r") as f:
            self.assertEqual(f.read(), "config_json_data")


if __name__ == "__main__":
    unittest.main()
