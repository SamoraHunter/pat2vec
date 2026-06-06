import unittest
import os
import shutil
import tempfile
from pat2vec.util.current_pat_batch_path_methods import PathsClass


class TestCurrentPatBatchPathMethods(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_paths_class_initialization(self):
        """Verify directory creation upon object instantiation."""
        suffix = "_v1"
        pc = PathsClass(self.test_dir, suffix, "my_outputs", create_dirs=True)

        # PathsClass appends a trailing slash to all paths
        bloods_dir = os.path.join(
            self.test_dir, f"current_pat_bloods_batches{suffix}", ""
        )
        self.assertTrue(os.path.exists(bloods_dir))
        self.assertIn(bloods_dir, pc.all_paths)
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, "my_outputs")))
