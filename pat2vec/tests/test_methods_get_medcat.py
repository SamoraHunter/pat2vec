import unittest
from unittest.mock import patch, MagicMock
import sys
import os

from pat2vec.util.methods_get_medcat import get_cat
from pat2vec.util.get_dummy_data_medcat_annotation import dummy_CAT


class TestMethodsGetMedcat(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_config.verbosity = 0
        self.mock_config.medcat = True
        self.mock_config.testing = False
        self.mock_config.dummy_medcat_model = False
        self.mock_config.override_medcat_model_path = None

    def test_get_cat_disabled(self):
        """Test returning None when medcat is disabled."""
        self.mock_config.medcat = False
        result = get_cat(self.mock_config)
        self.assertIsNone(result)

    def test_get_cat_dummy_mode(self):
        """Test loading a dummy_CAT instance for testing."""
        self.mock_config.testing = True
        self.mock_config.dummy_medcat_model = True
        result = get_cat(self.mock_config)
        self.assertIsInstance(result, dummy_CAT)

    @patch("pat2vec.util.methods_get_medcat.CAT")
    def test_get_cat_explicit_path(self, mock_cat_cls):
        """Test loading a model from an explicit override path."""
        self.mock_config.override_medcat_model_path = "/path/to/model.zip"
        get_cat(self.mock_config)
        mock_cat_cls.load_model_pack.assert_called_with("/path/to/model.zip")

    @patch("os.path.exists", return_value=True)
    @patch("pat2vec.util.methods_get_medcat.CAT")
    def test_get_cat_paths_py_import(self, mock_cat_cls, mock_exists):
        """Test loading a path from a local paths.py file."""
        # Mock the dynamic import of 'paths.py'
        with patch.dict(
            "sys.modules", {"paths": MagicMock(medcat_path="/import/path.zip")}
        ):
            get_cat(self.mock_config)
            mock_cat_cls.load_model_pack.assert_called_with("/import/path.zip")

    @patch("os.path.exists")
    @patch("os.listdir")
    @patch("pat2vec.util.methods_get_medcat.CAT")
    def test_get_cat_auto_detection(self, mock_cat_cls, mock_listdir, mock_exists):
        """Test the 'auto' detection logic that searches sys.path."""
        self.mock_config.override_medcat_model_path = "auto"

        # Setup: simulate finding a zip file in a 'medcat_models' directory in one of the sys.path entries
        test_sys_path = "/sys/path/entry"

        def side_effect_exists(path):
            return path == os.path.join(test_sys_path, "medcat_models")

        mock_exists.side_effect = side_effect_exists
        mock_listdir.return_value = ["model1.zip"]

        with patch.object(sys, "path", [test_sys_path]):
            get_cat(self.mock_config)

        expected_path = os.path.join(test_sys_path, "medcat_models", "model1.zip")
        mock_cat_cls.load_model_pack.assert_called_with(expected_path)

    def test_get_cat_no_path_raises_error(self):
        """Test that a ValueError is raised if MedCAT is enabled but no path is found."""
        with self.assertRaises(ValueError):
            get_cat(self.mock_config)


if __name__ == "__main__":
    unittest.main()
