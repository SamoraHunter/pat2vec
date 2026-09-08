import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from pat2vec.util.presentation_methods import (
    create_powerpoint_slides,
    group_images_by_suffix,
)


class TestPresentationMethods(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch("pathlib.Path.iterdir")
    def test_group_images_by_suffix(self, mock_iterdir):
        """Test correctly grouping image files by their patient ID suffix."""
        mock_entry1 = MagicMock()
        mock_entry1.is_file.return_value = True
        mock_entry1.name = "plot_roc_P1.png"
        mock_entry2 = MagicMock()
        mock_entry2.is_file.return_value == True
        mock_entry2.name = "plot_pr_P1.png"
        mock_entry3 = MagicMock()
        mock_entry3.is_file.return_value = True
        mock_entry3.name = "plot_roc_P2.png"
        mock_entry4 = MagicMock()
        mock_entry4.is_file.return_value = False
        mock_entry4.name = "readme.txt"
        mock_iterdir.return_value = [mock_entry1, mock_entry2, mock_entry3, mock_entry4]

        groups = group_images_by_suffix(self.test_dir)

        self.assertEqual(len(groups), 2)
        self.assertIn("P1", groups)
        self.assertEqual(len(groups["P1"]), 2)
        self.assertIn("P2", groups)
        self.assertEqual(len(groups["P2"]), 1)

    @patch("pat2vec.util.presentation_methods.Presentation")
    @patch("pat2vec.util.presentation_methods.Inches")
    def test_create_powerpoint_slides(self, mock_inches, mock_presentation_cls):
        """Test that PowerPoint generation creates slides for each image."""
        mock_pres_instance = MagicMock()
        mock_presentation_cls.return_value = mock_pres_instance

        images = ["img1.png", "img2.png"]
        output_path = os.path.join(self.test_dir, "test.pptx")

        create_powerpoint_slides(images, self.test_dir, output_path)

        self.assertEqual(mock_pres_instance.slides.add_slide.call_count, 2)
        mock_pres_instance.save.assert_called_with(output_path)


if __name__ == "__main__":
    unittest.main()
