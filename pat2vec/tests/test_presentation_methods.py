import unittest
from unittest.mock import patch, MagicMock
import os
import tempfile
import shutil

from pat2vec.util.presentation_methods import (
    group_images_by_suffix,
    create_powerpoint_slides,
)


class TestPresentationMethods(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch("os.listdir")
    def test_group_images_by_suffix(self, mock_listdir):
        """Test correctly grouping image files by their patient ID suffix."""
        mock_listdir.return_value = [
            "plot_roc_P1.png",
            "plot_pr_P1.png",
            "plot_roc_P2.png",
            "readme.txt",
        ]

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
