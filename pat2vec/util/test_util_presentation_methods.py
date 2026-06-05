import unittest
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch
from pat2vec.util.presentation_methods import (
    group_images_by_suffix,
    create_powerpoint_slides,
    create_powerpoint_from_images,
)


class TestPresentationMethods(unittest.TestCase):
    """Unit tests for PowerPoint presentation generation utilities."""

    def setUp(self):
        """Set up temporary directory."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    def test_group_images_by_suffix(self):
        """Test grouping logic based on filename suffixes."""
        # Create some dummy files in the temporary directory
        filenames = [
            "plot_roc_P1.png",
            "plot_pr_P1.png",
            "plot_roc_P2.png",
            "not_an_image.txt",
            "chart_P2.jpg",
        ]
        for f in filenames:
            open(os.path.join(self.test_dir, f), "w").close()

        groups = group_images_by_suffix(self.test_dir)

        self.assertIn("P1", groups)
        self.assertIn("P2", groups)
        self.assertEqual(len(groups["P1"]), 2)
        self.assertEqual(len(groups["P2"]), 2)
        self.assertIn("plot_roc_P1.png", groups["P1"])
        self.assertIn("chart_P2.jpg", groups["P2"])

        # Check that non-images are ignored
        all_grouped_files = [f for group in groups.values() for f in group]
        self.assertNotIn("not_an_image.txt", all_grouped_files)

    @patch("pat2vec.util.presentation_methods.Presentation")
    @patch("pat2vec.util.presentation_methods.Inches")
    def test_create_powerpoint_slides(self, mock_inches, mock_presentation_cls):
        """Verify PowerPoint slide creation and image insertion."""
        # Mock the presentation object and its nested components
        mock_pres = MagicMock()
        mock_presentation_cls.return_value = mock_pres
        mock_inches.side_effect = (
            lambda x: x
        )  # Simply return the input value for positioning

        images = ["img1.png", "img2.jpg"]
        folder = self.test_dir
        output = os.path.join(self.test_dir, "output.pptx")

        create_powerpoint_slides(images, folder, output)

        # Check if Presentation was instantiated
        mock_presentation_cls.assert_called_once()

        # Check if two slides were added (one for each image)
        self.assertEqual(mock_pres.slides.add_slide.call_count, 2)

        # Check if shapes.add_picture was called for each image on the resulting slides
        # The return value of add_slide is mocked, so we check its shapes attribute
        mock_slide = mock_pres.slides.add_slide.return_value
        self.assertEqual(mock_slide.shapes.add_picture.call_count, 2)

        # Check if the output file was saved
        mock_pres.save.assert_called_with(output)

    @patch("pat2vec.util.presentation_methods.create_powerpoint_slides")
    def test_create_powerpoint_from_images_orchestration(self, mock_create_slides):
        """Test the orchestration of creating a presentation from a directory scan."""
        # Populate temp dir with mixed files
        open(os.path.join(self.test_dir, "a.png"), "w").close()
        open(os.path.join(self.test_dir, "b.jpg"), "w").close()
        open(os.path.join(self.test_dir, "c.txt"), "w").close()

        create_powerpoint_from_images(self.test_dir)

        # Verify that create_powerpoint_slides was called with ONLY valid image files
        args, _ = mock_create_slides.call_args
        found_images = args[0]
        self.assertCountEqual(found_images, ["a.png", "b.jpg"])
        self.assertEqual(args[1], self.test_dir)
        self.assertEqual(
            args[2], os.path.join(self.test_dir, "output_presentation.pptx")
        )


if __name__ == "__main__":
    unittest.main()
