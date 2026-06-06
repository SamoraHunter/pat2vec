import unittest
from unittest.mock import patch, mock_open
from pat2vec.util.get_dummy_data_medcat_annotation import (
    random_sample,
    dummy_medcat_annotation_generator,
    dummy_CAT,
    augment_dummy_annotations_file,
)


class TestGetDummyDataMedcatAnnotation(unittest.TestCase):
    def test_random_sample(self):
        """Verify random sampling from annotation dictionary."""
        data = {"entities": {"e1": 1, "e2": 2, "e3": 3}}
        res = random_sample(data, 2)
        self.assertEqual(len(res["entities"]), 2)

    def test_dummy_cat_get_entities(self):
        """Test single text annotation with dummy CAT."""
        cat = dummy_CAT()
        res = cat.get_entities("some text")
        self.assertIn("entities", res)

    def test_dummy_cat_get_entities_multi_texts(self):
        """Test batch text annotation with dummy CAT."""
        cat = dummy_CAT()
        res = cat.get_entities_multi_texts(["t1", "t2"])
        self.assertEqual(len(res), 2)
        self.assertIn("entities", res[0])

    @patch("pat2vec.util.get_dummy_data_medcat_annotation.pickle.load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.join")
    def test_dummy_medcat_annotation_generator(
        self, mock_os_path_join, mock_open, mock_pickle_load
    ):
        """Test that dummy_medcat_annotation_generator loads and samples correctly."""
        mock_os_path_join.return_value = "/fake/path/sample_annotations.pickle"
        mock_pickle_load.return_value = {
            "entities": {"e1": 1, "e2": 2, "e3": 3, "e4": 4, "e5": 5}
        }

        result = dummy_medcat_annotation_generator()
        self.assertIn("entities", result)
        self.assertGreater(len(result["entities"]), 0)
        mock_open.assert_called_once_with("/fake/path/sample_annotations.pickle", "rb")

    @patch("pat2vec.util.get_dummy_data_medcat_annotation.pickle.dump")
    @patch("pat2vec.util.get_dummy_data_medcat_annotation.pickle.load")
    @patch("os.path.exists", return_value=True)
    @patch("builtins.open", new_callable=mock_open)
    def test_augment_dummy_annotations_file(
        self, mock_file, mock_exists, mock_load, mock_dump
    ):
        """Test the utility to expand the dummy annotation pool."""
        mock_load.return_value = {
            "entities": {
                "e1": {"id": 1, "acc": 0.5, "start": 10, "end": 20, "meta_anns": {}}
            }
        }
        # Silence print output during test
        with patch("builtins.print"):
            augment_dummy_annotations_file(target_count=5)

        self.assertTrue(mock_dump.called)
        saved_data = mock_dump.call_args[0][0]
        self.assertGreaterEqual(len(saved_data["entities"]), 5)
