import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
from pat2vec.util.post_processing_annotations import (
    filter_annot_dataframe2,
    produce_filtered_annotation_dataframe,
    filter_and_select_rows,
)


class TestPostProcessingAnnotations(unittest.TestCase):
    def test_filter_annot_dataframe2_logic(self):
        df = pd.DataFrame(
            {
                "types": ["['disorder']", "['procedure']"],
                "acc": [0.9, 0.5],
                "Presence_Value": ["True", "False"],
            }
        )
        filters = {"types": ["disorder"], "acc": 0.8}
        result = filter_annot_dataframe2(df, filters)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["acc"], 0.9)

    @patch("pat2vec.util.post_processing_annotations.get_df_from_db")
    def test_produce_filtered_annotation_dataframe_db_path(self, mock_get_db):
        config = MagicMock()
        config.storage_backend = "database"
        mock_get_db.return_value = pd.DataFrame({"client_idcode": ["P1"], "cui": [101]})

        result = produce_filtered_annotation_dataframe(
            config_obj=config, pat_list=["P1"], mct=False
        )
        self.assertEqual(len(result), 1)
        mock_get_db.assert_called_once()

    def test_filter_and_select_rows(self):
        df = pd.DataFrame(
            {
                "cui": [101, 101, 102],
                "updatetime": [
                    datetime(2021, 1, 1),
                    datetime(2021, 1, 10),
                    datetime(2021, 1, 5),
                ],
            }
        )
        res = filter_and_select_rows(df, filter_list=[101], mode="latest")
        self.assertEqual(res.iloc[0]["updatetime"], datetime(2021, 1, 10))
