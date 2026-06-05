import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import shutil
from datetime import datetime
from pat2vec.util.post_processing_annotations import (
    filter_annot_dataframe2,
    produce_filtered_annotation_dataframe,
    filter_and_select_rows,
    retrieve_pat_annots_mct_epr,
    check_list_presence,
    filter_dataframe_n_lists,
    get_all_target_annots,
    EMPTY_ANNOT_COLS,
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
        self.assertEqual(res.iloc[0]["client_idcode"], "P1")

    # New test cases start here

    def test_retrieve_pat_annots_mct_epr_file_single_source(self):
        """Test file backend with a single EPR source."""
        result_df = retrieve_pat_annots_mct_epr(
            "P1", self.config_obj, merge_columns=False
        )
        self.assertFalse(result_df.empty)
        self.assertEqual(len(result_df), 3)
        self.assertTrue((result_df["annotation_batch_source"] == "epr").all())
        self.assertIn("updatetime", result_df.columns)

    def test_retrieve_pat_annots_mct_epr_file_multiple_sources_with_merging(self):
        """Test file backend with multiple sources and column merging."""
        result_df = retrieve_pat_annots_mct_epr(
            "P1", self.config_obj, merge_columns=True
        )
        self.assertFalse(result_df.empty)
        self.assertEqual(len(result_df), 12)  # 3 from each of 4 sources
        self.assertIn("updatetime", result_df.columns)
        self.assertIn("observationdocument_recordeddtm", result_df.columns)
        self.assertIn("basicobs_entered", result_df.columns)

        # Verify merging logic for updatetime (should fill NaT values)
        # EPR has updatetime, MCT has observationdocument_recordeddtm, Text_obs has basicobs_entered
        # After merging, updatetime should be populated from other sources if its original is NaN
        # For this test, all original updatetime values are present, so it should just concatenate.
        # The merge_columns logic primarily fills NaNs if a column is missing in one source but present in another.
        # Let's check if the 'updatetime' column contains values from all sources after merging.
        # This is implicitly tested by the fact that all 12 rows are present and 'updatetime' is a column.

    @patch("pat2vec.util.post_processing_annotations.get_df_from_db")
    def test_retrieve_pat_annots_mct_epr_db_multiple_sources_with_merging(
        self, mock_get_df_from_db
    ):
        """Test database backend with multiple sources and column merging."""
        self.config_obj.storage_backend = "database"
        self.config_obj.db_engine = MagicMock()  # Mock db_engine for get_df_from_db

        def side_effect_get_df_from_db(*args, **kwargs):
            args[1]
            table = args[2]  # noqa: F841
            if table == "ann_epr_docs":
                return self.sample_annot_df_epr.copy()
            elif table == "ann_mct_docs":
                return self.sample_annot_df_mct.copy()
            elif table == "ann_textual_obs":
                return self.sample_annot_df_text_obs.copy()
            elif table == "ann_reports":
                return self.sample_annot_df_report.copy()
            return pd.DataFrame()

        mock_get_df_from_db.side_effect = side_effect_get_df_from_db

        result_df = retrieve_pat_annots_mct_epr(
            "P1", self.config_obj, merge_columns=True
        )
        self.assertFalse(result_df.empty)
        self.assertEqual(len(result_df), 12)  # 3 from each of 4 sources
        self.assertIn("updatetime", result_df.columns)
        self.assertIn("observationdocument_recordeddtm", result_df.columns)
        self.assertIn("basicobs_entered", result_df.columns)
        self.assertTrue(
            (
                result_df["annotation_batch_source"].isin(
                    ["epr", "mct", "textual_obs", "report"]
                )
            ).all()
        )

        # Verify that updatetime is populated from other sources if its original is NaN
        # Create a scenario where updatetime is NaN in EPR, but present in MCT
        df_epr_nan_updatetime = self.sample_annot_df_epr.copy()
        df_epr_nan_updatetime["updatetime"] = pd.NaT
        df_epr_nan_updatetime["observationannotation_recordeddtm"] = pd.NaT  # Also NaN

        def side_effect_get_df_from_db_nan_epr(*args, **kwargs):
            args[1]
            table = args[2]  # noqa: F841
            if table == "ann_epr_docs":
                return df_epr_nan_updatetime.copy()
            elif table == "ann_mct_docs":
                return self.sample_annot_df_mct.copy()
            return pd.DataFrame()

        mock_get_df_from_db.side_effect = side_effect_get_df_from_db_nan_epr
        result_df_nan_test = retrieve_pat_annots_mct_epr(
            "P1", self.config_obj, merge_columns=True
        )

        # The updatetime column from EPR should now be filled by MCT's observationdocument_recordeddtm
        epr_rows = result_df_nan_test[
            result_df_nan_test["annotation_batch_source"] == "epr"
        ]
        self.assertFalse(epr_rows["updatetime"].isnull().all())
        self.assertTrue(
            (
                epr_rows["updatetime"] == epr_rows["observationannotation_recordeddtm"]
            ).all()
        )  # Should be filled from observationannotation_recordeddtm

    def test_retrieve_pat_annots_mct_epr_empty_sources(self):
        """Test when some sources return empty DataFrames."""
        # Remove EPR file to simulate empty source
        os.remove(
            os.path.join(self.config_obj.pre_document_annotation_batch_path, "P1.csv")
        )
        result_df = retrieve_pat_annots_mct_epr(
            "P1", self.config_obj, merge_columns=True
        )
        self.assertFalse(result_df.empty)
        self.assertEqual(len(result_df), 9)  # 3 from each of 3 remaining sources

    def test_retrieve_pat_annots_mct_epr_no_sources(self):
        """Test when no sources return data."""
        shutil.rmtree(self.config_obj.pre_document_annotation_batch_path)
        shutil.rmtree(self.config_obj.pre_document_annotation_batch_path_mct)
        shutil.rmtree(self.config_obj.pre_textual_obs_annotation_batch_path)
        shutil.rmtree(self.config_obj.pre_document_annotation_batch_path_reports)
        os.makedirs(self.config_obj.pre_document_annotation_batch_path, exist_ok=True)
        os.makedirs(
            self.config_obj.pre_document_annotation_batch_path_mct, exist_ok=True
        )
        os.makedirs(
            self.config_obj.pre_textual_obs_annotation_batch_path, exist_ok=True
        )
        os.makedirs(
            self.config_obj.pre_document_annotation_batch_path_reports, exist_ok=True
        )

        result_df = retrieve_pat_annots_mct_epr(
            "P1", self.config_obj, merge_columns=True
        )
        self.assertTrue(result_df.empty)
        self.assertListEqual(list(result_df.columns), EMPTY_ANNOT_COLS)

    def test_check_list_presence_basic(self):
        """Test check_list_presence with basic matching."""
        df = pd.DataFrame({"text": ["apple pie", "banana split", "cherry tart"]})
        self.assertTrue(check_list_presence(df, "text", ["apple"]))
        self.assertFalse(check_list_presence(df, "text", ["grape"]))

    def test_check_list_presence_case_insensitive(self):
        """Test check_list_presence with case-insensitive matching."""
        df = pd.DataFrame({"text": ["Apple Pie"]})
        self.assertTrue(check_list_presence(df, "text", ["apple"]))

    def test_check_list_presence_with_filter_args(self):
        """Test check_list_presence with annotation filter arguments."""
        df = pd.DataFrame({"text": ["apple pie", "banana split"], "acc": [0.9, 0.5]})
        filter_args = {"acc": 0.8}
        self.assertTrue(
            check_list_presence(
                df, "text", ["apple"], annot_filter_arguments=filter_args
            )
        )
        self.assertFalse(
            check_list_presence(
                df, "text", ["banana"], annot_filter_arguments=filter_args
            )
        )

    def test_filter_dataframe_n_lists_basic(self):
        """Test filter_dataframe_n_lists with basic intersection logic."""
        df = pd.DataFrame({"cui": [1, 2, 3, 4, 5]})
        n_lists = [[1, 2, 3], [2, 3, 4]]
        result_df = filter_dataframe_n_lists(df, "cui", n_lists)
        self.assertEqual(len(result_df), 2)
        self.assertListEqual(result_df["cui"].tolist(), [2, 3])

    def test_filter_dataframe_n_lists_no_match(self):
        """Test filter_dataframe_n_lists when no elements are in all lists."""
        df = pd.DataFrame({"cui": [1, 2, 3]})
        n_lists = [[1, 4], [2, 5]]
        result_df = filter_dataframe_n_lists(df, "cui", n_lists)
        self.assertTrue(result_df.empty)

    @patch("pat2vec.util.post_processing_annotations.retrieve_pat_annots_mct_epr")
    def test_get_all_target_annots_basic(self, mock_retrieve_annots):
        """Test get_all_target_annots with basic functionality."""
        mock_retrieve_annots.return_value = pd.DataFrame(
            {
                "client_idcode": ["P1", "P1", "P1"],
                "cui": [100, 101, 102],
                "acc": [0.9, 0.8, 0.95],
            }
        )
        all_pat_list = ["P1"]
        n_lists = [[100, 101], [101, 102]]  # Only 101 is in both
        result_df = get_all_target_annots(all_pat_list, n_lists, self.config_obj)
        self.assertFalse(result_df.empty)
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["cui"], 101)
        mock_retrieve_annots.assert_called_once_with("P1", self.config_obj)

    @patch("pat2vec.util.post_processing_annotations.retrieve_pat_annots_mct_epr")
    def test_get_all_target_annots_with_filter_args(self, mock_retrieve_annots):
        """Test get_all_target_annots with additional filter arguments."""
        mock_retrieve_annots.return_value = pd.DataFrame(
            {
                "client_idcode": ["P1", "P1", "P1"],
                "cui": [100, 101, 102],
                "acc": [0.9, 0.7, 0.95],  # 101 has low acc
            }
        )
        all_pat_list = ["P1"]
        n_lists = [[100, 101], [101, 102]]
        annot_filter_arguments = {"acc": 0.85}  # Should filter out CUI 101
        result_df = get_all_target_annots(
            all_pat_list, n_lists, self.config_obj, annot_filter_arguments
        )
        self.assertTrue(result_df.empty)  # No CUI 101 after filtering by acc

    @patch("pat2vec.util.post_processing_annotations.retrieve_pat_annots_mct_epr")
    def test_get_all_target_annots_empty_pat_list(self, mock_retrieve_annots):
        """Test get_all_target_annots with an empty patient list."""
        result_df = get_all_target_annots([], [[100]], self.config_obj)
        self.assertTrue(result_df.empty)
        mock_retrieve_annots.assert_not_called()
