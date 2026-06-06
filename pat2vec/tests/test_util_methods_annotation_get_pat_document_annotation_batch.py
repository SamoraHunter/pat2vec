import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
    get_pat_document_annotation_batch,
    get_pat_document_annotation_batch_mct,
    get_pat_document_annotation_batch_reports,
    get_pat_batch_textual_obs_annotation_batch,
)


class TestAnnotationBatchOrchestration(unittest.TestCase):
    def setUp(self):
        self.pat_id = "P101"
        self.batch = pd.DataFrame(
            {
                "body_analysed": ["text"],
                "document_guid": ["G1"],
                "updatetime": ["2023-01-01"],
            }
        )
        self.cat = MagicMock()
        self.config = MagicMock()
        self.config.include_text_sample_in_annots = False
        self.t = MagicMock()

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df"
    )
    def test_get_pat_document_annotation_batch_flow(self, mock_to_df, mock_annot):
        """Verify that the orchestration function calls annotation and conversion steps."""
        # Setup
        dummy_annots = [{"entities": {"1": {}}}]
        mock_annot.return_value = dummy_annots
        expected_df = pd.DataFrame({"client_idcode": [self.pat_id]})
        mock_to_df.return_value = expected_df

        # Act
        result = get_pat_document_annotation_batch(
            self.pat_id, self.batch, self.cat, self.config, self.t
        )

        # Assert
        self.assertEqual(id(result), id(expected_df))
        mock_annot.assert_called_once()

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_mct"
    )
    def test_get_pat_document_annotation_batch_mct_flow(self, mock_to_df, mock_annot):
        """Verify orchestration for MCT documents."""
        mock_annot.return_value = []
        get_pat_document_annotation_batch_mct(
            self.pat_id, self.batch, self.cat, self.config, self.t
        )
        mock_annot.assert_called_once_with(
            current_pat_client_idcode=self.pat_id,
            pat_batch=self.batch,
            cat=self.cat,
            config_obj=self.config,
            t=self.t,
            text_column="observation_valuetext_analysed",
        )

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_reports"
    )
    def test_get_pat_document_annotation_batch_reports_flow(
        self, mock_to_df, mock_annot
    ):
        """Verify orchestration for Reports."""
        mock_annot.return_value = []
        get_pat_document_annotation_batch_reports(
            self.pat_id, self.batch, self.cat, self.config, self.t
        )
        mock_annot.assert_called_once()

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_textual_obs"
    )
    def test_get_pat_batch_textual_obs_annotation_batch_flow(
        self, mock_to_df, mock_annot
    ):
        """Verify orchestration for textual observations."""
        mock_annot.return_value = []
        get_pat_batch_textual_obs_annotation_batch(
            self.pat_id, self.batch, self.cat, self.config, self.t
        )
        mock_annot.assert_called_once()
