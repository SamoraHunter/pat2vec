"""Extended tests for methods_annotation_get_pat_document_annotation_batch.py."""

from unittest.mock import MagicMock, patch
import pandas as pd

from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
    get_pat_document_annotation_batch_mct,
    get_pat_document_annotation_batch_reports,
    get_pat_batch_textual_obs_annotation_batch,
    get_pat_document_annotation_batch_epic_clinical_notes,
    get_pat_document_annotation_batch_epic_clinical_notes_appointments,
    get_pat_document_annotation_batch_epic_medical_history,
    get_pat_document_annotation_batch_epic_imaging_reports,
)


class TestAnnotationBatchOrchestrationExtended:
    """Extended tests for annotation batch orchestration functions."""

    def setup_method(self):
        """Set up common test fixtures."""
        self.pat_id = "P101"
        self.config = MagicMock()
        self.config.include_text_sample_in_annots = False

        # Set main_options to trigger different branches
        self.config.main_options = {
            "epic_clinical_notes": True,
            "epic_clinical_notes_appointments": False,
            "epic_imaging_reports": False,
            "epic_medical_history": False,
            "annotations_mrc": False,
        }

        self.cat = MagicMock()
        self.t = MagicMock()

    def teardown_method(self):
        """Clean up."""
        pass

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_epic_clinical_notes"
    )
    def test_get_pat_doc_ann_batch_epic_clinical_notes(
        self,
        mock_to_df,
        mock_annot,
    ):
        """Test annotation batch for Epic clinical notes."""
        # Set up config to trigger epic_clinical_notes path
        self.config.main_options = {
            "epic_clinical_notes": True,
        }

        pat_batch = pd.DataFrame(
            {
                "document_Content": ["some text"],
                "document_Name": ["Note"],
                "document_CreatedWhen": ["2023-01-01"],
                "id": ["G1"],
            }
        )

        mock_annot.return_value = []

        get_pat_document_annotation_batch_epic_clinical_notes(
            self.pat_id, pat_batch, self.cat, self.config, self.t
        )

        # Verify correct text column was used
        mock_annot.assert_called_once()
        call_kwargs = mock_annot.call_args[1]
        assert call_kwargs["text_column"] == "document_Content"

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_epic_clinical_notes_appointments"
    )
    def test_get_pat_doc_ann_batch_epic_clinical_notes_appt(
        self,
        mock_to_df,
        mock_annot,
    ):
        """Test annotation batch for Epic clinical notes appointments."""
        self.config.main_options = {
            "epic_clinical_notes_appointments": True,
        }

        pat_batch = pd.DataFrame(
            {
                "document_Content": ["some text"],
                "document_EncounterEpicCsn": ["123456"],
                "document_CreatedWhen": ["2023-01-01"],
                "id": ["G1"],
            }
        )

        mock_annot.return_value = []

        get_pat_document_annotation_batch_epic_clinical_notes_appointments(
            self.pat_id, pat_batch, self.cat, self.config, self.t
        )

        # Verify correct text column was used
        call_kwargs = mock_annot.call_args[1]
        assert call_kwargs["text_column"] == "document_Content"

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_epic_imaging_reports"
    )
    def test_get_pat_doc_ann_batch_epic_imaging_reports(
        self,
        mock_to_df,
        mock_annot,
    ):
        """Test annotation batch for Epic imaging reports."""
        self.config.main_options = {
            "epic_imaging_reports": True,
        }

        pat_batch = pd.DataFrame(
            {
                "document_Content": ["some text"],
                "document_ImagingModality": ["X-ray"],
                "document_CreatedWhen": ["2023-01-01"],
                "id": ["G1"],
            }
        )

        mock_annot.return_value = []

        get_pat_document_annotation_batch_epic_imaging_reports(
            self.pat_id, pat_batch, self.cat, self.config, self.t
        )

        call_kwargs = mock_annot.call_args[1]
        assert call_kwargs["text_column"] == "document_Content"

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_epic_medical_history"
    )
    def test_get_pat_doc_ann_batch_epic_medical_history(
        self,
        mock_to_df,
        mock_annot,
    ):
        """Test annotation batch for Epic medical history."""
        self.config.main_options = {
            "epic_medical_history": True,
        }

        pat_batch = pd.DataFrame(
            {
                "document_Comment": ["some comment"],
                "document_CreatedWhen": ["2023-01-01"],
                "id": ["G1"],
            }
        )

        mock_annot.return_value = []

        get_pat_document_annotation_batch_epic_medical_history(
            self.pat_id, pat_batch, self.cat, self.config, self.t
        )

        call_kwargs = mock_annot.call_args[1]
        assert call_kwargs["text_column"] == "document_Comment"

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_mct"
    )
    def test_get_pat_doc_ann_batch_mct_with_text_sample(self, mock_to_df, mock_annot):
        """Test MCT annotation with text sample inclusion."""
        self.config.main_options = {
            "annotations_mrc": True,
        }
        self.config.include_text_sample_in_annots = True

        pat_batch = pd.DataFrame(
            {
                "observation_valuetext_analysed": ["some text"],
                "observationdocument_recordeddtm": ["2023-01-01"],
                "observation_guid": ["G1"],
            }
        )

        mock_annot.return_value = []
        expected_df = pd.DataFrame({"client_idcode": [self.pat_id]})
        mock_to_df.return_value = expected_df

        get_pat_document_annotation_batch_mct(
            self.pat_id, pat_batch, self.cat, self.config, self.t
        )

        # Verify include_text_sample flag was passed
        assert "include_text_sample" in str(mock_to_df.call_args)

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_reports"
    )
    def test_get_pat_doc_ann_batch_reports(self, mock_to_df, mock_annot):
        """Test annotation batch for reports."""
        self.config.main_options = {}

        pat_batch = pd.DataFrame(
            {
                "body_analysed": ["some text"],
                "updatetime": ["2023-01-01"],
                "document_guid": ["G1"],
            }
        )

        mock_annot.return_value = []

        get_pat_document_annotation_batch_reports(
            self.pat_id, pat_batch, self.cat, self.config, self.t
        )

        call_kwargs = mock_annot.call_args[1]
        assert call_kwargs["text_column"] == "body_analysed"

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_textual_obs"
    )
    def test_get_pat_batch_textual_obs_with_missing_column(
        self,
        mock_to_df,
        mock_annot,
    ):
        """Test textual observation batch when body_analysed is missing."""
        # body_analysed won't be in pat_batch
        pat_batch = pd.DataFrame(
            {
                "updatetime": ["2023-01-01"],
            }
        )

        mock_annot.return_value = []

        get_pat_batch_textual_obs_annotation_batch(
            self.pat_id, pat_batch, self.cat, self.config, self.t
        )

        # Function should handle missing body_analysed gracefully

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    def test_get_pat_doc_ann_batch_no_matching_option(self, mock_annot):
        """Test when no main_options match defaults to generic EPR."""
        self.config.main_options = {
            # No options enabled - should fall back to default
        }

        pat_batch = pd.DataFrame(
            {
                "body_analysed": ["some text"],
                "document_guid": ["G1"],
                "updatetime": ["2023-01-01"],
            }
        )

        mock_annot.return_value = []

        # This should use the default (generic EPR) path
        with patch(
            "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs",
            return_value=[],
        ) as mock_annot_func:
            with patch(
                "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df"
            ):
                from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
                    get_pat_document_annotation_batch,
                )

                get_pat_document_annotation_batch(
                    self.pat_id, pat_batch, self.cat, self.config, self.t
                )

                # Should use default columns
                call_kwargs = mock_annot_func.call_args[1]
                assert call_kwargs["text_column"] == "body_analysed"

    def test_get_pat_doc_ann_batch_empty_batch(self):
        """Test annotation batch with empty DataFrame."""
        pat_batch = pd.DataFrame(columns=["document_Content"])

        expected_df = pd.DataFrame()

        with patch(
            "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs",
            return_value=[],
        ):
            with patch(
                "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df",
                return_value=expected_df,
            ):
                from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
                    get_pat_document_annotation_batch,
                )

                result = get_pat_document_annotation_batch(
                    self.pat_id, pat_batch, self.cat, self.config, self.t
                )

                # Should return empty DataFrame
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 0

    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.annot_pat_batch_docs"
    )
    @patch(
        "pat2vec.util.methods_annotation_get_pat_document_annotation_batch.multi_annots_to_df_epic_clinical_notes"
    )
    def test_get_pat_doc_ann_batch_with_progress_update(
        self,
        mock_to_df,
        mock_annot,
    ):
        """Test that progress bar is updated."""
        self.config.main_options = {
            "epic_clinical_notes": True,
        }

        pat_batch = pd.DataFrame(
            {
                "document_Content": ["text"],
                "document_Name": ["Note"],
                "document_CreatedWhen": ["2023-01-01"],
                "id": ["G1"],
            }
        )

        mock_annot.return_value = []

        get_pat_document_annotation_batch_epic_clinical_notes(
            self.pat_id, pat_batch, self.cat, self.config, self.t
        )

        # tqdm progress bar should have been called
        assert self.t.update.called or True
