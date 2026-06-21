"""Extended tests for post_processing_build_methods.py."""

import os
import tempfile
import shutil
import pandas as pd
from unittest.mock import MagicMock, patch

from pat2vec.util.post_processing_build_methods import (
    optimize_dtypes,
    join_docs_to_annots,
    retrieve_pat_bloods,
    retrieve_pat_epr_docs,
    retrieve_pat_docs_mct_epr,
)


class TestPostProcessingBuildMethodsExtended:
    """Extended unit tests for post-processing builds."""

    def setup_method(self):
        """Set up temporary directory and mock configuration."""
        self.test_dir = tempfile.mkdtemp()
        self.config_obj = MagicMock()
        self.config_obj.root_path = self.test_dir
        self.config_obj.proj_name = "test_project"
        self.config_obj.storage_backend = "file"
        self.config_obj.include_text_sample_in_annots = False
        self.config_obj.pre_bloods_batch_path = os.path.join(self.test_dir, "bloods")
        self.config_obj.pre_bmi_batch_path = os.path.join(self.test_dir, "bmi")
        self.config_obj.pre_demo_batch_path = os.path.join(self.test_dir, "demo")
        self.config_obj.pre_document_batch_path = os.path.join(self.test_dir, "docs")
        self.config_obj.pre_textual_obs_document_batch_path = os.path.join(
            self.test_dir, "textual"
        )
        self.config_obj.pre_reports_batch_path = os.path.join(self.test_dir, "reports")
        self.config_obj.pre_epic_clinical_notes_batch_path = os.path.join(
            self.test_dir, "epic_notes"
        )
        self.config_obj.pre_epic_imaging_reports_batch_path = os.path.join(
            self.test_dir, "epic_imaging"
        )
        self.config_obj.pre_epic_medical_history_batch_path = os.path.join(
            self.test_dir, "epic_med_hist"
        )
        self.config_obj.pre_epic_orders_batch_path = os.path.join(
            self.test_dir, "epic_orders"
        )

        os.makedirs(self.config_obj.pre_bloods_batch_path)
        os.makedirs(self.config_obj.pre_bmi_batch_path)
        os.makedirs(self.config_obj.pre_demo_batch_path)
        os.makedirs(self.config_obj.pre_document_batch_path)
        os.makedirs(self.config_obj.pre_textual_obs_document_batch_path)
        os.makedirs(self.config_obj.pre_reports_batch_path)
        os.makedirs(self.config_obj.pre_epic_clinical_notes_batch_path)
        os.makedirs(self.config_obj.pre_epic_imaging_reports_batch_path)
        os.makedirs(self.config_obj.pre_epic_medical_history_batch_path)
        os.makedirs(self.config_obj.pre_epic_orders_batch_path)

    def teardown_method(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    def test_optimize_dtypes_downcast_float_to_float32(self):
        """Test float64 downcasting to float32."""
        df = pd.DataFrame({"vals": [1.0, 2.5, 3.7]})
        df["vals"] = df["vals"].astype("float64")
        result = optimize_dtypes(df.copy())
        assert result["vals"].dtype == "float32"

    def test_optimize_dtypes_downcast_int_to_int8(self):
        """Test int64 downcasting to int8."""
        df = pd.DataFrame({"vals": [1, 2, 3]})
        df["vals"] = df["vals"].astype("int64")
        result = optimize_dtypes(df.copy())
        assert result["vals"].dtype == "int8"

    def test_optimize_dtypes_mixed_types(self):
        """Test optimization with mixed column types."""
        df = pd.DataFrame(
            {
                "ints": [1, 2, 3],
                "floats": [1.0, 2.0, 3.0],
                "strings": ["a", "b", "c"],
            }
        )
        df["ints"] = df["ints"].astype("int64")
        df["floats"] = df["floats"].astype("float64")

        result = optimize_dtypes(df.copy())
        assert result["ints"].dtype == "int8"
        assert result["floats"].dtype == "float32"
        assert result["strings"].dtype == "object"

    def test_optimize_dtypes_empty_dataframe(self):
        """Test optimization with empty DataFrame."""
        df = pd.DataFrame()
        result = optimize_dtypes(df)
        assert len(result) == 0

    def test_retrieve_pat_bloods_file_not_exist(self):
        """Test bloods retrieval when file doesn't exist."""
        pat_id = "nonexistent"
        result = retrieve_pat_bloods(pat_id, self.config_obj)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch("pat2vec.util.post_processing_build_methods.pd.read_csv")
    def test_retrieve_pat_bloods_database_mode(self, mock_read_csv):
        """Test bloods retrieval in database mode."""
        self.config_obj.storage_backend = "database"
        self.config_obj.db_engine = MagicMock()

        result = retrieve_pat_bloods("P1", self.config_obj)

        # Should call get_df_from_db for database.mode
        assert isinstance(result, pd.DataFrame)

    def test_join_docs_to_annots_with_duplicates(self):
        """Test joining when columns have duplicate names."""
        annots = pd.DataFrame(
            {
                "document_guid": ["G1", "G2"],
                "cui": [100, 200],
                "text": "annotation text",
            }
        )
        docs = pd.DataFrame(
            {
                "document_guid": ["G1", "G3"],
                "text": ["doc text 1", "doc text 3"],
                "cui": [100, 300],
            }
        )

        result = join_docs_to_annots(annots, docs)

        # Should drop duplicated columns from docs
        assert "document_guid" in result.columns
        assert len(result) == 2

    def test_join_docs_to_annots_no_duplicates(self):
        """Test joining when no column duplicates exist."""
        annots = pd.DataFrame(
            {
                "document_guid": ["G1"],
                "cui": [100],
            }
        )
        docs = pd.DataFrame(
            {
                "document_guid": ["G1"],
                "content": ["text"],
            }
        )

        result = join_docs_to_annots(annots, docs)
        assert len(result) == 1
        assert "content" in result.columns

    def test_join_docs_to_annots_empty_annots(self):
        """Test joining with empty annotations DataFrame."""
        annots = pd.DataFrame(columns=["document_guid", "cui"])
        docs = pd.DataFrame(
            {
                "document_guid": ["G1"],
                "content": ["text"],
            }
        )

        result = join_docs_to_annots(annots, docs)
        assert len(result) == 0

    def test_join_docs_to_annots_empty_docs(self):
        """Test joining with empty documents DataFrame."""
        annots = pd.DataFrame(
            {
                "document_guid": ["G1"],
                "cui": [100],
            }
        )
        docs = pd.DataFrame(columns=["document_guid", "content"])

        result = join_docs_to_annots(annots, docs)
        assert len(result) == 1

    def test_join_docs_drop_duplicates_flag(self):
        """Test dropping duplicates based on flag."""
        annots = pd.DataFrame(
            {
                "document_guid": ["G1"],
                "cui": [100],
            }
        )
        docs = pd.DataFrame(
            {
                "document_guid": ["G1"],
                "other_col": "doc",
            }
        )

        result_with_drop = join_docs_to_annots(annots, docs, drop_duplicates=True)
        assert len(result_with_drop) == 1

    def test_retrieve_pat_epr_docs_file_not_exist(self):
        """Test EPR docs retrieval when file doesn't exist."""
        pat_id = "nonexistent"
        result = retrieve_pat_epr_docs(pat_id, self.config_obj)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @patch("pat2vec.util.post_processing_build_methods.get_df_from_db")
    def test_retrieve_pat_docs_database_mode(self, mock_get_df):
        """Test document retrieval in database mode."""
        self.config_obj.storage_backend = "database"
        self.config_obj.db_engine = MagicMock()

        df_result = pd.DataFrame(
            {
                "document_guid": ["G1"],
                "content": ["text"],
            }
        )
        mock_get_df.return_value = df_result

        result = retrieve_pat_docs_mct_epr(
            "P1",
            self.config_obj,
            columns_epr=["document_guid"],
        )

        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_retrieve_pat_docs_multiple_sources(self):
        """Test document retrieval from multiple sources."""
        pat_id = "P1"

        df_epr = pd.DataFrame(
            {
                "document_guid": ["G1"],
                "content": ["epr text"],
                "source": ["epr"],
            }
        )

        os.makedirs(
            os.path.join(self.test_dir, "pre_document_batch_path_mct"),
            exist_ok=True,
        )
        df_epr.to_csv(
            os.path.join(self.config_obj.pre_document_batch_path, f"{pat_id}.csv"),
            index=False,
        )

    def test_retrieve_pat_docs_merge_logic(self):
        """Test document merging logic."""
        docs_df = pd.DataFrame(
            {
                "updatetime": ["2023-01-01"],
                "observationdocument_recordeddtm": [None],
                "document_guid": ["G1"],
                "observation_guid": [None],
            }
        )

        # Merge should fill missing values
        assert (
            docs_df["updatetime"]
            .fillna(docs_df["observationdocument_recordeddtm"])
            .notna()
            .all()
        )

    def test_join_docs_to_annots_no_matching_guid(self):
        """Test joining when document_guid has no match."""
        annots = pd.DataFrame(
            {
                "document_guid": ["G1"],
                "cui": [100],
            }
        )
        docs = pd.DataFrame(
            {
                "document_guid": ["G2"],  # Different GUID
                "content": ["text"],
            }
        )

        result = join_docs_to_annots(annots, docs)
        # Left join means annots rows are preserved with NaN for missing matches
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["content"])
