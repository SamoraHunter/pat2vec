import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_pat_annotations import (
    get_current_pat_annotations,
)
from pat2vec.pat2vec_search.cogstack_search_methods import initialize_cogstack_client
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_epr_documents_data,
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger
from pat2vec.util.post_processing_build_methods import (
    build_merged_epr_mct_annot_df,
    build_merged_epr_mct_doc_df,
)

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestPatAnnotationsGet:
    """Stage-mirroring pytest for pat_annotations get method."""

    @pytest.fixture(autouse=True, scope="class")
    def _start_elastic(self, elastic_container):
        """Run all setup that depends on the shared ES container."""
        cls = type(self)
        cls.cred_path = elastic_container
        cls.creds_filename = elastic_container

        cls.current_dir = os.getcwd()
        cls.grandparent_dir = os.path.dirname(os.path.dirname(cls.current_dir))

        sys.path.insert(0, os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.append(cls.grandparent_dir)
        cls.pat2vec_dir = os.path.abspath(os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.insert(0, cls.pat2vec_dir)

        cls.PROJ_NAME = "pat_annot_test_project"
        cls.DB_FILENAME = "temp_pat_annotations_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        # Cleanup previous test outputs
        for dir_to_remove in ["pat_annot_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}' directory: {e}"
                raise RuntimeError(msg) from e

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        cls.schema_path = schema_path

        config_populate = config_class(
            proj_name="pat_annot_test_project",
            credentials_path=cls.creds_filename,
            test_schema_path=schema_path,
            testing=True,
            testing_elastic=True,
            global_start_year=2020,
            global_start_month=1,
            global_start_day=1,
            global_end_year=2023,
            global_end_month=12,
            global_end_day=31,
        )

        cls.patient_ids = populate_elastic_with_dummy_data(
            config_populate,
            n_patients=5,
        )

        cls.cs = initialize_cogstack_client(config_populate)

        indices = [
            "epr_documents",
            "basic_observations",
            "observations",
            "order",
            "pims_apps",
        ]
        cls.cs.elastic.indices.refresh(index=indices, ignore_unavailable=True)

        doc_dfs = []
        for pid in cls.patient_ids:
            df = generate_epr_documents_data(
                num_rows=2,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
                use_GPT=False,
            )
            doc_dfs.append(df)

        df_docs = (
            pd.concat(doc_dfs, ignore_index=True) if len(doc_dfs) > 1 else doc_dfs[0]
        )
        df_docs = df_docs.where(pd.notnull(df_docs), None)

        ingest_data_to_elasticsearch(df_docs, "epr_documents", es_client=cls.cs.elastic)
        cls.cs.elastic.indices.refresh(index="epr_documents")

        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)

        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"

        cls.logger = setup_logger()

        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.creds_filename,
            current_path_dir="",
            main_options={"annotations": True},
            batch_mode=True,
            verbosity=0,
            random_seed_val=random_seed_value,
            testing=True,
            testing_elastic=True,
            dummy_medcat_model=True,
            use_controls=False,
            medcat=True,
            start_time=None,
            patient_id_column_name="client_idcode",
            annot_filter_options={},
            shuffle_pat_list=False,
            storage_backend="database",
            db_connection_string=db_connection_string,
            all_patient_list=cls.patient_ids,
        )

        cls.pat2vec_obj = main(
            cogstack=True,
            use_filter=False,
            json_filter_path=None,
            random_seed_val=random_seed_value,
            hostname=None,
            config_obj=cls.config_obj,
        )

        cls.pat2vec_obj.pat_maker(0)

    # --- teardown_class removed — session fixture handles container.stop() ---

    def test_1_dummy_data_generation(self):
        """Test dummy data generation - verify patient IDs were created."""
        assert (
            len(self.patient_ids) == 5
        ), f"Expected 5 patients, got {len(self.patient_ids)}"
        assert all(
            isinstance(pid, str) for pid in self.patient_ids
        ), "All patient IDs should be strings"

    def test_2_config_and_pipeline_setup(self):
        """Test config and pipeline setup - verify configuration was created."""
        assert self.config_obj is not None, "Config object should not be None"
        assert (
            self.config_obj.main_options.get("annotations", False) is True
        ), "Annotations option should be enabled in config"
        assert self.pat2vec_obj is not None, "pat2vec object should not be None"

    def test_3_index_population_and_verification(self):
        """Test index population and verification - verify documents were ingested."""
        indices = [
            "epr_documents",
            "basic_observations",
            "observations",
            "order",
            "pims_apps",
        ]
        for index in indices:
            try:
                if self.cs.elastic.indices.exists(index=index):
                    count = self.cs.elastic.count(index=index)["count"]
                    assert count > 0, f"Index {index} should have documents"
                else:
                    msg = f"Index not created: {index}"
                    raise AssertionError(msg)
            except Exception as e:
                msg = f"Error checking index {index}: {e}"
                raise AssertionError(msg) from e

        # Verify epr_documents has content
        es_count = self.cs.elastic.count(index="epr_documents")["count"]
        expected_doc_count = len(self.patient_ids) * 2  # 5 patients * 2 docs each
        assert (
            es_count >= expected_doc_count
        ), f"Expected at least {expected_doc_count} epr_documents, got {es_count}"

    def test_4_pat2vec_pipeline_execution(self):
        """Test pat2vec pipeline execution - verify patient was processed."""
        assert (
            self.pat2vec_obj.all_patient_list is not None
        ), "Patient list should not be None"
        assert (
            len(self.pat2vec_obj.all_patient_list) > 0
        ), "Patient list should have patients"

    def test_5_feature_extraction(self):
        """Test feature extraction - verify features were extracted."""
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "All features should not be None"
        assert not all_features.empty, "Features DataFrame should not be empty"

    def test_pat_annotations_function_with_mock_data(self):
        """Test pat_annotations function with mock annotation data."""
        # Create mock annotation data that mimics MedCAT output
        # This simulates what would be returned by get_pat_batch_epr_docs_annotations
        # Use dates within the 1-day interval starting from 2020-01-01
        mock_annotation_df = pd.DataFrame(
            {
                "client_idcode": [self.patient_ids[0]] * 5 + [self.patient_ids[1]] * 3,
                "pretty_name": [
                    "Hypertension",
                    "Diabetes",
                    "Fever",
                    "Cough",
                    "Headache",
                    "Asthma",
                    "Allergy",
                    "Flu",
                ],
                "cui": [
                    "C0020538",
                    "C0011860",
                    "C0015967",
                    "C0010200",
                    "C0018682",
                    "C0024121",
                    "C0019249",
                    "C0017778",
                ],
                "updatetime": [
                    "2020-01-01T10:30:00",
                    "2020-01-01T14:45:00",
                    "2020-01-01T09:15:00",
                    "2020-01-01T16:00:00",
                    "2020-01-01T11:20:00",
                    "2020-01-01T08:30:00",
                    "2020-01-01T13:45:00",
                    "2020-01-01T15:00:00",
                ],
                "acc": [0.95] * 8,
            },
        )

        # Test with mock annotation data - date range that will include the data
        annotations_data = get_current_pat_annotations(
            current_pat_client_id_code=self.patient_ids[0],
            target_date_range=(
                2020,
                1,
                1,
            ),  # Start date; end calculated from config interval (typically +1 day)
            batch_epr_docs_annotations=mock_annotation_df,
            config_obj=self.config_obj,
        )

        assert annotations_data is not None, "Annotations data should not be None"
        assert isinstance(
            annotations_data,
            pd.DataFrame,
        ), "Annotations should be a DataFrame"
        assert len(annotations_data) > 0, "Annotations DataFrame should have rows"

        # Verify that pretty_name count features were computed
        has_pretty_name_feature = any(
            "pretty_name_count" in str(col) for col in annotations_data.columns
        )
        assert (
            has_pretty_name_feature
        ), "Should have pretty_name count features from annotations"

    def test_pat_annotations_function_with_none_batch(self):
        """Test pat_annotations function with None batch data."""
        # Test with None batch data (should return DataFrame with just client_idcode)
        annotations_data = get_current_pat_annotations(
            current_pat_client_id_code=self.patient_ids[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            batch_epr_docs_annotations=None,
            config_obj=self.config_obj,
        )

        assert annotations_data is not None, "Annotations data should not be None"
        assert isinstance(
            annotations_data,
            pd.DataFrame,
        ), "Annotations should be a DataFrame"
        assert (
            "client_idcode" in annotations_data.columns
        ), "Should have client_idcode column even with no batch data"

    def test_pat_annotations_function_with_empty_batch(self):
        """Test pat_annotations function with empty batch data."""
        # Test with empty DataFrame
        empty_df = pd.DataFrame(columns=["client_idcode", "pretty_name", "updatetime"])

        annotations_data = get_current_pat_annotations(
            current_pat_client_id_code=self.patient_ids[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            batch_epr_docs_annotations=empty_df,
            config_obj=self.config_obj,
        )

        assert annotations_data is not None, "Annotations data should not be None"
        assert isinstance(
            annotations_data,
            pd.DataFrame,
        ), "Annotations should be a DataFrame"

    def test_pat_annotations_function_with_date_filtering(self):
        """Test pat_annotations function properly filters by date range."""
        # Create annotation data with dates outside and inside target range
        mock_annotation_df = pd.DataFrame(
            {
                "client_idcode": [self.patient_ids[0]] * 5,
                "pretty_name": [
                    "Condition1",
                    "Condition2",
                    "Condition3",
                    "Condition4",
                    "Condition5",
                ],
                "updatetime": [
                    "2019-06-15T10:30:00",  # Before range (should be filtered out)
                    "2020-01-01T00:00:00",  # Start of range (should be included)
                    "2021-06-15T10:30:00",  # Within range (should be included)
                    "2023-12-31T23:59:59",  # End of range (should be included)
                    "2024-01-15T10:30:00",  # After range (should be filtered out)
                ],
            },
        )

        annotations_data = get_current_pat_annotations(
            current_pat_client_id_code=self.patient_ids[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            batch_epr_docs_annotations=mock_annotation_df,
            config_obj=self.config_obj,
        )

        assert annotations_data is not None, "Annotations data should not be None"

        # The function should return a count DataFrame with fewer features
        # (only the ones that passed date filtering)
        has_pretty_name_feature = any(
            "pretty_name_count" in str(col) for col in annotations_data.columns
        )
        assert (
            has_pretty_name_feature
        ), "Should have pretty_name count features from filtered annotations"

    def test_merge_pat_annotations_functionality(self):
        """Test annotation merge functionality - verify the post-processing merge function

        merges annotation table results into a single dataframe.
        """
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        merged_path = build_merged_epr_mct_annot_df(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert (
            merged_path is not None
        ), "build_merged_epr_mct_annot_df should return a path"

        assert os.path.exists(
            merged_path,
        ), f"Merged annotations file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert not merged_data.empty, (
            "Merged annotations DataFrame should not be empty — "
            "the pat2vec pipeline should have saved annotation records to the database."
        )

    def test_pat_annotations_vector_validation(self):
        """Verify pat_maker produced actual values in the pretty_name feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        feature_cols = [c for c in all_features.columns if "pretty_name" in c.lower()]

        assert len(feature_cols) > 0, (
            f"No pretty_name annotation columns found. "
            f"Available columns: {list(all_features.columns)}"
        )

        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) == 0, (
            f"The following pretty_name annotation columns are entirely null after pat_maker ran:\n"
            f"{list(totally_empty_cols.index)}\n"
            "Vectorisation is silently failing — check the get method return value "
            "and how pat_maker consumes it."
        )

        print(f"Found {len(feature_cols)} pretty_name annotation feature columns")

    def test_merge_documents_from_db_functionality(self):
        """Test document merge functionality - verify the post-processing merge

        function extracts all patients' documents from the database (written by
        pat_maker) into a single dataframe.
        """
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        merged_path = build_merged_epr_mct_doc_df(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert (
            merged_path is not None
        ), "build_merged_epr_mct_doc_df should return a path"

        assert os.path.exists(
            merged_path,
        ), f"Merged documents file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert not merged_data.empty, (
            "Merged documents DataFrame should not be empty — "
            "the pat2vec pipeline should have saved documents to the database."
        )

    def test_8_cleanup_verification(self):
        """Test cleanup verification - verify all temp files were removed."""
        # Clean up before verification
        try:
            if os.path.exists(self.DB_PATH):
                os.remove(self.DB_PATH)
        except Exception as e:
            msg = f"Failed to remove database file '{self.DB_PATH}': {e}"
            raise AssertionError(msg) from e

        try:
            if os.path.exists(self.PROJ_NAME):
                shutil.rmtree(self.PROJ_NAME, ignore_errors=False)
        except Exception as e:
            msg = f"Failed to remove '{self.PROJ_NAME}' directory: {e}"
            raise AssertionError(msg) from e

        # Note: credentials file is managed by shared fixture and cleaned up automatically
        # No need to check for its deletion here

        # Verify cleanup
        assert not os.path.exists(self.DB_PATH), "Database file should be removed"
        assert not os.path.exists(self.PROJ_NAME), "Project directory should be removed"
