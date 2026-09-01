import os
import random
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_search.cogstack_search_methods import initialize_cogstack_client
from pat2vec.tests.test_get_methods.temp_setup import (
    cleanup_test_temp_dir,
    setup_test_temp_dir,
)
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_cohort_searcher import populate_elastic_with_dummy_data
from pat2vec.util.get_dummy_data_cohort_searcher import generate_epr_documents_data
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.helper_functions import save_raw_patient_batch
from pat2vec.util.logger_setup import setup_logger
from pat2vec.util.post_processing_build_methods import (
    build_merged_epr_mct_annot_df,
    build_merged_epr_mct_doc_df,
)

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestAnnotationsGet:
    """Stage-mirroring pytest for test_annotations_get.ipynb."""

    @pytest.fixture(autouse=True, scope="class")
    def _start_elastic(self, elastic_container, request):
        """Run all setup that depends on the shared ES container."""
        cls = type(self)
        cls.cred_path = elastic_container
        cls.creds_filename = elastic_container

        # Set up temp directory for this test class
        temp_dir, repo_root = setup_test_temp_dir()
        cls.temp_dir = temp_dir

        # Store original cwd and change to temp dir for relative paths
        cls._original_cwd = os.getcwd()
        os.chdir(temp_dir)

        def _cleanup():
            os.chdir(cls._original_cwd)
            cleanup_test_temp_dir(cls.temp_dir)

        request.addfinalizer(_cleanup)

        sys.path.insert(0, os.path.join(repo_root, "pat2vec"))
        sys.path.append(repo_root)
        pat2vec_dir = os.path.abspath(os.path.join(repo_root, "pat2vec"))
        sys.path.insert(0, pat2vec_dir)

        cls.PROJ_NAME = "annotations_test_project"
        cls.DB_FILENAME = "temp_annotations_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        # Create config for population using absolute schema path from repo_root
        schema_path = os.path.join(repo_root, "test_files", "elastic_schemas.json")
        config_populate = config_class(
            proj_name="annotations_test_project",
            credentials_path=cls.cred_path,
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

        # Populate dummy data
        cls.patient_ids = populate_elastic_with_dummy_data(
            config_populate,
            n_patients=5,
        )

        # Setup CohStack client and index
        cls.cs = initialize_cogstack_client(config_populate)

        indices = [
            "epr_documents",
            "basic_observations",
            "observations",
            "order",
            "pims_apps",
        ]
        cls.cs.elastic.indices.refresh(index=indices, ignore_unavailable=True)

        # Initialize database
        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)

        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"

        # Set the same db_connection_string on config_populate so EPR docs can be saved during population
        config_populate.db_connection_string = db_connection_string
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        config_populate.cohort_searcher_with_terms_and_search = (
            cohort_searcher_with_terms_and_search_dummy
        )

        # Recreate the engine since db_connection_string was set after initialization
        if not config_populate.db_engine or "sqlite:///:memory:" in str(
            config_populate.db_engine.url
        ):
            from sqlalchemy import create_engine
            from sqlalchemy.pool import StaticPool

            config_populate.db_engine = create_engine(
                db_connection_string,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool if "sqlite" in db_connection_string else None,
            )

        cls.logger = setup_logger()
        # Generate and save EPR documents data to DB for annotation processing
        epr_docs_df = generate_epr_documents_data(
            num_rows=random.randint(1, 5),
            entered_list=cls.patient_ids,
            global_start_year=int(config_populate.global_start_year),
            global_start_month=int(config_populate.global_start_month),
            global_end_year=int(config_populate.global_end_year),
            global_end_month=int(config_populate.global_end_month),
            global_end_day=int(config_populate.global_end_day),
        )
        epr_docs_df = epr_docs_df.where(pd.notnull(epr_docs_df), None)
        for patient_id in cls.patient_ids:
            # Use cls.config_obj which has the correct db_connection_string for DB operations
            save_raw_patient_batch(
                epr_docs_df[epr_docs_df["client_idcode"] == patient_id],
                patient_id,
                "raw_epr_docs",
                config_populate,
            )

        # Generate and save appointments data for appointments feature extraction
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            generate_appointments_data,
        )

        app_df_list = []
        for pid in cls.patient_ids:
            df = generate_appointments_data(
                num_rows=3,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            app_df_list.append(df)
        app_df = (
            pd.concat(app_df_list, ignore_index=True) if app_df_list else pd.DataFrame()
        )
        app_df = app_df.where(pd.notnull(app_df), None)
        for patient_id in cls.patient_ids:
            df_filter = (
                app_df[app_df["HospitalID"] == patient_id]
                if not app_df.empty
                else pd.DataFrame()
            )
            # Use cls.config_obj which has the correct db_connection_string for DB operations
            save_raw_patient_batch(
                df_filter,
                patient_id,
                "raw_appointments",
                config_populate,
                id_column="HospitalID",
            )

        # Create main config
        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.cred_path,
            current_path_dir="",
            main_options={"annotations": True},
            batch_mode=True,
            verbosity=0,
            random_seed_val=random_seed_value,
            testing=True,
            testing_elastic=True,
            dummy_medcat_model=True,
            use_controls=False,
            medcat=False,
            start_time=None,
            patient_id_column_name="client_idcode",
            annot_filter_options={},
            shuffle_pat_list=False,
            storage_backend="database",
            db_connection_string=db_connection_string,
            all_patient_list=cls.patient_ids,
            overwrite_stored_pat_docs=True,
        )

        # Run pat2vec pipeline
        cls.pat2vec_obj = main(
            cogstack=True,
            use_filter=False,
            json_filter_path=None,
            random_seed_val=random_seed_value,
            hostname=None,
            config_obj=cls.config_obj,
        )

        # Process first patient
        cls.pat2vec_obj.pat_maker(0)

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

    def test_annotations_vector_non_empty(self):
        """Verify pat_maker produced actual values in the feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        # Find annotation feature columns (format: pretty_name_count_epr_{name})
        feature_cols = [c for c in all_features.columns if "pretty_name" in c.lower()]
        assert len(feature_cols) > 0, (
            f"No annotation columns found in feature vector. "
            f"Available columns: {list(all_features.columns)}"
        )

        # Every annotation column must have at least one non-null value
        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) < len(feature_cols), (
            f"All annotation columns are empty - vectorisation is failing. "
            f"Null columns: {list(totally_empty_cols.index)}"
        )

        print(f"Found {len(feature_cols)} annotation feature columns")

    def test_annotations_data_retrieval(self):
        """Test annotations data retrieval - verify annotations features can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        # Get all features (annotations data is stored in the features table)
        data_retrieved = get_all_features(self.config_obj)

        assert data_retrieved is not None, "Annotations data should not be None"
        if isinstance(data_retrieved, list):
            assert len(data_retrieved) > 0, "Annotations data list should not be empty"
            assert not data_retrieved[
                0
            ].empty, "Annotations DataFrame should not be empty"
        else:
            assert not data_retrieved.empty, "Annotations DataFrame should not be empty"

    def test_merge_documents_from_db_functionality(self):
        """Test document merge functionality - verify the post-processing merge function

        extracts all patients' documents from the database (written by pat_maker)
        into a single dataframe.
        """
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        merged_docs_path = build_merged_epr_mct_doc_df(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert (
            merged_docs_path is not None
        ), "build_merged_epr_mct_doc_df should return a path"

        assert os.path.exists(
            merged_docs_path,
        ), f"Merged documents file should exist at {merged_docs_path}"

        merged_docs = pd.read_csv(merged_docs_path)
        assert not merged_docs.empty, (
            "Merged documents DataFrame should not be empty — "
            "the pat2vec pipeline should have saved documents to the database."
        )

    def test_merge_annotations_from_db_functionality(self):
        """Test annotation merge functionality - verify the post-processing merge function

        merges annotation table results into a single dataframe.
        """
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        merged_annots_path = build_merged_epr_mct_annot_df(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert (
            merged_annots_path is not None
        ), "build_merged_epr_mct_annot_df should return a path"

        assert os.path.exists(
            merged_annots_path,
        ), f"Merged annotations file should exist at {merged_annots_path}"

        merged_annots = pd.read_csv(merged_annots_path)
        assert not merged_annots.empty, (
            "Merged annotations DataFrame should not be empty — "
            "the pat2vec pipeline should have saved annotation records to the database."
        )

    def test_8_cleanup_verification(self):
        """Test cleanup verification - verify all project artifacts exist.

        Since temp directories are auto-cleaned by the fixture, this test
        verifies that the pipeline ran correctly rather than checking cleanup.
        """
        assert os.path.exists(
            self.DB_PATH,
        ), f"Database file should exist at {self.DB_PATH} (inside temp dir)"
        assert os.path.exists(
            self.PROJ_NAME,
        ), f"Project directory should exist at {self.PROJ_NAME} (inside temp dir)"
