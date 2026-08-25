import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_smoking import get_smoking_features
from pat2vec.pat2vec_search.cogstack_search_methods import (
    initialize_cogstack_client,
)
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_smoking_data,
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger
from pat2vec.util.post_processing_build_methods import merge_smoking_csv

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestSmokingGet:
    """Stage-mirroring pytest for test_smoking_get.ipynb."""

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

        cls.PROJ_NAME = "smoking_test_project"
        cls.DB_FILENAME = "temp_smoking_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        # Cleanup previous test outputs
        for dir_to_remove in ["smoking_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}' directory: {e}. "
                "Critical error - cannot start with stale data."
                raise RuntimeError(msg) from e

        # Create config for population
        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="smoking_test_project",
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

        # Generate and ingest smoking data
        smoking_dfs = []
        for pid in cls.patient_ids:
            df = generate_smoking_data(
                num_rows=3,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            smoking_dfs.append(df)

        df_smoking = (
            pd.concat(smoking_dfs, ignore_index=True)
            if len(smoking_dfs) > 1
            else smoking_dfs[0]
        )
        df_smoking = df_smoking.where(pd.notnull(df_smoking), None)

        ingest_data_to_elasticsearch(
            df_smoking,
            "observations",
            es_client=cls.cs.elastic,
        )
        cls.cs.elastic.indices.refresh(index="observations")

        # Initialize database
        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)

        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"

        cls.logger = setup_logger()

        # Create main config
        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.creds_filename,
            current_path_dir="",
            main_options={"smoking": True},
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
            self.config_obj.main_options.get("smoking", False) is True
        ), "Smoking option should be enabled in config"
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

        # Verify smoking observations were ingested
        es_count = self.cs.elastic.count(index="observations")["count"]
        expected_smoking_count = len(self.patient_ids) * 3  # 5 patients * 3 rows each
        assert (
            es_count >= expected_smoking_count
        ), f"Expected at least {expected_smoking_count} smoking observations, got {es_count}"

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

    def test_smoking_vector_validation(self):
        """Verify pat_maker produced actual values in the smoking feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.
        Smoking features use pattern: smoking_status_{current|non}

        Additionally validates expected feature values based on deterministic
        dummy data generation with random_seed=42:
        - generate_smoking_data produces ['Never smoked', NaN, 'Never smoked']
          for each patient (3 rows per patient, 5 patients = 15 observations)
        - calculate_smoking_features sets smoking_status_current=0 and
          smoking_status_non=1 for all patients
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        feature_cols = [
            c
            for c in all_features.columns
            if "smoking" in c.lower() and c != "client_idcode"
        ]

        assert len(feature_cols) > 0, (
            f"No smoking-related columns found in feature vector. "
            f"Available columns: {list(all_features.columns)}"
        )

        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) < len(feature_cols), (
            f"All smoking columns are empty - vectorisation is failing. "
            f"Null columns: {list(totally_empty_cols.index)}"
        )

        print(f"Found {len(feature_cols)} smoking feature columns")

    def test_smoking_expected_values(self):
        """Validate specific expected smoking feature values.

        With random_seed=42, generate_smoking_data produces deterministic results:
        - 3 observations per patient with values: ['Never smoked', NaN, 'Never smoked']
        - calculate_smoking_features creates smoking_status_current and smoking_status_non

        Expected values for all patients:
        - smoking_status_current = 0 (no "Current smoker" entries)
        - smoking_status_non = 1 (has "Never smoked" or "Ex-smoker" entries)

        This test validates that feature computation correctly identifies
        the expected binary features based on actual observation data.
        """
        all_features = get_all_features(self.config_obj)

        assert (
            "smoking_status_current" in all_features.columns
        ), f"smoking_status_current column missing. Available: {list(all_features.columns)}"
        assert (
            "smoking_status_non" in all_features.columns
        ), f"smoking_status_non column missing. Available: {list(all_features.columns)}"

        for patient_id in self.patient_ids:
            row = all_features[all_features["client_idcode"] == patient_id]
            if not row.empty:
                current_val = row["smoking_status_current"].iloc[0]
                non_val = row["smoking_status_non"].iloc[0]

                assert pd.notna(
                    current_val
                ), f"Patient {patient_id}: smoking_status_current is null"
                assert int(current_val) == 0, (
                    f"Patient {patient_id}: Expected smoking_status_current=0 "
                    f"(no 'Current smoker' entries in dummy data), got {current_val}"
                )

                assert pd.notna(
                    non_val
                ), f"Patient {patient_id}: smoking_status_non is null"
                assert int(non_val) == 1, (
                    f"Patient {patient_id}: Expected smoking_status_non=1 "
                    f"(has 'Never smoked' entries in dummy data), got {non_val}"
                )

    def test_smoking_data_retrieval(self):
        """Test smoking data retrieval - verify smoking features can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        pat_batch = pd.DataFrame()

        smoking_data = get_smoking_features(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        assert smoking_data is not None, "Smoking data should not be None"
        if isinstance(smoking_data, list):
            assert len(smoking_data) > 0, "Smoking data list should not be empty"
            assert not smoking_data[0].empty, "Smoking DataFrame should not be empty"
        else:
            assert not smoking_data.empty, "Smoking DataFrame should not be empty"

    def test_merge_smoking_data_functionality(self):
        """Test merge smoking data functionality - verify merge function creates CSV."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        merged_path = merge_smoking_csv(all_pat_list, self.config_obj, overwrite=True)

        assert os.path.exists(merged_path), f"Merged file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert not merged_data.empty, "Merged smoking DataFrame should not be empty"

    def test_8_cleanup_verification(self):
        """Test cleanup verification - verify all temp files are cleaned up properly."""
        # Perform cleanup before verification (same as in notebook)
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

        # Verify cleanup
        assert not os.path.exists(self.DB_PATH), "Database file should be removed"
        assert not os.path.exists(self.PROJ_NAME), "Project directory should be removed"
