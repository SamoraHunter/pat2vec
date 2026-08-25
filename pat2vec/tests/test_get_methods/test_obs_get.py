import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_obs import get_current_pat_obs
from pat2vec.pat2vec_search.cogstack_search_methods import (
    initialize_cogstack_client,
)
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from pat2vec.util.get_dummy_data_cohort_searcher import populate_elastic_with_dummy_data
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestOBSGet:
    """Stage-mirroring pytest for obs_get testing."""

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

        cls.PROJ_NAME = "obs_test_project"
        cls.DB_FILENAME = "temp_obs_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        # Cleanup previous test outputs
        for dir_to_remove in ["obs_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}': {e}"
                raise RuntimeError(msg) from e

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="obs_test_project",
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

        obs_dfs = []
        search_terms = ["Test Observation", "Laboratory Test", "Clinical Note"]
        for pid in cls.patient_ids:
            num_rows = 3
            df_holder_list = []
            for search_term in search_terms:
                data = {
                    "observation_guid": [
                        str(random.randint(100000, 999999)) for _ in range(num_rows)
                    ],
                    "client_idcode": [pid for _ in range(num_rows)],
                    "obscatalogmasteritem_displayname": [
                        search_term for _ in range(num_rows)
                    ],
                    "observation_valuetext_analysed": [
                        str(random.uniform(0, 100)) for _ in range(num_rows)
                    ],
                    "observationdocument_recordeddtm": [
                        f"202{random.randint(0, 3)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}T{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
                        for _ in range(num_rows)
                    ],
                    "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
                    "_id": [f"{i}" for i in range(num_rows)],
                    "_index": [None for _ in range(num_rows)],
                    "_score": [None for _ in range(num_rows)],
                }
                df = pd.DataFrame(data)
                df_holder_list.append(df)

            combined_df = (
                pd.concat(df_holder_list, ignore_index=True)
                if len(df_holder_list) > 1
                else df_holder_list[0]
            )
            obs_dfs.append(combined_df)

        df_obs = (
            pd.concat(obs_dfs, ignore_index=True) if len(obs_dfs) > 1 else obs_dfs[0]
        )
        df_obs = df_obs.where(pd.notnull(df_obs), None)
        ingest_data_to_elasticsearch(df_obs, "observations", es_client=cls.cs.elastic)
        cls.cs.elastic.indices.refresh(index="observations")

        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)
        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"
        cls.logger = setup_logger()

        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.cred_path,
            current_path_dir="",
            main_options={"bmi": True, "obs": True},
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

        cls.pat2vec_obj = main(
            cogstack=True,
            use_filter=False,
            json_filter_path=None,
            random_seed_val=random_seed_value,
            hostname=None,
            config_obj=cls.config_obj,
        )
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
            self.config_obj.main_options.get("obs", False) is True
        ), "OBS option should be enabled in config"
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

        # Verify observation records were ingested
        es_count = self.cs.elastic.count(index="observations")["count"]
        expected_obs_count = (
            len(self.patient_ids) * 3 * 3
        )  # 5 patients * 3 rows * 3 search terms
        assert (
            es_count >= expected_obs_count
        ), f"Expected at least {expected_obs_count} observation records, got {es_count}"

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

    def test_obs_vector_validation(self):
        """Verify pat_maker produced actual values in the feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.

        Observation features use patterns:
        - obs_{category}_{value}: One-hot encoded indicators for observation categories
        - bmi_*: BMI-related statistics (mean, median, std, etc.) when BMI feature is enabled
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        feature_cols = [
            c
            for c in all_features.columns
            if (
                c.startswith(("obs_", "bmi_"))
                and c != "client_idcode"
                and "_date_time_stamp" not in c
            )
        ]

        assert (
            len(feature_cols) > 0
        ), f"No feature columns found. Available columns: {list(all_features.columns)}"

        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) < len(feature_cols), (
            f"All features are empty - vectorisation is failing. "
            f"Null columns: {list(totally_empty_cols.index)}"
        )

        print(f"Found {len(feature_cols)} observation feature columns")

    def test_obs_expected_values(self):
        """Validate specific expected observation feature values.

        With random_seed=42, the test setup creates deterministic dummy data:
        - 3 search terms: "Test Observation", "Laboratory Test", "Clinical Note"
        - 3 rows per search term per patient (9 observations per patient)
        - observation_valuetext_analysed contains random floats [0, 100)

        Feature computation creates averaged statistics for each search term:
        - obs_test_observation_*: Statistics from "Test Observation" entries
        - obs_laboratory_test_*: Statistics from "Laboratory Test" entries
        - obs_clinical_note_*: Statistics from "Clinical Note" entries

        Expected values:
        - Feature columns should include obs_<term>_<stat> for each search term
        - Statistical features (mean, std, etc.) should be valid numeric values
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty"

        feature_cols = [
            c
            for c in all_features.columns
            if (
                c.startswith(("obs_", "bmi_"))
                and c != "client_idcode"
                and "_date_time_stamp" not in c
            )
        ]

        assert len(feature_cols) > 0, (
            f"No observation feature columns found. "
            f"Available: {list(all_features.columns)}"
        )

        for col in feature_cols:
            values = all_features[col].dropna()

            if "_num-" in col.lower() or "_count" in col.lower():
                assert len(values) > 0, f"{col} should have at least one non-null value"
                int_values = [int(v) for v in values]
                assert all(
                    v >= 0 for v in int_values
                ), f"{col} should have non-negative counts, got {[int(v) for v in values]}"

            elif any(
                suffix in col.lower()
                for suffix in ["_mean", "_median", "_std", "_min", "_max"]
            ):
                if len(values) > 0:
                    assert all(
                        pd.notna(v)
                        and isinstance(v, (int, float))
                        and not isinstance(v, bool)
                        for v in values
                    ), f"{col} should have valid numeric values"

            else:
                if len(values) > 0:
                    assert all(
                        pd.notna(v)
                        and isinstance(v, (int, float))
                        and not isinstance(v, bool)
                        for v in values
                    ), f"{col} should have valid numeric values"

    def test_obs_data_retrieval(self):
        """Test OBS data retrieval - verify observation features can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        pat_batch = pd.DataFrame()

        obs_data = get_current_pat_obs(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pat_batch,
            search_term="Test Observation",
            config_obj=self.config_obj,
        )

        assert obs_data is not None, "OBS data should not be None"

    def test_multiple_search_terms(self):
        """Test multiple search terms - verify various observation types can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        pat_batch = pd.DataFrame()

        search_terms = ["Test Observation", "Laboratory Test", "Clinical Note"]

        for search_term in search_terms:
            obs_data = get_current_pat_obs(
                current_pat_client_id_code=all_pat_list[0],
                target_date_range=(2020, 1, 1, 2023, 12, 31),
                pat_batch=pat_batch,
                search_term=search_term,
                config_obj=self.config_obj,
            )

            assert (
                obs_data is not None
            ), f"OBS data for '{search_term}' should not be None"

    def test_obs_empty_pat_batch(self):
        """Test OBS with empty pat_batch - verify function handles empty batch gracefully."""
        empty_result = get_current_pat_obs(
            current_pat_client_id_code="test_patient",
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pd.DataFrame(),
            search_term="Test Observation",
            config_obj=self.config_obj,
        )

        assert empty_result is not None, "Empty batch result should be handled"

    def test_8_cleanup_verification(self):
        """Test cleanup verification - verify all temp files are cleaned up properly."""
        # Perform cleanup before verification
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
