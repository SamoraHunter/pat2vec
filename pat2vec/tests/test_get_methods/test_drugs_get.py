import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_drugs import get_current_pat_drugs
from pat2vec.pat2vec_search.cogstack_search_methods import initialize_cogstack_client
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_basic_observations_data,
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger
from pat2vec.util.post_processing_build_methods import merge_drugs_csv

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestDrugsGet:
    """Stage-mirroring pytest for test_drugs_get.ipynb."""

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

        cls.PROJ_NAME = "drugs_test_project"
        cls.DB_FILENAME = "temp_drugs_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        # Cleanup previous test outputs
        for dir_to_remove in ["drugs_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}': {e}"
                raise RuntimeError(msg) from e

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        cls.config_populate = config_class(
            proj_name="drugs_test_project",
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

        cls.cs = initialize_cogstack_client(cls.config_populate)

        cls.patient_ids = populate_elastic_with_dummy_data(
            cls.config_populate,
            n_patients=5,
        )

        indices = [
            "epr_documents",
            "basic_observations",
            "observations",
            "order",
            "pims_apps",
        ]
        cls.cs.elastic.indices.refresh(index=indices, ignore_unavailable=True)

        drug_obs_dfs = []
        for pid in cls.patient_ids:
            df = generate_basic_observations_data(
                num_rows=3,
                entered_list=[pid],
                global_start_year=int(cls.config_populate.global_start_year),
                global_start_month=int(cls.config_populate.global_start_month),
                global_end_year=int(cls.config_populate.global_end_year),
                global_end_month=int(cls.config_populate.global_end_month),
            )
            drug_obs_dfs.append(df)

        df_drug_obs = (
            pd.concat(drug_obs_dfs, ignore_index=True)
            if len(drug_obs_dfs) > 1
            else drug_obs_dfs[0]
        )
        df_drug_obs = df_drug_obs.where(pd.notnull(df_drug_obs), None)

        ingest_data_to_elasticsearch(
            df_drug_obs,
            "basic_observations",
            es_client=cls.cs.elastic,
        )
        cls.cs.elastic.indices.refresh(index="basic_observations")

        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)
        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"

        cls.logger = setup_logger()

        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.cred_path,
            current_path_dir="",
            main_options={"drugs": True},
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
            self.config_obj.main_options.get("drugs", False) is True
        ), "Drugs option should be enabled in config"
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

        # Verify basic_observations with drug-related fields were ingested
        es_count = self.cs.elastic.count(index="basic_observations")["count"]
        expected_obs_count = len(self.patient_ids) * 3  # 5 patients * 3 rows each
        assert (
            es_count >= expected_obs_count
        ), f"Expected at least {expected_obs_count} drug-related observations, got {es_count}"

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

    def test_drugs_vector_non_empty(self):
        """Verify pat_maker produced actual values in the drugs feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        # Find drugs-related columns (excluding client_idcode and date columns)
        feature_cols = [
            c
            for c in all_features.columns
            if (
                "drug" in c.lower()
                or "_num-drug-order" in c.lower()
                or "_days-since-last-drug" in c.lower()
            )
            and "client_idcode" not in c.lower()
            and "date" not in c.lower()
        ]

        assert len(feature_cols) > 0, (
            f"No drugs-related columns found in feature vector. "
            f"Available columns: {list(all_features.columns)}"
        )

        # Every feature column must have at least one non-null value
        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) == 0, (
            f"The following drugs-related columns are entirely null after pat_maker ran:\n"
            f"{list(totally_empty_cols.index)}\n"
            "Vectorisation is silently failing — check the get method return value "
            "and how pat_maker consumes it."
        )

    def test_drugs_data_retrieval(self):
        """Test drugs data retrieval - verify drugs features can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        pat_batch = pd.DataFrame()

        drugs_data = get_current_pat_drugs(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        assert drugs_data is not None, "Drugs data should not be None"
        if isinstance(drugs_data, list):
            assert len(drugs_data) > 0, "Drugs data list should not be empty"
            assert not drugs_data[0].empty, "Drugs DataFrame should not be empty"
        else:
            assert not drugs_data.empty, "Drugs DataFrame should not be empty"

    def test_merge_drugs_data_functionality(self):
        """Test merge drugs data functionality - verify merge function creates CSV."""
        all_pat_list = self.pat2vec_obj.all_patient_list

        try:
            merge_drugs_csv(all_pat_list, self.config_obj, overwrite=True)
        except Exception as e:
            msg = f"merge_drugs_csv raised exception: {e}"
            raise AssertionError(msg)

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
