import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_demo import get_demographics_data
from pat2vec.pat2vec_search.cogstack_search_methods import (
    initialize_cogstack_client,
)
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_basic_observations_data,
    generate_bmi_data,
    generate_diagnostic_orders_data,
    generate_drug_orders_data,
    generate_epr_documents_data,
    generate_epr_documents_personal_data,
    generate_news_data,
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestDemoGet:
    """Stage-mirroring pytest for test_demo_get.ipynb."""

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

        cls.PROJ_NAME = "demo_test_project"
        cls.DB_FILENAME = "temp_demo_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        # Cleanup previous test outputs
        for dir_to_remove in ["demo_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}': {e}"
                raise RuntimeError(msg) from e

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="demo_test_project",
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

        # Ensure data is ingested using the SAME client (cls.cs) that will be used in tests
        df_epr = generate_epr_documents_data(
            num_rows=random.randint(1, 5),
            entered_list=cls.patient_ids,
            global_start_year=int(config_populate.global_start_year),
            global_start_month=int(config_populate.global_start_month),
            global_end_year=int(config_populate.global_end_year),
            global_end_month=int(config_populate.global_end_month),
            use_GPT=False,
        )
        df_epr_personal = generate_epr_documents_personal_data(
            num_rows=1,
            entered_list=cls.patient_ids,
            global_start_year=int(config_populate.global_start_year),
            global_start_month=int(config_populate.global_start_month),
            global_end_year=int(config_populate.global_end_year),
            global_end_month=int(config_populate.global_end_month),
        )
        df_epr_merged = pd.merge(
            df_epr,
            df_epr_personal.drop(columns=["updatetime"]),
            on="client_idcode",
            how="left",
        )
        df_epr_merged = df_epr_merged.where(pd.notnull(df_epr_merged), None)
        ingest_data_to_elasticsearch(
            df_epr_merged, "epr_documents", es_client=cls.cs.elastic
        )

        df_basic_obs = generate_basic_observations_data(
            num_rows=random.randint(1, 10),
            entered_list=cls.patient_ids,
            global_start_year=int(config_populate.global_start_year),
            global_start_month=int(config_populate.global_start_month),
            global_end_year=int(config_populate.global_end_year),
            global_end_month=int(config_populate.global_end_month),
        )
        df_basic_all = df_basic_obs.copy()
        for col in df_basic_all.select_dtypes(include=[np.number]).columns:
            df_basic_all[col] = df_basic_all[col].astype(object)
        df_basic_all = df_basic_all.where(pd.notnull(df_basic_all), None)
        ingest_data_to_elasticsearch(
            df_basic_all, "basic_observations", es_client=cls.cs.elastic
        )

        obs_dfs = []
        try:
            obs_dfs.append(
                generate_bmi_data(
                    num_rows=random.randint(1, 5),
                    entered_list=cls.patient_ids,
                    global_start_year=int(config_populate.global_start_year),
                    global_start_month=int(config_populate.global_start_month),
                    global_end_year=int(config_populate.global_end_year),
                    global_end_month=int(config_populate.global_end_month),
                )
            )
        except Exception:
            pass
        try:
            obs_dfs.append(
                generate_news_data(
                    num_rows=random.randint(1, 5),
                    entered_list=cls.patient_ids,
                    global_start_year=int(config_populate.global_start_year),
                    global_start_month=int(config_populate.global_start_month),
                    global_end_year=int(config_populate.global_end_year),
                    global_end_month=int(config_populate.global_end_month),
                )
            )
        except Exception:
            pass
        if obs_dfs:
            df_obs = pd.concat(obs_dfs, ignore_index=True)
            df_obs = df_obs.where(pd.notnull(df_obs), None)
            ingest_data_to_elasticsearch(
                df_obs, "observations", es_client=cls.cs.elastic
            )

        order_dfs = []
        try:
            order_dfs.append(
                generate_drug_orders_data(
                    num_rows=random.randint(1, 5),
                    entered_list=cls.patient_ids,
                    global_start_year=int(config_populate.global_start_year),
                    global_start_month=int(config_populate.global_start_month),
                    global_end_year=int(config_populate.global_end_year),
                    global_end_month=int(config_populate.global_end_month),
                )
            )
        except Exception:
            pass
        try:
            order_dfs.append(
                generate_diagnostic_orders_data(
                    num_rows=random.randint(1, 5),
                    entered_list=cls.patient_ids,
                    global_start_year=int(config_populate.global_start_year),
                    global_start_month=int(config_populate.global_start_month),
                    global_end_year=int(config_populate.global_end_year),
                    global_end_month=int(config_populate.global_end_month),
                )
            )
        except Exception:
            pass
        if order_dfs:
            df_orders = pd.concat(order_dfs, ignore_index=True)
            df_orders = df_orders.where(pd.notnull(df_orders), None)
            ingest_data_to_elasticsearch(df_orders, "order", es_client=cls.cs.elastic)

        cls.cs.elastic.indices.refresh(index=indices, ignore_unavailable=True)

        demo_dfs = []
        for pid in cls.patient_ids:
            df = generate_epr_documents_personal_data(
                num_rows=1,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            demo_dfs.append(df)

        df_demo = (
            pd.concat(demo_dfs, ignore_index=True) if len(demo_dfs) > 1 else demo_dfs[0]
        )
        df_demo = df_demo.where(pd.notnull(df_demo), None)
        ingest_data_to_elasticsearch(df_demo, "epr_documents", es_client=cls.cs.elastic)
        cls.cs.elastic.indices.refresh(index="epr_documents")

        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)
        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"
        cls.logger = setup_logger()

        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.cred_path,
            current_path_dir="",
            main_options={"demo": True},
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
            store_pat_batch_docs=True,
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
            self.config_obj.main_options.get("demo", False) is True
        ), "Demo option should be enabled in config"
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

        # Verify epr_documents index has patient data
        es_count = self.cs.elastic.count(index="epr_documents")["count"]
        assert es_count >= len(
            self.patient_ids,
        ), f"Expected at least {len(self.patient_ids)} documents, got {es_count}"

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

    def test_demo_vector_validation(self):
        """Verify pat_maker produced actual values in the demo feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        feature_cols = [
            c
            for c in all_features.columns
            if any(x in c.lower() for x in ["age", "male", "dead", "census"])
            and c != "client_idcode"
        ]

        assert len(feature_cols) > 0, (
            f"No demo-related columns found in feature vector. "
            f"Available columns: {list(all_features.columns)}"
        )

        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        # At least some demo feature columns must have values
        assert len(totally_empty_cols) < len(feature_cols), (
            f"All demo columns are empty - vectorisation is failing. "
            f"Null columns: {list(totally_empty_cols.index)}"
        )

    def test_demo_expected_values(self):
        """Validate specific expected demographic feature values.

        With random_seed=42, generate_epr_documents_personal_data produces deterministic
        demographic data for patients:
        - Each patient has exactly 1 personal data document
        - The data includes fields like Age, Male/Female gender

        Feature computation creates binary indicators based on this data.
        This test validates that the computed features match expected patterns.

        Expected values (based on dummy data generation):
        - demo_age_* columns should have valid positive numeric values (age)
        - demo_male and demo_female should be 0 or 1 (binary gender indicators)
        """
        all_features = get_all_features(self.config_obj)

        assert not all_features.empty, "Feature DataFrame is empty"

        # Find demo-related feature columns
        feature_cols = [
            c
            for c in all_features.columns
            if any(x in c.lower() for x in ["age", "male", "dead", "census"])
            and c != "client_idcode"
        ]

        assert (
            len(feature_cols) > 0
        ), f"No demo-related columns found. Available: {list(all_features.columns)}"

        # Validate feature values meet expected constraints
        for col in feature_cols:
            values = all_features[col].dropna()
            if len(values) == 0:
                continue

            col_lower = col.lower()

            if "age" in col_lower:
                assert (
                    values > 0
                ).all(), f"Column '{col}' should have positive age values, got min={values.min()}"
            elif "male" in col_lower or "female" in col_lower or "gender" in col_lower:
                int_values = [int(v) for v in values]
                assert all(
                    v in [0, 1] for v in int_values
                ), f"Column '{col}' should have binary values (0 or 1)"

    def test_merge_demo_data_functionality(self):
        """Test merge demo data functionality - verify merge function creates CSV."""
        from pat2vec.util.post_processing_build_methods import merge_demographics_csv

        all_pat_list = self.pat2vec_obj.all_patient_list
        merged_path = merge_demographics_csv(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert os.path.exists(merged_path), f"Merged file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert not merged_data.empty, "Merged demo DataFrame should not be empty"

    def test_demographics_data_retrieval(self):
        """Test demographics data retrieval - verify demographic features can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        demo_data = get_demographics_data(
            self.pat2vec_obj,
            pat_list=all_pat_list[:1],
        )

        assert demo_data is not None, "Demographics data should not be None"
        assert not demo_data.empty, "Demographics DataFrame should not be empty"

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
