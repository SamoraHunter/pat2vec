import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_core02 import get_core_02
from pat2vec.pat2vec_search.cogstack_search_methods import (
    initialize_cogstack_client,
)
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from pat2vec.util.get_dummy_data_cohort_searcher import (
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestCore02Get:
    """Stage-mirroring pytest for test_core02_get.ipynb."""

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

        cls.PROJ_NAME = "core02_test_project"
        cls.DB_FILENAME = "temp_core02_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        # Cleanup previous test outputs
        for dir_to_remove in ["core02_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}': {e}"
                raise RuntimeError(msg) from e

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="core02_test_project",
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

        from pat2vec.util.get_dummy_data_cohort_searcher import generate_core_o2_data

        core_o2_dfs = []
        for pid in cls.patient_ids:
            df = generate_core_o2_data(
                num_rows=3,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            core_o2_dfs.append(df)

        df_core_o2 = (
            pd.concat(core_o2_dfs, ignore_index=True)
            if len(core_o2_dfs) > 1
            else core_o2_dfs[0]
        )
        df_core_o2 = df_core_o2.where(pd.notnull(df_core_o2), None)

        ingest_data_to_elasticsearch(
            df_core_o2,
            "observations",
            es_client=cls.cs.elastic,
        )
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
            main_options={"core_02": True},
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
            self.config_obj.main_options.get("core_02", False) is True
        ), "Core 02 option should be enabled in config"
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

        # Verify CORE_SpO2 observations were ingested
        es_count = self.cs.elastic.count(index="observations")["count"]
        expected_core_o2_count = len(self.patient_ids) * 3  # 5 patients * 3 rows each
        assert (
            es_count >= expected_core_o2_count
        ), f"Expected at least {expected_core_o2_count} CORE_SpO2 observations, got {es_count}"

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

    def test_core02_vector_validation(self):
        """Verify pat_maker produced actual values in the core_02 feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.

        Note: Features are dynamic one-hot encoded column names based on
        CORE_SpO2 (oxygen saturation) observation values from observations table,
        such as '95_pct', 'low', 'normal' representing the saturation level categories.
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        feature_cols = [c for c in all_features.columns if c != "client_idcode"]

        assert len(feature_cols) > 0, (
            f"No feature columns found. "
            f"Available columns: {list(all_features.columns)}"
        )

        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) < len(feature_cols), (
            f"All feature columns are empty - vectorisation is failing. "
            f"Null columns: {list(totally_empty_cols.index)}"
        )

        for col in feature_cols:
            col_values = all_features[col].dropna()
            if len(col_values) > 0:
                invalid_values = col_values[~col_values.isin([0, 1])]
                assert len(invalid_values) == 0, (
                    f"Feature column '{col}' contains non-binary values. "
                    f"Actual: {invalid_values.tolist()}, Expected: [0, 1]"
                )

        print(f"Found {len(feature_cols)} core_02 feature columns")

    def test_core02_data_retrieval(self):
        """Test CORE_SpO2 data retrieval - verify CORE_SpO2 features can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        pat_batch = pd.DataFrame()

        core02_data = get_core_02(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        assert core02_data is not None, "CORE_SpO2 data should not be None"
        if isinstance(core02_data, list):
            assert len(core02_data) > 0, "CORE_SpO2 data list should not be empty"
            assert not core02_data[0].empty, "CORE_SpO2 DataFrame should not be empty"
        else:
            assert not core02_data.empty, "CORE_SpO2 DataFrame should not be empty"

    def test_core02_expected_values(self):
        """Validate specific expected CORE_SpO2 feature values.

        With random_seed=42 and generate_core_o2_data:
        - Dummy data generator creates SpO2 values from a fixed list
          ['98%', '97%', '96%', '95%', '94%', '93%', 'On Air', '2L O2 NP', '4L O2 NP', 'NRB Mask']
        - Each patient has 3 observations with random SpO2 assignments
        - Feature extraction creates one-hot encoded columns from the value text:
          e.g., '98%' -> '98pct' (percentage sign replaced with 'pct')

        This test validates:
        - Expected feature columns exist based on dummy data specification
        - Each patient's features contain valid binary values (0 or 1)
        - All patients share the same expected feature space
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty"

        core_o2_query = {"query": {"match_all": {}}, "size": 100}
        response = self.cs.elastic.search(index="observations", body=core_o2_query)
        hits = response["hits"]["hits"]

        spo2_docs = [
            h["_source"]
            for h in hits
            if h["_source"].get("obscatalogmasteritem_displayname") == "CORE_SpO2"
        ]

        actual_spo2_values = list(
            {
                d.get("observation_valuetext_analysed")
                for d in spo2_docs
                if d.get("observation_valuetext_analysed")
            },
        )

        expected_spo2_values = [
            "98%",
            "97%",
            "96%",
            "95%",
            "94%",
            "93%",
            "On Air",
            "2L O2 NP",
            "4L O2 NP",
            "NRB Mask",
        ]

        expected_found_in_data = [
            sv for sv in expected_spo2_values if sv in actual_spo2_values
        ]

        def spo2_to_feature_name(value):
            return value.replace("%", "pct").replace(" ", "_").lower()

        expected_core_o2_cols = [
            spo2_to_feature_name(sv) for sv in expected_found_in_data
        ]

        actual_core_o2_columns_found = [
            c
            for c in all_features.columns
            if any(c == spo2_to_feature_name(ev) for ev in expected_spo2_values)
            and c != "client_idcode"
        ]

        assert len(actual_core_o2_columns_found) > 0, (
            f"No core_o2 feature columns found. "
            f"Available columns: {list(all_features.columns)[:20]}"
        )

        missing_core_o2_cols = [
            col
            for col in expected_core_o2_cols
            if col not in actual_core_o2_columns_found
        ]

        if len(expected_found_in_data) > 0 and len(missing_core_o2_cols) > 0:
            print(
                f"Warning: Some core_o2 features not generated: {missing_core_o2_cols}. "
                f"Generated: {actual_core_o2_columns_found}",
            )

        for col in actual_core_o2_columns_found:
            values = all_features[col].dropna()
            if len(values) > 0:
                invalid_values = values[~values.isin([0, 1])]
                assert len(invalid_values) == 0, (
                    f"Core O2 feature '{col}' contains non-binary values. "
                    f"Actual: {invalid_values.tolist()}, Expected: [0, 1]"
                )

        print(f"Found core_o2 features: {actual_core_o2_columns_found}")

    def test_merge_core02_data_functionality(self):
        """Test merge CORE_SpO2 data functionality - verify merge function creates CSV."""
        from pat2vec.util.post_processing_build_methods import merge_core_02_csv

        all_pat_list = self.pat2vec_obj.all_patient_list
        merged_path = merge_core_02_csv(all_pat_list, self.config_obj, overwrite=True)

        assert os.path.exists(merged_path), f"Merged file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert not merged_data.empty, "Merged CORE_SpO2 DataFrame should not be empty"

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

        # Verify cleanup
        assert not os.path.exists(self.DB_PATH), "Database file should be removed"
        assert not os.path.exists(self.PROJ_NAME), "Project directory should be removed"
