import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_vte_status import get_vte_status_features
from pat2vec.pat2vec_search.cogstack_search_methods import (
    initialize_cogstack_client,
)
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_vte_status_data,
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestVteStatusGet:
    """Stage-mirroring pytest for test_vte_status_get.ipynb."""

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

        cls.PROJ_NAME = "vte_status_test_project"
        cls.DB_FILENAME = "temp_vte_status_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        # Cleanup previous test outputs
        for dir_to_remove in ["vte_status_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                error_msg = f"Failed to clean up '{dir_to_remove}': {e}"
                raise RuntimeError(error_msg) from e

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="vte_status_test_project",
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

        vte_dfs = []
        for pid in cls.patient_ids:
            df = generate_vte_status_data(
                num_rows=3,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            vte_dfs.append(df)

        df_vte = (
            pd.concat(vte_dfs, ignore_index=True) if len(vte_dfs) > 1 else vte_dfs[0]
        )
        df_vte = df_vte.where(pd.notnull(df_vte), None)
        ingest_data_to_elasticsearch(df_vte, "observations", es_client=cls.cs.elastic)
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
            main_options={"vte_status": True},
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
            self.config_obj.main_options.get("vte_status", False) is True
        ), "VTE status option should be enabled in config"
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

        # Verify VTE status observations were ingested
        es_count = self.cs.elastic.count(index="observations")["count"]
        expected_vte_count = len(self.patient_ids) * 3  # 5 patients * 3 rows each
        assert (
            es_count >= expected_vte_count
        ), f"Expected at least {expected_vte_count} VTE observations, got {es_count}"

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

    def test_vte_status_vector_validation(self):
        """Verify pat_maker produced actual values in the vte_status feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.
        Uses the actual feature engineering logic from get_method_vte_status.py:
        - calculate_vte_features produces: vte_status_mean, vte_status_median,
          vte_status_std, vte_status_max, vte_status_min, vte_status_n (6 columns)
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        feature_cols = [
            c
            for c in all_features.columns
            if c.startswith("vte_status_") and "_date_time_stamp" not in c
        ]

        assert len(feature_cols) > 0, (
            f"No VTE status-related columns found in feature vector. "
            f"Available columns: {list(all_features.columns)}"
        )

        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) < len(feature_cols), (
            f"All VTE status columns are empty - vectorisation is failing. "
            f"Null columns: {list(totally_empty_cols.index)}"
        )

        print(f"Found {len(feature_cols)} VTE status feature columns")

    def test_vte_status_expected_values(self):
        """Validate specific expected VTE status feature values.

        With random_seed=42, generate_vte_data produces deterministic results:
        - 3 observations per patient with values randomly selected from:
          ['High risk of VTE High risk of bleeding', 'High risk of VTE Low risk of bleeding']
        - calculate_vte_features maps these to: 1 and 0 respectively
        - Creates statistical features: mean, median, std, max, min, n

        Expected feature columns for each patient:
        - vte_status_mean: Average of mapped values (between 0 and 1)
        - vte_status_median: Median of mapped values (0 or 1)
        - vte_status_std: Standard deviation
        - vte_status_max: Maximum value (0 or 1)
        - vte_status_min: Minimum value (0 or 1)
        - vte_status_n: Count of non-null observations

        All statistical values should be valid numeric types.

        Note: The count n may be less than 3 if some observations contain NaN values
        due to the use of maybe_nan() in dummy data generation.
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty"

        feature_cols = [
            c
            for c in all_features.columns
            if c.startswith("vte_status_") and "_date_time_stamp" not in c
        ]

        assert (
            len(feature_cols) > 0
        ), f"No VTE status-related columns found. Available: {list(all_features.columns)}"

        for col in feature_cols:
            values = all_features[col].dropna()

            if len(values) == 0:
                continue

            suffix = col.split("_")[-1]

            if suffix == "n":
                assert (
                    values >= 1
                ).all(), (
                    f"{col} should have positive count values, got min={values.min()}"
                )
                int_values = [int(v) for v in values]
                assert all(
                    isinstance(v, int) for v in int_values
                ), f"{col} should be integer type"

            elif suffix == "mean":
                assert (
                    (values >= 0) & (values <= 1)
                ).all(), f"{col} mean should be between 0 and 1, got range [{values.min()}, {values.max()}]"

            elif suffix in {"max", "min"}:
                int_values = [int(v) for v in values]
                assert all(
                    v in [0, 1] for v in int_values
                ), f"{col} should be 0 or 1 (binary), got {[int(v) for v in values]}"

            elif suffix == "std":
                assert (
                    values >= 0
                ).all(), f"{col} std should be non-negative, got min={values.min()}"
                assert all(
                    pd.notna(v)
                    and isinstance(v, (int, float))
                    and not isinstance(v, bool)
                    for v in values
                ), f"{col} should have valid numeric values"

            elif suffix == "median":
                int_values = [int(v) for v in values]
                assert all(
                    v in [0, 1] for v in int_values
                ), f"{col} median should be 0 or 1 (binary), got {[int(v) for v in values]}"

        for patient_id in self.patient_ids:
            row = all_features[all_features["client_idcode"] == patient_id]
            if not row.empty:
                max_val = int(row["vte_status_max"].iloc[0])
                min_val = int(row["vte_status_min"].iloc[0])
                assert (
                    max_val >= min_val
                ), f"Patient {patient_id}: vte_status_max ({max_val}) should be >= vte_status_min ({min_val})"

    def test_vte_status_value_mapping(self):
        """Validate VTE status value mapping produces correct statistical features.

        This test validates that the feature computation correctly maps
        VTE status strings to binary values and computes accurate statistics:

        - 'High risk of VTE High risk of bleeding' -> 1
        - 'High risk of VTE Low risk of bleeding' -> 0

        With random_seed=42, generate_vte_data produces deterministic
        combinations of these two statuses (with possible NaN values).
        This test validates that the computed mean reflects the actual
        proportion of high-risk (value=1) observations.
        """
        all_features = get_all_features(self.config_obj)

        assert (
            "vte_status_mean" in all_features.columns
        ), f"vte_status_mean column missing. Available: {list(all_features.columns)}"
        assert (
            "vte_status_n" in all_features.columns
        ), f"vte_status_n column missing. Available: {list(all_features.columns)}"

        for patient_id in self.patient_ids:
            row = all_features[all_features["client_idcode"] == patient_id]
            if row.empty:
                continue

            mean_val = row["vte_status_mean"].iloc[0]
            n_val = int(row["vte_status_n"].iloc[0])
            max_val = int(row["vte_status_max"].iloc[0])
            min_val = int(row["vte_status_min"].iloc[0])

            assert pd.notna(mean_val), f"{patient_id}: vte_status_mean is null"
            assert (
                0 <= mean_val <= 1
            ), f"{patient_id}: vte_status_mean ({mean_val}) should be between 0 and 1"

            assert n_val >= 1, f"{patient_id}: vte_status_n should be at least 1"

            assert pd.notna(max_val), f"{patient_id}: vte_status_max is null"
            assert pd.notna(min_val), f"{patient_id}: vte_status_min is null"

            assert max_val in [
                0,
                1,
            ], f"{patient_id}: vte_status_max should be 0 or 1 (binary), got {max_val}"
            assert min_val in [
                0,
                1,
            ], f"{patient_id}: vte_status_min should be 0 or 1 (binary), got {min_val}"

            assert (
                max_val >= min_val
            ), f"{patient_id}: vte_status_max ({max_val}) >= vte_status_min ({min_val})"

            if max_val == min_val:
                assert (
                    mean_val == max_val
                ), f"{patient_id}: All values are the same (all {max_val}), so mean should equal max/min"

            expected_proportion = mean_val
            high_risk_count = round(expected_proportion * n_val)

            if high_risk_count == 0:
                assert (
                    min_val == 0
                ), f"{patient_id}: All values should be 0 (low risk), got max={max_val}"
                assert (
                    max_val == 0
                ), f"{patient_id}: All values should be 0 (low risk), got max={max_val}"
            elif high_risk_count == n_val:
                assert (
                    min_val == 1
                ), f"{patient_id}: All values should be 1 (high risk), got min={min_val}"
                assert (
                    max_val == 1
                ), f"{patient_id}: All values should be 1 (high risk), got min={min_val}"

    def test_vte_status_data_retrieval(self):
        """Test VTE status data retrieval - verify VTE status features can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        pat_batch = pd.DataFrame()

        vte_status_data = get_vte_status_features(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        assert vte_status_data is not None, "VTE status data should not be None"
        if isinstance(vte_status_data, list):
            assert len(vte_status_data) > 0, "VTE status data list should not be empty"
            if len(vte_status_data) > 0:
                assert not vte_status_data[
                    0
                ].empty, "VTE status DataFrame should not be empty"
        else:
            assert not vte_status_data.empty, "VTE status DataFrame should not be empty"

    def test_merge_vte_status_csv_functionality(self):
        """Test merge VTE status CSV functionality - verify merge creates CSV."""
        from pat2vec.util.post_processing_build_methods import merge_vte_status_csv

        all_pat_list = self.pat2vec_obj.all_patient_list
        merged_path = merge_vte_status_csv(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert os.path.exists(merged_path), f"Merged file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert not merged_data.empty, "Merged VTE status DataFrame should not be empty"

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
