import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_ascribe_translog import (
    get_ascribe_translog,
)
from pat2vec.pat2vec_search.cogstack_search_methods import initialize_cogstack_client
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_ascribe_translog_data,
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger
from pat2vec.util.post_processing_build_methods import merge_ascribe_translog_csv

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestAscribeTranslogGet:
    """Stage-mirroring pytest for ascribe_translog feature extraction."""

    @pytest.fixture(autouse=True, scope="class")
    def _start_elastic(self, elastic_container, tmp_path_factory):
        cls = type(self)
        cls.cred_path = elastic_container
        cls.creds_filename = elastic_container

        cls.current_dir = os.getcwd()
        cls.grandparent_dir = os.path.dirname(os.path.dirname(cls.current_dir))

        sys.path.insert(0, os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.append(cls.grandparent_dir)
        cls.pat2vec_dir = os.path.abspath(os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.insert(0, cls.pat2vec_dir)

        cls.TEMP_DIR = str(tmp_path_factory.mktemp("ascribe_translog_test_project"))

        cls.PROJ_NAME = "ascribe_translog_test_project"
        cls.DB_FILENAME = "temp_ascribe_translog_db.sqlite"
        cls.DB_PATH = os.path.join(
            cls.TEMP_DIR,
            cls.PROJ_NAME,
            "outputs",
            cls.DB_FILENAME,
        )

        temp_proj_dir_populate = os.path.join(cls.TEMP_DIR, cls.PROJ_NAME)
        os.makedirs(temp_proj_dir_populate, exist_ok=True)

        schema_path = os.path.abspath("test_files/elastic_schemas.json")

        # Generate hospital casenumber-style patient IDs (hospital numbers)
        cls.patient_ids = [f"CASE_{random.randint(100000, 999999)}" for _ in range(5)]

        config_populate = config_class(
            proj_name="ascribe_translog_test_project",
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

        # Set patient IDs as casenumbers (hospital numbers) for ascribe_translog data matching
        config_populate.all_patient_list = cls.patient_ids

        populate_elastic_with_dummy_data(
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
            "ascribe_translog",
        ]
        cls.cs.elastic.indices.refresh(index=indices, ignore_unavailable=True)

        ascribe_dfs = []
        for pid in cls.patient_ids:
            df = generate_ascribe_translog_data(
                num_rows=3,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            ascribe_dfs.append(df)

        df_ascribe = (
            pd.concat(ascribe_dfs, ignore_index=True)
            if len(ascribe_dfs) > 1
            else ascribe_dfs[0]
        )
        df_ascribe = df_ascribe.where(pd.notnull(df_ascribe), None)

        ingest_data_to_elasticsearch(
            df_ascribe,
            "ascribe_translog",
            es_client=cls.cs.elastic,
        )
        cls.cs.elastic.indices.refresh(index="ascribe_translog")

        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)
        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"
        cls.logger = setup_logger()

        temp_proj_dir_main = os.path.join(cls.TEMP_DIR, cls.PROJ_NAME)

        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.cred_path,
            root_path=temp_proj_dir_main,
            main_options={"ascribe_translog": True},
            batch_mode=True,
            verbosity=0,
            random_seed_val=random_seed_value,
            testing=True,
            testing_elastic=True,
            dummy_medcat_model=True,
            use_controls=False,
            medcat=False,
            start_time=None,
            patient_id_column_name="casenumber",
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
        assert (
            len(self.patient_ids) == 5
        ), f"Expected 5 patients, got {len(self.patient_ids)}"
        assert all(
            isinstance(pid, str) and pid.startswith("CASE_") for pid in self.patient_ids
        ), "All patient IDs should be casenumber (hospital number) strings"

    def test_2_config_and_pipeline_setup(self):
        print(f"\nDEBUG TEST 2: main_options = {self.config_obj.main_options}")
        print(
            f"DEBUG TEST 2: ascribe_translog value = {self.config_obj.main_options.get('ascribe_translog', 'NOT FOUND')}",
        )
        assert self.config_obj is not None, "Config object should not be None"
        assert (
            self.config_obj.main_options.get("ascribe_translog", False) is True
        ), f"Ascribe translog option should be enabled in config. Got: {self.config_obj.main_options}"
        assert self.pat2vec_obj is not None, "pat2vec object should not be None"

    def test_3_index_population_and_verification(self):
        indices = [
            "epr_documents",
            "basic_observations",
            "observations",
            "order",
            "pims_apps",
            "ascribe_translog",
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

        es_count = self.cs.elastic.count(index="ascribe_translog")["count"]
        expected_count = len(self.patient_ids) * 3
        assert (
            es_count >= expected_count
        ), f"Expected at least {expected_count} ascribe translog records, got {es_count}"

    def test_4_pat2vec_pipeline_execution(self):
        assert (
            self.pat2vec_obj.all_patient_list is not None
        ), "Patient list should not be None"
        assert (
            len(self.pat2vec_obj.all_patient_list) > 0
        ), "Patient list should have patients"

    def test_5_feature_extraction(self):
        all_features = get_all_features(self.config_obj)
        assert all_features is not None, "All features should not be None"
        assert not all_features.empty, "Features DataFrame should not be empty"

    def test_ascribe_translog_vector_validation(self):
        """Verify pat_maker produced actual values in the ascribe translog feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        feature_cols = [
            c
            for c in all_features.columns
            if any(
                c.startswith(prefix)
                for prefix in [
                    "ascribe_translog_description_",
                    "ascribe_translog_kind_",
                    "ascribe_translog_ward_",
                    "ascribe_translog_consultant_",
                    "ascribe_translog_specialty_",
                    "ascribe_translog_transtype_",
                    "ascribe_translog_storesdescription_",
                    "ascribe_translog_directioncode_",
                    "ascribe_translog_pack_quantity_",
                ]
            )
        ]

        assert len(feature_cols) > 0, (
            f"No ascribe_translog-related columns found in feature vector. "
            f"Available columns: {list(all_features.columns)}"
        )

        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) < len(feature_cols), (
            f"Ascribe translog columns are empty - vectorisation is failing. "
            f"Null columns: {list(totally_empty_cols.index)}"
        )

        print(f"Found {len(feature_cols)} ascribe_translog feature columns")

    def test_ascribe_translog_expected_values(self):
        """Validate specific expected ascribe translog feature values.

        With random_seed=42, generate_ascribe_translog_data produces deterministic results:
        - 3 records per patient with random description, kind, ward, consultant, specialty, transtype
        - Feature computation creates one-hot encoded binary indicators

        All binary feature values should be valid integers (0 or 1).
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty"

        feature_cols = [
            c
            for c in all_features.columns
            if any(
                c.startswith(prefix)
                for prefix in [
                    "ascribe_translog_description_",
                    "ascribe_translog_kind_",
                    "ascribe_translog_ward_",
                    "ascribe_translog_consultant_",
                    "ascribe_translog_specialty_",
                    "ascribe_translog_transtype_",
                    "ascribe_translog_storesdescription_",
                    "ascribe_translog_directioncode_",
                    "ascribe_translog_pack_quantity_",
                ]
            )
        ]

        assert (
            len(feature_cols) > 0
        ), f"No ascribe translog-related columns found. Available: {list(all_features.columns)}"

        for patient_id in self.patient_ids:
            row = all_features[all_features["client_idcode"] == patient_id]
            if row.empty:
                continue

            for col in feature_cols:
                val = row[col].iloc[0]

                assert pd.notna(
                    val,
                ), f"{patient_id}: {col} should have a value, got null"
                int_val = int(val)

                assert int_val in [
                    0,
                    1,
                ], f"{patient_id}: {col} should be binary (0 or 1), got {int_val}"

        non_zero_cols = []
        for col in feature_cols:
            if all_features[col].sum() > 0:
                non_zero_cols.append(col)

        assert len(non_zero_cols) > 0, (
            f"Ascribe translog feature columns are zero - no features were detected. "
            f"Total columns: {len(feature_cols)}"
        )

    def test_ascribe_translog_data_retrieval(self):
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"
        pat_batch = pd.DataFrame()
        ascribe_translog_data = get_ascribe_translog(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )
        assert (
            ascribe_translog_data is not None
        ), "Ascribe translog data should not be None"
        if isinstance(ascribe_translog_data, list):
            assert (
                len(ascribe_translog_data) > 0
            ), "Ascribe translog data list should not be empty"
            assert not ascribe_translog_data[
                0
            ].empty, "Ascribe translog DataFrame should not be empty"
        else:
            assert (
                not ascribe_translog_data.empty
            ), "Ascribe translog DataFrame should not be empty"

    def test_merge_ascribe_translog_functionality(self):
        all_pat_list = self.pat2vec_obj.all_patient_list
        merged_path = merge_ascribe_translog_csv(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert os.path.exists(merged_path), f"Merged file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert (
            not merged_data.empty
        ), "Merged ascribe translog DataFrame should not be empty"
        assert len(merged_data.columns) > 0, "Merged data should have columns"

    def test_8_cleanup_verification(self):
        try:
            if os.path.exists(self.DB_PATH):
                os.remove(self.DB_PATH)
        except Exception as e:
            msg = f"Failed to remove database file '{self.DB_PATH}': {e}"
            raise AssertionError(msg) from e

        try:
            if os.path.exists(self.TEMP_DIR):
                shutil.rmtree(self.TEMP_DIR, ignore_errors=False)
        except Exception as e:
            msg = f"Failed to remove '{self.TEMP_DIR}' directory: {e}"
            raise AssertionError(msg) from e

        assert not os.path.exists(self.DB_PATH), "Database file should be removed"
        assert not os.path.exists(self.TEMP_DIR), "Project directory should be removed"
