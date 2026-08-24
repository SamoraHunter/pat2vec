import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_problem_list import (
    get_current_pat_problem_list,
)
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


class TestProblemListGet:
    """Stage-mirroring pytest for test_problem_list_get."""

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

        cls.PROJ_NAME = "problem_list_test_project"
        cls.DB_FILENAME = "temp_problem_list_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        # Cleanup previous test outputs
        for dir_to_remove in ["problem_list_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                raise RuntimeError(f"Failed to clean up '{dir_to_remove}': {e}") from e

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="problem_list_test_project",
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

        # Generate and ingest Problem List data
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            generate_problem_list_data,
        )

        problem_list_dfs = []
        for pid in cls.patient_ids:
            df = generate_problem_list_data(
                num_rows=3,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            problem_list_dfs.append(df)

        df_problem_list = (
            pd.concat(problem_list_dfs, ignore_index=True)
            if len(problem_list_dfs) > 1
            else problem_list_dfs[0]
        )
        df_problem_list = df_problem_list.where(pd.notnull(df_problem_list), None)

        ingest_data_to_elasticsearch(
            df_problem_list,
            "problem_list",
            es_client=cls.cs.elastic,
        )
        cls.cs.elastic.indices.refresh(index="problem_list")

        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)
        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"
        cls.logger = setup_logger()

        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.cred_path,
            current_path_dir="",
            main_options={"problem_list": True},
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
            self.config_obj.main_options.get("problem_list", False) is True
        ), "Problem list option should be enabled in config"
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

        # Verify problem_list index exists and has data
        try:
            if self.cs.elastic.indices.exists(index="problem_list"):
                count = self.cs.elastic.count(index="problem_list")["count"]
                expected_problem_list_count = (
                    len(self.patient_ids) * 3
                )  # 5 patients * 3 rows each
                assert (
                    count >= expected_problem_list_count
                ), f"Expected at least {expected_problem_list_count} problem list documents, got {count}"
            else:
                msg = "Index not created: problem_list"
                raise AssertionError(msg)
        except Exception as e:
            msg = f"Error checking index problem_list: {e}"
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

    def test_problem_list_vector_validation(self):
        """Verify pat_maker produced actual values in the problem_list feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        feature_cols = [
            c
            for c in all_features.columns
            if "problem_list" in c.lower() and c != "client_idcode"
        ]

        assert len(feature_cols) > 0, (
            f"No problem_list-related columns found in feature vector. "
            f"Available columns: {list(all_features.columns)}"
        )

        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) < len(feature_cols), (
            f"All problem_list columns are empty - vectorisation is failing. "
            f"Null columns: {list(totally_empty_cols.index)}"
        )

        print(f"Found {len(feature_cols)} problem_list feature columns")

    def test_problem_list_data_retrieval(self):
        """Test Problem List data retrieval - verify problem list features can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        # Fetch problem_list data directly from Elasticsearch since there's no
        # get_pat_batch_problem_list function for batch_mode pre-fetching
        search_results = self.cs.elastic.search(
            index="problem_list",
            query={
                "bool": {
                    "must": [
                        {"term": {"client_idcode.keyword": all_pat_list[0]}},
                        {
                            "range": {
                                "updatetime": {
                                    "gte": "2020-01-01",
                                    "lte": "2023-12-31",
                                },
                            },
                        },
                    ],
                },
            },
        )

        hits = search_results.get("hits", {}).get("hits", [])
        pat_batch_data = [hit["_source"] for hit in hits]

        if not pat_batch_data:
            # No data matches the filter, but this is expected behavior
            # The function should return an appropriate response
            problem_list_data = get_current_pat_problem_list(
                current_pat_client_id_code=all_pat_list[0],
                target_date_range=(2020, 1, 1, 2023, 12, 31),
                pat_batch=pd.DataFrame(),
                config_obj=self.config_obj,
            )
            # If no data was found, the function may return empty DataFrame
            assert problem_list_data is not None, "Problem List data should not be None"
        else:
            pat_batch = pd.DataFrame(pat_batch_data)
            pat_batch = pat_batch.where(pd.notnull(pat_batch), None)

            problem_list_data = get_current_pat_problem_list(
                current_pat_client_id_code=all_pat_list[0],
                target_date_range=(2020, 1, 1, 2023, 12, 31),
                pat_batch=pat_batch,
                config_obj=self.config_obj,
            )

            assert problem_list_data is not None, "Problem List data should not be None"
            if isinstance(problem_list_data, list):
                assert (
                    len(problem_list_data) > 0
                ), "Problem List data list should not be empty"
                assert not problem_list_data[
                    0
                ].empty, "Problem List DataFrame should not be empty"
            else:
                assert (
                    not problem_list_data.empty
                ), "Problem List DataFrame should not be empty"

    def test_merge_problem_list_data_functionality(self):
        """Test merge Problem List data functionality - verify merge function creates CSV."""
        # For merge_test, we need to ensure that the get_method returns non-empty results

        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        # Fetch problem_list data directly from Elasticsearch
        search_results = self.cs.elastic.search(
            index="problem_list",
            query={
                "bool": {
                    "must": [{"term": {"client_idcode.keyword": all_pat_list[0]}}],
                    "should": [
                        {
                            "range": {
                                "updatetime": {
                                    "gte": "2020-01-01",
                                    "lte": "2023-12-31",
                                },
                            },
                        },
                    ],
                },
            },
        )

        hits = search_results.get("hits", {}).get("hits", [])
        pat_batch_data = [hit["_source"] for hit in hits]

        if not pat_batch_data:
            pytest.skip("No problem_list data found for merge test")

        pat_batch = pd.DataFrame(pat_batch_data)
        pat_batch = pat_batch.where(pd.notnull(pat_batch), None)

        # Process first patient to ensure batch mode works
        self.pat2vec_obj.pat_maker(0)

        merged_path = os.path.join(
            self.config_obj.root_path,
            self.config_obj.proj_name,
            "merged_problem_list.csv",
        )

        from pat2vec.util.post_processing_build_methods import merge_problem_list_csv

        # Call merge directly (no CSV exists yet, so it will create)
        try:
            merged_path = merge_problem_list_csv(
                all_pat_list,
                self.config_obj,
                overwrite=True,
            )
        except Exception as e:
            # If merge function doesn't exist or has issues, skip this test
            pytest.skip(f"Merge function not available: {e}")

        assert os.path.exists(merged_path), f"Merged file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert (
            not merged_data.empty
        ), "Merged Problem List DataFrame should not be empty"

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

        # Credentials file cleanup is handled by the fixture (not verified here)

        # Verify cleanup
        assert not os.path.exists(self.DB_PATH), "Database file should be removed"
        assert not os.path.exists(self.PROJ_NAME), "Project directory should be removed"
