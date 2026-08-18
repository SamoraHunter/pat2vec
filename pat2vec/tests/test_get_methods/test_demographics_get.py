import os
import random
import shutil
import sys

import numpy as np
import pandas as pd

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestDemographicsGet:
    """Stage-mirroring pytest for demographics get method."""

    @classmethod
    def setup_class(cls: type) -> None:
        """Set up shared state for all tests."""
        cls.current_dir = os.getcwd()
        cls.grandparent_dir = os.path.dirname(os.path.dirname(cls.current_dir))

        sys.path.insert(0, os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.append(cls.grandparent_dir)
        cls.pat2vec_dir = os.path.abspath(os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.insert(0, cls.pat2vec_dir)

        cls.PROJ_NAME = "demographics_test_project"
        cls.DB_FILENAME = "temp_demographics_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)
        cls.creds_filename = "test_elastic_credentials.py"

        # Cleanup previous test outputs
        for dir_to_remove in ["demographics_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}' directory: {e}. "
                "Critical error - cannot start with stale data."
                raise RuntimeError(msg) from e

        # Start Elasticsearch container
        from pat2vec.util.docker_elastic import ElasticContainer

        cls.es_container = ElasticContainer()
        cls.es_container.stop()

        if not cls.es_container.start():
            msg = "Failed to start Elasticsearch container. Check if Docker is running."
            raise RuntimeError(msg)

        host, username, password = cls.es_container.get_credentials()

        creds_content = f"""
username = "{username}"
password = "{password}"
api_key = None
hosts = ["{host}"]
"""

        with open(cls.creds_filename, "w") as f:
            f.write(creds_content)

        # Create config for population
        from pat2vec.util.config_pat2vec import config_class

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="demographics_test_project",
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

        # Setup CohStack client for index cleanup BEFORE populating dummy data
        from pat2vec.pat2vec_search.cogstack_search_methods import (
            initialize_cogstack_client,
        )

        cls.cs = initialize_cogstack_client(config_populate)

        # Delete all non-system indices to ensure clean state (including any epic_* indices)
        try:
            indices = cls.cs.elastic.cat.indices(format="json")
            user_indices = [
                i["index"]
                for i in indices
                if not i["index"].startswith(".") and i["index"] != "hidden"
            ]
            if user_indices:
                cls.cs.elastic.indices.delete(index=",".join(user_indices))
                print(f"Deleted existing indices: {user_indices}")
        except Exception:
            pass

        # Populate dummy data
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            populate_elastic_with_dummy_data,
        )

        cls.patient_ids = populate_elastic_with_dummy_data(
            config_populate,
            n_patients=3,
        )

        # Refresh relevant indices to ensure dummy data is searchable
        cls.cs.elastic.indices.refresh(
            index=["epr_documents", "basic_observations", "observations"],
            ignore_unavailable=True,
        )

        # Generate and ingest additional demographics data for precise test control
        from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            generate_epr_documents_personal_data,
        )

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

        ingest_data_to_elasticsearch(
            df_demo.copy(),
            "epr_documents",
            es_client=cls.cs.elastic,
        )
        cls.cs.elastic.indices.refresh(index="epr_documents")

        # Initialize database
        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)

        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"

        from pat2vec.util.logger_setup import setup_logger

        cls.logger = setup_logger()

        # Create main config
        from pat2vec.util.config_pat2vec import config_class

        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.creds_filename,
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
        )

        # Run pat2vec pipeline
        from pat2vec.main_pat2vec import main

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

    @classmethod
    def teardown_class(cls: type) -> None:
        """Clean up after all tests."""
        # Stop Elasticsearch container first
        try:
            if hasattr(cls, "es_container") and cls.es_container is not None:
                cls.es_container.stop()
        except Exception as e:
            print(f"Warning: Failed to stop Elasticsearch container: {e}")

        # Remove database file
        try:
            if os.path.exists(cls.DB_PATH):
                os.remove(cls.DB_PATH)
        except Exception as e:
            print(f"Warning: Failed to remove database file '{cls.DB_PATH}': {e}")

        # Remove project directory
        try:
            if os.path.exists(cls.PROJ_NAME):
                shutil.rmtree(cls.PROJ_NAME, ignore_errors=False)
        except Exception as e:
            msg = f"Failed to remove '{cls.PROJ_NAME}' directory: {e}"
            print(msg)

        # Remove credentials file - do this in teardown_class AFTER all tests complete
        try:
            if os.path.exists(cls.creds_filename):
                os.remove(cls.creds_filename)
        except Exception as e:
            print(
                f"Warning: Failed to remove Elasticsearch credentials file '{cls.creds_filename}': {e}",
            )

    def test_1_dummy_data_generation(self):
        """Test dummy data generation - verify patient IDs were created."""
        assert (
            len(self.patient_ids) == 3
        ), f"Expected 3 patients, got {len(self.patient_ids)}"
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
        indices = ["epr_documents"]
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

        # Verify demographics data was ingested
        es_count = self.cs.elastic.count(index="epr_documents")["count"]
        expected_demo_count = len(self.patient_ids)  # 5 patients * 1 row each
        assert (
            es_count >= expected_demo_count
        ), f"Expected at least {expected_demo_count} demographics records, got {es_count}"

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
        from pat2vec.util.helper_functions import get_all_features

        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "All features should not be None"
        assert not all_features.empty, "Features DataFrame should not be empty"

    def test_demographics_data_retrieval(self):
        """Test demographics data retrieval - verify demographic features can be retrieved."""
        from pat2vec.pat2vec_get_methods.get_method_demographics import get_demo

        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        pat_batch = pd.DataFrame()

        demo_data = get_demo(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        assert demo_data is not None, "Demographics data should not be None"
        if isinstance(demo_data, list):
            assert len(demo_data) > 0, "Demographics data list should not be empty"
            assert not demo_data[0].empty, "Demographics DataFrame should not be empty"
        else:
            assert not demo_data.empty, "Demographics DataFrame should not be empty"

    def test_merge_demographics_data_functionality(self):
        """Test merge demographics data functionality - verify merge function creates CSV."""
        from pat2vec.util.post_processing_build_methods import merge_demographics_csv

        all_pat_list = self.pat2vec_obj.all_patient_list
        merged_path = merge_demographics_csv(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert os.path.exists(merged_path), f"Merged file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert (
            not merged_data.empty
        ), "Merged demographics DataFrame should not be empty"

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

        try:
            if os.path.exists(self.creds_filename):
                os.remove(self.creds_filename)
        except Exception as e:
            msg = f"Failed to remove Elasticsearch credentials file '{self.creds_filename}': {e}"
            raise AssertionError(msg) from e

        # Verify cleanup
        assert not os.path.exists(self.DB_PATH), "Database file should be removed"
        assert not os.path.exists(self.PROJ_NAME), "Project directory should be removed"
        assert not os.path.exists(
            self.creds_filename,
        ), "Credentials file should be removed"
