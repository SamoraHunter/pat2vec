import os
import random
import shutil
import sys

import numpy as np
import pandas as pd

from pat2vec.main_pat2vec import main
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.docker_elastic import ElasticContainer
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_epic_patients_data,
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger
from pat2vec.util.post_processing_build_methods import merge_epic_patients_csv
from pat2vec.pat2vec_get_methods.get_method_epic_patients import (
    get_epic_patients,
    search_epic_patients,
)
from pat2vec.pat2vec_search.cogstack_search_methods import (
    initialize_cogstack_client,
)
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestEpicPatientsGet:
    """Stage-mirroring pytest for epic patients feature extraction."""

    @classmethod
    def setup_class(cls: type) -> None:
        """Set up shared state for all tests."""
        cls.current_dir = os.getcwd()
        cls.grandparent_dir = os.path.dirname(os.path.dirname(cls.current_dir))

        sys.path.insert(0, os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.append(cls.grandparent_dir)
        cls.pat2vec_dir = os.path.abspath(os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.insert(0, cls.pat2vec_dir)

        cls.PROJ_NAME = "epic_patients_test_project"
        cls.DB_FILENAME = "temp_epic_patients_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)
        cls.creds_filename = "test_elastic_credentials.py"

        for dir_to_remove in ["epic_patients_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}' directory: {e}. "
                "Critical error - cannot start with stale data."
                raise RuntimeError(msg) from e

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

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="epic_patients_test_project",
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
            "epic_patients",
        ]
        cls.cs.elastic.indices.refresh(index=indices, ignore_unavailable=True)

        epic_dfs = []
        for pid in cls.patient_ids:
            df = generate_epic_patients_data(
                num_rows=1,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            epic_dfs.append(df)

        df_epic = (
            pd.concat(epic_dfs, ignore_index=True) if len(epic_dfs) > 1 else epic_dfs[0]
        )
        df_epic = df_epic.where(pd.notnull(df_epic), None)

        ingest_data_to_elasticsearch(df_epic, "epic_patients", es_client=cls.cs.elastic)
        cls.cs.elastic.indices.refresh(index="epic_patients")

        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)

        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"

        cls.logger = setup_logger()

        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.creds_filename,
            current_path_dir="",
            main_options={"epic_patients": True},
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

    @classmethod
    def teardown_class(cls: type) -> None:
        """Clean up after all tests."""
        cls.es_container.stop()

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
            self.config_obj.main_options.get("epic_patients", False) is True
        ), "epic_patients option should be enabled in config"
        assert self.pat2vec_obj is not None, "pat2vec object should not be None"

    def test_3_index_population_and_verification(self):
        """Test index population and verification - verify documents were ingested."""
        indices = [
            "epr_documents",
            "basic_observations",
            "observations",
            "order",
            "pims_apps",
            "epic_patients",
        ]
        for idx in indices:
            try:
                if self.cs.elastic.indices.exists(index=idx):
                    count = self.cs.elastic.count(index=idx)["count"]
                    assert count > 0, f"Index {idx} should have documents"
                else:
                    msg = f"Index not created: {idx}"
                    raise AssertionError(msg)
            except Exception as e:
                msg = f"Error checking index {idx}: {e}"
                raise AssertionError(msg) from e

        epic_count = self.cs.elastic.count(index="epic_patients")["count"]
        expected_epic_count = len(self.patient_ids) * 1
        assert (
            epic_count >= expected_epic_count
        ), f"Expected at least {expected_epic_count} epic patients records, got {epic_count}"

    def test_4_pat2vec_pipeline_execution(self) -> None:
        """Test pat2vec pipeline execution - verify patient was processed."""
        assert self.pat2vec_obj.all_patient_list is not None
        assert len(self.pat2vec_obj.all_patient_list) > 0

    def test_5_feature_extraction(self) -> None:
        """Test feature extraction - verify features were extracted."""
        all_features = get_all_features(self.config_obj)

        assert all_features is not None
        assert not all_features.empty

    def test_epic_patients_data_retrieval(self) -> None:
        """Test epic patients data retrieval - verify epic patients features can be retrieved."""
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0

        pat_batch = pd.DataFrame()

        epic_data = get_epic_patients(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        assert epic_data is not None
        if isinstance(epic_data, list):
            assert len(epic_data) > 0
            assert not epic_data[0].empty
        else:
            assert not epic_data.empty

    def test_merge_epic_patients_data_functionality(self) -> None:
        """Test merge epic patients data functionality - verify merge function exists."""
        from pat2vec.util.post_processing_build_methods import merge_epic_patients_csv

        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        # Verify merge function is available and callable
        merged_path = merge_epic_patients_csv(
            all_pat_list, self.config_obj, overwrite=True
        )
        assert os.path.exists(merged_path), "Merged file should exist"

    def test_8_cleanup_verification(self) -> None:
        """Test cleanup verification - verify all temp files are cleaned up properly."""
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

        assert not os.path.exists(self.DB_PATH)
        assert not os.path.exists(self.PROJ_NAME)
        assert not os.path.exists(self.creds_filename)
