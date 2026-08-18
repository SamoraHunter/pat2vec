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
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.logger_setup import setup_logger
from pat2vec.pat2vec_get_methods.get_method_news import get_news
from pat2vec.pat2vec_search.cogstack_search_methods import (
    initialize_cogstack_client,
)
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestNEWSGet:
    """Stage-mirroring pytest for test_news_get.ipynb."""

    @classmethod
    def setup_class(cls: type) -> None:
        cls.current_dir = os.getcwd()
        cls.grandparent_dir = os.path.dirname(os.path.dirname(cls.current_dir))

        sys.path.insert(0, os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.append(cls.grandparent_dir)
        cls.pat2vec_dir = os.path.abspath(os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.insert(0, cls.pat2vec_dir)

        cls.PROJ_NAME = "news_test_project"
        cls.DB_FILENAME = "temp_news_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)
        cls.creds_filename = "test_elastic_credentials.py"

        for dir_to_remove in ["news_test_project"]:
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
            proj_name="news_test_project",
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
        ]
        cls.cs.elastic.indices.refresh(index=indices, ignore_unavailable=True)

        news_dfs = []
        search_terms = [
            "NEWS2_Score",
            "NEWS_Systolic_BP",
            "NEWS_Diastolic_BP",
            "NEWS_Respiration_Rate",
            "NEWS_Heart_Rate",
            "NEWS_Oxygen_Saturation",
            "NEWS Temperature",
            "NEWS_AVPU",
        ]
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
                        str(random.uniform(0, 15)) for _ in range(num_rows)
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
            news_dfs.append(combined_df)

        df_news = (
            pd.concat(news_dfs, ignore_index=True) if len(news_dfs) > 1 else news_dfs[0]
        )
        df_news = df_news.where(pd.notnull(df_news), None)

        ingest_data_to_elasticsearch(
            df_news,
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
            credentials_path=cls.creds_filename,
            current_path_dir="",
            main_options={"news": True},
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
        # Stop Elasticsearch container first
        try:
            if hasattr(cls, "es_container") and cls.es_container is not None:
                cls.es_container.stop()
        except Exception as e:
            print(f"Warning: Failed to stop Elasticsearch container: {e}")

        try:
            if os.path.exists(cls.DB_PATH):
                os.remove(cls.DB_PATH)
        except Exception as e:
            print(f"Warning: Failed to remove database file '{cls.DB_PATH}': {e}")

        try:
            if os.path.exists(cls.PROJ_NAME):
                shutil.rmtree(cls.PROJ_NAME, ignore_errors=False)
        except Exception as e:
            msg = f"Failed to remove '{cls.PROJ_NAME}' directory: {e}"
            print(msg)

        try:
            if os.path.exists(cls.creds_filename):
                os.remove(cls.creds_filename)
        except Exception as e:
            print(
                f"Warning: Failed to remove Elasticsearch credentials file '{cls.creds_filename}': {e}",
            )

    def test_1_dummy_data_generation(self):
        assert (
            len(self.patient_ids) == 5
        ), f"Expected 5 patients, got {len(self.patient_ids)}"
        assert all(
            isinstance(pid, str) for pid in self.patient_ids
        ), "All patient IDs should be strings"

    def test_2_config_and_pipeline_setup(self):
        assert self.config_obj is not None, "Config object should not be None"
        assert (
            self.config_obj.main_options.get("news", False) is True
        ), "NEWS option should be enabled in config"
        assert self.pat2vec_obj is not None, "pat2vec object should not be None"

    def test_3_index_population_and_verification(self):
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

        es_count = self.cs.elastic.count(index="observations")["count"]
        expected_news_count = len(self.patient_ids) * 3 * 8
        assert (
            es_count >= expected_news_count
        ), f"Expected at least {expected_news_count} NEWS observations, got {es_count}"

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

    def test_news_data_retrieval(self):
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        pat_batch = pd.DataFrame()

        news_data = get_news(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        assert news_data is not None, "NEWS data should not be None"
        if isinstance(news_data, list):
            assert len(news_data) > 0, "NEWS data list should not be empty"
            assert not news_data[0].empty, "NEWS DataFrame should not be empty"
        else:
            assert not news_data.empty, "NEWS DataFrame should not be empty"

    def test_merge_news_data_functionality(self):
        from pat2vec.util.post_processing_build_methods import merge_news_csv

        all_pat_list = self.pat2vec_obj.all_patient_list
        merged_path = merge_news_csv(all_pat_list, self.config_obj, overwrite=True)

        assert os.path.exists(merged_path), f"Merged file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert not merged_data.empty, "Merged NEWS DataFrame should not be empty"

    def test_8_cleanup_verification(self):
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

        assert not os.path.exists(self.DB_PATH), "Database file should be removed"
        assert not os.path.exists(self.PROJ_NAME), "Project directory should be removed"
        assert not os.path.exists(
            self.creds_filename
        ), "Credentials file should be removed"
