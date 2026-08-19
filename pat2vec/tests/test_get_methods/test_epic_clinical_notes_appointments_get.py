import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_search.cogstack_search_methods import (
    initialize_cogstack_client,
)
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_epic_clinical_notes_appointments_data,
    populate_elastic_with_dummy_data,
)
from pat2vec.util.helper_functions import get_all_features, get_df_from_db
from pat2vec.util.logger_setup import setup_logger

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestEpicClinicalNotesAppointmentsGet:
    """Stage-mirroring pytest for epic_clinical_notes_appointments tests."""

    @pytest.fixture(autouse=True, scope="class")
    def _start_elastic(self, elastic_container):
        cls = type(self)
        cls.cred_path = elastic_container
        cls.creds_filename = elastic_container

        cls.current_dir = os.getcwd()
        cls.grandparent_dir = os.path.dirname(os.path.dirname(cls.current_dir))

        sys.path.insert(0, os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.append(cls.grandparent_dir)
        cls.pat2vec_dir = os.path.abspath(os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.insert(0, cls.pat2vec_dir)

        cls.PROJ_NAME = "epic_clinical_notes_appointments_test_project"
        cls.DB_FILENAME = "temp_epic_clinical_notes_appointments_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        for dir_to_remove in ["epic_clinical_notes_appointments_test_project"]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}' directory: {e}. "
                "Critical error - cannot start with stale data."
                raise RuntimeError(msg) from e

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="epic_clinical_notes_appointments_test_project",
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

        epic_app_dfs = []
        for pid in cls.patient_ids:
            df = generate_epic_clinical_notes_appointments_data(
                num_rows=3,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            epic_app_dfs.append(df)

        df_epic_app = (
            pd.concat(epic_app_dfs, ignore_index=True)
            if len(epic_app_dfs) > 1
            else epic_app_dfs[0]
        )
        df_epic_app = df_epic_app.where(pd.notnull(df_epic_app), None)

        ingest_data_to_elasticsearch(
            df_epic_app,
            "epic_clinical_notes_appointments",
            es_client=cls.cs.elastic,
        )
        cls.cs.elastic.indices.refresh(index="epic_clinical_notes_appointments")

        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)

        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

        db_connection_string = f"sqlite:///{cls.DB_PATH}"

        cls.logger = setup_logger()

        cls.config_obj = config_class(
            proj_name=cls.PROJ_NAME,
            credentials_path=cls.cred_path,
            current_path_dir="",
            main_options={"epic_clinical_notes_appointments_annotations": True},
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
        assert (
            len(self.patient_ids) == 5
        ), f"Expected 5 patients, got {len(self.patient_ids)}"
        assert all(
            isinstance(pid, str) for pid in self.patient_ids
        ), "All patient IDs should be strings"

    def test_2_config_and_pipeline_setup(self):
        assert self.config_obj is not None, "Config object should not be None"
        assert (
            self.config_obj.main_options.get(
                "epic_clinical_notes_appointments_annotations",
                False,
            )
            is True
        ), "epic_clinical_notes_appointments option should be enabled in config"
        assert self.pat2vec_obj is not None, "pat2vec object should not be None"

    def test_3_index_population_and_verification(self):
        indices = [
            "epr_documents",
            "basic_observations",
            "observations",
            "order",
            "pims_apps",
            "epic_clinical_notes_appointments",
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

        es_count = self.cs.elastic.count(index="epic_clinical_notes_appointments")[
            "count"
        ]
        expected_count = len(self.patient_ids) * 3
        assert (
            es_count >= expected_count
        ), f"Expected at least {expected_count} records, got {es_count}"

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

    def test_epic_clinical_notes_appointments_data_retrieval(self):
        from pat2vec.pat2vec_get_methods.get_method_epic_clinical_notes_appointments_annotations import (
            get_current_pat_epic_clinical_notes_appointments_annotations,
        )

        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        annotations_data = get_df_from_db(
            self.config_obj,
            "annotations",
            "ann_epic_clinical_notes_appointments",
            patient_ids=[all_pat_list[0]],
        )

        assert annotations_data is not None, "Annotations data should not be None"
        assert not annotations_data.empty, "Annotations data should not be empty"

        features_data = get_current_pat_epic_clinical_notes_appointments_annotations(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=(2020, 1, 1, 2023, 12, 31),
            epic_clinical_notes_appointments_annotations=annotations_data,
            config_obj=self.config_obj,
        )

        assert features_data is not None, "Features data should not be None"
        if isinstance(features_data, list):
            assert len(features_data) > 0, "Features data list should not be empty"
            assert not features_data[0].empty, "Features DataFrame should not be empty"
        else:
            assert not features_data.empty, "Features DataFrame should not be empty"

    def test_merge_epic_clinical_notes_appointments_functionality(self):
        all_pat_list = self.pat2vec_obj.all_patient_list

        merged_data = get_all_features(self.config_obj)

        if merged_data.empty:
            error_msg = "merge_epic_clinical_notes_appointments_functionality() returned empty DataFrame"
            raise AssertionError(error_msg)

        output_dir = os.path.join(self.PROJ_NAME, "outputs")
        os.makedirs(output_dir, exist_ok=True)
        merged_path = os.path.join(
            output_dir,
            "merged_epic_clinical_notes_appointments_annotations_data.csv",
        )

        merged_data.to_csv(merged_path, index=False)

        assert os.path.exists(merged_path), f"Merged file should exist at {merged_path}"

        csv_data = pd.read_csv(merged_path)
        assert not csv_data.empty, "CSV file should contain data"

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

        assert not os.path.exists(self.DB_PATH), "Database file should be removed"
        assert not os.path.exists(self.PROJ_NAME), "Project directory should be removed"
