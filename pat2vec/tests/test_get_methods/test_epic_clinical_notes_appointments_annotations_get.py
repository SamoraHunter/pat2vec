import os
import random
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_epic_clinical_notes_appointments_annotations import (
    get_current_pat_epic_clinical_notes_appointments_annotations,
)
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
from pat2vec.util.post_processing_build_methods import (
    build_merged_epr_mct_annot_df,
    build_merged_epr_mct_doc_df,
)

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestEpicClinicalNotesAppointmentsAnnotationsGet:
    """Stage-mirroring pytest for test_epic_clinical_notes_appointments_annotations_get.ipynb."""

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

        cls.PROJ_NAME = "epic_clinical_notes_appointments_annotations_test_project"
        cls.DB_FILENAME = "temp_epic_clinical_notes_appointments_db.sqlite"
        cls.DB_PATH = os.path.join(cls.PROJ_NAME, "outputs", cls.DB_FILENAME)

        for dir_to_remove in [
            "epic_clinical_notes_appointments_annotations_test_project",
        ]:
            try:
                shutil.rmtree(dir_to_remove, ignore_errors=True)
            except Exception as e:
                msg = f"Failed to clean up '{dir_to_remove}' directory: {e}. "
                "Critical error - cannot start with stale data."
                raise RuntimeError(msg) from e

        schema_path = os.path.abspath("test_files/elastic_schemas.json")
        config_populate = config_class(
            proj_name="epic_clinical_notes_appointments_annotations_test_project",
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
            "epic_clinical_notes_appointments",
        ]
        cls.cs.elastic.indices.refresh(index=indices, ignore_unavailable=True)

        notes_dfs = []
        for pid in cls.patient_ids:
            df = generate_epic_clinical_notes_appointments_data(
                num_rows=3,
                entered_list=[pid],
                global_start_year=int(config_populate.global_start_year),
                global_start_month=int(config_populate.global_start_month),
                global_end_year=int(config_populate.global_end_year),
                global_end_month=int(config_populate.global_end_month),
            )
            notes_dfs.append(df)

        df_notes = (
            pd.concat(notes_dfs, ignore_index=True)
            if len(notes_dfs) > 1
            else notes_dfs[0]
        )
        df_notes = df_notes.where(pd.notnull(df_notes), None)

        ingest_data_to_elasticsearch(
            df_notes,
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
            main_options={
                "annotations": True,  # Required to fetch and save EPR docs for merge
                "epic_clinical_notes_appointments_annotations": True,
            },
            batch_mode=True,
            verbosity=0,
            random_seed_val=random_seed_value,
            testing=True,
            testing_elastic=True,
            dummy_medcat_model=True,
            overwrite_stored_pat_docs=True,
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
        ), "Epic clinical notes appointments annotations option should be enabled in config"
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
        expected_notes_count = len(self.patient_ids) * 3
        assert (
            es_count >= expected_notes_count
        ), f"Expected at least {expected_notes_count} clinical notes appointments, got {es_count}"

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

    def test_epic_clinical_notes_appointments_annotations_data_retrieval(self):
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        annotations_data = get_df_from_db(
            self.config_obj,
            "annotations",
            "ann_epic_clinical_notes_appointments",
            patient_ids=[all_pat_list[0]],
        )

        assert annotations_data is not None, "Annotations data should not be None"

        target_date_range = (
            int(self.config_obj.global_start_year),
            int(self.config_obj.global_start_month),
            int(self.config_obj.global_start_day),
            int(self.config_obj.global_end_year),
            int(self.config_obj.global_end_month),
            int(self.config_obj.global_end_day),
        )

        features_data = get_current_pat_epic_clinical_notes_appointments_annotations(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=target_date_range,
            epic_clinical_notes_appointments_annotations=annotations_data,
            config_obj=self.config_obj,
        )

        assert features_data is not None, "Features data should not be None"
        if isinstance(features_data, list):
            assert len(features_data) > 0, "Features data list should not be empty"
            assert not features_data[0].empty, "Features DataFrame should not be empty"
        else:
            assert not features_data.empty, "Features DataFrame should not be empty"

    def test_epic_clinical_notes_appointments_annotations_count_features(self):
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        annotations_data = get_df_from_db(
            self.config_obj,
            "annotations",
            "ann_epic_clinical_notes_appointments",
            patient_ids=all_pat_list[:2],
        )

        target_date_range = (
            int(self.config_obj.global_start_year),
            int(self.config_obj.global_start_month),
            int(self.config_obj.global_start_day),
            int(self.config_obj.global_end_year),
            int(self.config_obj.global_end_month),
            int(self.config_obj.global_end_day),
        )

        features_data = get_current_pat_epic_clinical_notes_appointments_annotations(
            current_pat_client_id_code=all_pat_list[0],
            target_date_range=target_date_range,
            epic_clinical_notes_appointments_annotations=annotations_data,
            config_obj=self.config_obj,
        )

        assert features_data is not None, "Features data should not be None"

    def test_merge_epic_clinical_notes_appointments_annotations_functionality(self):
        """Test annotation merge functionality - verify the post-processing merge

        function merges annotation table results into a single dataframe.
        """
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        merged_path = build_merged_epr_mct_annot_df(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert (
            merged_path is not None
        ), "build_merged_epr_mct_annot_df should return a path"

        assert os.path.exists(
            merged_path,
        ), f"Merged annotations file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert not merged_data.empty, (
            "Merged annotations DataFrame should not be empty — "
            "the pat2vec pipeline should have saved annotation records to the database."
        )

    def test_epic_clinical_notes_appointments_annotations_vector_validation(self):
        """Verify pat_maker produced actual values in the epic_clinical_notes_appointments annotation feature vector.

        Catches the case where vectorisation silently fails — the DataFrame
        has columns but all values are null or empty.

        Epic clinical notes appointments annotations use pattern:
        pretty_name_count_epic_clinical_notes_{pretty_name_value} where pretty_name_value
        is derived from the 'pretty_name' field in the annotations database (ann_epic_clinical_notes_appointments table).
        """
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "get_all_features returned None"
        assert not all_features.empty, "Feature DataFrame is empty — no rows written"

        feature_cols = [
            c
            for c in all_features.columns
            if c.startswith("pretty_name_count_epic_clinical_notes_")
        ]

        assert len(feature_cols) > 0, (
            f"No epic_clinical_notes annotation columns found. "
            f"Available columns: {list(all_features.columns)}"
        )

        feature_data = all_features[feature_cols]
        non_null_counts = feature_data.notna().sum()
        totally_empty_cols = non_null_counts[non_null_counts == 0]

        assert len(totally_empty_cols) < len(feature_cols), (
            f"All epic_clinical_notes annotation columns are empty - vectorisation is failing. "
            f"Null columns: {list(totally_empty_cols.index)}"
        )

        print(
            f"Found {len(feature_cols)} epic_clinical_notes annotation feature columns",
        )

    def test_merge_documents_from_db_functionality(self):
        """Test document merge functionality - verify the post-processing merge

        function extracts all patients' documents from the database (written by
        pat_maker) into a single dataframe.
        """
        all_pat_list = self.pat2vec_obj.all_patient_list
        assert len(all_pat_list) > 0, "Patient list should not be empty"

        merged_path = build_merged_epr_mct_doc_df(
            all_pat_list,
            self.config_obj,
            overwrite=True,
        )

        assert (
            merged_path is not None
        ), "build_merged_epr_mct_doc_df should return a path"

        assert os.path.exists(
            merged_path,
        ), f"Merged documents file should exist at {merged_path}"

        merged_data = pd.read_csv(merged_path)

        assert not merged_data.empty, (
            "Merged documents DataFrame should not be empty — "
            "the pat2vec pipeline should have saved documents to the database."
        )

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
