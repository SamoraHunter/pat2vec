"""Integration tests for BMI feature extraction pipeline (test_bmi_get.ipynb).

This test file covers all stages from the original notebook:
1. Setup and cleanup
2. Elasticsearch container initialization
3. Dummy data population
4. pat2vec pipeline execution
5. Feature extraction and BMI data retrieval
6. Merge functionality

All tests are non-mocking (real database, real data generation).
"""

import os
import shutil
from datetime import timedelta

import pandas as pd
import pytest

from pat2vec.main_pat2vec import main
from pat2vec.pat2vec_get_methods.get_method_bmi import get_bmi_features
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.docker_elastic import ElasticContainer
from pat2vec.util.dummy_data_generation.observations.bmi import generate_bmi_data
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.helper_functions import (
    get_all_features,
    get_df_from_db,
    save_raw_patient_batch,
)
from pat2vec.util.post_processing_build_methods import merge_bmi_csv


def cleanup_test_artifacts(proj_name: str, creds_filename: str) -> None:
    """Clean up test artifacts including database and project files."""
    db_path = os.path.abspath(os.path.join(proj_name, "outputs", "temp_bmi_db.sqlite"))

    proj_dir = os.path.abspath(proj_name)
    creds_abs = os.path.abspath(creds_filename)

    for path in [db_path, proj_dir, creds_abs]:
        try:
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
        except Exception as e:
            pytest.fail(f"Failed to clean up '{path}': {e}")


class TestBMIIntegration:
    """Test class for BMI feature extraction pipeline."""

    def setup_class(self) -> None:
        """Setup once for all tests in the class."""
        self.proj_name = "bmi_test_integration"
        self.creds_filename = "test_elastic_credentials_bmi_integration.py"

        cleanup_test_artifacts(self.proj_name, self.creds_filename)

        es_container = ElasticContainer()
        es_container.stop()

        result = es_container.start()
        assert result, "Elasticsearch container failed to start"

        host, username, password = es_container.get_credentials()

        creds_content = f"""
username = "{username}"
password = "{password}"
api_key = None
hosts = ["{host}"]
"""

        with open(self.creds_filename, "w") as f:
            f.write(creds_content)

        assert os.path.exists(self.creds_filename), "Credentials file not created"

        self.base_date = pd.Timestamp("2023-06-15")
        self.patient_id = "P_BMI_TEST_001"

    def teardown_class(self) -> None:
        """Teardown after all tests in the class."""
        cleanup_test_artifacts(self.proj_name, self.creds_filename)

    def test_1_dummy_data_generation(self) -> None:
        """Test dummy BMI data generation."""
        raw_bmi_df = generate_bmi_data(
            num_rows=3,
            entered_list=[self.patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
        )

        assert not raw_bmi_df.empty, "Generated BMI data is empty"
        assert len(raw_bmi_df) == 3, f"Expected 3 rows, got {len(raw_bmi_df)}"

    def test_2_config_and_pipeline_setup(self) -> None:
        """Test configuration and pipeline setup."""
        config_obj = config_class(
            proj_name=self.proj_name,
            credentials_path=self.creds_filename,
            current_path_dir="",
            main_options={"bmi": True},
            batch_mode=True,
            verbosity=0,
            testing=True,
            testing_elastic=False,
            storage_backend="database",
            db_connection_string=f"sqlite:///{self.proj_name}/outputs/temp_bmi_db.sqlite",
            all_patient_list=[self.patient_id],
        )

        assert (
            config_obj.storage_backend == "database"
        ), "Storage backend not set correctly"
        assert config_obj.batch_mode is True, "Batch mode not enabled"

    def test_3_data_ingestion_to_database(self) -> None:
        """Test ingesting raw BMI data into database."""
        config_obj = config_class(
            proj_name=self.proj_name,
            credentials_path=self.creds_filename,
            current_path_dir="",
            main_options={"bmi": True},
            batch_mode=True,
            verbosity=0,
            testing=True,
            testing_elastic=False,
            storage_backend="database",
            db_connection_string=f"sqlite:///{self.proj_name}/outputs/temp_bmi_db.sqlite",
            all_patient_list=[self.patient_id],
        )

        raw_bmi_df = generate_bmi_data(
            num_rows=3,
            entered_list=[self.patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
        )

        time_field = getattr(
            config_obj,
            "bmi_time_field",
            "observationdocument_recordeddtm",
        )
        raw_bmi_df[time_field] = [
            (self.base_date + timedelta(days=i - 2)).strftime("%Y-%m-%dT%H:%M:%S")
            for i in range(len(raw_bmi_df))
        ]

        save_raw_patient_batch(raw_bmi_df, self.patient_id, "raw_bmi", config_obj)

        db_raw_bmi = get_df_from_db(
            config_obj,
            "raw_data",
            "raw_bmi",
            patient_ids=[self.patient_id],
        )

        assert not db_raw_bmi.empty, "Raw BMI data not saved to database"
        assert len(db_raw_bmi) == 3, f"Expected 3 rows in DB, got {len(db_raw_bmi)}"

    def test_4_pat2vec_pipeline_execution(self) -> None:
        """Test pat2vec pipeline execution with BMI mode."""
        config_obj = config_class(
            proj_name=self.proj_name,
            credentials_path=self.creds_filename,
            current_path_dir="",
            main_options={"bmi": True},
            batch_mode=True,
            verbosity=0,
            testing=True,
            testing_elastic=False,
            storage_backend="database",
            db_connection_string=f"sqlite:///{self.proj_name}/outputs/temp_bmi_db.sqlite",
            all_patient_list=[self.patient_id],
        )

        raw_bmi_df = generate_bmi_data(
            num_rows=3,
            entered_list=[self.patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
        )

        time_field = getattr(
            config_obj,
            "bmi_time_field",
            "observationdocument_recordeddtm",
        )
        raw_bmi_df[time_field] = [
            (self.base_date + timedelta(days=i - 2)).strftime("%Y-%m-%dT%H:%M:%S")
            for i in range(len(raw_bmi_df))
        ]

        save_raw_patient_batch(raw_bmi_df, self.patient_id, "raw_bmi", config_obj)

        try:
            pat2vec_obj = main(
                cogstack=True,
                use_filter=False,
                json_filter_path=None,
                random_seed_val=42,
                hostname=None,
                config_obj=config_obj,
            )
        except Exception as e:
            pytest.fail(f"Failed to initialize pat2vec: {e}")

        assert pat2vec_obj is not None, "pat2vec object not created"
        assert len(pat2vec_obj.all_patient_list) > 0, "No patients in patient list"

        try:
            pat2vec_obj.pat_maker(0)
        except Exception as e:
            pytest.fail(f"Failed to process patient: {e}")

        features = get_all_features(config_obj)
        assert not features.empty, "Features should be extracted after pat_maker call"

    def test_5_bmi_data_retrieval(self) -> None:
        """Test BMI feature extraction via get_bmi_features."""
        config_obj = config_class(
            proj_name=self.proj_name,
            credentials_path=self.creds_filename,
            current_path_dir="",
            main_options={"bmi": True},
            batch_mode=True,
            verbosity=0,
            testing=True,
            testing_elastic=False,
            storage_backend="database",
            db_connection_string=f"sqlite:///{self.proj_name}/outputs/temp_bmi_db.sqlite",
            all_patient_list=[self.patient_id],
        )

        raw_bmi_df = generate_bmi_data(
            num_rows=3,
            entered_list=[self.patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
        )

        time_field = "observationdocument_recordeddtm"
        raw_bmi_df[time_field] = [
            (self.base_date + timedelta(days=i - 2)).strftime("%Y-%m-%dT%H:%M:%S")
            for i in range(len(raw_bmi_df))
        ]

        save_raw_patient_batch(raw_bmi_df, self.patient_id, "raw_bmi", config_obj)

        pat_batch = get_df_from_db(
            config_obj,
            "raw_data",
            "raw_bmi",
            patient_ids=[self.patient_id],
        )

        target_date_range = (
            self.base_date - timedelta(days=5),
            self.base_date + timedelta(days=5),
        )

        start_year, start_month, end_year, end_month, start_day, end_day = (
            get_start_end_year_month(target_date_range, config_obj=config_obj)
        )

        current_pat_batch = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            time_field,
        )

        bmi_data = get_bmi_features(
            current_pat_client_id_code=self.patient_id,
            target_date_range=target_date_range,
            pat_batch=current_pat_batch,
            config_obj=config_obj,
        )

        assert isinstance(
            bmi_data,
            pd.DataFrame,
        ), "get_bmi_features should return DataFrame"
        assert not bmi_data.empty, "BMI features DataFrame is empty"

    def test_6_merge_bmi_csv_functionality(self) -> None:
        """Test merge_bmi_csv functionality."""
        config_obj = config_class(
            proj_name=self.proj_name,
            credentials_path=self.creds_filename,
            current_path_dir="",
            main_options={"bmi": True},
            batch_mode=True,
            verbosity=0,
            testing=True,
            testing_elastic=False,
            storage_backend="database",
            db_connection_string=f"sqlite:///{self.proj_name}/outputs/temp_bmi_db.sqlite",
            all_patient_list=[self.patient_id],
        )

        raw_bmi_df = generate_bmi_data(
            num_rows=3,
            entered_list=[self.patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
        )

        time_field = "observationdocument_recordeddtm"
        raw_bmi_df[time_field] = [
            (self.base_date + timedelta(days=i - 2)).strftime("%Y-%m-%dT%H:%M:%S")
            for i in range(len(raw_bmi_df))
        ]

        save_raw_patient_batch(raw_bmi_df, self.patient_id, "raw_bmi", config_obj)

        all_pat_list = [self.patient_id]
        merged_path = merge_bmi_csv(all_pat_list, config_obj, overwrite=True)

        assert os.path.exists(merged_path), f"Merged CSV not created at {merged_path}"

        merged_data = pd.read_csv(merged_path)
        assert not merged_data.empty, "Merged BMI data is empty"
