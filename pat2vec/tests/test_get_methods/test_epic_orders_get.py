import os
import random
import sys

import numpy as np
import pandas as pd

from pat2vec.pat2vec_get_methods.get_method_epic_orders import get_epic_orders
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_cohort_searcher import generate_epic_orders_data
from pat2vec.util.helper_functions import (
    get_all_features,
)

random_seed_value = 42

np.random.seed(random_seed_value)
random.seed(random_seed_value)


class TestEpicOrdersGet:
    """Stage-mirroring pytest for epic_orders get method.

    This test follows the same pattern as test_bmi_get.py but focuses on
    testing the get_epic_orders() function which retrieves and processes
    Epic Orders data from a pat_batch DataFrame.

    The key difference from BMI: epic orders uses batch_mode=True but we directly
    pass the raw data in pat_batch rather than loading it through the pipeline.
    """

    @classmethod
    def setup_class(cls: type) -> None:
        """Set up shared state for all tests."""
        cls.current_dir = os.getcwd()
        cls.grandparent_dir = os.path.dirname(os.path.dirname(cls.current_dir))

        sys.path.insert(0, os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.append(cls.grandparent_dir)
        cls.pat2vec_dir = os.path.abspath(os.path.join(cls.grandparent_dir, "pat2vec"))
        sys.path.insert(0, cls.pat2vec_dir)

        base_year = 2023
        base_month = 6

        cls.config_obj = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            global_start_year=base_year - 1,
            global_start_month=base_month,
            global_start_day=15,
            all_patient_list=["P_TEST_EPIC_ORDERS_001", "P_TEST_EPIC_ORDERS_002"],
            batch_mode=True,  # Use batch mode with pat_batch
            main_options={
                "epic_orders": True,
            },
        )

    @classmethod
    def teardown_class(cls: type) -> None:
        """Clean up after all tests."""
        if hasattr(cls.config_obj, "db_engine"):
            cls.config_obj.db_engine.dispose()

    def test_1_dummy_data_generation(
        self,
    ):
        """Test dummy data generation - verify Epic Orders raw data can be created."""
        base_year = 2023
        base_month = 6

        raw_orders_df = generate_epic_orders_data(
            num_rows=3,
            entered_list=["P_TEST_EPIC_ORDERS_001"],
            global_start_year=base_year,
            global_start_month=base_month,
            global_end_year=base_year,
            global_end_month=base_month,
        )

        assert raw_orders_df is not None, "Raw orders DataFrame should not be None"
        assert len(raw_orders_df) == 3, f"Expected 3 rows, got {len(raw_orders_df)}"

    def test_2_config_and_pipeline_setup(
        self,
    ):
        """Test config and pipeline setup - verify configuration was created."""
        assert self.config_obj is not None, "Config object should not be None"
        assert (
            self.config_obj.storage_backend == "database"
        ), "Storage backend should be database"
        assert (
            self.config_obj.batch_mode is True
        ), "Batch mode enabled for epic_orders test"
        assert (
            self.config_obj.main_options.get("epic_orders", False) is True
        ), "Epic Orders option should be enabled in config"

    def test_3_database_ingestion(self):
        """Test database ingestion - verify data can be manually saved to database."""
        raw_orders_df = generate_epic_orders_data(
            num_rows=3,
            entered_list=["P_TEST_EPIC_ORDERS_001"],
            global_start_year=2023,
            global_start_month=6,
            global_end_year=2023,
            global_end_month=6,
        )

        raw_orders_df["document_UpdatedWhen"] = [
            "2023-06-15T10:00:00" for _ in range(len(raw_orders_df))
        ]
        raw_orders_df["client_idcode"] = "P_TEST_EPIC_ORDERS_001"

        from pat2vec.util.helper_functions import save_raw_patient_batch

        save_raw_patient_batch(
            raw_orders_df,
            "P_TEST_EPIC_ORDERS_001",
            "raw_epic_orders",
            self.config_obj,
            id_column="document_PatientDurableKey",
        )

        from pat2vec.util.helper_functions import get_df_from_db

        db_raw_orders = get_df_from_db(
            self.config_obj,
            "raw_data",
            "raw_epic_orders",
            patient_ids=["P_TEST_EPIC_ORDERS_001"],
        )

        assert not db_raw_orders.empty, "Database should contain epic orders data"
        assert (
            len(db_raw_orders) == 3
        ), f"Expected 3 records in DB, got {len(db_raw_orders)}"

    def test_4_pat2vec_pipeline_execution(self):
        """Test pat2vec pipeline execution - verify config is properly set up."""
        all_features = get_all_features(self.config_obj)

        assert all_features is not None, "All features should not be None"
        assert isinstance(all_features, pd.DataFrame), "Features should be a DataFrame"

    def test_5_feature_extraction(self):
        """Test feature extraction - verify Epic Orders features can be extracted from pat_batch."""
        # Generate data and use it as pat_batch
        raw_orders_df = generate_epic_orders_data(
            num_rows=3,
            entered_list=["P_TEST_EPIC_ORDERS_001"],
            global_start_year=2023,
            global_start_month=6,
            global_end_year=2023,
            global_end_month=6,
        )

        raw_orders_df["document_UpdatedWhen"] = [
            "2023-06-15T10:00:00" for _ in range(len(raw_orders_df))
        ]
        raw_orders_df["client_idcode"] = "P_TEST_EPIC_ORDERS_001"

        pat_batch = raw_orders_df

        epic_orders_data = get_epic_orders(
            current_pat_client_id_code="P_TEST_EPIC_ORDERS_001",
            target_date_range=(2023, 6, 15),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        assert epic_orders_data is not None, "Epic Orders data should not be None"
        assert isinstance(epic_orders_data, pd.DataFrame), "Should return a DataFrame"

    def test_epic_orders_data_retrieval(self):
        """Test Epic Orders data retrieval - verify client_idcode and data in pat_batch."""
        raw_orders_df = generate_epic_orders_data(
            num_rows=3,
            entered_list=["P_TEST_EPIC_ORDERS_001"],
            global_start_year=2023,
            global_start_month=6,
            global_end_year=2023,
            global_end_month=6,
        )

        raw_orders_df["document_UpdatedWhen"] = [
            "2023-06-15T10:00:00" for _ in range(len(raw_orders_df))
        ]
        raw_orders_df["client_idcode"] = "P_TEST_EPIC_ORDERS_001"

        pat_batch = raw_orders_df

        epic_orders_data = get_epic_orders(
            current_pat_client_id_code="P_TEST_EPIC_ORDERS_001",
            target_date_range=(2023, 6, 15),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        assert (
            "client_idcode" in epic_orders_data.columns
        ), "DataFrame should have client_idcode"
        assert len(epic_orders_data) == 1, "Should return single patient row"

    def test_epic_orders_features_extraction(self):
        """Test Epic Orders features extraction - verify binary features are created."""
        raw_orders_df = generate_epic_orders_data(
            num_rows=3,
            entered_list=["P_TEST_EPIC_ORDERS_001"],
            global_start_year=2023,
            global_start_month=6,
            global_end_year=2023,
            global_end_month=6,
        )

        raw_orders_df["document_UpdatedWhen"] = [
            "2023-06-15T10:00:00" for _ in range(len(raw_orders_df))
        ]
        raw_orders_df["client_idcode"] = "P_TEST_EPIC_ORDERS_001"

        pat_batch = raw_orders_df

        epic_orders_data = get_epic_orders(
            current_pat_client_id_code="P_TEST_EPIC_ORDERS_001",
            target_date_range=(2023, 6, 15),
            pat_batch=pat_batch,
            config_obj=self.config_obj,
        )

        expected_features = [
            col
            for col in epic_orders_data.columns
            if col.startswith(("epic_order_class_", "epic_order_status_"))
        ]

        assert (
            len(expected_features) > 0
        ), "Should extract binary order class/status features"

    def test_epic_orders_multiple_patients(self):
        """Test Epic Orders retrieval for multiple patients."""
        results = []
        for patient_id in ["P_TEST_EPIC_ORDERS_001", "P_TEST_EPIC_ORDERS_002"]:
            pat_batch = generate_epic_orders_data(
                num_rows=3,
                entered_list=[patient_id],
                global_start_year=2023,
                global_start_month=6,
                global_end_year=2023,
                global_end_month=6,
            )

            pat_batch["document_UpdatedWhen"] = [
                "2023-06-15T10:00:00" for _ in range(len(pat_batch))
            ]
            pat_batch["client_idcode"] = patient_id

            epic_orders_data = get_epic_orders(
                current_pat_client_id_code=patient_id,
                target_date_range=(2023, 6, 15),
                pat_batch=pat_batch,
                config_obj=self.config_obj,
            )
            results.append(epic_orders_data)
            assert (
                epic_orders_data is not None
            ), f"Data should not be None for patient {patient_id}"

    def test_8_cleanup_verification(self):
        """Test cleanup verification - verify database engine can be disposed."""
        db_engine = self.config_obj.db_engine
        assert db_engine is not None, "Database engine should exist"
        assert hasattr(db_engine, "dispose"), "Engine should have dispose method"
