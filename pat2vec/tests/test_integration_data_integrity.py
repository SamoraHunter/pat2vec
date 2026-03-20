import unittest
import shutil
from unittest.mock import patch
import os
import pandas as pd
import tempfile
from pat2vec.util.config_pat2vec import config_class
from pat2vec.main_pat2vec import main
from pat2vec.util.post_processing_build_methods import merge_bmi_csv, merge_news_csv
from pat2vec.util.helper_functions import get_df_from_db
from pat2vec.util.get_dummy_data_cohort_searcher import generate_patient_timeline_faker


class TestIntegrationDataIntegrity(unittest.TestCase):
    """Integration tests to verify data integrity through the pipeline."""

    def setUp(self):
        """Set up the test environment with a temporary directory."""
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "integration_test_project"

        # --- Ephemeral Database Setup ---
        # Using a file-based DB allows for inspection if tests fail
        self.db_path = os.path.join(self.test_dir, f"{self.project_name}.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

        # Initialize config with testing enabled (uses dummy data)
        self.config = config_class(
            testing=True,
            dummy_medcat_model=True,
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            proj_name=self.project_name,
            root_path=self.test_dir,
            verbosity=1,
            main_options={
                "bmi": True,
                "news": True,
                "vte_status": True,
                # Enable all modules to ensure data generation covers them
                "annotations": True,
                "annotations_mrc": True,
                "annotations_reports": True,
                "textual_obs": True,
                "demo": True,
                "bloods": True,
                "drugs": True,
                "diagnostics": True,
                "core_02": True,
                "bed": True,
                "hosp_site": True,
                "core_resus": True,
                "smoking": True,
                "covid": True,
                "appointments": True,
            },
        )

        # Initialize the main orchestrator
        self.pat2vec_obj = main(config_obj=self.config, cogstack=True)

    def tearDown(self):
        """Clean up temporary directories."""
        # Dispose engine to release file locks before deletion
        if hasattr(self.config, "db_engine") and self.config.db_engine:
            self.config.db_engine.dispose()
        shutil.rmtree(self.test_dir)

    @patch("pat2vec.util.get_dummy_data_cohort_searcher.get_patient_timeline_dummy")
    def test_comprehensive_data_generation(self, mock_get_timeline):
        """Test that all enabled data sources are correctly generated and stored."""
        mock_get_timeline.side_effect = generate_patient_timeline_faker

        # 1. Run pat_maker for the first patient in the dummy list
        # This should trigger the dummy data generator for BMI and NEWS
        patient_index = 0
        self.pat2vec_obj.pat_maker(patient_index)

        patient_id = str(self.pat2vec_obj.all_patient_list[patient_index])

        # 2. Verify all raw data tables are populated
        # Map descriptive name to (schema, table_name, id_column)
        tables_to_check = {
            "Demographics": ("raw_data", "raw_demographics", "client_idcode"),
            "BMI": ("raw_data", "raw_bmi", "client_idcode"),
            "Bloods": ("raw_data", "raw_bloods", "client_idcode"),
            "Drugs": ("raw_data", "raw_drugs", "client_idcode"),
            "Diagnostics": ("raw_data", "raw_diagnostics", "client_idcode"),
            "Core O2": ("raw_data", "raw_core_02", "client_idcode"),
            "Bed": ("raw_data", "raw_bed", "client_idcode"),
            "VTE": ("raw_data", "raw_vte", "client_idcode"),
            "Hosp Site": ("raw_data", "raw_hospsite", "client_idcode"),
            "Resus": ("raw_data", "raw_resus", "client_idcode"),
            "NEWS": ("raw_data", "raw_news", "client_idcode"),
            "Smoking": ("raw_data", "raw_smoking", "client_idcode"),
            "COVID": ("raw_data", "raw_covid", "client_idcode"),
            "EPR Docs": ("raw_data", "raw_epr_docs", "client_idcode"),
            "MCT Docs": ("raw_data", "raw_mct_docs", "client_idcode"),
            "Textual Obs": ("raw_data", "raw_textual_obs", "client_idcode"),
            "Reports": ("raw_data", "raw_reports", "client_idcode"),
            "Appointments": ("raw_data", "raw_appointments", "HospitalID"),
        }

        for name, (schema, table, id_col) in tables_to_check.items():
            with self.subTest(data_source=name):
                df = get_df_from_db(
                    self.config,
                    schema,
                    table,
                    patient_ids=[patient_id],
                    patient_id_column=id_col,
                )
                self.assertFalse(
                    df.empty,
                    f"No {name} data found in database table {schema}.{table} for patient {patient_id}",
                )
                # Specific check for NEWS to ensure values are reasonable
                if name == "NEWS":
                    self.assertTrue("observation_valuetext_analysed" in df.columns)
                    # Basic validation that we have values
                    self.assertTrue(len(df) > 0)

        # 2. Run post-processing merge functions
        merged_bmi_path = merge_bmi_csv(
            [patient_id], self.pat2vec_obj.config_obj, overwrite=True
        )
        merged_news_path = merge_news_csv(
            [patient_id], self.pat2vec_obj.config_obj, overwrite=True
        )

        # 3. Assert Data Integrity
        # Read the merged files
        df_bmi = pd.read_csv(merged_bmi_path)
        df_news = pd.read_csv(merged_news_path)

        # Check that files are not empty
        self.assertFalse(
            df_bmi.empty, "Merged BMI DataFrame is empty. Data generation failed."
        )
        self.assertFalse(
            df_news.empty, "Merged NEWS DataFrame is empty. Data generation failed."
        )

        # Optional: Check specific columns to ensure data structure is correct
        self.assertIn("client_idcode", df_bmi.columns)
        self.assertIn("observation_valuetext_analysed", df_bmi.columns)
        self.assertIn("client_idcode", df_news.columns)
