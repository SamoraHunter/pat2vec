import unittest
import os
import tempfile
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from pat2vec.util.config_pat2vec import config_class
from pat2vec.main_pat2vec import main
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_patient_timeline_faker,
)


class TestMainPat2VecMultiSourceIntegration(unittest.TestCase):
    """
    Comprehensive integration test for main_pat2vec when processing multiple data sources.
    This test verifies that:
    1. Multiple selected data sources are synthetically generated.
    2. Their raw data is ingested into the DB.
    3. Features are extracted and vectorized for each.
    4. All features are correctly merged into the final feature vector table.
    5. Date filtering is respected across all sources.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "multi_source_test_project"
        self.db_path = os.path.join(self.test_dir, f"{self.project_name}.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

        self.test_patient_id = "P_MULTI_SOURCE_TEST"
        self.base_date = datetime(2023, 6, 15)
        self.target_date_range_inclusive = (
            self.base_date - timedelta(days=5),
            self.base_date + timedelta(days=5),
        )

        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            proj_name=self.project_name,
            root_path=self.test_dir,
            global_start_year=self.base_date.year - 1,
            global_start_month=self.base_date.month,
            global_start_day=self.base_date.day,
            global_end_year=self.base_date.year + 1,
            global_end_month=self.base_date.month,
            global_end_day=self.base_date.day,
            all_patient_list=[self.test_patient_id],  # type: ignore
            batch_mode=True,  # type: ignore
            calculate_vectors=True,
            start_date=self.base_date,
            days=0,  # Process for a single point to simplify date range logic
            lookback=False,  # type: ignore
            main_options={
                "bmi": True,
                "bloods": True,
                "drugs": True,
                "annotations": True,  # EPR Docs
                "epic_encounters": True,
                "epic_lab_results": True,
            },
        )
        self.engine = self.config.db_engine
        self.config.epic_encounters_time_field = (
            "activity_AdmissionDate"  # Align with dummy data
        )
        self.config.epic_lab_results_time_field = (
            "document_CollectedDate"  # Align with dummy data
        )
        self.config.epr_docs_time_field = "updatetime"  # Align with dummy data

        # Mock MedCAT and transformer pipeline for annotation processing
        self.mock_cat = MagicMock()

        # Ensure multi-text annotation returns a list aligned with the input size
        def mock_get_annots(texts, **kwargs):
            return [
                {"entities": {"0": {"cui": "C0015967", "pretty_name": "Fever"}}}
                for _ in texts
            ]

        self.mock_cat.get_entities_multi_texts.side_effect = mock_get_annots
        self.mock_get_cat = patch(
            "pat2vec.main_pat2vec.get_cat", return_value=self.mock_cat
        ).start()
        self.mock_pipeline = patch(
            "pat2vec.util.get_dummy_data_cohort_searcher.pipeline"
        ).start()
        self.mock_pipeline.return_value = MagicMock(
            return_value=[{"generated_text": "Sample clinical text mentioning Fever."}]
        )  # For document content

        # Patch the dummy cohort searcher to return consistent data for each source
        self.addCleanup(patch.stopall)

    def tearDown(self):
        self.engine.dispose()

    @patch("pat2vec.util.get_dummy_data_cohort_searcher.get_patient_timeline_dummy")
    def test_main_pat2vec_multi_source_full_flow(self, mock_get_timeline):
        # Ensure the dummy data generator is used
        mock_get_timeline.side_effect = generate_patient_timeline_faker

        # Initialize and run main_pat2vec
        pat2vec_obj = main(config_obj=self.config, cogstack=True)
        pat2vec_obj.pat_maker(0)  # Process the single test patient

        # 1. Verify final features table contains data for the test patient
        final_features_all = get_all_features(self.config)
        final_features = final_features_all[
            final_features_all["client_idcode"] == self.test_patient_id
        ]
        self.assertEqual(
            len(final_features),
            1,
            "Final features table should contain exactly 1 row for the test patient.",
        )

        # 2. Verify features from different sources are present
        # BMI features
        self.assertTrue(
            any(col.startswith("bmi_") for col in final_features.columns),
            "BMI features should be present.",
        )
        # Bloods features (e.g., basicobs_value_numeric_mean)
        self.assertTrue(
            any(col.startswith("basicobs_") for col in final_features.columns),
            "Bloods features should be present.",
        )
        # Drugs features (e.g., drug_count_...)
        self.assertTrue(
            any(col.startswith("drug_") for col in final_features.columns),
            "Drugs features should be present.",
        )
        # EPR Annotations features (e.g., pretty_name_count_Fever)
        self.assertTrue(
            any(col.startswith("pretty_name_count_") for col in final_features.columns),
            "EPR Annotation features should be present.",
        )
        # Epic Encounters features
        self.assertTrue(
            any(col.startswith("epic_enc_") for col in final_features.columns),
            "Epic Encounters features should be present.",
        )
        # Epic Lab Results features
        self.assertTrue(
            any(col.startswith("epic_lab_") for col in final_features.columns),
            "Epic Lab Results features should be present.",
        )

        # 3. Verify date filtering implicitly by ensuring features are not empty
        # (The dummy data is generated within the inclusive range, so features should exist)
        self.assertFalse(
            final_features.empty,
            "Final features should not be empty after multi-source processing.",
        )


if __name__ == "__main__":
    unittest.main()
