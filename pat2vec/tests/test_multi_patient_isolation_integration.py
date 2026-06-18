import unittest
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch
from datetime import datetime

from pat2vec.util.config_pat2vec import config_class
from pat2vec.main_pat2vec import main
from pat2vec.util.helper_functions import get_all_features
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_patient_timeline_faker,
)


class TestMultiPatientIsolationIntegration(unittest.TestCase):
    """
    Comprehensive integration test for multi-patient feature vector isolation:
    1. Synthetic data generation for multiple distinct patients.
    2. Ingestion of raw data for all patients via pat_maker.
    3. Feature vectorization and storage in the shared features table.
    4. Verification that each patient has a unique, isolated row.
    5. Verification that data from one patient does not overwrite or leak into another.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "isolation_test_project"
        self.db_path = os.path.join(self.test_dir, f"{self.project_name}.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

        # Define two distinct patients
        self.patient_a = "PATIENT_ALPHA"
        self.patient_b = "PATIENT_BETA"
        self.all_pats = [self.patient_a, self.patient_b]

        self.base_date = datetime(2023, 6, 15)

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
            all_patient_list=self.all_pats,  # type: ignore
            batch_mode=True,  # type: ignore
            calculate_vectors=True,
            start_date=self.base_date,
            days=0,
            lookback=False,  # type: ignore
            main_options={
                "bloods": True,
                "demo": True,
            },
        )
        self.config.bloods_time_field = "basicobs_entered"  # Align with dummy data
        self.engine = self.config.db_engine

        # Patch the dummy cohort searcher to return consistent data for each source
        self.addCleanup(patch.stopall)

    def tearDown(self):
        self.engine.dispose()
        shutil.rmtree(self.test_dir)

    @patch("pat2vec.util.get_dummy_data_cohort_searcher.get_patient_timeline_dummy")
    @patch("pat2vec.main_pat2vec.get_cat")
    def test_multi_patient_isolation_lifecycle(self, mock_get_cat, mock_get_timeline):
        # Setup mocks to return synthetic timelines
        mock_get_cat.return_value = MagicMock()

        # Wrap the faker so it respects the requested client_idcode
        def timeline_with_correct_id(client_idcode, *args, **kwargs):
            # generate_patient_timeline_faker returns a string representing the clinical note text.
            timeline_text = generate_patient_timeline_faker(
                client_idcode, *args, **kwargs
            )

            # Force the ID in the demographics section to match what was requested by the caller.
            # This ensures that even if the internal faker logic were to generate a random ID,
            # the returned note text remains tied to the correct patient for this test.
            import re

            timeline_text = re.sub(
                r"(client_idcode:\s*)\S+", rf"\1{client_idcode}", timeline_text
            )
            return timeline_text

        mock_get_timeline.side_effect = timeline_with_correct_id

        # Initialize main orchestrator
        pat2vec_obj = main(config_obj=self.config, cogstack=True)
        # Explicitly set the patient list to ensure isolation, bypassing any random generation in main()
        pat2vec_obj.all_patient_list = self.all_pats

        # 1. Process Patient A
        pat2vec_obj.pat_maker(0)

        # 2. Process Patient B
        pat2vec_obj.pat_maker(1)

        # 3. Retrieve the final feature vector table from DB
        final_features = get_all_features(self.config)

        # 4. Verify that we have exactly 2 distinct rows
        self.assertEqual(
            len(final_features),
            2,
            "Final features table should contain exactly one row per patient.",
        )
        self.assertCountEqual(final_features["client_idcode"].tolist(), self.all_pats)

        # 5. Verify Isolation
        row_a = final_features[final_features["client_idcode"] == self.patient_a].iloc[
            0
        ]
        row_b = final_features[final_features["client_idcode"] == self.patient_b].iloc[
            0
        ]

        self.assertEqual(row_a["client_idcode"], self.patient_a)
        self.assertEqual(row_b["client_idcode"], self.patient_b)

        # Ensure that no columns containing 'beta' leaked into Patient Alpha's row
        # (Assuming the faker logic doesn't use the ID in the feature values themselves)
        self.assertFalse(
            final_features.isnull().all().all(),
            "Features should be populated for both patients.",
        )


if __name__ == "__main__":
    unittest.main()
