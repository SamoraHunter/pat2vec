import unittest
import pandas as pd
import os
import shutil
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

from pat2vec.util.config_pat2vec import config_class
from pat2vec.main_pat2vec import main
from pat2vec.util.helper_functions import save_raw_patient_batch, get_all_features


class TestLookbackWindowIntegration(unittest.TestCase):
    """
    Integration test for the Lookback Window logic with individual patient windows (IPW).
    Verifies that when lookback=True:
    1. main_pat2vec correctly calculates the window as [Anchor - Duration, Anchor].
    2. Data points *after* the anchor date are excluded from feature calculation.
    3. Data points *before* the lookback start are excluded.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "lookback_test_project"
        self.db_path = os.path.join(self.test_dir, f"{self.project_name}.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

        self.test_patient_id = "P_LOOKBACK_TEST"
        # Anchor Date: 2023-01-01. With 1 year lookback, window is 2022-01-01 to 2023-01-01
        self.anchor_date = datetime(2023, 1, 1)

        self.ipw_df = pd.DataFrame(
            {"client_idcode": [self.test_patient_id], "anchor_date": [self.anchor_date]}
        )

        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            proj_name=self.project_name,
            root_path=self.test_dir,
            all_patient_list=[self.test_patient_id],
            individual_patient_window=True,
            individual_patient_window_df=self.ipw_df,
            individual_patient_window_start_column_name="anchor_date",
            years=1,
            lookback=True,  # Enable lookback logic
            batch_mode=True,
            calculate_vectors=True,  # type: ignore
            main_options={
                "bloods": True,
            },
        )
        self.config.bloods_time_field = "updatetime"
        self.engine = self.config.db_engine

    def tearDown(self):
        self.engine.dispose()
        shutil.rmtree(self.test_dir)

    @patch("pat2vec.main_pat2vec.initialize_cogstack_client")
    @patch("pat2vec.main_pat2vec.get_cat")
    def test_lookback_window_filtering_lifecycle(self, mock_get_cat, mock_init_cs):
        mock_get_cat.return_value = MagicMock()

        # 1. Ingest raw data for the test patient
        # Goal: Window is 2022-01-01 to 2023-01-01
        raw_data = pd.DataFrame(
            [
                # 100.0: Outside - Too early (2021)
                {
                    "client_idcode": self.test_patient_id,
                    "updatetime": "2021-06-01T12:00:00",
                    "basicobs_itemname_analysed": "Sodium",
                    "basicobs_value_numeric": 100.0,
                },
                # 10.0: Inside - Mid window (2022)
                {
                    "client_idcode": self.test_patient_id,
                    "updatetime": "2022-06-01T12:00:00",
                    "basicobs_itemname_analysed": "Sodium",
                    "basicobs_value_numeric": 10.0,
                },
                # 20.0: Inside - Right at the end boundary (2023-01-01 00:00:00)
                {
                    "client_idcode": self.test_patient_id,
                    "updatetime": "2023-01-01T00:00:00",
                    "basicobs_itemname_analysed": "Sodium",
                    "basicobs_value_numeric": 20.0,
                },
                # 200.0: Outside - Too late (2023-01-02)
                {
                    "client_idcode": self.test_patient_id,
                    "updatetime": "2023-01-02T12:00:00",
                    "basicobs_itemname_analysed": "Sodium",
                    "basicobs_value_numeric": 200.0,
                },
            ]
        )

        save_raw_patient_batch(
            raw_data, self.test_patient_id, "raw_bloods", self.config
        )

        # 2. Run main orchestrator
        pat2vec_obj = main(config_obj=self.config, cogstack=True)
        pat2vec_obj.pat_maker(0)

        # 3. Retrieve and Verify Final Features
        final_features = get_all_features(self.config)

        # Diagnostics to investigate why row count is 367 (timeline) vs 1 (summary)
        print(f"\n[DEBUG] final_features shape: {final_features.shape}")
        print(
            f"[DEBUG] columns: {final_features.columns.tolist()[:10]}... (total {len(final_features.columns)})"
        )
        print(
            f"[DEBUG] Patient row count: {len(final_features[final_features['client_idcode'] == self.test_patient_id])}"
        )
        print("[DEBUG] Value counts of client_idcode:")
        print(final_features["client_idcode"].value_counts().head(5))

        # Filter by the patient of interest to avoid interference from other tests sharing the environment
        patient_features = final_features[
            final_features["client_idcode"] == self.test_patient_id
        ]

        # If years=1 and default interval=1 day, we expect a timeline (~367 rows).
        # We assert that data was found and then check the final anchor point.
        self.assertGreaterEqual(
            len(patient_features), 1, "No features were generated for the test patient."
        )

        # Mean should be (10 + 20) / 2 = 15.0
        # It must ignore the 100.0 (too early) and 200.0 (after anchor)
        # iloc[-1] corresponds to the vector generated at the anchor date boundary.
        row = patient_features.iloc[-1]
        self.assertEqual(row["Sodium_mean"], 15.0)

        # Count should be 2
        self.assertEqual(row["Sodium_num-tests"], 2)


if __name__ == "__main__":
    unittest.main()
