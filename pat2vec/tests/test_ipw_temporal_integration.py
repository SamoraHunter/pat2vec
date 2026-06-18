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


class TestIPWTemporalIntegration(unittest.TestCase):
    """
    System-wide integration test for Individual Patient Window (IPW) logic.
    Verifies that when individual anchor dates are provided for patients:
    1. main_pat2vec correctly routes to the IPW logic.
    2. Features are calculated using only data within each patient's relative window.
    3. Data outside the patient-specific windows is correctly ignored.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "ipw_test_project"
        self.db_path = os.path.join(self.test_dir, f"{self.project_name}.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

        # Define two distinct patients with different anchor dates
        self.patient_1 = "P_IPW_ALPHA"
        self.patient_2 = "P_IPW_BETA"

        # Patient 1: Anchor 2022-01-01. Window (1 year duration): 2022-01-01 to 2023-01-01
        self.anchor_1 = datetime(2022, 1, 1)
        # Patient 2: Anchor 2023-01-01. Window (1 year duration): 2023-01-01 to 2024-01-01
        self.anchor_2 = datetime(2023, 1, 1)

        self.ipw_df = pd.DataFrame(
            {
                "client_idcode": [self.patient_1, self.patient_2],
                "anchor_date": [self.anchor_1, self.anchor_2],
            }
        )

        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            proj_name=self.project_name,
            root_path=self.test_dir,
            all_patient_list=[self.patient_1, self.patient_2],
            individual_patient_window=True,
            individual_patient_window_df=self.ipw_df,
            individual_patient_window_start_column_name="anchor_date",
            individual_patient_id_column_name="client_idcode",
            years=1,  # 1 year window duration
            lookback=False,
            batch_mode=True,  # type: ignore
            calculate_vectors=True,
            main_options={
                "bloods": True,
            },
        )
        self.config.bloods_time_field = "updatetime"  # Align with dummy data
        self.engine = self.config.db_engine

    def tearDown(self):
        self.engine.dispose()
        shutil.rmtree(self.test_dir)

    @patch("pat2vec.main_pat2vec.initialize_cogstack_client")
    @patch("pat2vec.main_pat2vec.get_cat")
    def test_ipw_temporal_isolation_lifecycle(self, mock_get_cat, mock_init_cs):
        mock_get_cat.return_value = MagicMock()

        # 1. Ingest raw data for both patients
        # For P1 (Window 2022): 10.0 (Inside), 100.0 (Outside - Early), 200.0 (Outside - Late)
        p1_data = pd.DataFrame(
            [
                {
                    "client_idcode": self.patient_1,
                    "updatetime": "2021-06-01T00:00:00",
                    "basicobs_itemname_analysed": "Glucose",
                    "basicobs_value_numeric": 100.0,
                },
                {
                    "client_idcode": self.patient_1,
                    "updatetime": "2022-06-01T00:00:00",
                    "basicobs_itemname_analysed": "Glucose",
                    "basicobs_value_numeric": 10.0,
                },
                {
                    "client_idcode": self.patient_1,
                    "updatetime": "2023-06-01T00:00:00",
                    "basicobs_itemname_analysed": "Glucose",
                    "basicobs_value_numeric": 200.0,
                },
            ]
        )

        # For P2 (Window 2023): 20.0 (Inside), 100.0 (Outside - Early), 200.0 (Outside - Late)
        p2_data = pd.DataFrame(
            [
                {
                    "client_idcode": self.patient_2,
                    "updatetime": "2022-06-01T00:00:00",
                    "basicobs_itemname_analysed": "Glucose",
                    "basicobs_value_numeric": 100.0,
                },
                {
                    "client_idcode": self.patient_2,
                    "updatetime": "2023-06-01T00:00:00",
                    "basicobs_itemname_analysed": "Glucose",
                    "basicobs_value_numeric": 20.0,
                },
                {
                    "client_idcode": self.patient_2,
                    "updatetime": "2024-06-01T00:00:00",
                    "basicobs_itemname_analysed": "Glucose",
                    "basicobs_value_numeric": 200.0,
                },
            ]
        )

        save_raw_patient_batch(
            pd.concat([p1_data, p2_data]), self.patient_1, "raw_bloods", self.config
        )

        # 2. Run main orchestrator
        pat2vec_obj = main(config_obj=self.config, cogstack=True)
        pat2vec_obj.pat_maker(0)  # Process Patient 1
        pat2vec_obj.pat_maker(1)  # Process Patient 2

        # 3. Retrieve and Verify Final Features
        final_features = get_all_features(self.config)

        # Patient 1 mean should be 10.0 (ignores the 100.0 and 200.0 outliers)
        feat_p1 = final_features[
            final_features["client_idcode"] == self.patient_1
        ].iloc[0]
        self.assertEqual(feat_p1["Glucose_mean"], 10.0)

        # Patient 2 mean should be 20.0 (ignores the 100.0 and 200.0 outliers)
        feat_p2 = final_features[
            final_features["client_idcode"] == self.patient_2
        ].iloc[0]
        self.assertEqual(feat_p2["Glucose_mean"], 20.0)


if __name__ == "__main__":
    unittest.main()
