import unittest
import pandas as pd
import numpy as np
import os
import shutil
import tempfile
from sklearn.preprocessing import StandardScaler

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.helper_functions import save_patient_features, get_all_features
from pat2vec.util.impute_data_for_pipe import mean_impute_dataframe


class TestMLPipelineDatasetIntegration(unittest.TestCase):
    """
    Integration test for the construction of a machine learning dataset:
    1. Generate and persist feature vectors for multiple patients.
    2. Join features with an external labels file (binary classification target).
    3. Handle numeric imputation to resolve missing clinical data points.
    4. Apply standard scaling to normalize feature ranges.
    5. Verify the final data matrix 'D' is correctly structured for model training.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "ml_readiness_project"
        self.db_path = os.path.join(self.test_dir, f"{self.project_name}.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

        # Define cohort: Cases and Controls
        self.all_pats = ["P_CASE_1", "P_CASE_2", "P_CTRL_1", "P_CTRL_2"]
        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            proj_name=self.project_name,
            root_path=self.test_dir,
            all_patient_list=self.all_pats,
        )
        self.config.bloods_time_field = "basicobs_entered"  # Align with dummy data

    def tearDown(self):
        if hasattr(self.config, "db_engine") and self.config.db_engine:
            self.config.db_engine.dispose()
        shutil.rmtree(self.test_dir)

    def test_ml_dataset_construction_lifecycle(self):
        # 1. Setup Synthetic Features in DB
        # Features: Blood Glucose (range 4-10) and BMI (range 20-40)
        # P_CTRL_2 is intentionally missing a numeric observation
        features_df = pd.DataFrame(
            [
                {
                    "client_idcode": "P_CASE_1",
                    "basicobs_Glucose_mean": 6.8,
                    "bmi_mean": 32.5,
                },
                {
                    "client_idcode": "P_CASE_2",
                    "basicobs_Glucose_mean": 7.2,
                    "bmi_mean": 28.1,
                },
                {
                    "client_idcode": "P_CTRL_1",
                    "basicobs_Glucose_mean": 5.1,
                    "bmi_mean": 24.5,
                },
                {
                    "client_idcode": "P_CTRL_2",
                    "basicobs_Glucose_mean": np.nan,
                    "bmi_mean": 22.0,
                },
            ]
        )

        for pid in self.all_pats:
            pat_feat = features_df[features_df["client_idcode"] == pid]
            save_patient_features(pat_feat, pid, self.config)

        # 2. Create Ground Truth Labels File (e.g. Readmission Status)
        labels_df = pd.DataFrame(
            {"client_idcode": self.all_pats, "label": [1, 1, 0, 0]}
        )
        labels_path = os.path.join(self.test_dir, "outcome_labels.csv")
        labels_df.to_csv(labels_path, index=False)

        # 3. Retrieve and Join
        feature_matrix = get_all_features(self.config)

        # Align features with labels based on patient ID
        full_dataset = pd.merge(
            feature_matrix, labels_df, on="client_idcode", how="inner"
        )

        self.assertEqual(len(full_dataset), 4)
        self.assertIn("label", full_dataset.columns)

        # 4. Numeric Imputation
        # P_CTRL_2 is missing Glucose. Impute using mean of others: (6.8 + 7.2 + 5.1) / 3 = 6.36
        imputed_df = mean_impute_dataframe(
            full_dataset.copy(), y_vars=["label", "client_idcode"], random_state=42
        )

        # Verify that no NaNs remain
        self.assertFalse(
            imputed_df.isnull().any().any(),
            "Imputed dataset should have no missing values.",
        )

        # Check specifically imputed value for P_CTRL_2
        ctrl_2_glucose = imputed_df[imputed_df["client_idcode"] == "P_CTRL_2"][
            "basicobs_Glucose_mean"
        ].iloc[0]
        self.assertAlmostEqual(ctrl_2_glucose, 5.95, places=2)

        # 5. Feature Scaling
        # Isolate features from IDs and targets for normalization
        X = imputed_df.drop(columns=["client_idcode", "label"])

        scaler = StandardScaler()
        X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=X.columns)

        # Verify scaling: Means should be close to 0, standard deviation close to 1
        self.assertAlmostEqual(X_scaled["basicobs_Glucose_mean"].mean(), 0.0, places=10)
        self.assertAlmostEqual(
            X_scaled["bmi_mean"].std(), 1.0, delta=0.2
        )  # High delta due to small N

        # 6. Final Readiness
        # Verify the matrix is fully numeric and aligned
        self.assertTrue(
            all(pd.api.types.is_numeric_dtype(X_scaled[c]) for c in X_scaled.columns)
        )
        self.assertEqual(X_scaled.shape, (4, 2))


if __name__ == "__main__":
    unittest.main()
