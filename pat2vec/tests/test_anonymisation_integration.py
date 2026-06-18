import unittest
import pandas as pd
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.helper_functions import save_patient_features, get_all_features
from pat2vec.util.anonymisation_data_methods import (
    anonymize_feature_names,
    deanonymize_feature_names,
)


class TestAnonymisationIntegration(unittest.TestCase):
    """
    Integration test for the Anonymisation lifecycle:
    1. Generate features with identifiable names (e.g., CUI pretty names).
    2. Save features to the database feature table.
    3. Retrieve the full feature set and apply feature name anonymisation.
    4. Verify that data values are preserved but labels are obscured.
    5. Verify that deanonymisation correctly restores the original labels.
    """

    def setUp(self):
        self.test_patient_id = "P_ANON_TEST"
        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            all_patient_list=[self.test_patient_id],
        )
        self.engine = self.config.db_engine

    def tearDown(self):
        self.engine.dispose()

    def test_anonymisation_lifecycle(self):
        # 1. Setup features with identifiable clinical names
        identifiable_features = pd.DataFrame(
            {
                "client_idcode": [self.test_patient_id],
                "pretty_name_count_epr_Asthma": [1.0],
                "pretty_name_count_epr_Diabetes": [0.0],
                "basicobs_Glucose_value_numeric_mean": [5.5],
                "bmi_weight_mean": [75.0],
            }
        )

        # 2. Persist to DB
        save_patient_features(identifiable_features, self.test_patient_id, self.config)

        # 3. Retrieve and Anonymise
        df_from_db = get_all_features(self.config)
        self.assertIn("pretty_name_count_epr_Asthma", df_from_db.columns)

        anon_df, mapping_key = anonymize_feature_names(df_from_db)

        # 4. Verify Anonymisation
        # Identifiable parts should be replaced with generic concept IDs
        self.assertNotIn("pretty_name_count_epr_Asthma", anon_df.columns)
        self.assertNotIn("pretty_name_count_epr_Diabetes", anon_df.columns)

        # Verify data integrity: find which concept mapped to 'Asthma'
        asthma_val = identifiable_features["pretty_name_count_epr_Asthma"].iloc[0]
        asthma_concept_id = next(
            k for k, v in mapping_key.items() if v == "pretty_name_count_epr_Asthma"
        )

        # Construct expected anonymized column name
        expected_anon_col = asthma_concept_id
        self.assertIn(expected_anon_col, anon_df.columns)
        self.assertEqual(anon_df[expected_anon_col].iloc[0], asthma_val)

        # 5. Verify Deanonymisation
        recovered_names = deanonymize_feature_names(
            anon_df.columns.tolist(), mapping_key
        )

        # Ensure all original names are recovered correctly
        for col in identifiable_features.columns:
            self.assertIn(col, recovered_names)

        self.assertEqual(len(recovered_names), len(identifiable_features.columns))


if __name__ == "__main__":
    unittest.main()
