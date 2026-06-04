import unittest
import pandas as pd
from pat2vec.util.anonymisation_data_methods import (
    anonymize_feature_names,
    deanonymize_feature_names,
)


class TestAnonymisationDataMethods(unittest.TestCase):
    def test_anonymize_and_deanonymize(self):
        # Create a sample dataframe with complex names
        data = {
            "client_idcode:P123": [1],
            "census_white": [1],
            "bmi_height_mean": [170],
            "outcome_var_death": [0],
            "unknown_feature": [42],
        }
        df = pd.DataFrame(data)

        # Anonymize
        anon_df, key = anonymize_feature_names(df)

        # Verify names are changed but keep structures where possible
        self.assertIn("client_idcode:concept_0", anon_df.columns)
        self.assertIn("census_concept_1", anon_df.columns)
        self.assertIn("bmi_concept_2_mean", anon_df.columns)
        self.assertIn("outcome_var_concept_3", anon_df.columns)
        # 'unknown_feature' doesn't match predefined prefix/suffix lists
        self.assertIn("feature_4", anon_df.columns)

        # Deanonymize
        recovered_names = deanonymize_feature_names(anon_df.columns.tolist(), key)
        self.assertListEqual(recovered_names, list(data.keys()))

    def test_consistency(self):
        """Ensure the same core concept gets the same anonymized ID."""
        data = {
            "bmi_weight_mean": [70],
            "bmi_weight_median": [70],
        }
        df = pd.DataFrame(data)
        _, key = anonymize_feature_names(df)

        # Find the concept IDs
        concepts = [k for k in key.keys()]
        # Both should share 'weight' as the core concept
        self.assertTrue(concepts[0].split("_")[1] == concepts[1].split("_")[1])

    def test_anonymize_empty_dataframe(self):
        """Test anonymization on an empty DataFrame."""
        df = pd.DataFrame()
        anon_df, key = anonymize_feature_names(df)
        self.assertTrue(anon_df.empty)
        self.assertEqual(len(key), 0)

    def test_deanonymize_with_missing_key(self):
        """Test deanonymization when some columns are not in the key."""
        key = {"concept_0": "known"}
        cols = ["concept_0", "unknown_col"]
        # The implementation returns None for unknown keys and logs a warning
        recovered = deanonymize_feature_names(cols, key)
        self.assertListEqual(recovered, ["known", None])


if __name__ == "__main__":
    unittest.main()
