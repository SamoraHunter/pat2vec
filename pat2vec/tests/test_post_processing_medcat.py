import unittest
import pandas as pd
from pat2vec.util.post_processing_medcat import (
    sample_by_terms,
    coerce_document_df_to_medcat_trainer_input,
)


class TestPostProcessingMedcat(unittest.TestCase):
    def test_sample_by_terms(self):
        df = pd.DataFrame(
            {
                "body_analysed": [
                    "Patient has asthma",
                    "History of hypertension",
                    "Complains of cough",
                    "Asthma symptoms present",
                    "Hypertension diagnosed",
                ]
            }
        )
        term_groups = [["asthma"], ["hypertension"]]

        result = sample_by_terms(
            df,
            column="body_analysed",
            term_groups=term_groups,
            min_samples_per_term=1,
            total_sample_size=4,
        )

        # Should have at least one from each group
        self.assertGreaterEqual(len(result), 2)
        self.assertTrue(
            result["body_analysed"].str.contains("asthma", case=False).any()
        )
        self.assertTrue(
            result["body_analysed"].str.contains("hypertension", case=False).any()
        )

    def test_coerce_trainer_input(self):
        df = pd.DataFrame(
            {
                "_id": ["D1", "D1", "D2"],  # Duplicate IDs
                "body_analysed": ["Text 1", "Text 1 copy", "Text 2"],
            }
        )

        result = coerce_document_df_to_medcat_trainer_input(df)

        # Should have exactly two columns
        self.assertListEqual(list(result.columns), ["name", "text"])
        # Should have made names unique
        self.assertEqual(len(result["name"].unique()), 3)
        self.assertIn("D1_1", result["name"].values)

    def test_coerce_missing_cols(self):
        df = pd.DataFrame({"wrong_col": [1]})
        with self.assertRaises(KeyError):
            coerce_document_df_to_medcat_trainer_input(df)


if __name__ == "__main__":
    unittest.main()
