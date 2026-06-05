import unittest
import pandas as pd
from pat2vec.util.post_processing_medcat import (
    sample_by_terms,
    coerce_document_df_to_medcat_trainer_input,
)


class TestPostProcessingMedcat(unittest.TestCase):
    """Unit tests for MedCAT data preparation and sampling utilities."""

    def test_sample_by_terms_logic(self):
        """Test stratified fuzzy sampling logic."""
        df = pd.DataFrame(
            {
                "text": [
                    "Patient has severe asthma",
                    "Chronic Obstructive Pulmonary Disease noted",
                    "Diabetes type 2",
                    "Asthma and cough",
                    "COPD exacerbation",
                    "Healthy patient",
                ]
            }
        )
        # Group 1: Asthma, Group 2: COPD
        term_groups = [["asthma"], ["COPD", "Chronic Obstructive"]]

        # Request min 1 per group, total 4
        result = sample_by_terms(
            df,
            column="text",
            term_groups=term_groups,
            min_samples_per_term=1,
            total_sample_size=4,
            threshold=80,
        )

        # Should find at least 2 asthma and 2 COPD entries
        self.assertGreaterEqual(len(result), 2)
        self.assertIn("matched_term", result.columns)

        # Verify one of the matches
        asthma_match = result[result["matched_term"] == "asthma"]
        self.assertFalse(asthma_match.empty)

    def test_coerce_document_df_to_medcat_trainer_input(self):
        """Test conversion to MedCATTrainer format with duplicate handling."""
        df = pd.DataFrame(
            {
                "doc_id": ["A1", "A1", "B2", "C3"],
                "content": ["text 1", "text 1 copy", "text 2", "text 3"],
            }
        )

        result = coerce_document_df_to_medcat_trainer_input(
            df, text_column_value="content", name_value="doc_id"
        )

        self.assertEqual(len(result), 4)
        self.assertCountEqual(result.columns, ["name", "text"])

        # Verify that the duplicate 'A1' was handled
        names = result["name"].tolist()
        self.assertIn("A1", names)
        self.assertIn("A1_1", names)


if __name__ == "__main__":
    unittest.main()
