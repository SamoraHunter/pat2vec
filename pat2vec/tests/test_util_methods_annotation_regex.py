import unittest
import pandas as pd
from pat2vec.util.methods_annotation_regex import append_regex_term_counts


class TestMethodsAnnotationRegex(unittest.TestCase):
    def test_append_regex_term_counts(self):
        df = pd.DataFrame(
            {
                "body_analysed": [
                    "Patient has diabetes and hypertension.",
                    "No significant history.",
                    "Diabetes mentioned twice. Diabetes.",
                ]
            }
        )
        terms = ["diabetes", "hypertension", "asthma"]
        result_df = append_regex_term_counts(df, terms)

        self.assertEqual(result_df.iloc[0]["diabetes"], 1)
        self.assertEqual(result_df.iloc[2]["diabetes"], 2)
        self.assertEqual(result_df.iloc[1]["diabetes"], 0)

    def test_append_regex_term_counts_advanced(self):
        """Test with custom column name and debug flag enabled."""
        df = pd.DataFrame(
            {
                "clinical_notes": [
                    "Patient has fever and dry cough. Fever persistent.",
                    "No symptoms recorded.",
                ]
            }
        )
        terms = ["fever", "cough"]

        # Verify that the debug flag triggers logging
        with self.assertLogs(
            "pat2vec.util.methods_annotation_regex", level="DEBUG"
        ) as cm:
            result_df = append_regex_term_counts(
                df, terms, text_column="clinical_notes", debug=True
            )
            self.assertTrue(
                any("append_regex_term_counts df:" in line for line in cm.output)
            )

        self.assertEqual(result_df.iloc[0]["fever"], 2)
        self.assertEqual(result_df.iloc[0]["cough"], 1)
        self.assertEqual(result_df.iloc[1]["fever"], 0)
        self.assertEqual(result_df.iloc[1]["cough"], 0)

    def test_append_regex_term_counts_complex_patterns(self):
        """Test with complex regex patterns and numeric values."""
        df = pd.DataFrame({"body_analysed": ["Weight is 75kg, height is 180cm"]})
        # Match weight or height patterns
        terms = [r"\d+kg", r"\d+cm"]
        result = append_regex_term_counts(df, terms)
        self.assertEqual(result.iloc[0][r"\d+kg"], 1)
        self.assertEqual(result.iloc[0][r"\d+cm"], 1)
