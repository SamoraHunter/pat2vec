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
