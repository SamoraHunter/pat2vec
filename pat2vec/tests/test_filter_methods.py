import unittest
from unittest.mock import MagicMock
import pandas as pd
from pat2vec.util.filter_methods import (
    filter_dataframe_by_fuzzy_terms,
    apply_data_type_epr_docs_filters,
)


class TestFilterMethods(unittest.TestCase):
    def test_filter_dataframe_by_fuzzy_terms(self):
        """Test filtering with fuzzy matches."""
        df = pd.DataFrame(
            {
                "document_description": [
                    "Discharge Summary",
                    "Clinic Letter",
                    "Random Note",
                ]
            }
        )

        # Search for 'Discharge'
        result = filter_dataframe_by_fuzzy_terms(
            df, ["Discharge"], column_name="document_description"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["document_description"], "Discharge Summary")

    def test_apply_data_type_epr_docs_filters(self):
        """Test applying both fuzzy and regex filters based on config."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {
                "epr_docs": ["Letter"],
                "epr_docs_term_regex": ["asthma"],
            }
        }

        df = pd.DataFrame(
            {
                "document_description": ["Letter 1", "Note 1"],
                "body_analysed": ["Patient has asthma", "Healthy patient"],
            }
        )

        # act
        result = apply_data_type_epr_docs_filters(mock_config, df)

        # Result should only contain the row matching 'Letter'
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["document_description"], "Letter 1")
        # Should also have the regex count column
        self.assertIn("asthma", result.columns)
        self.assertEqual(result.iloc[0]["asthma"], 1)


if __name__ == "__main__":
    unittest.main()
