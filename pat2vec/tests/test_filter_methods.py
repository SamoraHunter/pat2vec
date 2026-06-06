import unittest
from unittest.mock import MagicMock
import pandas as pd
from pat2vec.util.filter_methods import (
    filter_dataframe_by_fuzzy_terms,
    apply_data_type_epr_docs_filters,
    apply_bloods_data_type_filter,
    apply_data_type_mct_docs_filters,
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

    def test_apply_bloods_data_type_filter(self):
        """Test applying data type filters to bloods data."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {
                "bloods": ["Glucose"],
            }
        }

        df = pd.DataFrame(
            {
                "basicobs_itemname_analysed": ["Blood Glucose", "Hemoglobin A1c"],
                "value": [100, 5.5],
            }
        )

        result = apply_bloods_data_type_filter(mock_config, df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["basicobs_itemname_analysed"], "Blood Glucose")

    def test_apply_bloods_data_type_filter_no_filter(self):
        """Test applying bloods filter when no filter is specified."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {"filter_term_lists": {}}

        df = pd.DataFrame(
            {
                "basicobs_itemname_analysed": ["Blood Glucose", "Hemoglobin A1c"],
                "value": [100, 5.5],
            }
        )

        result = apply_bloods_data_type_filter(mock_config, df)
        pd.testing.assert_frame_equal(result, df)

    def test_apply_data_type_mct_docs_filters(self):
        """Test applying data type filters to MCT documents."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {
                "mct_docs": ["Summary"],
                "mct_docs_term_regex": ["cancer"],
            }
        }

        df = pd.DataFrame(
            {
                "document_description": ["Clinical Summary", "Discharge Note"],
                "body_analysed": ["Patient has cancer", "No significant findings"],
            }
        )

        result = apply_data_type_mct_docs_filters(mock_config, df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["document_description"], "Clinical Summary")
        self.assertIn("cancer", result.columns)
        self.assertEqual(result.iloc[0]["cancer"], 1)

    def test_apply_data_type_mct_docs_filters_empty_df(self):
        """Test applying MCT filters to an empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"mct_docs": ["Summary"]}
        }
        df = pd.DataFrame(columns=["document_description", "body_analysed"])

        result = apply_data_type_mct_docs_filters(mock_config, df)
        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
