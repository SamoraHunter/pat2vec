import unittest
import pandas as pd
import os
import tempfile
import shutil
from unittest.mock import MagicMock, patch
from pat2vec.util.pre_get_drug_treatment_docs import (
    get_treatment_records_by_drug_order_name,
    iterative_drug_treatment_search,
)


class TestPreGetDrugTreatmentDocs(unittest.TestCase):
    """Unit tests for drug treatment record retrieval utilities."""

    def setUp(self):
        """Set up temporary directory and mock project objects."""
        self.test_dir = tempfile.mkdtemp()
        self.output_file = os.path.join(self.test_dir, "drug_results.csv")

        # Mock pat2vec_obj and its nested config_obj
        self.mock_config = MagicMock()
        self.mock_config.global_start_year = "2020"
        self.mock_config.global_start_month = "01"
        self.mock_config.global_start_day = "01"
        self.mock_config.global_end_year = "2021"
        self.mock_config.global_end_month = "01"
        self.mock_config.global_end_day = "01"
        self.mock_config.drug_time_field = "order_entered"
        self.mock_config.testing = True
        self.mock_config.client_idcode_term_name = "client_idcode"

        self.mock_pat2vec = MagicMock()
        self.mock_pat2vec.config_obj = self.mock_config

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    def test_get_treatment_records_input_validation(self):
        """Verify ValueError is raised for null or invalid inputs."""
        with self.assertRaises(ValueError):
            get_treatment_records_by_drug_order_name(None, "aspirin")
        with self.assertRaises(ValueError):
            get_treatment_records_by_drug_order_name(self.mock_pat2vec, 123)

    @patch(
        "pat2vec.util.pre_get_drug_treatment_docs.cohort_searcher_with_terms_and_search_dummy"
    )
    def test_get_treatment_records_fuzzy_filtering(self, mock_dummy_searcher):
        """Test that results are correctly filtered by fuzzy matching scores."""
        # Setup mock return data from the searcher
        mock_data = pd.DataFrame(
            {
                "order_name": [
                    "Aspirin 75mg",
                    "Ibuprofen",
                    "Aspirine",
                ],  # 'Aspirine' is a close fuzzy match
                "order_summaryline": [
                    "low dose aspirin",
                    "not related",
                    "aspirin variant",
                ],
                "order_holdreasontext": [None, None, None],
            }
        )
        mock_dummy_searcher.return_value = mock_data

        # Search for 'Aspirin' with a threshold check (internally hardcoded at 80 in the source)
        result = get_treatment_records_by_drug_order_name(
            self.mock_pat2vec, "Aspirin", column_fields_to_match=["order_name"]
        )

        # Should match "Aspirin 75mg" and "Aspirine"
        self.assertEqual(len(result), 2)
        # Verify the dynamic column name added by the function
        self.assertIn("matched_aspirin", result.columns)

    @patch(
        "pat2vec.util.pre_get_drug_treatment_docs.get_treatment_records_by_drug_order_name"
    )
    def test_iterative_drug_treatment_search_aggregation(self, mock_get_records):
        """Test the iterative search flow, merging, and deduplication."""
        # Mock responses for two different drug terms
        df_aspirin = pd.DataFrame(
            {
                "order_guid": ["G1", "G2"],
                "order_name": ["Aspirin", "Aspirin 75mg"],
                "client_idcode": ["P1", "P2"],
            }
        )
        df_statin = pd.DataFrame(
            {
                "order_guid": ["G1", "G3"],  # G1 is a duplicate across terms
                "order_name": ["Atorvastatin", "Statin"],
                "client_idcode": ["P1", "P3"],
            }
        )

        mock_get_records.side_effect = [df_aspirin, df_statin]

        search_terms = ["Aspirin", "Statin"]

        result = iterative_drug_treatment_search(
            self.mock_pat2vec, search_terms, self.output_file, drop_duplicates=True
        )

        # Total unique guids should be 3 (G1, G2, G3)
        self.assertEqual(len(result), 3)
        self.assertTrue(os.path.exists(self.output_file))

        # Verify that 'searched_term' is aggregated for the duplicate G1
        g1_row = result[result["order_guid"] == "G1"].iloc[0]
        self.assertIn("Aspirin", g1_row["searched_term"])
        self.assertIn("Statin", g1_row["searched_term"])

    @patch(
        "pat2vec.util.pre_get_drug_treatment_docs.get_treatment_records_by_drug_order_name"
    )
    def test_iterative_search_no_results(self, mock_get_records):
        """Test behavior when none of the search terms return data."""
        mock_get_records.return_value = pd.DataFrame()

        result = iterative_drug_treatment_search(
            self.mock_pat2vec, ["missing"], self.output_file
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
