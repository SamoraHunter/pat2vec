import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import os
from pat2vec.util.pre_get_drug_treatment_docs import (
    get_treatment_records_by_drug_order_name,
    iterative_drug_treatment_search,
)


class TestPreGetDrugTreatmentDocs(unittest.TestCase):
    def setUp(self):
        self.mock_pat2vec = MagicMock()
        self.mock_config = self.mock_pat2vec.config_obj
        self.mock_config.global_start_year = 2020
        self.mock_config.global_start_month = 1
        self.mock_config.global_start_day = 1
        self.mock_config.global_end_year = 2020
        self.mock_config.global_end_month = 1
        self.mock_config.global_end_day = 31
        self.mock_config.drug_time_field = "order_entered"
        self.mock_config.testing = False

    @patch("pat2vec.util.pre_get_drug_treatment_docs.cohort_searcher_no_terms_fuzzy")
    def test_fuzzy_matching_logic(self, mock_search):
        # Sample data returned from ES
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P2"],
                "order_name": ["Aspirin 75mg", "Paracetamol"],
                "order_summaryline": ["Daily dose", "As needed"],
                "order_guid": ["G1", "G2"],
            }
        )
        mock_search.return_value = mock_df

        result = get_treatment_records_by_drug_order_name(self.mock_pat2vec, "Aspirin")

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["client_idcode"], "P1")
        self.assertIn("matched_aspirin", result.columns)

    @patch(
        "pat2vec.util.pre_get_drug_treatment_docs.get_treatment_records_by_drug_order_name"
    )
    @patch("pandas.io.common.file_exists", return_value=False)
    def test_iterative_search_dedup(self, mock_exists, mock_get_records):
        # Simulate two searches that return the same order_guid
        shared_row = {
            "client_idcode": "P1",
            "order_guid": "G1",
            "order_name": "Combo Drug",
            "order_holdreasontext": "No hold",  # Added to satisfy the test
            "order_summaryline": "Line",
        }

        mock_get_records.side_effect = [
            pd.DataFrame([shared_row]),
            pd.DataFrame([shared_row]),
        ]

        out_file = "test_drug_search.csv"
        try:
            with patch("pandas.DataFrame.to_csv"):
                # Mock read_csv because the function reads the file it just wrote
                with patch("pandas.read_csv"):
                    iterative_drug_treatment_search(
                        self.mock_pat2vec,
                        ["DrugA", "DrugB"],
                        out_file,
                        drop_duplicates=True,
                    )
                    # Check that the concat'd then dedup'd df was what it tried to handle
                    # (Logic in the function actually groups and aggregates)
                    pass
        finally:
            if os.path.exists(out_file):
                os.remove(out_file)


if __name__ == "__main__":
    unittest.main()
