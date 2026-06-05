import unittest
import pandas as pd
from unittest.mock import MagicMock, patch
from pat2vec.pat2vec_get_methods.get_method_demo import get_demographics_data


class TestGetMethodDemo(unittest.TestCase):
    """Unit tests for the get_method_demo module."""

    def setUp(self):
        """Set up mock pat2vec_obj and config_obj for tests."""
        self.mock_config = MagicMock()
        self.mock_config.global_start_year = "2020"
        self.mock_config.global_start_month = "01"
        self.mock_config.global_start_day = "01"
        self.mock_config.global_end_year = "2021"
        self.mock_config.global_end_month = "01"
        self.mock_config.global_end_day = "01"
        self.mock_config.client_idcode_term_name = "client_idcode"
        self.mock_config.verbosity = 0
        self.mock_config.ethnicity_column = "client_racecode"
        self.mock_config.ethnicity_abstractor_enabled = True

        self.mock_pat2vec_obj = MagicMock()
        self.mock_pat2vec_obj.config_obj = self.mock_config

        self.pat_list = ["P1", "P2"]

    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.cohort_searcher_with_terms_and_search"
    )
    @patch("pat2vec.pat2vec_get_methods.get_method_demo.demo_to_latest")
    @patch("pat2vec.pat2vec_get_methods.get_method_demo.calculate_age_append")
    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.EthnicityAbstractor.abstractEthnicity"
    )
    def test_get_demographics_data_success(
        self,
        mock_abstract_ethnicity,
        mock_calculate_age,
        mock_demo_to_latest,
        mock_cohort_searcher,
    ):
        """Test successful retrieval and processing of demographic data."""
        # Mock searcher to return some data
        mock_search_df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P1", "P2"],
                "client_dob": ["1990-01-01", "1990-01-01", "1985-05-10"],
                "updatetime": ["2020-01-01", "2020-06-01", "2020-03-15"],
                "client_racecode": ["White", "White", "Asian"],
            }
        )
        mock_cohort_searcher.return_value = mock_search_df

        # Mock intermediate processing steps
        mock_demo_to_latest.return_value = mock_search_df.drop_duplicates(
            subset=["client_idcode"], keep="last"
        )
        mock_calculate_age.return_value = mock_search_df.assign(age=[30, 30, 35])
        mock_abstract_ethnicity.return_value = mock_search_df.assign(
            census=["white", "white", "asian"]
        )

        result_df = get_demographics_data(self.mock_pat2vec_obj, self.pat_list)

        # Assertions
        mock_cohort_searcher.assert_called_once()
        mock_demo_to_latest.assert_called_once()
        mock_calculate_age.assert_called_once()
        mock_abstract_ethnicity.assert_called_once()

        self.assertFalse(result_df.empty)
        self.assertIn("age", result_df.columns)
        self.assertIn("census", result_df.columns)
        self.assertEqual(
            len(result_df), 2
        )  # Should be 2 unique patients after demo_to_latest

    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.cohort_searcher_with_terms_and_search"
    )
    @patch("pat2vec.pat2vec_get_methods.get_method_demo.demo_to_latest")
    @patch("pat2vec.pat2vec_get_methods.get_method_demo.calculate_age_append")
    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.EthnicityAbstractor.abstractEthnicity"
    )
    def test_get_demographics_data_no_results(
        self,
        mock_abstract_ethnicity,
        mock_calculate_age,
        mock_demo_to_latest,
        mock_cohort_searcher,
    ):
        """Test handling of no results from the searcher."""
        mock_cohort_searcher.return_value = pd.DataFrame()

        result_df = get_demographics_data(self.mock_pat2vec_obj, self.pat_list)

        mock_cohort_searcher.assert_called_once()
        mock_demo_to_latest.assert_not_called()
        mock_calculate_age.assert_not_called()
        mock_abstract_ethnicity.assert_not_called()

        self.assertTrue(result_df.empty)

    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.cohort_searcher_with_terms_and_search"
    )
    @patch("pat2vec.pat2vec_get_methods.get_method_demo.demo_to_latest")
    @patch("pat2vec.pat2vec_get_methods.get_method_demo.calculate_age_append")
    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.EthnicityAbstractor.abstractEthnicity"
    )
    def test_get_demographics_data_ethnicity_disabled(
        self,
        mock_abstract_ethnicity,
        mock_calculate_age,
        mock_demo_to_latest,
        mock_cohort_searcher,
    ):
        """Test that ethnicity abstraction is skipped when disabled in config."""
        self.mock_config.ethnicity_abstractor_enabled = False
        mock_search_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "client_dob": ["1990-01-01"],
                "updatetime": ["2020-01-01"],
                "client_racecode": ["White"],
            }
        )
        mock_cohort_searcher.return_value = mock_search_df
        mock_demo_to_latest.return_value = mock_search_df
        mock_calculate_age.return_value = mock_search_df.assign(age=[30])

        result_df = get_demographics_data(self.mock_pat2vec_obj, self.pat_list)

        mock_abstract_ethnicity.assert_not_called()
        self.assertFalse(result_df.empty)
        self.assertNotIn("census", result_df.columns)


if __name__ == "__main__":
    unittest.main()
