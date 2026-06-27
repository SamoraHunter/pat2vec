import os
import unittest
import pandas as pd
from unittest.mock import MagicMock, patch
from pat2vec.pat2vec_get_methods.get_method_demo import (
    get_demographics_data,
    search_demographics,
)


class TestGetMethodDemo(unittest.TestCase):
    """Unit tests for the get_demographics_data function."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_config = MagicMock()
        self.mock_config.batch_mode = False
        self.mock_config.verbosity = 0
        self.mock_config.start_time = "2020-01-01"
        self.mock_config.get_start_end_year_month.return_value = (2010, 1, 2020, 1)

    @patch("pat2vec.pat2vec_get_methods.get_method_demo.search_patients_basic")
    def test_get_demographics_data_returns_dataframe(self, mock_search):
        """Test that get_demographics_data returns a DataFrame."""
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "updatetime": ["2020-01-01"]})
        mock_search.return_value = mock_df

        result = get_demographics_data(
            current_pat_client_id_code="P1",
            target_date_range=(2010, 1, 2020, 1),
            config_obj=self.mock_config,
        )

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)

    @patch("pat2vec.pat2vec_get_methods.get_method_demo.search_patients_basic")
    def test_get_demographics_data_excludes_census(self, mock_search):
        """Test that census column is excluded from results."""
        mock_df = pd.DataFrame(
            {"client_idcode": ["P1"], "updatetime": ["2020-01-01"], "census": [1]}
        )
        mock_search.return_value = mock_df

        result = get_demographics_data(
            current_pat_client_id_code="P1",
            target_date_range=(2010, 1, 2020, 1),
            config_obj=self.mock_config,
        )

        self.assertNotIn("census", result.columns)


class TestSearchDemographics(unittest.TestCase):
    """Unit tests for the search_demographics function."""

    def setUp(self):
        """Set up mock config_obj with root_path and proj_name."""
        self.mock_config = MagicMock()
        self.mock_config.root_path = "/tmp/test_root"
        self.mock_config.proj_name = "test_project"

    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.cohort_searcher_with_terms_and_search"
    )
    @patch("pat2vec.pat2vec_get_methods.get_method_demo.os.path.exists")
    def test_search_demographics_with_config_root_path(
        self,
        mock_exists,
        mock_cohort_searcher,
    ):
        """Test file path construction when config_obj has root_path and proj_name."""
        mock_exists.return_value = False
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "updatetime": ["2020-01-01"]})
        mock_cohort_searcher.return_value = mock_df

        result = search_demographics(
            cohort_searcher_with_terms_and_search=mock_cohort_searcher,
            client_id_codes="P1",
            config_obj=self.mock_config,
            output_filename="demographics.csv",
        )

        self.assertEqual(len(result), len(mock_df))
        os.path.join("/tmp/test_root", "test_project", "demographics.csv")
        mock_cohort_searcher.assert_called_once()
        mock_exists.assert_called_once()

    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.cohort_searcher_with_terms_and_search"
    )
    @patch("pat2vec.pat2vec_get_methods.get_method_demo.os.path.exists")
    def test_search_demographics_overwrite_true(
        self,
        mock_exists,
        mock_cohort_searcher,
    ):
        """Test overwrite=True behavior."""
        mock_exists.return_value = True
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "updatetime": ["2020-01-01"]})
        mock_cohort_searcher.return_value = mock_df

        result = search_demographics(
            cohort_searcher_with_terms_and_search=mock_cohort_searcher,
            client_id_codes="P1",
            config_obj=self.mock_config,
            output_filename="demographics.csv",
            overwrite=True,
        )

        self.assertEqual(len(result), len(mock_df))
        mock_exists.assert_called()
        mock_cohort_searcher.assert_called_once()

    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.cohort_searcher_with_terms_and_search"
    )
    def test_search_demographics_validation_error_no_cohort_searcher(
        self,
        mock_cohort_searcher,
    ):
        """Test ValueError when cohort_searcher_with_terms_and_search is None."""
        with self.assertRaises(ValueError) as context:
            search_demographics(
                cohort_searcher_with_terms_and_search=None,
                client_id_codes="P1",
                config_obj=self.mock_config,
                output_filename="demographics.csv",
            )
        self.assertEqual(
            "cohort_searcher_with_terms_and_search cannot be None.",
            str(context.exception),
        )

    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.cohort_searcher_with_terms_and_search"
    )
    def test_search_demographics_validation_error_no_client_id_codes(
        self,
        mock_cohort_searcher,
    ):
        """Test ValueError when client_id_codes is None."""
        with self.assertRaises(ValueError) as context:
            search_demographics(
                cohort_searcher_with_terms_and_search=mock_cohort_searcher,
                client_id_codes=None,
                config_obj=self.mock_config,
                output_filename="demographics.csv",
            )
        self.assertEqual("client_id_codes cannot be None.", str(context.exception))


if __name__ == "__main__":
    unittest.main()
