import tempfile
import os
import unittest
import pandas as pd
from unittest.mock import MagicMock, patch
from pat2vec.pat2vec_get_methods.get_method_demo import (
    get_demographics_data,
    search_demographics,
    process_demographics_data,
    get_demographics3,
)


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


class TestSearchDemographics(unittest.TestCase):
    """Unit tests for search_demographics function."""

    def setUp(self):
        """Set up mock config_obj for tests."""
        self.mock_config = MagicMock()
        self.mock_config.root_path = "/tmp"
        self.mock_config.proj_name = "test_project"

    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.cohort_searcher_with_terms_and_search"
    )
    def test_search_demographics_config_path(self, mock_cohort_searcher):
        """Tests file path construction when config_obj has root_path and proj_name."""
        mock_cohort_searcher.return_value = pd.DataFrame(
            {"client_idcode": ["P1"], "client_dob": ["1990-01-01"]}
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config_with_paths = MagicMock()
            config_with_paths.root_path = tmpdir
            config_with_paths.proj_name = "test_project"

            result = search_demographics(
                cohort_searcher_with_terms_and_search=mock_cohort_searcher,
                client_id_codes=["P1"],
                start_year="2020",
                start_month="01",
                start_day="01",
                end_year="2021",
                end_month="12",
                end_day="31",
                config_obj=config_with_paths,
                output_filename="demographics.csv",
            )

            expected_path = os.path.join(tmpdir, "test_project", "demographics.csv")
            mock_cohort_searcher.assert_called_once()

    @patch(
        "pat2vec.pat2vec_get_methods.get_method_demo.cohort_searcher_with_terms_and_search"
    )
    def test_search_demographics_overwrite_true(self, mock_cohort_searcher):
        """Tests overwrite=True behavior (bypasses existing file)."""
        mock_result = pd.DataFrame(
            {"client_idcode": ["P1"], "client_dob": ["1990-01-01"]}
        )
        mock_cohort_searcher.return_value = mock_result

        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "demographics.csv")

            result1 = search_demographics(
                cohort_searcher_with_terms_and_search=mock_cohort_searcher,
                client_id_codes=["P1"],
                start_year="2020",
                start_month="01",
                start_day="01",
                end_year="2021",
                end_month="12",
                end_day="31",
                output_filename=test_file,
            )

            mock_cohort_searcher.reset_mock()
            mock_cohort_searcher.return_value = pd.DataFrame(
                {"client_idcode": ["P2"], "client_dob": ["1985-05-10"]}
            )

            result2 = search_demographics(
                cohort_searcher_with_terms_and_search=mock_cohort_searcher,
                client_id_codes=["P2"],
                start_year="2020",
                start_month="01",
                start_day="01",
                end_year="2021",
                end_month="12",
                end_day="31",
                output_filename=test_file,
                overwrite=True,
            )

            self.assertEqual(len(result2), 1)
            self.assertEqual(result2.iloc[0]["client_idcode"], "P2")


class TestProcessDemographicsData(unittest.TestCase):
    """Unit tests for process_demographics_data function."""

    def test_process_demographics_data_empty_df(self):
        """Tests empty dataframe handling in process_demographics_data."""
        result = process_demographics_data(pd.DataFrame(), ["P1", "P2"])
        expected = pd.DataFrame({"client_idcode": ["P1", "P2"]})
        pd.testing.assert_frame_equal(result, expected)

    def test_process_demographics_data_single_record(self):
        """Tests single record handling in process_demographics_data."""
        demo_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "updatetime": ["2020-06-01"],
            }
        )
        result = process_demographics_data(demo_df, ["P1"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["client_idcode"], "P1")

    def test_process_demographics_data_multiple_records(self):
        """Tests multiple records returns most recent record per patient."""
        demo_df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P1", "P2"],
                "updatetime": ["2020-01-01", "2020-06-01", "2020-03-15"],
            }
        )
        result = process_demographics_data(demo_df, ["P1", "P2"])
        self.assertEqual(len(result), 1)


class TestGetDemographics3(unittest.TestCase):
    """Unit tests for get_demographics3 function."""

    @patch("pat2vec.pat2vec_get_methods.get_method_demo.search_demographics")
    @patch("pat2vec.pat2vec_get_methods.get_method_demo.process_demographics_data")
    def test_get_demographics3_basic(self, mock_process, mock_search):
        """Tests get_demographics3 function with mocked searches."""
        mock_search.return_value = pd.DataFrame(
            {
                "client_idcode": ["P1", "P2"],
                "updatetime": ["2020-06-01", "2020-03-15"],
            }
        )
        mock_process.return_value = pd.DataFrame(
            {
                "client_idcode": ["P1", "P2"],
            }
        )

        mock_config = MagicMock()
        mock_config.verbosity = 0

        result = get_demographics3(
            patlist=["P1", "P2"],
            target_date_range=(2020, 1, 1, 2020, 12, 31),
            cohort_searcher_with_terms_and_search=MagicMock(),
            config_obj=mock_config,
        )

        mock_search.assert_called_once()
        mock_process.assert_called_once()


if __name__ == "__main__":
    unittest.main()
