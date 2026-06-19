import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
from datetime import datetime
from pat2vec.util.pre_processing import get_all_patient_list

from pat2vec.util.pre_processing import (
    get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy,
    draw_document_samples,
    demo_to_latest,
    calculate_age_append,
    search_cohort,
)


class TestPreProcessing(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_config.verbosity = 0
        # Ensure mock attributes are None to allow fall-through logic in tests
        self.mock_config.all_patient_list = None
        self.mock_config.all_patient_list_path = None
        self.mock_config.all_patient_list_column = None
        self.mock_config.pre_document_batch_path = None
        self.mock_config.root_path = "/fake/root"
        self.mock_config.lookback = False
        self.mock_config.global_start_day = "01"
        self.mock_config.global_start_month = "01"
        self.mock_config.global_start_year = "2020"
        self.mock_config.global_end_day = "31"
        self.mock_config.global_end_month = "12"
        self.mock_config.global_end_year = "2020"
        self.mock_config.testing = False
        self.mock_config.testing_elastic = False
        self.mock_config.client_idcode_term_name = "client_idcode"
        self.mock_config.main_options = {
            "annotations": True,
            "annotations_mrc": True,
            "textual_obs": True,
        }

        self.mock_pat2vec_obj = MagicMock()
        self.mock_pat2vec_obj.config_obj = self.mock_config
        self.mock_pat2vec_obj.treatment_doc_filename = "treatment_docs.csv"

        # Patch os.path.exists for the pre_processing module globally for this class
        self.patcher_exists = patch("pat2vec.util.pre_processing.os.path.exists")
        self.mock_exists = self.patcher_exists.start()
        self.mock_exists.return_value = (
            False  # Default to False to prevent accidental read_csv
        )
        self.addCleanup(self.patcher_exists.stop)

    def setup_searcher_mocks(self, mock_epr, mock_mct, mock_text):
        """Helper to ensure searcher mocks return DataFrames instead of MagicMocks."""
        mock_epr.return_value = pd.DataFrame()
        mock_mct.return_value = pd.DataFrame()
        mock_text.return_value = pd.DataFrame()

    @patch("pandas.read_csv")
    def test_get_all_patient_list_from_csv(self, mock_read_csv):
        """Test extracting patient IDs from a source CSV file."""
        self.mock_exists.return_value = True
        mock_read_csv.return_value = pd.DataFrame({"client_idcode": ["P1", "P2", "P3"]})

        self.mock_config.all_patient_list_path = "patients.csv"
        self.mock_config.all_patient_list_column = "client_idcode"

        result = get_all_patient_list(self.mock_config)
        self.assertEqual(result, ["P1", "P2", "P3"])

    def test_get_all_patient_list_direct_config(self):
        """Test when the list is already provided in the config object."""
        self.mock_config.all_patient_list = ["P100", "P200"]
        self.mock_config.all_patient_list_path = None

        result = get_all_patient_list(self.mock_config)
        self.assertEqual(result, ["P100", "P200"])

    @patch("os.path.isdir")
    @patch("os.listdir")
    def test_get_all_patient_list_from_directory(self, mock_listdir, mock_isdir):
        """Test identifying patients based on filenames in a directory."""
        mock_isdir.return_value = True
        self.mock_config.pre_document_batch_path = "/data/batch"
        mock_listdir.return_value = ["P1.csv", "P2.csv", "metadata.txt"]

        result = get_all_patient_list(self.mock_config)
        # Should extract 'P1' and 'P2' from .csv files
        self.assertIn("P1", result)
        self.assertIn("P2", result)
        self.assertNotIn("metadata", result)
        self.assertEqual(len(result), 2)

    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    def test_get_treatment_docs_basic_flow(
        self,
        mock_to_csv,
        mock_makedirs,
        mock_search_textual_obs,
        mock_search_mct,
        mock_search_epr,
    ):
        """Test basic flow of get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy."""
        self.mock_exists.return_value = False  # Ensure output file doesn't exist
        mock_search_epr.return_value = pd.DataFrame(
            {"col1": [1], "updatetime": ["2020-01-01"]}
        )
        mock_search_mct.return_value = pd.DataFrame(
            {"col2": [2], "updatetime": ["2020-01-02"]}
        )
        mock_search_textual_obs.return_value = pd.DataFrame(
            {"col3": [3], "updatetime": ["2020-01-03"]}
        )

        term_list = ["term1"]
        result_df = (
            get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
                self.mock_pat2vec_obj, term_list
            )
        )

        self.assertFalse(result_df.empty)
        self.assertEqual(len(result_df), 3)
        mock_search_epr.assert_called_once()
        mock_search_mct.assert_called_once()
        mock_search_textual_obs.assert_called_once()
        mock_to_csv.assert_called_once()

    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    @patch("pandas.read_csv")
    def test_get_treatment_docs_existing_file_no_overwrite(
        self,
        mock_read_csv,
        mock_to_csv,
        mock_makedirs,
        mock_search_textual_obs,
        mock_search_mct,
        mock_search_epr,
    ):
        """Test behavior when file exists and overwrite is False."""
        self.mock_exists.return_value = True
        mock_read_csv.return_value = pd.DataFrame({"existing_col": [10]})
        self.mock_config.mct = False
        self.mock_config.textual_obs = False
        self.mock_config.verbosity = 1
        mock_search_epr.return_value = pd.DataFrame()

        term_list = ["term1"]
        result_df = (
            get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
                self.mock_pat2vec_obj, term_list, overwrite=False
            )
        )

        mock_read_csv.assert_called_once()
        mock_search_epr.assert_not_called()
        mock_to_csv.assert_not_called()
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["existing_col"], 10)

    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    def test_get_treatment_docs_append_mode(
        self,
        mock_to_csv,
        mock_makedirs,
        mock_search_textual_obs,
        mock_search_mct,
        mock_search_epr,
    ):
        """Test append mode for existing files."""
        self.mock_exists.return_value = True
        mock_search_epr.return_value = pd.DataFrame(
            {"col1": [1], "updatetime": ["2020-01-01"]}
        )
        mock_search_mct.return_value = pd.DataFrame(
            {"col2": [2], "updatetime": ["2020-01-02"]}
        )
        mock_search_textual_obs.return_value = pd.DataFrame(
            {"col3": [3], "updatetime": ["2020-01-03"]}
        )

        term_list = ["term1"]
        result_df = (
            get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
                self.mock_pat2vec_obj, term_list, append=True
            )
        )

        mock_to_csv.assert_called_once_with(
            os.path.join("/fake/root", "treatment_docs.csv"),
            index=False,
            mode="a",
            header=False,
            escapechar="\\",
        )
        self.assertFalse(result_df.empty)

    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    def test_get_treatment_docs_lookback_true(
        self,
        mock_to_csv,
        mock_makedirs,
        mock_search_textual_obs,
        mock_search_mct,
        mock_search_epr,
    ):
        """Test lookback logic for date ranges."""
        self.mock_exists.return_value = False
        self.mock_config.lookback = True
        self.mock_config.global_start_day = "01"
        self.mock_config.global_start_month = "01"
        self.mock_config.global_start_year = "2020"
        self.mock_config.global_end_day = "31"
        self.mock_config.global_end_month = "12"
        self.mock_config.global_end_year = "2021"

        mock_search_epr.return_value = pd.DataFrame(
            {"col1": [1], "updatetime": ["2021-12-31"]}
        )
        mock_search_mct.return_value = pd.DataFrame(
            {"col2": [2], "updatetime": ["2021-12-31"]}
        )
        mock_search_textual_obs.return_value = pd.DataFrame(
            {"col3": [3], "updatetime": ["2021-12-31"]}
        )

        term_list = ["term1"]
        get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
            self.mock_pat2vec_obj, term_list
        )

        mock_search_epr.assert_called_once_with(
            term_list,
            os.path.join("/fake/root", "treatment_docs.csv"),
            start_day="31",
            start_month="12",
            start_year="2021",
            end_day="01",
            end_month="01",
            end_year="2020",
            append=True,
            additional_filters=None,
            all_fields=False,
            method="fuzzy",
            fuzzy=2,
            slop=1,
            testing=False,
            testing_elastic=False,
            debug=False,
        )

    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("pat2vec.util.pre_processing.cohort_searcher_with_terms_and_search_dummy")
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    def test_get_treatment_docs_testing_mode(
        self,
        mock_to_csv,
        mock_makedirs,
        mock_dummy_search,
        mock_search_to,
        mock_search_mct,
    ):
        """Test behavior in testing mode (dummy search)."""
        self.mock_exists.return_value = False
        self.setup_searcher_mocks(MagicMock(), mock_search_mct, mock_search_to)
        self.mock_config.testing = True
        mock_dummy_search.return_value = pd.DataFrame({"dummy_col": ["dummy_val"]})

        term_list = ["term1"]
        result_df = (
            get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
                self.mock_pat2vec_obj, term_list
            )
        )

        self.assertGreaterEqual(mock_dummy_search.call_count, 1)
        self.assertFalse(result_df.empty)
        self.assertEqual(result_df.iloc[0]["dummy_col"], "dummy_val")
        self.assertTrue(mock_to_csv.called)

    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    def test_get_treatment_docs_no_mct_no_textual_obs(
        self,
        mock_to_csv,
        mock_makedirs,
        mock_search_textual_obs,
        mock_search_mct,
        mock_search_epr,
    ):
        """Test when both MCT and textual_obs are disabled."""
        self.mock_exists.return_value = False
        self.mock_config.mct = False
        self.mock_config.textual_obs = False
        mock_search_epr.return_value = pd.DataFrame(
            {"col1": [1], "updatetime": ["2020-01-01"]}
        )

        term_list = ["term1"]
        result_df = (
            get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
                self.mock_pat2vec_obj, term_list, mct=False, textual_obs=False
            )
        )

        mock_search_epr.assert_called_once()
        mock_search_mct.assert_not_called()
        mock_search_textual_obs.assert_not_called()
        self.assertFalse(result_df.empty)
        self.assertEqual(len(result_df), 1)

    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    def test_get_treatment_docs_additional_filters(
        self,
        mock_to_csv,
        mock_makedirs,
        mock_search_textual_obs,
        mock_search_mct,
        mock_search_epr,
    ):
        """Test additional filters are passed to searchers."""
        self.mock_exists.return_value = False
        mock_search_epr.return_value = pd.DataFrame(
            {"col1": [1], "updatetime": ["2020-01-01"]}
        )
        mock_search_mct.return_value = pd.DataFrame(
            {"col2": [2], "updatetime": ["2020-01-02"]}
        )
        mock_search_textual_obs.return_value = pd.DataFrame(
            {"col3": [3], "updatetime": ["2020-01-03"]}
        )

        term_list = ["term1"]
        additional_filters = ["AND some_field:value"]
        get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
            self.mock_pat2vec_obj, term_list, additional_filters=additional_filters
        )

        mock_search_epr.assert_called_once_with(
            term_list,
            os.path.join("/fake/root", "treatment_docs.csv"),
            start_day="01",
            start_month="01",
            start_year="2020",
            end_day="31",
            end_month="12",
            end_year="2020",
            additional_filters=additional_filters,
            all_fields=False,
            method="fuzzy",
            fuzzy=2,
            slop=1,
            testing=False,
            debug=False,
            testing_elastic=False,
            append=True,  # Added missing default parameter
        )
        mock_search_mct.assert_called_once_with(
            term_list,
            os.path.join("/fake/root", "treatment_docs.csv"),
            start_day="01",
            start_month="01",
            start_year="2020",
            end_day="31",
            end_month="12",
            end_year="2020",
            debug=False,  # Added missing default parameter
            append=True,
            all_fields=False,
            method="fuzzy",
            fuzzy=2,
            slop=1,
            testing=False,
            testing_elastic=False,
            additional_filters=additional_filters,  # Added missing parameter
        )
        mock_search_textual_obs.assert_called_once_with(
            term_list,
            os.path.join("/fake/root", "treatment_docs.csv"),
            start_day="01",
            start_month="01",
            start_year="2020",
            end_day="31",
            end_month="12",
            end_year="2020",
            debug=False,  # Added missing default parameter
            append=True,
            all_fields=False,
            method="fuzzy",
            fuzzy=2,
            slop=1,
            testing=False,
            testing_elastic=False,
            additional_filters=additional_filters,  # Added missing parameter
        )

    @patch("pat2vec.util.pre_processing.cohort_searcher_with_terms_and_search_dummy")
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    def test_get_treatment_docs_testing_elastic_mode(
        self,
        mock_to_csv,
        mock_makedirs,
        mock_search_textual_obs,
        mock_search_mct,
        mock_search_epr,
        mock_dummy_search,
    ):
        """Test behavior in testing_elastic mode (real search but with testing flag)."""
        self.mock_exists.return_value = False
        self.mock_config.testing = True
        self.mock_config.testing_elastic = True
        mock_search_epr.return_value = pd.DataFrame(
            {"col1": [1], "updatetime": ["2020-01-01"]}
        )
        mock_search_mct.return_value = pd.DataFrame(
            {"col2": [2], "updatetime": ["2020-01-02"]}
        )
        mock_search_textual_obs.return_value = pd.DataFrame(
            {"col3": [3], "updatetime": ["2020-01-03"]}
        )

        term_list = ["term1"]
        result_df = (
            get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
                self.mock_pat2vec_obj, term_list
            )
        )

        mock_search_epr.assert_called_once()
        mock_search_mct.assert_called_once()
        mock_search_textual_obs.assert_called_once()
        self.assertFalse(result_df.empty)
        mock_to_csv.assert_called_once()
        mock_dummy_search.assert_not_called()  # Should not call dummy search in testing_elastic mode

    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    def test_get_treatment_docs_create_output_directory(
        self,
        mock_to_csv,
        mock_makedirs,
        mock_search_textual_obs,
        mock_search_mct,
        mock_search_epr,
    ):
        """Test that output directory is created if it doesn't exist."""
        # Simulate output file not existing for all relevant checks
        self.mock_exists.return_value = False
        mock_search_epr.return_value = pd.DataFrame(
            {"col1": [1], "updatetime": ["2020-01-01"]}
        )
        mock_search_mct.return_value = pd.DataFrame(
            {"col2": [2], "updatetime": ["2020-01-02"]}
        )
        mock_search_textual_obs.return_value = pd.DataFrame(
            {"col3": [3], "updatetime": ["2020-01-03"]}
        )

        term_list = ["term1"]
        get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
            self.mock_pat2vec_obj, term_list
        )

        mock_makedirs.assert_called_once_with("/fake/root", exist_ok=True)
        mock_to_csv.assert_called_once()

    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct"
    )
    @patch(
        "pat2vec.util.pre_processing.iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs"
    )
    @patch("pat2vec.util.pre_processing.os.path.exists", return_value=False)
    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_csv")
    def test_get_treatment_docs_merge_columns(
        self,
        mock_to_csv,
        mock_makedirs,
        mock_exists,
        mock_search_textual_obs,
        mock_search_mct,
        mock_search_epr,
    ):
        """Test merging of body_analysed and updatetime columns."""
        epr_df = pd.DataFrame(
            {
                "col1": [1],
                "updatetime": ["2020-01-01"],
                "body_analysed": ["EPR text"],
            }
        )
        mct_df = pd.DataFrame(
            {
                "col2": [2],
                "updatetime": [None],  # Simulate missing updatetime
                "body_analysed": [None],  # Simulate missing body_analysed
                "observation_valuetext_analysed": ["MCT text"],
                "basicobs_entered": ["2020-01-02"],
            }
        )
        textual_obs_df = pd.DataFrame(
            {
                "col3": [3],
                "updatetime": [None],  # Simulate missing updatetime
                "body_analysed": [None],  # Simulate missing body_analysed
                "textualObs": ["TextualObs text"],
                "observationdocument_recordeddtm": ["2020-01-03"],
            }
        )

        mock_search_epr.return_value = epr_df
        mock_search_mct.return_value = mct_df
        mock_search_textual_obs.return_value = textual_obs_df

        term_list = ["term1"]
        result_df = (
            get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
                self.mock_pat2vec_obj, term_list
            )
        )

        self.assertEqual(len(result_df), 3)
        # Verify body_analysed merge
        self.assertEqual(
            result_df.loc[result_df["col1"] == 1, "body_analysed"].iloc[0], "EPR text"
        )
        self.assertEqual(
            result_df.loc[result_df["col2"] == 2, "body_analysed"].iloc[0], "MCT text"
        )
        self.assertEqual(
            result_df.loc[result_df["col3"] == 3, "body_analysed"].iloc[0],
            "TextualObs text",
        )

        # Verify updatetime merge
        self.assertEqual(
            result_df.loc[result_df["col1"] == 1, "updatetime"].iloc[0], "2020-01-01"
        )
        self.assertEqual(
            result_df.loc[result_df["col2"] == 2, "updatetime"].iloc[0], "2020-01-02"
        )
        self.assertEqual(
            result_df.loc[result_df["col3"] == 3, "updatetime"].iloc[0], "2020-01-03"
        )

    def test_draw_document_samples_basic(self):
        """Test basic sampling functionality."""
        df = pd.DataFrame(
            {
                "search_term": ["A", "A", "B", "B", "B", "C"],
                "value": [1, 2, 3, 4, 5, 6],
            }
        )
        sampled_df = draw_document_samples(df, 2)
        self.assertEqual(len(sampled_df[sampled_df["search_term"] == "A"]), 2)
        self.assertEqual(len(sampled_df[sampled_df["search_term"] == "B"]), 2)
        self.assertEqual(
            len(sampled_df[sampled_df["search_term"] == "C"]), 1
        )  # Only 1 available

    def test_draw_document_samples_empty_df(self):
        """Test sampling with an empty DataFrame."""
        df = pd.DataFrame(columns=["search_term", "value"])
        sampled_df = draw_document_samples(df, 5)
        self.assertTrue(sampled_df.empty)

    def test_demo_to_latest(self):
        """Test demo_to_latest function to get the most recent record."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P001", "P001", "P002", "P002"],
                "updatetime": [
                    "2020-01-01",
                    "2020-03-15",
                    "2019-11-01",
                    "2020-02-20",
                ],
                "value": [1, 2, 3, 4],
            }
        )
        latest_df = demo_to_latest(df)
        self.assertEqual(len(latest_df), 2)
        self.assertEqual(
            latest_df[latest_df["client_idcode"] == "P001"]["value"].iloc[0], 2
        )
        self.assertEqual(
            latest_df[latest_df["client_idcode"] == "P002"]["value"].iloc[0], 4
        )

    def test_demo_to_latest_empty_df(self):
        """Test demo_to_latest with an empty DataFrame."""
        df = pd.DataFrame(columns=["client_idcode", "updatetime", "value"])
        latest_df = demo_to_latest(df)
        self.assertTrue(latest_df.empty)

    def test_calculate_age_append_valid_dob(self):
        """Test age calculation with valid date of birth."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P001", "P002"],
                "client_dob": ["1990-01-01", "1985-05-10"],
            }
        )
        # Mock datetime.now() for consistent testing
        with patch("pat2vec.util.pre_processing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2023, 6, 15)
            mock_dt.side_effect = lambda *args, **kw: datetime(
                *args, **kw
            )  # Allow datetime constructor
            result_df = calculate_age_append(df)

        self.assertIn("age", result_df.columns)
        # Age for 1990-01-01 on 2023-06-15 is 33
        self.assertEqual(
            result_df[result_df["client_idcode"] == "P001"]["age"].iloc[0], 33
        )
        # Age for 1985-05-10 on 2023-06-15 is 38
        self.assertEqual(
            result_df[result_df["client_idcode"] == "P002"]["age"].iloc[0], 38
        )

    def test_calculate_age_append_invalid_dob(self):
        """Test age calculation with invalid or missing DOBs."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P001", "P002", "P003"],
                "client_dob": ["1990-01-01", "invalid-date", None],
            }
        )
        with patch("pat2vec.util.pre_processing.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2023, 6, 15)
            mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)
            result_df = calculate_age_append(df)

        self.assertEqual(len(result_df), 1)  # Only P001 should remain
        self.assertEqual(result_df.iloc[0]["client_idcode"], "P001")
        self.assertEqual(result_df.iloc[0]["age"], 33)

    def test_calculate_age_append_empty_df(self):
        """Test age calculation with an empty DataFrame."""
        df = pd.DataFrame(columns=["client_idcode", "client_dob"])
        result_df = calculate_age_append(df)
        self.assertTrue(result_df.empty)
        self.assertIn("age", result_df.columns)  # Column should still be added

    def test_search_cohort_basic(self):
        """Test basic search_cohort functionality."""
        patlist = ["P1"]
        start_year, start_month, start_day = "2020", "01", "01"
        end_year, end_month, end_day = "2020", "12", "31"

        # Mock the cohort_searcher_with_terms_and_search method of pat2vec_obj
        self.mock_pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "client_idcode": ["P1"],
                    "client_firstname": ["John"],
                    "client_lastname": ["Doe"],
                    "client_dob": ["1990-01-01"],
                    "client_gendercode": ["M"],
                    "client_racecode": ["W"],
                    "client_deceaseddtm": [None],
                    "updatetime": ["2020-06-01"],
                }
            )
        )

        result_df = search_cohort(
            patlist,
            self.mock_pat2vec_obj,
            start_year,
            start_month,
            start_day,
            end_year,
            end_month,
            end_day,
        )

        self.assertFalse(result_df.empty)
        self.assertEqual(result_df.iloc[0]["client_idcode"], "P1")
        self.mock_pat2vec_obj.cohort_searcher_with_terms_and_search.assert_called_once()

    def test_search_cohort_additional_filters(self):
        """Test search_cohort with additional filters."""
        patlist = ["P1"]
        start_year, start_month, start_day = "2020", "01", "01"
        end_year, end_month, end_day = "2020", "12", "31"
        additional_filters = ["AND client_gendercode:M"]

        self.mock_pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "client_idcode": ["P1"],
                    "client_firstname": ["John"],
                    "client_lastname": ["Doe"],
                    "client_dob": ["1990-01-01"],
                    "client_gendercode": ["M"],
                    "client_racecode": ["W"],
                    "client_deceaseddtm": [None],
                    "updatetime": ["2020-06-01"],
                }
            )
        )

        search_cohort(
            patlist,
            self.mock_pat2vec_obj,
            start_year,
            start_month,
            start_day,
            end_year,
            end_month,
            end_day,
            additional_filters,
        )

        expected_search_string = (
            "updatetime:[2020-01-01 TO 2020-12-31] AND client_gendercode:M"
        )
        self.mock_pat2vec_obj.cohort_searcher_with_terms_and_search.assert_called_once_with(
            index_name="epr_documents",
            fields_list=[
                "client_idcode",
                "client_firstname",
                "client_lastname",
                "client_dob",
                "client_gendercode",
                "client_racecode",
                "client_deceaseddtm",
                "updatetime",
            ],
            term_name="client_idcode",
            entered_list=patlist,
            search_string=expected_search_string,
        )

    def test_search_cohort_empty_patlist(self):
        """Test search_cohort with an empty patient list."""
        patlist = []
        start_year, start_month, start_day = "2020", "01", "01"
        end_year, end_month, end_day = "2020", "12", "31"

        self.mock_pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame()
        )

        result_df = search_cohort(
            patlist,
            self.mock_pat2vec_obj,
            start_year,
            start_month,
            start_day,
            end_year,
            end_month,
            end_day,
        )
        self.assertTrue(result_df.empty)
        self.mock_pat2vec_obj.cohort_searcher_with_terms_and_search.assert_called_once()  # It should still be called, but with an empty entered_list


if __name__ == "__main__":
    unittest.main()
