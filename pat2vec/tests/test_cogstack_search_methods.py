import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import urllib3.exceptions
from pat2vec.pat2vec_search.cogstack_search_methods import (
    CogStack,
    check_patients_existence,
    list_chunker,
    set_index_safe_wrapper,
    initialize_cogstack_client,
    iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_clinical_notes,
    get_all_fields_for_method,
)
from pat2vec.util.get_method_index_map import GET_METHOD_INDEX_MAP


class TestCogstackSearchMethods(unittest.TestCase):
    """Unit tests for the CogStack search utility module."""

    def test_list_chunker(self):
        """Test splitting a list into chunks of 10,000."""
        big_list = list(range(25000))
        chunks = list_chunker(big_list)
        self.assertEqual(len(chunks), 3)
        self.assertEqual(len(chunks[0]), 10000)
        self.assertEqual(len(chunks[2]), 5000)

    def test_set_index_safe_wrapper_failure(self):
        """Test graceful failure when the 'id' column is missing."""
        df = pd.DataFrame({"not_id": [1, 2]})
        # Should return original and not raise exception
        result = set_index_safe_wrapper(df.copy())
        self.assertIsNone(result.index.name)

    @patch("pat2vec.pat2vec_search.cogstack_search_methods.initialize_cogstack_client")
    def test_check_patients_existence_basic(self, mock_initialize):
        """Test patient existence check using terms aggregation."""
        # Setup mocks
        mock_cs = MagicMock()
        mock_initialize.return_value = mock_cs

        # Mock aggregation response
        mock_response = {
            "aggregations": {
                "existing_ids": {"buckets": [{"key": "P1"}, {"key": "P2"}]}
            }
        }
        mock_cs.elastic.search.return_value = mock_response

        patient_ids = ["P1", "P2", "P3"]
        result = check_patients_existence(
            patient_ids, index_name="test_index", id_field="pid"
        )

        # P1 and P2 found, P3 missing
        self.assertCountEqual(result, ["P1", "P2"])

    @patch("pat2vec.pat2vec_search.cogstack_search_methods.initialize_cogstack_client")
    def test_check_patients_existence_fielddata_fallback(self, mock_initialize):
        """Test fallback to search-based check when fielddata is disabled (aggregation fails)."""
        mock_cs = MagicMock()
        mock_initialize.return_value = mock_cs

        # First call fails with fielddata error
        mock_cs.elastic.search.side_effect = [
            Exception("fielddata=true"),  # Simplified error message for mocking
            {"hits": {"hits": [{"_source": {"pid": "P1"}}]}},
        ]

        patient_ids = ["P1"]
        result = check_patients_existence(patient_ids, id_field="pid")

        self.assertCountEqual(result, ["P1"])
        self.assertEqual(mock_cs.elastic.search.call_count, 2)

    @patch("importlib.util.spec_from_file_location")
    def test_initialize_cogstack_client_from_config_path(self, mock_spec):
        """Test initializing the client using a specific credentials path in config."""
        mock_config = MagicMock()
        mock_config.credentials_path = "/fake/path/creds.py"

        with patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.CogStack"
        ) as mock_cogstack:
            initialize_cogstack_client(mock_config)
            mock_cogstack.assert_called_once()

    def test_reindex_with_duplicate_columns(self):
        """Test that reindexing handles duplicate columns by deduplicating first."""
        # This simulates the logic inside the searchers that caused the ValueError
        df_new = pd.DataFrame([[1, 2, 1]], columns=["A", "B", "A"])  # Duplicate 'A'
        existing_df = pd.DataFrame(columns=["A", "B", "C"])

        # deduplicate first (this is the fix)
        df_fixed = df_new.loc[:, ~df_new.columns.duplicated()]
        existing_fixed = existing_df.loc[:, ~existing_df.columns.duplicated()]

        combined_cols = existing_fixed.columns.union(df_fixed.columns)

        result = df_fixed.reindex(columns=combined_cols)
        self.assertCountEqual(result.columns, ["A", "B", "C"])

    @patch("pat2vec.pat2vec_search.cogstack_search_methods.pd.read_csv")
    @patch("pat2vec.pat2vec_search.cogstack_search_methods.os.path.exists")
    @patch(
        "pat2vec.pat2vec_search.cogstack_search_methods.cohort_searcher_no_terms_fuzzy"
    )
    @patch("pat2vec.pat2vec_search.cogstack_search_methods.pd.DataFrame.to_csv")
    @patch(
        "pat2vec.pat2vec_search.cogstack_search_methods.initialize_cogstack_client"
    )  # Mock global cs init
    def test_iterative_clinical_notes_handles_duplicate_columns_on_append(
        self,
        mock_init_cs,
        mock_to_csv,
        mock_cohort_searcher,
        mock_exists,
        mock_read_csv,
    ):
        """
        Test that iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_clinical_notes
        correctly handles duplicate columns when appending to an existing file,
        without raising a ValueError. This specifically tests the rename and reindex logic.
        """
        # Simulate an existing CSV file
        mock_exists.return_value = True
        mock_read_csv.return_value = pd.DataFrame(
            {
                "client_idcode": ["P_OLD"],
                "updatetime": ["2023-01-01"],
                "body_analysed": ["Old clinical note"],
                "document_guid": ["OLD_GUID"],
                "document_Name": [
                    "Old Document Name"
                ],  # This will become 'document_description'
                "search_term": ["old_term"],
                "some_other_field": ["extra_data"],
            }
        )

        # Simulate new search results from cohort_searcher_no_terms_fuzzy
        # This DataFrame will be passed to the iterative function, which then renames columns.
        # We want to ensure that even if the *original* `docs` from `cohort_searcher_no_terms_fuzzy`
        # has fields that, after renaming, would clash with other fields in `docs` or `existing_data`,
        # the deduplication logic handles it.
        mock_cohort_searcher.return_value = pd.DataFrame(
            {
                "document_PatientDurableKey": [
                    "P_NEW"
                ],  # Will be renamed to client_idcode
                "document_CreatedWhen": ["2023-01-02"],  # Will be renamed to updatetime
                "document_Content": [
                    "New clinical note"
                ],  # Will be renamed to body_analysed
                "id": ["NEW_GUID"],  # Will be renamed to document_guid
                "document_Name": [
                    "New Document Name"
                ],  # Will be renamed to document_description
                # Add a column that might exist in existing_data or clash after rename
                "client_idcode": [
                    "P_NEW_CLASH"
                ],  # This will cause a duplicate 'client_idcode' after rename
                "_index": ["epic_clinical_notes"],
                "_score": [1.0],
            }
        )

        # Mock the global cs object
        mock_init_cs.return_value = MagicMock()

        # Call the function under test with append=True and all_fields=True to maximize potential for clashes
        result_df = iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_clinical_notes(
            terms_list=["new_term"],
            treatment_doc_filename="test_clinical_notes.csv",
            start_year="2023",
            start_month="01",
            start_day="01",
            end_year="2023",
            end_month="01",
            end_day="31",
            append=True,
            debug=True,
            all_fields=True,  # Request all fields to increase chance of column clashes
        )

        # Assert that no ValueError was raised (the test would fail before this if it was)
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertFalse(result_df.empty)

        # Assert that the final DataFrame has unique columns
        self.assertEqual(len(result_df.columns), len(set(result_df.columns)))

        # Assert that both existing and new data are present
        self.assertEqual(len(result_df), 2)
        self.assertIn("P_OLD", result_df["client_idcode"].tolist())
        self.assertIn("P_NEW", result_df["client_idcode"].tolist())  # The renamed one
        self.assertNotIn(
            "P_NEW_CLASH", result_df["client_idcode"].tolist()
        )  # The duplicate should have been dropped

        self.assertIn("Old clinical note", result_df["body_analysed"].tolist())
        self.assertIn("New clinical note", result_df["body_analysed"].tolist())

        # Verify that to_csv was called to save the updated data
        mock_to_csv.assert_called_once()

    @patch("pat2vec.pat2vec_search.cogstack_search_methods.initialize_cogstack_client")
    def test_epic_indices_integration(self, mock_initialize_cogstack_client):
        """
        Test that Epic indices are correctly integrated by verifying their presence
        in GET_METHOD_INDEX_MAP and attempting to retrieve fields.
        """
        mock_cs_instance = MagicMock()
        mock_initialize_cogstack_client.return_value = mock_cs_instance
        mock_cs_instance.get_index_fields.return_value = ["field1", "field2"]

        # Filter for Epic-related methods in GET_METHOD_INDEX_MAP
        epic_methods = {
            method: index
            for method, index in GET_METHOD_INDEX_MAP.items()
            if method.startswith("get_epic_")
        }

        self.assertGreater(
            len(epic_methods), 0, "No Epic methods found in GET_METHOD_INDEX_MAP"
        )

        for method_name, expected_index in epic_methods.items():
            with self.subTest(method=method_name, index=expected_index):
                # Call get_all_fields_for_method for each Epic method
                fields = get_all_fields_for_method(method_name)

                # Assert that initialize_cogstack_client was called
                mock_initialize_cogstack_client.assert_called()

                # Assert that get_index_fields was called with the correct index
                mock_cs_instance.get_index_fields.assert_any_call(expected_index)

                # Assert that fields are returned (even if dummy)
                self.assertIsInstance(fields, list)
                self.assertGreater(len(fields), 0)

                # Reset mocks for the next subtest
                mock_cs_instance.get_index_fields.reset_mock()
                mock_initialize_cogstack_client.reset_mock()

    @patch("pat2vec.pat2vec_search.cogstack_search_methods.elasticsearch.Elasticsearch")
    def test_cogstack_verify_certs_disabled(self, mock_es_class):
        """Test that CogStack initializes with verify_certs=False for both API and basic auth."""
        # Test API key authentication
        _ = CogStack(hosts=["https://cogstack01"], api_key="test_key", api=True)
        mock_es_class.assert_any_call(
            hosts=["https://cogstack01"],
            api_key="test_key",
            verify_certs=False,
        )

        # Test basic authentication
        _ = CogStack(
            hosts=["https://cogstack01"], username="user", password="pass", api=False
        )
        mock_es_class.assert_any_call(
            hosts=["https://cogstack01"],
            basic_auth=("user", "pass"),
            verify_certs=False,
        )

    @patch("pat2vec.pat2vec_search.cogstack_search_methods.elasticsearch.Elasticsearch")
    def test_cogstack_insecure_warning_suppressed(self, mock_es_class):
        """Test that InsecureRequestWarning is suppressed during CogStack initialization."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _ = CogStack(hosts=["https://cogstack01"], api_key="test_key", api=True)

            # Check no InsecureRequestWarning was raised
            insecure_warnings = [
                warning
                for warning in w
                if issubclass(
                    warning.category, urllib3.exceptions.InsecureRequestWarning
                )
            ]
            self.assertEqual(len(insecure_warnings), 0)
