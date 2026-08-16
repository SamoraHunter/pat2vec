import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from pat2vec.pat2vec_search.search_multiprocess import (
    cohort_searcher_with_terms_and_search_multi,
    pull_and_write,
)


class TestSearchMultiprocess(unittest.TestCase):
    """Test suite for search_multiprocess module functions."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists("temp_search_store.csv"):
            os.remove("temp_search_store.csv")
        shutil.rmtree(self.test_dir)

    @patch(
        "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
    )
    def test_pull_and_write_writes_to_file(self, mock_searcher):
        """Test pull_and_write function writes data to CSV file."""
        mock_df = pd.DataFrame({"field1": ["value1"], "field2": ["value2"]})
        mock_searcher.return_value = mock_df

        args = ("test_index", ["field1", "field2"], "term", ["value1"], "")

        pull_and_write(
            args[0],  # index_name
            args[1],  # fields_list
            args[2],  # term_name
            args[3],  # entered_list
            args[4],  # search_string
        )

        self.assertTrue(os.path.exists("temp_search_store.csv"))

    @patch(
        "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
    )
    def test_pull_and_write_appends_data(self, mock_searcher):
        """Test pull_and_write appends data to existing file."""
        mock_df = pd.DataFrame({"field1": ["value1"], "field2": ["value2"]})
        mock_searcher.return_value = mock_df

        fields_list = ["field1", "field2"]

        with open("temp_search_store.csv", "w") as f:
            f.write(",".join(fields_list) + "\n")

        args = ("test_index", fields_list, "term", ["value1"], "")

        pull_and_write(
            args[0],  # index_name
            args[1],  # fields_list
            args[2],  # term_name
            args[3],  # entered_list
            args[4],  # search_string
        )

        with open("temp_search_store.csv", "r") as f:
            lines = f.readlines()

        self.assertEqual(len(lines), 2)

    @patch(
        "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
    )
    def test_pull_and_write_creates_new_file(self, mock_searcher):
        """Test pull_and_write creates new file if it doesn't exist."""
        mock_df = pd.DataFrame({"field1": ["value1"], "field2": ["value2"]})
        mock_searcher.return_value = mock_df

        args = ("test_index", ["field1", "field2"], "term", ["value1"], "")

        pull_and_write(
            args[0],  # index_name
            args[1],  # fields_list
            args[2],  # term_name
            args[3],  # entered_list
            args[4],  # search_string
        )

        self.assertTrue(os.path.exists("temp_search_store.csv"))

    @patch(
        "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
    )
    def test_pull_and_write_calls_searcher_correctly(self, mock_searcher):
        """Test pull_and_write calls the searcher with correct arguments."""
        mock_df = pd.DataFrame({"field1": ["value1"], "field2": ["value2"]})
        mock_searcher.return_value = mock_df

        args = (
            "test_index",
            ["field1", "field2"],
            "term_name",
            ["item1"],
            "search_query",
        )

        pull_and_write(
            args[0],  # index_name
            args[1],  # fields_list
            args[2],  # term_name
            args[3],  # entered_list
            args[4],  # search_string
        )

        mock_searcher.assert_called_once_with(
            index_name=args[0],
            fields_list=args[1],
            term_name=args[2],
            entered_list=args[3],
            search_string=args[4],
        )

    @patch(
        "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
    )
    def test_pull_and_write_empty_dataframe(self, mock_searcher):
        """Test pull_and_write handles empty DataFrame correctly."""
        mock_df = pd.DataFrame(columns=["field1", "field2"])
        mock_searcher.return_value = mock_df

        args = ("test_index", ["field1", "field2"], "term", [], "")

        pull_and_write(
            args[0],  # index_name
            args[1],  # fields_list
            args[2],  # term_name
            args[3],  # entered_list
            args[4],  # search_string
        )

        self.assertTrue(os.path.exists("temp_search_store.csv"))

    @patch(
        "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
    )
    def test_pull_and_write_prints_progress(self, mock_searcher):
        """Test pull_and_write prints progress message."""
        mock_df = pd.DataFrame({"field1": ["value1"], "field2": ["value2"]})
        mock_searcher.return_value = mock_df

        args = ("test_index", ["field1", "field2"], "term", ["value1"] * 5, "")

        with patch("builtins.print") as mock_print:
            pull_and_write(
                args[0],  # index_name
                args[1],  # fields_list
                args[2],  # term_name
                args[3],  # entered_list
                args[4],  # search_string
            )
            mock_print.assert_called_once_with(f"running...{len(args[3])}")

    @patch("pat2vec.pat2vec_search.search_multiprocess.Pool")
    def test_cohort_searcher_with_terms_and_search_multi(self, mock_pool):
        """Test cohort_searcher_with_terms_and_search_multi parallel processing."""
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        fields_list = ["field1", "field2"]
        entered_list = [f"item_{i}" for i in range(6)]

        mock_df = pd.DataFrame({"field1": ["value1"], "field2": ["value2"]})

        def capture_args(func, args_list):
            return iter([None] * len(args_list))

        mock_pool_instance.imap_unordered = MagicMock(side_effect=capture_args)

        with (
            patch("multiprocessing.cpu_count", return_value=2),
            patch(
                "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
            ) as mock_searcher,
            patch("pandas.read_csv", return_value=mock_df),
        ):
            mock_searcher.return_value = mock_df

            result = cohort_searcher_with_terms_and_search_multi(
                index_name="test_index",
                fields_list=fields_list,
                term_name="term",
                entered_list=entered_list,
                search_string="",
            )

            self.assertIsNotNone(result)
            self.assertIsInstance(result, pd.DataFrame)

    @patch("pat2vec.pat2vec_search.search_multiprocess.Pool")
    def test_cohort_searcher_with_terms_and_search_multi_empty_list(self, mock_pool):
        """Test cohort_searcher_with_terms_and_search_multi with empty entered_list."""
        fields_list = ["field1", "field2"]

        mock_df = pd.DataFrame(columns=fields_list)

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.imap_unordered = lambda func, args_list: iter([])

        with (
            patch("multiprocessing.cpu_count", return_value=4),
            patch(
                "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
            ) as mock_searcher,
            patch("pandas.read_csv", return_value=mock_df),
        ):
            mock_searcher.return_value = mock_df

            result = cohort_searcher_with_terms_and_search_multi(
                index_name="test_index",
                fields_list=fields_list,
                term_name="term",
                entered_list=[],
                search_string="",
            )

            self.assertIsInstance(result, pd.DataFrame)
            self.assertTrue(result.empty)

    @patch("pat2vec.pat2vec_search.search_multiprocess.Pool")
    def test_cohort_searcher_with_terms_and_search_multi_single_item(self, mock_pool):
        """Test cohort_searcher_with_terms_and_search_multi with single item."""
        fields_list = ["field1", "field2"]

        mock_df = pd.DataFrame({"field1": ["value1"], "field2": ["value2"]})

        def capture_args(func, args_list):
            return iter([None] * len(args_list))

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.imap_unordered = MagicMock(side_effect=capture_args)

        with (
            patch("multiprocessing.cpu_count", return_value=4),
            patch(
                "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
            ) as mock_searcher,
            patch("pandas.read_csv", return_value=mock_df),
        ):
            mock_searcher.return_value = mock_df

            result = cohort_searcher_with_terms_and_search_multi(
                index_name="test_index",
                fields_list=fields_list,
                term_name="term",
                entered_list=["single_item"],
                search_string="",
            )

            self.assertEqual(len(result), 1)

    @patch("pat2vec.pat2vec_search.search_multiprocess.Pool")
    def test_cohort_searcher_with_terms_and_search_multi_large_list(self, mock_pool):
        """Test cohort_searcher_with_terms_and_search_multi with many items."""
        fields_list = ["field1", "field2"]

        mock_df = pd.DataFrame({"field1": ["value1"], "field2": ["value2"]})

        num_items = 50
        entered_list = [f"item_{i}" for i in range(num_items)]

        def capture_args(func, args_list):
            return iter([None] * len(args_list))

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.imap_unordered = MagicMock(side_effect=capture_args)

        with (
            patch("multiprocessing.cpu_count", return_value=4),
            patch(
                "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
            ) as mock_searcher,
            patch("pandas.read_csv", return_value=pd.concat([mock_df] * num_items)),
        ):
            mock_searcher.return_value = mock_df

            result = cohort_searcher_with_terms_and_search_multi(
                index_name="test_index",
                fields_list=fields_list,
                term_name="term",
                entered_list=entered_list,
                search_string="",
            )

            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), num_items)

    @patch("pat2vec.pat2vec_search.search_multiprocess.Pool")
    def test_cohort_searcher_with_terms_and_search_multi_result_contains_data(
        self, mock_pool
    ):
        """Test that the result contains data from all sub-processes."""
        fields_list = ["field1", "field2"]

        mock_df = pd.DataFrame({"field1": ["value1"], "field2": ["value2"]})

        def capture_args(func, args_list):
            return iter([None] * len(args_list))

        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.imap_unordered = MagicMock(side_effect=capture_args)

        with (
            patch("multiprocessing.cpu_count", return_value=2),
            patch(
                "pat2vec.pat2vec_search.search_multiprocess.cohort_searcher_with_terms_and_search"
            ) as mock_searcher,
            patch("pandas.read_csv", return_value=pd.concat([mock_df] * 5)),
        ):
            mock_searcher.return_value = mock_df

            result = cohort_searcher_with_terms_and_search_multi(
                index_name="test_index",
                fields_list=fields_list,
                term_name="term",
                entered_list=["item_1", "item_2"],
                search_string="",
            )

            self.assertEqual(len(result), 5)


if __name__ == "__main__":
    unittest.main()
