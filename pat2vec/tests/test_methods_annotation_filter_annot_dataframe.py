import unittest
import pandas as pd
from pat2vec.util.methods_annotation_filter_annot_dataframe import (
    filter_annot_dataframe,
)


class TestFilterAnnotDataframe(unittest.TestCase):
    """Unit tests for the filter_annot_dataframe function."""

    def setUp(self):
        """Set up a sample DataFrame for testing."""
        self.df = pd.DataFrame(
            {
                "types": [
                    "['disease', 'finding']",
                    "['procedure']",
                    "['medication']",
                    "['observation', 'disease']",
                ],
                "Time_Value": ["Recent", "Past", "Future", "Recent"],
                "Presence_Value": ["True", "True", "False", "True"],
                "Subject_Value": ["Patient", "Family", "Patient", "Patient"],
                "Time_Confidence": [0.9, 0.8, 0.7, 0.95],
                "Presence_Confidence": [0.95, 0.7, 0.8, 0.9],
                "Subject_Confidence": [0.8, 0.9, 0.95, 0.7],
                "acc": [0.85, 0.95, 0.6, 0.9],
                "other_numeric": [10, 20, 5, 15],
            }
        )
        self.df.index = [
            10,
            11,
            12,
            13,
        ]  # Use non-standard index to check mask alignment

    def test_filter_by_types(self):
        """Test filtering by the 'types' column."""
        filter_args = {"types": ["procedure", "observation"]}
        result = filter_annot_dataframe(self.df, filter_args)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result.index.tolist(), [11, 13])

    def test_filter_by_value_list(self):
        """Test filtering by columns with a list of allowed values."""
        filter_args = {"Time_Value": ["Recent", "Past"]}
        result = filter_annot_dataframe(self.df, filter_args)
        self.assertEqual(len(result), 3)
        self.assertListEqual(result.index.tolist(), [10, 11, 13])

    def test_filter_by_single_value(self):
        """Test filtering by columns with a single allowed value."""
        filter_args = {"Subject_Value": "Patient"}
        result = filter_annot_dataframe(self.df, filter_args)
        self.assertEqual(len(result), 3)
        self.assertListEqual(result.index.tolist(), [10, 12, 13])

    def test_filter_by_confidence_scores(self):
        """Test filtering by confidence score columns (>=)."""
        filter_args = {"acc": 0.9}
        result = filter_annot_dataframe(self.df, filter_args)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result.index.tolist(), [11, 13])

        filter_args = {"Time_Confidence": 0.9}
        result = filter_annot_dataframe(self.df, filter_args)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result.index.tolist(), [10, 13])

    def test_filter_by_other_numeric_column(self):
        """Test the generic 'else' condition for numeric filtering."""
        filter_args = {"other_numeric": 15}
        result = filter_annot_dataframe(self.df, filter_args)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result.index.tolist(), [11, 13])

    def test_combined_filters(self):
        """Test applying multiple filters at once."""
        filter_args = {"types": ["disease"], "acc": 0.8, "Presence_Value": ["True"]}
        # Row 10: types=['disease'], acc=0.85, Presence_Value='True' -> PASS
        # Row 11: types=['procedure'], acc=0.95, Presence_Value='True' -> FAIL (type)
        # Row 12: types=['medication'], acc=0.6, Presence_Value='False' -> FAIL (acc, presence)
        # Row 13: types=['disease'], acc=0.9, Presence_Value='True' -> PASS
        result = filter_annot_dataframe(self.df, filter_args)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result.index.tolist(), [10, 13])

    def test_empty_filter_args(self):
        """Test that an empty filter dictionary returns the original DataFrame."""
        result = filter_annot_dataframe(self.df, {})
        pd.testing.assert_frame_equal(result, self.df)
        self.assertEqual(len(result), 4)

    def test_non_existent_column_filter(self):
        """Test that a filter on a non-existent column is ignored."""
        filter_args = {"non_existent_col": 100}
        result = filter_annot_dataframe(self.df, filter_args)
        pd.testing.assert_frame_equal(result, self.df)
        self.assertEqual(len(result), 4)

    def test_empty_dataframe(self):
        """Test filtering on an empty DataFrame."""
        empty_df = pd.DataFrame(columns=self.df.columns)
        filter_args = {"acc": 0.8}
        result = filter_annot_dataframe(empty_df, filter_args)
        self.assertTrue(result.empty)

    def test_no_matching_rows(self):
        """Test a filter that results in no matching rows."""
        filter_args = {"acc": 0.99}
        result = filter_annot_dataframe(self.df, filter_args)
        self.assertTrue(result.empty)

    def test_case_insensitivity_in_types(self):
        """Test that 'types' filtering is case-insensitive."""
        filter_args = {"types": ["DISEASE"]}  # uppercase
        result = filter_annot_dataframe(self.df, filter_args)
        self.assertEqual(len(result), 2)
        self.assertListEqual(result.index.tolist(), [10, 13])


if __name__ == "__main__":
    unittest.main()
