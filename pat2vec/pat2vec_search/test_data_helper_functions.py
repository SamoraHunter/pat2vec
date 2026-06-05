import unittest
import pandas as pd
from datetime import date
from pat2vec.pat2vec_search.data_helper_functions import (
    appendAge,
    appendAgeAtRecord,
    append_age_at_record_series,
    df_column_uniquify,
)


class TestDataHelperFunctions(unittest.TestCase):
    """Unit tests for the data helper functions in the search module."""

    def test_appendAge(self):
        """Test calculating and appending current age."""
        df = pd.DataFrame(
            {"client_dob": ["1990-01-01T00:00:00", "2000-05-20T12:00:00.500"]}
        )
        result = appendAge(df.copy())

        self.assertIn("age", result.columns)

        # Manually calculate age based on current date
        def calc_age(born_str):
            from datetime import datetime

            born = datetime.strptime(born_str.split(".")[0], "%Y-%m-%dT%H:%M:%S").date()
            today = date.today()
            return (
                today.year
                - born.year
                - ((today.month, today.day) < (born.month, born.day))
            )

        self.assertEqual(result.iloc[0]["age"], calc_age("1990-01-01T00:00:00"))
        self.assertEqual(result.iloc[1]["age"], calc_age("2000-05-20T12:00:00.500"))

    def test_appendAgeAtRecord(self):
        """Test calculating and appending age at the time of a record."""
        df = pd.DataFrame(
            {
                "client_dob": ["1980-06-15T00:00:00"],
                "updatetime": ["2010-06-15T00:00:00"],
            }
        )
        result = appendAgeAtRecord(df.copy())

        self.assertIn("ageAtRecord", result.columns)
        # Exactly 30 years apart on the same day
        self.assertEqual(result.iloc[0]["ageAtRecord"], 30)

    def test_append_age_at_record_series(self):
        """Test calculating age at record time for a single pandas Series (row)."""
        s = pd.Series(
            {
                "client_id": "P1",
                "client_dob": "1995-12-31T00:00:00",
                "updatetime": pd.Timestamp("2020-01-01"),
            }
        )
        result = append_age_at_record_series(s.copy())
        self.assertEqual(result["age"], 24)

    def test_df_column_uniquify(self):
        """Test ensuring unique column names in a DataFrame."""
        df = pd.DataFrame([[1, 2, 3, 4]], columns=["score", "score", "age", "score"])
        result = df_column_uniquify(df)

        expected_columns = ["score", "score_1", "age", "score_2"]
        self.assertListEqual(list(result.columns), expected_columns)


if __name__ == "__main__":
    unittest.main()
