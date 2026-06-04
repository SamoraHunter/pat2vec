import unittest
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch

from pat2vec.util.methods_get import (
    convert_timestamp_to_tuple,
    convert_date,
    build_patient_dict,
    add_offset_column,
    get_empty_date_vector,
    enum_target_date_vector,
    enum_exact_target_date_vector,
)
from pat2vec.util.get_dummy_data_cohort_searcher import (
    is_safe_host,
    maybe_nan,
    extract_date_range,
)


class TestMethodsGet(unittest.TestCase):
    """Comprehensive tests for pat2vec.util.methods_get."""

    def test_convert_timestamp_to_tuple(self):
        # Test robust parsing
        self.assertEqual(
            convert_timestamp_to_tuple("2023-05-20T10:30:00.000+0000"), (2023, 5)
        )
        self.assertEqual(convert_timestamp_to_tuple("2024/01/15"), (2024, 1))

    def test_convert_date(self):
        self.assertEqual(convert_date("2023-05-20"), datetime(2023, 5, 20))
        self.assertEqual(convert_date("2023-05-20T12:00:00"), datetime(2023, 5, 20))

    def test_build_patient_dict(self):
        data = {
            "client_idcode": ["P1", "P2"],
            "start": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            "end": [datetime(2023, 2, 1), None],
        }
        df = pd.DataFrame(data)
        result = build_patient_dict(df, "client_idcode", "start", "end")
        self.assertEqual(len(result), 1)
        self.assertEqual(result["P1"], (datetime(2023, 1, 1), datetime(2023, 2, 1)))

    def test_add_offset_column(self):
        df = pd.DataFrame({"start": ["25/12/23 14.30.45", "2023-01-01", "invalid"]})
        offset = timedelta(days=1)
        result = add_offset_column(df, "start", "end", offset, verbose=0)
        self.assertEqual(result["end"].iloc[0], pd.Timestamp("2023-12-26 14:30:45"))
        self.assertEqual(result["end"].iloc[1], pd.Timestamp("2023-01-02 00:00:00"))
        self.assertTrue(pd.isna(result["end"].iloc[2]))

    def test_get_empty_date_vector(self):
        config = MagicMock()
        config.start_date = "2023-01-01"
        config.years = 0
        config.months = 1
        config.days = 0
        config.time_window_interval_delta = timedelta(days=30)

        with patch(
            "pat2vec.util.methods_get.generate_date_list", return_value=[(2023, 1, 1)]
        ):
            result = get_empty_date_vector(config)
            self.assertIn("(2023, 1, 1)_date_time_stamp", result.columns)
            self.assertEqual(result.shape, (1, 1))

    def test_enum_target_date_vector(self):
        config = MagicMock()
        with patch("pat2vec.util.methods_get.get_empty_date_vector") as mock_empty:
            mock_empty.return_value = pd.DataFrame(
                data=0.0, index=[0], columns=["(2023, 1, 1)_date_time_stamp"]
            )
            result = enum_target_date_vector((2023, 1, 1), "P1", config)
            self.assertEqual(result.at[0, "(2023, 1, 1)_date_time_stamp"], 1)
            self.assertEqual(result.at[0, "client_idcode"], "P1")

    def test_enum_exact_target_date_vector(self):
        result = enum_exact_target_date_vector((2023, 5, 5), "P_EXACT", None)
        col = "(2023, 5, 5)_date_time_stamp"
        self.assertIn(col, result.columns)
        self.assertEqual(result.at[0, col], 1)


class TestDummyDataLogic(unittest.TestCase):
    """Tests for pat2vec.util.get_dummy_data_cohort_searcher utility logic."""

    def test_is_safe_host(self):
        self.assertTrue(is_safe_host("localhost"))
        self.assertTrue(is_safe_host("172.17.0.1"))
        self.assertFalse(is_safe_host("api.google.com"))

    def test_maybe_nan(self):
        self.assertEqual(maybe_nan("val", probability=0), "val")
        self.assertTrue(np.isnan(maybe_nan("val", probability=1.0)))

    def test_extract_date_range(self):
        s = "updatetime:[2022-01-01 TO 2023-12-31]"
        self.assertEqual(extract_date_range(s), (2022, 1, 1, 2023, 12, 31))


if __name__ == "__main__":
    unittest.main()
