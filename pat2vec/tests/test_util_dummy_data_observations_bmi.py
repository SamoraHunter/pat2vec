import unittest

import pandas as pd

from pat2vec.util.dummy_data_generation.observations.bmi import (
    generate_bmi_data,
    generate_uuid_string,
)


class TestBMIFunctions(unittest.TestCase):
    def setUp(self):
        self.base_kwargs = {
            "num_rows": 5,
            "entered_list": ["CLIENT001", "CLIENT002"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }

    def test_generate_bmi_data_returns_dataframe(self):
        result = generate_bmi_data(**self.base_kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_bmi_data_correct_columns(self):

        result = generate_bmi_data(**self.base_kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
            "obscatalogmasteritem_unitofmeasure",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_bmi_data_row_count(self):
        result = generate_bmi_data(**self.base_kwargs)
        expected_rows = 10
        self.assertEqual(len(result), expected_rows)

    def test_generate_bmi_data_single_client(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_bmi_data(**kwargs)
        self.assertEqual(len(result), 5)
        self.assertTrue(all(result["client_idcode"] == "CLIENT001"))

    def test_generate_bmi_data_multiple_clients(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001", "CLIENT002", "CLIENT003"]
        result = generate_bmi_data(**kwargs)
        self.assertEqual(len(result), 15)

    def test_generate_bmi_data_empty_entered_list(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = []
        result = generate_bmi_data(**kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
            "obscatalogmasteritem_unitofmeasure",
        ]
        self.assertEqual(list(result.columns), expected_cols)
        self.assertEqual(len(result), 0)

    def test_generate_bmi_data_custom_fields_list(self):
        kwargs = self.base_kwargs.copy()
        custom_fields = ["client_idcode", "observation_valuetext_analysed"]
        kwargs["fields_list"] = custom_fields
        result = generate_bmi_data(**kwargs)
        self.assertEqual(list(result.columns), custom_fields)

    def test_generate_bmi_data_zero_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 0
        result = generate_bmi_data(**kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
            "obscatalogmasteritem_unitofmeasure",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_bmi_data_date_format(self):
        result = generate_bmi_data(**self.base_kwargs)
        date_col = result["observationdocument_recordeddtm"]
        for val in date_col:
            if pd.notna(val) and val != "":
                self.assertRegex(val, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_generate_bmi_data_obsguid_format(self):
        result = generate_bmi_data(**self.base_kwargs)
        guid_col = result["observation_guid"]
        for val in guid_col:
            if pd.notna(val) and val != "":
                parts = val.split("-")
                self.assertEqual(len(parts), 5)
                self.assertTrue(
                    (all(len(p) in [8, 4] for p in parts[:4]) and len(parts[4]) == 12)
                    or all(len(p) == 4 for p in parts[:3]),
                )

    def test_generate_bmi_data_clientvisit_visitidcode_format(self):
        result = generate_bmi_data(**self.base_kwargs)
        visit_col = result["clientvisit_visitidcode"]
        for val in visit_col:
            if pd.notna(val) and val != "":
                self.assertTrue(val.startswith("visit_"))

    def test_generate_bmi_data_observation_valuetext_analysed_format(self):
        result = generate_bmi_data(**self.base_kwargs)
        value_col = result["observation_valuetext_analysed"]
        for val in value_col:
            if pd.notna(val) and val != "":
                try:
                    float(val)
                except ValueError:
                    self.fail(f"Value {val} is not a valid number")

    def test_generate_bmi_data_obscatalogmasteritem_displayname_values(self):
        result = generate_bmi_data(**self.base_kwargs)
        displayname_col = result["obscatalogmasteritem_displayname"]
        valid_types = {"OBS BMI Calculation", "OBS Weight", "OBS Height"}
        for val in displayname_col:
            self.assertIn(val, valid_types)

    def test_generate_bmi_data_different_num_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 10
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_bmi_data(**kwargs)
        self.assertEqual(len(result), 10)

    def test_generate_bmi_data_deterministic_output(self):
        kwargs = self.base_kwargs.copy()
        result1 = generate_bmi_data(**kwargs)
        result2 = generate_bmi_data(**kwargs)
        pd.testing.assert_frame_equal(result1, result2)

    def test_generate_uuid_string_returns_valid_format(self):
        uuid_str = generate_uuid_string(42)
        parts = uuid_str.split("-")
        self.assertEqual(len(parts), 5)
        self.assertEqual(len(parts[0]), 8)
        self.assertEqual(len(parts[1]), 4)
        self.assertEqual(len(parts[2]), 4)
        self.assertEqual(len(parts[3]), 4)
        self.assertEqual(len(parts[4]), 12)

    def test_generate_uuid_string_deterministic(self):
        uuid1 = generate_uuid_string(100)
        uuid2 = generate_uuid_string(100)
        self.assertEqual(uuid1, uuid2)

    def test_generate_bmi_data_all_observation_types_present(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 3
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_bmi_data(**kwargs)

        obs_types = result["obscatalogmasteritem_displayname"].tolist()
        self.assertIn("OBS BMI Calculation", obs_types)
        self.assertIn("OBS Weight", obs_types)
        self.assertIn("OBS Height", obs_types)

    def test_generate_bmi_data_value_ranges(self):
        result = generate_bmi_data(**self.base_kwargs)

        for idx, row in result.iterrows():
            value_text = row["observation_valuetext_analysed"]
            if pd.notna(value_text) and isinstance(value_text, (int, float)):
                val = float(value_text)
            elif pd.notna(value_text):
                try:
                    val = float(value_text)
                except (ValueError, TypeError):
                    continue
            else:
                continue

            obs_type = row["obscatalogmasteritem_displayname"]
            if "BMI" in obs_type:
                self.assertGreaterEqual(val, 12.0)
                self.assertLessEqual(val, 50.0)
            elif "Weight" in obs_type:
                self.assertGreaterEqual(val, 35.0)
                self.assertLessEqual(val, 200.0)
            elif "Height" in obs_type:
                self.assertGreaterEqual(val, 120.0)
                self.assertLessEqual(val, 230.0)


class TestBMIEdgeCases(unittest.TestCase):
    def test_generate_bmi_data_empty_list_no_crash(self):
        kwargs = {
            "num_rows": 5,
            "entered_list": [],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }
        result = generate_bmi_data(**kwargs)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)

    def test_generate_bmi_data_zero_rows_no_crash(self):
        kwargs = {
            "num_rows": 0,
            "entered_list": ["CLIENT001"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }
        result = generate_bmi_data(**kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_bmi_data_single_row(self):
        kwargs = {
            "num_rows": 1,
            "entered_list": ["CLIENT001"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }
        result = generate_bmi_data(**kwargs)
        self.assertEqual(len(result), 1)

    def test_generate_bmi_data_large_row_count(self):
        kwargs = {
            "num_rows": 50,
            "entered_list": ["CLIENT001"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }
        result = generate_bmi_data(**kwargs)
        self.assertEqual(len(result), 50)

    def test_generate_bmi_data_custom_date_range(self):
        kwargs = {
            "num_rows": 3,
            "entered_list": ["CLIENT001"],
            "global_start_year": 2018,
            "global_start_month": 6,
            "global_end_year": 2019,
            "global_end_month": 6,
        }
        result = generate_bmi_data(**kwargs)
        self.assertEqual(len(result), 3)


if __name__ == "__main__":
    unittest.main()
