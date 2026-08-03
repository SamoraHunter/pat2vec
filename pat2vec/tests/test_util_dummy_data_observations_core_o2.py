import unittest
import pandas as pd
from pat2vec.util.dummy_data_generation.observations.core_o2 import (
    generate_core_o2_data,
    generate_clinically_coherent_spO2,
)


class TestCoreO2Functions(unittest.TestCase):
    def setUp(self):
        self.base_kwargs = {
            "num_rows": 5,
            "entered_list": ["CLIENT001", "CLIENT002"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }

    def test_generate_core_o2_data_returns_dataframe(self):
        result = generate_core_o2_data(**self.base_kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_core_o2_data_correct_columns(self):
        result = generate_core_o2_data(**self.base_kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_core_o2_data_row_count(self):
        result = generate_core_o2_data(**self.base_kwargs)
        expected_rows = 10
        self.assertEqual(len(result), expected_rows)

    def test_generate_core_o2_data_single_client(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_core_o2_data(**kwargs)
        self.assertEqual(len(result), 5)
        self.assertTrue(all(result["client_idcode"] == "CLIENT001"))

    def test_generate_core_o2_data_multiple_clients(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001", "CLIENT002", "CLIENT003"]
        result = generate_core_o2_data(**kwargs)
        self.assertEqual(len(result), 15)

    def test_generate_core_o2_data_empty_entered_list(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = []
        result = generate_core_o2_data(**kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)
        self.assertEqual(len(result), 0)

    def test_generate_core_o2_data_custom_fields_list(self):
        kwargs = self.base_kwargs.copy()
        custom_fields = ["client_idcode", "observation_valuetext_analysed"]
        kwargs["fields_list"] = custom_fields
        result = generate_core_o2_data(**kwargs)
        self.assertEqual(list(result.columns), custom_fields)

    def test_generate_core_o2_data_allows_more_fields(self):
        kwargs = self.base_kwargs.copy()
        extra_fields = [
            "client_idcode",
            "observation_valuetext_analysed",
            "extra_field_1",
            "extra_field_2",
        ]
        kwargs["fields_list"] = extra_fields
        result = generate_core_o2_data(**kwargs)
        self.assertEqual(list(result.columns), extra_fields)

    def test_generate_core_o2_data_default_fields_list(self):
        from pat2vec.pat2vec_get_methods.get_method_core02 import CORE_O2_FIELDS

        kwargs = self.base_kwargs.copy()
        kwargs["fields_list"] = CORE_O2_FIELDS
        result = generate_core_o2_data(**kwargs)
        self.assertEqual(list(result.columns), CORE_O2_FIELDS)

    def test_generate_core_o2_data_date_format(self):
        result = generate_core_o2_data(**self.base_kwargs)
        date_col = result["observationdocument_recordeddtm"]
        for val in date_col:
            if pd.notna(val) and val != "":
                self.assertRegex(val, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_generate_core_o2_data_obsguid_format(self):
        result = generate_core_o2_data(**self.base_kwargs)
        guid_col = result["observation_guid"]
        for val in guid_col:
            if pd.notna(val) and val != "":
                parts = val.split("-")
                self.assertEqual(len(parts), 4)
                self.assertEqual(len(parts[0]), 4)
                self.assertTrue(all(len(p) == 4 for p in parts))

    def test_generate_core_o2_data_clientvisit_visitidcode_format(self):
        result = generate_core_o2_data(**self.base_kwargs)
        visit_col = result["clientvisit_visitidcode"]
        for val in visit_col:
            if pd.notna(val) and val != "":
                self.assertTrue(val.startswith("visit_"))

    def test_generate_core_o2_data_observation_valuetext_analysed_format(self):
        result = generate_core_o2_data(**self.base_kwargs)
        value_col = result["observation_valuetext_analysed"]
        for val in value_col:
            if pd.notna(val) and val != "":
                self.assertTrue(
                    val.endswith("%")
                    or "OnAir" in val
                    or "O2 NP" in val
                    or "NRB Mask" in val
                )

    def test_generate_core_o2_data_obscatalogmasteritem_displayname(self):
        result = generate_core_o2_data(**self.base_kwargs)
        displayname_col = result["obscatalogmasteritem_displayname"]
        self.assertTrue(all(val == "CORE_SpO2" for val in displayname_col))

    def test_generate_core_o2_data_different_num_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 10
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_core_o2_data(**kwargs)
        self.assertEqual(len(result), 10)

    def test_generate_clinically_coherent_spO2_returns_string(self):
        result = generate_clinically_coherent_spO2("normal")
        self.assertIsInstance(result, str)

    def test_generate_clinically_coherent_spO2_normal_severity(self):
        result = generate_clinically_coherent_spO2("normal")
        self.assertTrue(
            result == "OnAir" or (result.endswith("%") and int(result[:-1]) >= 95)
        )

    def test_generate_clinically_coherent_spO2_mild_hypoxia(self):
        result = generate_clinically_coherent_spO2("mild_hypoxia")
        self.assertTrue(
            result in ["2L O2 NP", "4L O2 NP"]
            or (result.endswith("%") and 90 <= int(result[:-1]) <= 94)
        )

    def test_generate_clinically_coherent_spO2_moderate_hypoxia(self):
        result = generate_clinically_coherent_spO2("moderate_hypoxia")
        self.assertTrue(
            result in ["2L O2 NP", "4L O2 NP", "NRB Mask"]
            or (result.endswith("%") and 86 <= int(result[:-1]) <= 89)
        )

    def test_generate_clinically_coherent_spO2_severe_hypoxia(self):
        result = generate_clinically_coherent_spO2("severe_hypoxia")
        self.assertTrue(
            result == "NRB Mask"
            or (result.endswith("%") and 70 <= int(result[:-1]) <= 85)
        )

    def test_generate_clinically_coherent_spO2_default_severity(self):
        result = generate_clinically_coherent_spO2()
        self.assertIsInstance(result, str)

    def test_generate_core_o2_data_deterministic_output(self):
        kwargs = self.base_kwargs.copy()
        result1 = generate_core_o2_data(**kwargs)
        result2 = generate_core_o2_data(**kwargs)
        pd.testing.assert_frame_equal(result1, result2)


if __name__ == "__main__":
    unittest.main()
