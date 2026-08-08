import unittest

import pandas as pd

from pat2vec.util.dummy_data_generation.observations.bed import (
    _generate_bed_assignment_for_ward,
    generate_bed_data,
)


class TestBedFunctions(unittest.TestCase):
    def setUp(self):
        self.base_kwargs = {
            "num_rows": 3,
            "entered_list": ["CLIENT001", "CLIENT002"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }

    def test_generate_bed_data_returns_dataframe(self):
        result = generate_bed_data(**self.base_kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_bed_data_correct_columns(self):

        result = generate_bed_data(**self.base_kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_bed_data_row_count(self):
        result = generate_bed_data(**self.base_kwargs)
        expected_rows = 6
        self.assertEqual(len(result), expected_rows)

    def test_generate_bed_data_single_client(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_bed_data(**kwargs)
        self.assertEqual(len(result), 3)
        self.assertTrue(all(result["client_idcode"] == "CLIENT001"))

    def test_generate_bed_data_multiple_clients(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001", "CLIENT002", "CLIENT003"]
        result = generate_bed_data(**kwargs)
        self.assertEqual(len(result), 9)

    def test_generate_bed_data_empty_entered_list(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = []
        result = generate_bed_data(**kwargs)
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

    def test_generate_bed_data_custom_fields_list(self):
        kwargs = self.base_kwargs.copy()
        custom_fields = ["client_idcode", "observation_valuetext_analysed"]
        kwargs["fields_list"] = custom_fields
        result = generate_bed_data(**kwargs)
        self.assertEqual(list(result.columns), custom_fields)

    def test_generate_bed_data_zero_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 0
        result = generate_bed_data(**kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_bed_data_date_format(self):
        result = generate_bed_data(**self.base_kwargs)
        date_col = result["observationdocument_recordeddtm"]
        for val in date_col:
            if pd.notna(val) and val != "":
                self.assertRegex(val, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_generate_bed_data_obsguid_format(self):
        result = generate_bed_data(**self.base_kwargs)
        guid_col = result["observation_guid"]
        for val in guid_col:
            if pd.notna(val) and val != "":
                parts = val.split("-")
                self.assertEqual(len(parts), 5)

    def test_generate_bed_data_clientvisit_visitidcode_format(self):
        result = generate_bed_data(**self.base_kwargs)
        visit_col = result["clientvisit_visitidcode"]
        for val in visit_col:
            if pd.notna(val) and val != "":
                self.assertTrue(val.startswith("visit_"))

    def test_generate_bed_data_obscatalogmasteritem_displayname(self):
        result = generate_bed_data(**self.base_kwargs)
        displayname_col = result["obscatalogmasteritem_displayname"]
        self.assertTrue(all(val == "CORE_BedNumber3" for val in displayname_col))

    def test_generate_bed_data_different_num_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 10
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_bed_data(**kwargs)
        self.assertEqual(len(result), 10)

    def test_generate_bed_data_deterministic_output(self):
        kwargs = self.base_kwargs.copy()
        result1 = generate_bed_data(**kwargs)
        result2 = generate_bed_data(**kwargs)
        pd.testing.assert_frame_equal(result1, result2)


class TestBedAssignmentFunctions(unittest.TestCase):
    def setUp(self):
        self.base_kwargs = {
            "num_rows": 3,
            "entered_list": ["CLIENT001", "CLIENT002"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }

    def test_generate_bed_assignment_for_ward_wd(self):
        ward_type = "WD"
        result = _generate_bed_assignment_for_ward(ward_type)

        valid_beds_wd = [
            "Bay A Bed 1",
            "Bay A Bed 2",
            "Bay A Bed 3",
            "Bay B Bed 1",
            "Bay B Bed 2",
            "Bed 1",
            "Bed 2",
            "Bed 3",
            "Side Room 1",
        ]
        self.assertIn(result, valid_beds_wd)

    def test_generate_bed_assignment_for_ward_hdu(self):
        ward_type = "HDU"
        result = _generate_bed_assignment_for_ward(ward_type)

        valid_beds_hdu = [
            "HDU Bed 1",
            "HDU Bed 2",
            "HDU Bed 3",
            "HDU Side Room",
        ]
        self.assertIn(result, valid_beds_hdu)

    def test_generate_bed_assignment_for_ward_itu(self):
        ward_type = "ITU"
        result = _generate_bed_assignment_for_ward(ward_type)

        valid_beds_itu = [
            "ITU Bed 1",
            "ITU Bed 2",
            "ITU Bed 3",
            "ITU Side Room",
        ]
        self.assertIn(result, valid_beds_itu)

    def test_generate_bed_assignment_for_ward_unknown_falls_back_to_wd(self):
        ward_type = "UNKNOWN"
        result = _generate_bed_assignment_for_ward(ward_type)

        valid_beds_wd = [
            "Bay A Bed 1",
            "Bay A Bed 2",
            "Bay A Bed 3",
            "Bay B Bed 1",
            "Bay B Bed 2",
            "Bed 1",
            "Bed 2",
            "Bed 3",
            "Side Room 1",
        ]
        self.assertIn(result, valid_beds_wd)

    def test_generate_bed_data_all_clients_have_correct_idcodes(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        result = generate_bed_data(**kwargs)

        client_ids = result["client_idcode"].unique().tolist()
        expected_ids = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        self.assertEqual(sorted(client_ids), sorted(expected_ids))

    def test_generate_bed_data_value_validity(self):
        result = generate_bed_data(**self.base_kwargs)

        value_col = result["observation_valuetext_analysed"]
        valid_beds_wd = [
            "Bay A Bed 1",
            "Bay A Bed 2",
            "Bay A Bed 3",
            "Bay B Bed 1",
            "Bay B Bed 2",
            "Bed 1",
            "Bed 2",
            "Bed 3",
            "Side Room 1",
        ]
        valid_beds_hdu = ["HDU Bed 1", "HDU Bed 2", "HDU Bed 3", "HDU Side Room"]
        valid_beds_itu = ["ITU Bed 1", "ITU Bed 2", "ITU Bed 3", "ITU Side Room"]
        all_valid_beds = valid_beds_wd + valid_beds_hdu + valid_beds_itu

        for val in value_col:
            if pd.notna(val) and val != "":
                self.assertIn(val, all_valid_beds)


class TestBedEdgeCases(unittest.TestCase):
    def test_generate_bed_data_empty_list_no_crash(self):
        kwargs = {
            "num_rows": 5,
            "entered_list": [],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }
        result = generate_bed_data(**kwargs)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)

    def test_generate_bed_data_zero_rows_no_crash(self):
        kwargs = {
            "num_rows": 0,
            "entered_list": ["CLIENT001"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }
        result = generate_bed_data(**kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_bed_data_single_row(self):
        kwargs = {
            "num_rows": 1,
            "entered_list": ["CLIENT001"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }
        result = generate_bed_data(**kwargs)
        self.assertEqual(len(result), 1)

    def test_generate_bed_data_large_row_count(self):
        kwargs = {
            "num_rows": 50,
            "entered_list": ["CLIENT001"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }
        result = generate_bed_data(**kwargs)
        self.assertEqual(len(result), 50)


if __name__ == "__main__":
    unittest.main()
