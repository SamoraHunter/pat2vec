import unittest

import pandas as pd

from pat2vec.util.dummy_data_generation.observations.textual import (
    generate_observations_MRC_text_data,
    generate_observations_Reports_text_data,
)


class TestMRCTextData(unittest.TestCase):
    def setUp(self):
        self.base_kwargs = {
            "num_rows": 3,
            "entered_list": ["CLIENT001", "CLIENT002"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }

    def test_generate_observations_MRC_text_data_returns_dataframe(self):
        result = generate_observations_MRC_text_data(**self.base_kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_observations_MRC_text_data_correct_columns(self):
        result = generate_observations_MRC_text_data(**self.base_kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
            "_id",
            "_index",
            "_score",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_observations_MRC_text_data_row_count(self):
        result = generate_observations_MRC_text_data(**self.base_kwargs)
        expected_rows = 6
        self.assertEqual(len(result), expected_rows)

    def test_generate_observations_MRC_text_data_single_client(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_observations_MRC_text_data(**kwargs)
        self.assertEqual(len(result), 3)
        self.assertTrue(all(result["client_idcode"] == "CLIENT001"))

    def test_generate_observations_MRC_text_data_multiple_clients(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001", "CLIENT002", "CLIENT003"]
        result = generate_observations_MRC_text_data(**kwargs)
        self.assertEqual(len(result), 9)

    def test_generate_observations_MRC_text_data_empty_entered_list(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = []
        result = generate_observations_MRC_text_data(**kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
            "_id",
            "_index",
            "_score",
        ]
        self.assertEqual(list(result.columns), expected_cols)
        self.assertEqual(len(result), 0)

    def test_generate_observations_MRC_text_data_custom_fields_list(self):
        kwargs = self.base_kwargs.copy()
        custom_fields = ["client_idcode", "observation_valuetext_analysed"]
        kwargs["fields_list"] = custom_fields
        result = generate_observations_MRC_text_data(**kwargs)
        self.assertEqual(list(result.columns), custom_fields)

    def test_generate_observations_MRC_text_data_zero_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 0
        result = generate_observations_MRC_text_data(**kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
            "_id",
            "_index",
            "_score",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_observations_MRC_text_data_use_gpt_false(self):
        kwargs = self.base_kwargs.copy()
        kwargs["use_GPT"] = False
        result = generate_observations_MRC_text_data(**kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_observations_MRC_text_data_date_format(self):
        result = generate_observations_MRC_text_data(**self.base_kwargs)
        date_col = result["observationdocument_recordeddtm"]
        for val in date_col:
            if pd.notna(val) and val != "":
                self.assertRegex(val, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_generate_observations_MRC_text_data_obsguid_format(self):
        result = generate_observations_MRC_text_data(**self.base_kwargs)
        guid_col = result["observation_guid"]
        for val in guid_col:
            if pd.notna(val) and val != "":
                parts = val.split("-")
                self.assertEqual(len(parts), 5)

    def test_generate_observations_MRC_text_data_clientvisit_visitidcode_format(self):
        result = generate_observations_MRC_text_data(**self.base_kwargs)
        visit_col = result["clientvisit_visitidcode"]
        for val in visit_col:
            if pd.notna(val) and val != "":
                self.assertTrue(val.startswith("visit_"))

    def test_generate_observations_MRC_text_data_obscatalogmasteritem_displayname(self):
        result = generate_observations_MRC_text_data(**self.base_kwargs)
        displayname_col = result["obscatalogmasteritem_displayname"]
        self.assertTrue(
            all(val == "AoMRC_ClinicalSummary_FT" for val in displayname_col)
        )

    def test_generate_observations_MRC_text_data_different_num_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 10
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_observations_MRC_text_data(**kwargs)
        self.assertEqual(len(result), 10)

    def test_generate_observations_MRC_text_data_all_clients_have_correct_idcodes(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        result = generate_observations_MRC_text_data(**kwargs)

        client_ids = result["client_idcode"].unique().tolist()
        expected_ids = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        self.assertEqual(sorted(client_ids), sorted(expected_ids))

    def test_generate_observations_MRC_text_data_body_analysed_not_empty(self):
        kwargs = self.base_kwargs.copy()
        kwargs["use_GPT"] = False
        result = generate_observations_MRC_text_data(**kwargs)

        body_col = result["observation_valuetext_analysed"]
        for val in body_col:
            if pd.notna(val) and val != "":
                self.assertGreater(len(val), 0)


class TestReportsTextData(unittest.TestCase):
    def setUp(self):
        self.base_kwargs = {
            "num_rows": 3,
            "entered_list": ["CLIENT001", "CLIENT002"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }

    def test_generate_observations_Reports_text_data_returns_dataframe(self):
        result = generate_observations_Reports_text_data(**self.base_kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_observations_Reports_text_data_correct_columns(self):
        result = generate_observations_Reports_text_data(**self.base_kwargs)
        expected_cols = [
            "basicobs_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_analysed",
            "textualObs",
            "updatetime",
            "clientvisit_visitidcode",
            "_id",
            "_index",
            "_score",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_observations_Reports_text_data_row_count(self):
        result = generate_observations_Reports_text_data(**self.base_kwargs)
        expected_rows = 6
        self.assertEqual(len(result), expected_rows)

    def test_generate_observations_Reports_text_data_single_client(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_observations_Reports_text_data(**kwargs)
        self.assertEqual(len(result), 3)
        self.assertTrue(all(result["client_idcode"] == "CLIENT001"))

    def test_generate_observations_Reports_text_data_multiple_clients(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001", "CLIENT002", "CLIENT003"]
        result = generate_observations_Reports_text_data(**kwargs)
        self.assertEqual(len(result), 9)

    def test_generate_observations_Reports_text_data_empty_entered_list(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = []
        result = generate_observations_Reports_text_data(**kwargs)
        expected_cols = [
            "basicobs_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_analysed",
            "textualObs",
            "updatetime",
            "clientvisit_visitidcode",
            "_id",
            "_index",
            "_score",
        ]
        self.assertEqual(list(result.columns), expected_cols)
        self.assertEqual(len(result), 0)

    def test_generate_observations_Reports_text_data_custom_fields(self):
        kwargs = self.base_kwargs.copy()
        custom_fields = ["client_idcode", "textualObs"]
        kwargs["fields_list"] = custom_fields
        result = generate_observations_Reports_text_data(**kwargs)
        self.assertEqual(list(result.columns), custom_fields)

    def test_generate_observations_Reports_text_data_zero_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 0
        result = generate_observations_Reports_text_data(**kwargs)
        expected_cols = [
            "basicobs_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_analysed",
            "textualObs",
            "updatetime",
            "clientvisit_visitidcode",
            "_id",
            "_index",
            "_score",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_observations_Reports_text_data_use_gpt_false(self):
        kwargs = self.base_kwargs.copy()
        kwargs["use_GPT"] = False
        result = generate_observations_Reports_text_data(**kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_observations_Reports_text_data_date_format(self):
        result = generate_observations_Reports_text_data(**self.base_kwargs)
        date_col = result["updatetime"]
        for val in date_col:
            if pd.notna(val) and val != "":
                self.assertRegex(val, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_generate_observations_Reports_text_data_basicobs_guid_format(self):
        result = generate_observations_Reports_text_data(**self.base_kwargs)
        guid_col = result["basicobs_guid"]
        for val in guid_col:
            if pd.notna(val) and val != "":
                parts = val.split("-")
                self.assertEqual(len(parts), 5)

    def test_generate_observations_Reports_text_data_clientvisit_visitidcode_format(
        self,
    ):
        result = generate_observations_Reports_text_data(**self.base_kwargs)
        visit_col = result["clientvisit_visitidcode"]
        for val in visit_col:
            if pd.notna(val) and val != "":
                self.assertTrue(val.startswith("visit_"))

    def test_generate_observations_Reports_text_data_basicobs_itemname_analysed(self):
        result = generate_observations_Reports_text_data(**self.base_kwargs)
        itemname_col = result["basicobs_itemname_analysed"]
        self.assertTrue(all(val == "Report" for val in itemname_col))

    def test_generate_observations_Reports_text_data_different_num_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 10
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_observations_Reports_text_data(**kwargs)
        self.assertEqual(len(result), 10)

    def test_generate_observations_Reports_text_data_all_clients_have_correct_idcodes(
        self,
    ):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        result = generate_observations_Reports_text_data(**kwargs)

        client_ids = result["client_idcode"].unique().tolist()
        expected_ids = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        self.assertEqual(sorted(client_ids), sorted(expected_ids))

    def test_generate_observations_Reports_text_data_textualObs_not_empty(self):
        kwargs = self.base_kwargs.copy()
        kwargs["use_GPT"] = False
        result = generate_observations_Reports_text_data(**kwargs)

        textual_obs_col = result["textualObs"]
        for val in textual_obs_col:
            if pd.notna(val) and val != "":
                self.assertGreater(len(val), 0)


if __name__ == "__main__":
    unittest.main()
