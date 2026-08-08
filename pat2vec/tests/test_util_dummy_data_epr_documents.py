import unittest

import pandas as pd

from pat2vec.util.dummy_data_generation.epr_documents import (
    generate_epr_documents_data,
    generate_epr_documents_personal_data,
)


class TestEPRDocumentsData(unittest.TestCase):
    def setUp(self):
        self.base_kwargs = {
            "num_rows": 3,
            "entered_list": ["CLIENT001", "CLIENT002"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }

    def test_generate_epr_documents_data_returns_dataframe(self):
        result = generate_epr_documents_data(**self.base_kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_epr_documents_data_correct_columns(self):
        result = generate_epr_documents_data(**self.base_kwargs)
        expected_cols = [
            "client_idcode",
            "document_guid",
            "document_description",
            "body_analysed",
            "updatetime",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_epr_documents_data_row_count(self):
        result = generate_epr_documents_data(**self.base_kwargs)
        expected_rows = 6
        self.assertEqual(len(result), expected_rows)

    def test_generate_epr_documents_data_single_client(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_epr_documents_data(**kwargs)
        self.assertEqual(len(result), 3)
        self.assertTrue(all(result["client_idcode"] == "CLIENT001"))

    def test_generate_epr_documents_data_multiple_clients(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001", "CLIENT002", "CLIENT003"]
        result = generate_epr_documents_data(**kwargs)
        self.assertEqual(len(result), 9)

    def test_generate_epr_documents_data_empty_entered_list(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = []
        result = generate_epr_documents_data(**kwargs)
        expected_cols = [
            "client_idcode",
            "document_guid",
            "document_description",
            "body_analysed",
            "updatetime",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)
        self.assertEqual(len(result), 0)

    def test_generate_epr_documents_data_custom_fields_list(self):
        kwargs = self.base_kwargs.copy()
        custom_fields = ["client_idcode", "body_analysed"]
        kwargs["fields_list"] = custom_fields
        result = generate_epr_documents_data(**kwargs)
        self.assertEqual(list(result.columns), custom_fields)

    def test_generate_epr_documents_data_zero_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 0
        result = generate_epr_documents_data(**kwargs)
        expected_cols = [
            "client_idcode",
            "document_guid",
            "document_description",
            "body_analysed",
            "updatetime",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_epr_documents_data_use_gpt_false(self):
        kwargs = self.base_kwargs.copy()
        kwargs["use_GPT"] = False
        result = generate_epr_documents_data(**kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_epr_documents_data_date_format(self):
        result = generate_epr_documents_data(**self.base_kwargs)
        date_col = result["updatetime"]
        for val in date_col:
            if pd.notna(val) and val != "":
                self.assertRegex(val, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_generate_epr_documents_data_document_guid_format(self):
        result = generate_epr_documents_data(**self.base_kwargs)
        guid_col = result["document_guid"]
        for val in guid_col:
            if pd.notna(val) and val != "":
                parts = val.split("-")
                self.assertEqual(len(parts), 1)

    def test_generate_epr_documents_data_document_description(self):
        result = generate_epr_documents_data(**self.base_kwargs)
        desc_col = result["document_description"]
        self.assertTrue(all(val == "clinical_note_summary" for val in desc_col))

    def test_generate_epr_documents_data_clientvisit_visitidcode_format(self):
        result = generate_epr_documents_data(**self.base_kwargs)
        visit_col = result["clientvisit_visitidcode"]
        for val in visit_col:
            if pd.notna(val) and val != "":
                parts = val.split("-")
                self.assertEqual(len(parts), 1)

    def test_generate_epr_documents_data_different_num_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 10
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_epr_documents_data(**kwargs)
        self.assertEqual(len(result), 10)

    def test_generate_epr_documents_data_all_clients_have_correct_idcodes(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        result = generate_epr_documents_data(**kwargs)

        client_ids = result["client_idcode"].unique().tolist()
        expected_ids = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        self.assertEqual(sorted(client_ids), sorted(expected_ids))

    def test_generate_epr_documents_data_body_analysed_not_empty(self):
        kwargs = self.base_kwargs.copy()
        kwargs["use_GPT"] = False
        result = generate_epr_documents_data(**kwargs)

        body_col = result["body_analysed"]
        for val in body_col:
            if pd.notna(val):
                self.assertGreater(len(val), 0)


class TestEPRDocumentsPersonalData(unittest.TestCase):
    def setUp(self):
        self.base_kwargs = {
            "num_rows": 3,
            "entered_list": ["CLIENT001", "CLIENT002"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }

    def test_generate_epr_documents_personal_data_returns_dataframe(self):
        result = generate_epr_documents_personal_data(**self.base_kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_epr_documents_personal_data_correct_columns(self):
        result = generate_epr_documents_personal_data(**self.base_kwargs)
        expected_cols = [
            "client_idcode",
            "client_firstname",
            "client_lastname",
            "client_dob",
            "client_gendercode",
            "client_racecode",
            "client_deceaseddtm",
            "updatetime",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_epr_documents_personal_data_row_count(self):
        result = generate_epr_documents_personal_data(**self.base_kwargs)
        expected_rows = 6
        self.assertEqual(len(result), expected_rows)

    def test_generate_epr_documents_personal_data_single_client(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_epr_documents_personal_data(**kwargs)
        self.assertEqual(len(result), 3)
        self.assertTrue(all(result["client_idcode"] == "CLIENT001"))

    def test_generate_epr_documents_personal_data_empty_entered_list(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = []
        result = generate_epr_documents_personal_data(**kwargs)
        expected_cols = [
            "client_idcode",
            "client_firstname",
            "client_lastname",
            "client_dob",
            "client_gendercode",
            "client_racecode",
            "client_deceaseddtm",
            "updatetime",
        ]
        self.assertEqual(list(result.columns), expected_cols)
        self.assertEqual(len(result), 0)

    def test_generate_epr_documents_personal_data_custom_fields(self):
        kwargs = self.base_kwargs.copy()
        custom_fields = ["client_idcode", "client_firstname"]
        kwargs["fields_list"] = custom_fields
        result = generate_epr_documents_personal_data(**kwargs)
        self.assertEqual(list(result.columns), custom_fields)

    def test_generate_epr_documents_personal_data_date_format(self):
        result = generate_epr_documents_personal_data(**self.base_kwargs)
        date_col = result["updatetime"]
        for val in date_col:
            if pd.notna(val) and val != "":
                self.assertRegex(val, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_generate_epr_documents_personal_data_client_dob_format(self):
        result = generate_epr_documents_personal_data(**self.base_kwargs)
        dob_col = result["client_dob"]
        for val in dob_col:
            if pd.notna(val) and val != "":
                self.assertRegex(val, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_generate_epr_documents_personal_data_client_gendercode_values(self):
        result = generate_epr_documents_personal_data(**self.base_kwargs)
        gender_col = result["client_gendercode"]
        valid_genders = {"male", "female"}
        for val in gender_col:
            if pd.notna(val) and val != "":
                self.assertIn(val, valid_genders)

    def test_generate_epr_documents_personal_data_different_num_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 5
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_epr_documents_personal_data(**kwargs)
        self.assertEqual(len(result), 5)

    def test_generate_epr_documents_personal_data_all_clients_have_correct_idcodes(
        self,
    ):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        result = generate_epr_documents_personal_data(**kwargs)

        client_ids = result["client_idcode"].unique().tolist()
        expected_ids = ["CLIENT_A", "CLIENT_B", "CLIENT_C"]
        self.assertEqual(sorted(client_ids), sorted(expected_ids))


if __name__ == "__main__":
    unittest.main()
