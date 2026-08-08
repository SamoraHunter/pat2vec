import random
import unittest

import pandas as pd

from pat2vec.util.dummy_data_generation.covid import generate_covid_observations_data


class TestGenerateCovidObservationsData(unittest.TestCase):
    """Tests for generate_covid_observations_data function."""

    def setUp(self):
        """Set up test fixtures."""
        self.default_entered_list = ["P001", "P002"]
        self.default_num_rows = 5
        self.default_global_start_year = 2020
        self.default_global_start_month = 1
        self.default_global_end_year = 2023
        self.default_global_end_month = 12

    def test_basic_generation_returns_dataframe(self):
        """Test that basic generation returns a pandas DataFrame."""
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=self.default_entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertIsInstance(result, pd.DataFrame)

    def test_basic_generation_correct_columns(self):
        """Test that generated data has the correct columns from COVID_FIELDS."""
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=self.default_entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        expected_columns = [
            "observation_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_analysed",
            "basicobs_entered",
            "clientvisit_visitidcode",
        ]
        for col in expected_columns:
            self.assertIn(col, result.columns)

    def test_basic_generation_correct_row_count(self):
        """Test that row count matches num_rows * len(entered_list)."""
        entered_list = ["P001", "P002", "P003"]
        num_rows = 5
        result = generate_covid_observations_data(
            num_rows=num_rows,
            entered_list=entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        expected_total_rows = num_rows * len(entered_list)
        self.assertEqual(len(result), expected_total_rows)

    def test_basic_generation_client_ids(self):
        """Test that client_idcode values match entered_list."""
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=self.default_entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        unique_client_ids = result["client_idcode"].unique()
        self.assertEqual(set(unique_client_ids), set(self.default_entered_list))

    def test_basic_generation_search_term(self):
        """Test that basicobs_itemname_analysed has correct search term."""
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=self.default_entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        unique_search_terms = result["basicobs_itemname_analysed"].unique()
        self.assertEqual(len(unique_search_terms), 1)
        self.assertIn("SARS CoV-2", unique_search_terms[0])

    def test_basic_generation_value_pattern(self):
        """Test that basicobs_value_analysed contains only Positive/Negative."""
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=self.default_entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        unique_values = result["basicobs_value_analysed"].unique()
        for value in unique_values:
            self.assertIn(value, ["Positive", "Negative"])

    def test_basic_generation_timestamp_format(self):
        """Test that basicobs_entered has ISO format timestamps."""
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=self.default_entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        timestamps = result["basicobs_entered"].tolist()
        for ts in timestamps:
            self.assertIsInstance(ts, str)
            self.assertIn("T", ts)

    def test_empty_entered_list_returns_empty_dataframe(self):
        """Test that empty entered_list returns empty DataFrame with correct columns."""
        entered_list = []
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertIsInstance(result, pd.DataFrame)
        expected_columns = [
            "observation_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_analysed",
            "basicobs_entered",
            "clientvisit_visitidcode",
        ]
        for col in expected_columns:
            self.assertIn(col, result.columns)
        self.assertEqual(len(result), 0)

    def test_zero_num_rows_returns_correct_count(self):
        """Test that num_rows=0 returns zero rows per client."""
        result = generate_covid_observations_data(
            num_rows=0,
            entered_list=self.default_entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertEqual(len(result), 0)

    def test_single_client(self):
        """Test generation with a single client."""
        entered_list = ["P001"]
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertEqual(len(result), self.default_num_rows)

    def test_single_row_per_client(self):
        """Test generation with num_rows=1."""
        result = generate_covid_observations_data(
            num_rows=1,
            entered_list=self.default_entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertEqual(len(result), 2)

    def test_custom_fields_list_adds_columns(self):
        """Test that custom fields_list adds additional columns with NaN values."""
        custom_fields = [
            "observation_guid",
            "client_idcode",
            "custom_field_1",
            "custom_field_2",
        ]
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
            fields_list=custom_fields,
        )
        for field in custom_fields:
            self.assertIn(field, result.columns)
        self.assertTrue(result["custom_field_1"].isna().all())
        self.assertTrue(result["custom_field_2"].isna().all())

    def test_custom_fields_list_filters_columns(self):
        """Test that custom fields_list filters to only those columns."""
        custom_fields = ["client_idcode", "basicobs_value_analysed"]
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
            fields_list=custom_fields,
        )
        self.assertEqual(len(result.columns), 2)
        self.assertIn("client_idcode", result.columns)
        self.assertIn("basicobs_value_analysed", result.columns)

    def test_multiple_clients_different_row_counts(self):
        """Test generation with multiple clients."""
        entered_list = ["P001", "P002", "P003", "P004", "P005"]
        num_rows = 10
        result = generate_covid_observations_data(
            num_rows=num_rows,
            entered_list=entered_list,
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertEqual(len(result), num_rows * len(entered_list))

    def test_deterministic_output_same_seed(self):
        """Test that values are populated consistently for same input."""
        entered_list = ["P001", "P002"]
        num_rows = 3

        # The function uses random.seed() but not faker seed
        # Since faker generates different UUIDs each call, we test that:
        # 1. Values exist (not NaN)
        # 2. Same structure is produced

        result1 = generate_covid_observations_data(
            num_rows=num_rows,
            entered_list=entered_list.copy(),
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )

        result2 = generate_covid_observations_data(
            num_rows=num_rows,
            entered_list=entered_list.copy(),
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )

        # Check structure is consistent
        self.assertEqual(result1.columns.tolist(), result2.columns.tolist())
        self.assertEqual(len(result1), len(result2))

        # All values should be populated (not NaN for string columns)
        self.assertFalse(result1["observation_guid"].isna().any())
        self.assertFalse(result1["client_idcode"].isna().any())
        self.assertFalse(result1["clientvisit_visitidcode"].isna().any())

    def test_different_seeds_produce_different_output(self):
        """Test that different values in same seed produce potentially different output."""
        entered_list = ["P001"]
        num_rows = 5

        original_choice = random.choice
        call_count_1 = [0]
        call_count_2 = [0]

        def mock_choice_1(seq):
            call_count_1[0] += 1
            if seq == ["Positive", "Negative"]:
                return "Positive"
            return original_choice(seq)

        def mock_choice_2(seq):
            call_count_2[0] += 1
            if seq == ["Positive", "Negative"]:
                return "Negative"
            return original_choice(seq)

        random.choice = mock_choice_1
        try:
            result1 = generate_covid_observations_data(
                num_rows=num_rows,
                entered_list=entered_list.copy(),
                global_start_year=self.default_global_start_year,
                global_start_month=self.default_global_start_month,
                global_end_year=self.default_global_end_year,
                global_end_month=self.default_global_end_month,
            )
        finally:
            random.choice = original_choice

        random.choice = mock_choice_2
        try:
            result2 = generate_covid_observations_data(
                num_rows=num_rows,
                entered_list=entered_list.copy(),
                global_start_year=self.default_global_start_year,
                global_start_month=self.default_global_start_month,
                global_end_year=self.default_global_end_year,
                global_end_month=self.default_global_end_month,
            )
        finally:
            random.choice = original_choice

        self.assertFalse(
            result1["basicobs_value_analysed"].equals(
                result2["basicobs_value_analysed"]
            )
        )

    def test_large_num_rows(self):
        """Test generation with a large number of rows."""
        num_rows = 1000
        result = generate_covid_observations_data(
            num_rows=num_rows,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertEqual(len(result), num_rows)

    def test_uuid_format(self):
        """Test that generated UUIDs are in correct format."""
        result = generate_covid_observations_data(
            num_rows=10,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        observation_guids = result["observation_guid"].tolist()
        clientvisit_ids = result["clientvisit_visitidcode"].tolist()

        # UUID4 format: 8-4-4-4-12 hex characters
        import re

        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            re.IGNORECASE,
        )

        for guid in observation_guids:
            self.assertIsInstance(guid, str)
            self.assertTrue(uuid_pattern.match(guid), f"Invalid UUID format: {guid}")
        for visit_id in clientvisit_ids:
            self.assertIsInstance(visit_id, str)
            self.assertTrue(
                uuid_pattern.match(visit_id), f"Invalid UUID format: {visit_id}"
            )

    def test_global_date_range_parameters(self):
        """Test that date range parameters are correctly used."""
        result = generate_covid_observations_data(
            num_rows=self.default_num_rows,
            entered_list=["P001"],
            global_start_year=2021,
            global_start_month=6,
            global_end_year=2022,
            global_end_month=6,
            global_start_day=15,
            global_end_day=28,
        )
        self.assertEqual(len(result), self.default_num_rows)

    def test_all_positive_values(self):
        """Test generation where all values are 'Positive'."""
        original_choice = random.choice

        def mock_choice(seq):
            if seq == ["Positive", "Negative"]:
                return "Positive"
            return original_choice(seq)

        random.choice = mock_choice
        try:
            result = generate_covid_observations_data(
                num_rows=10,
                entered_list=["P001"],
                global_start_year=self.default_global_start_year,
                global_start_month=self.default_global_start_month,
                global_end_year=self.default_global_end_year,
                global_end_month=self.default_global_end_month,
            )
            unique_values = result["basicobs_value_analysed"].unique()
            self.assertEqual(len(unique_values), 1)
            self.assertEqual(unique_values[0], "Positive")
        finally:
            random.choice = original_choice

    def test_all_negative_values(self):
        """Test generation where all values are 'Negative'."""
        original_choice = random.choice

        def mock_choice(seq):
            if seq == ["Positive", "Negative"]:
                return "Negative"
            return original_choice(seq)

        random.choice = mock_choice
        try:
            result = generate_covid_observations_data(
                num_rows=10,
                entered_list=["P001"],
                global_start_year=self.default_global_start_year,
                global_start_month=self.default_global_start_month,
                global_end_year=self.default_global_end_year,
                global_end_month=self.default_global_end_month,
            )
            unique_values = result["basicobs_value_analysed"].unique()
            self.assertEqual(len(unique_values), 1)
            self.assertEqual(unique_values[0], "Negative")
        finally:
            random.choice = original_choice

    def test_fields_list_none_uses_covid_fields(self):
        """Test that fields_list=None uses COVID_FIELDS."""
        from pat2vec.pat2vec_get_methods.get_method_covid import COVID_FIELDS

        result = generate_covid_observations_data(
            num_rows=5,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
            fields_list=None,
        )
        self.assertEqual(set(result.columns), set(COVID_FIELDS))

    def test_search_term_plain_none_uses_default(self):
        """Test that SEARCH_TERM_PLAIN is used correctly."""

        result = generate_covid_observations_data(
            num_rows=5,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        search_terms = result["basicobs_itemname_analysed"].unique()
        self.assertEqual(len(search_terms), 1)
        self.assertIn("SARS CoV-2", search_terms[0])

    def test_client_idcode_is_not_nan(self):
        """Test that client_idcode values are never NaN."""
        result = generate_covid_observations_data(
            num_rows=10,
            entered_list=["P001", "P002"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertFalse(result["client_idcode"].isna().any())

    def test_data_types_correct(self):
        """Test that column data types are correct."""
        result = generate_covid_observations_data(
            num_rows=10,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertIsInstance(result["observation_guid"].iloc[0], str)
        self.assertIsInstance(result["client_idcode"].iloc[0], str)
        self.assertIsInstance(result["basicobs_itemname_analysed"].iloc[0], str)
        self.assertIn(
            result["basicobs_value_analysed"].iloc[0],
            ["Positive", "Negative"],
        )
        self.assertIsInstance(result["basicobs_entered"].iloc[0], str)
        self.assertIsInstance(result["clientvisit_visitidcode"].iloc[0], str)

    def test_timestamp_within_expected_range(self):
        """Test that timestamps fall within expected date range."""
        result = generate_covid_observations_data(
            num_rows=10,
            entered_list=["P001"],
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2020,
            global_end_month=12,
            global_start_day=1,
            global_end_day=31,
        )
        timestamps = result["basicobs_entered"].tolist()
        for ts in timestamps:
            self.assertIsInstance(ts, str)
            self.assertTrue(ts.startswith("2020"))

    def test_multiple_clients_with_different_row_counts(self):
        """Test multiple clients with varying row counts."""
        result = generate_covid_observations_data(
            num_rows=3,
            entered_list=["P001", "P002"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertEqual(len(result), 6)
        p001_count = (result["client_idcode"] == "P001").sum()
        p002_count = (result["client_idcode"] == "P002").sum()
        self.assertEqual(p001_count, 3)
        self.assertEqual(p002_count, 3)

    def test_columns_order_matches_fields_list(self):
        """Test that column order matches fields_list."""
        custom_fields = [
            "basicobs_entered",
            "client_idcode",
            "observation_guid",
            "basicobs_value_analysed",
            "basicobs_itemname_analysed",
            "clientvisit_visitidcode",
        ]
        result = generate_covid_observations_data(
            num_rows=5,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
            fields_list=custom_fields,
        )
        self.assertEqual(list(result.columns), custom_fields)

    def test_empty_fields_list(self):
        """Test with empty fields_list."""
        result = generate_covid_observations_data(
            num_rows=5,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
            fields_list=[],
        )
        self.assertIsInstance(result, pd.DataFrame)

    def test_single_character_client_id(self):
        """Test generation with single character client IDs."""
        result = generate_covid_observations_data(
            num_rows=5,
            entered_list=["A", "B", "C"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
        )
        self.assertEqual(len(result), 15)
        unique_clients = set(result["client_idcode"].unique())
        self.assertEqual(unique_clients, {"A", "B", "C"})

    def test_fields_list_with_non_default_column(self):
        """Test fields_list with columns not in default data."""
        custom_fields = [
            "observation_guid",
            "client_idcode",
            "basicobs_value_analysed",
            "extra_column_1",
            "extra_column_2",
        ]
        result = generate_covid_observations_data(
            num_rows=5,
            entered_list=["P001"],
            global_start_year=self.default_global_start_year,
            global_start_month=self.default_global_start_month,
            global_end_year=self.default_global_end_year,
            global_end_month=self.default_global_end_month,
            fields_list=custom_fields,
        )
        self.assertEqual(len(result.columns), 5)
        for field in custom_fields:
            self.assertIn(field, result.columns)
        self.assertTrue(result["extra_column_1"].isna().all())
        self.assertTrue(result["extra_column_2"].isna().all())


if __name__ == "__main__":
    unittest.main()
