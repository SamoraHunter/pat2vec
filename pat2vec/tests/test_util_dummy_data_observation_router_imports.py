import unittest

import pandas as pd

from pat2vec.util.dummy_data_generation.observation_router_imports import (
    generate_epic_medical_history_data,
)


class TestEpicMedicalHistoryData(unittest.TestCase):
    def test_function_is_callable(self):
        """Test that the imported fallback function is callable."""
        self.assertTrue(callable(generate_epic_medical_history_data))

    def test_fallback_returns_dataframe(self):
        """Test that fallback implementation returns a pandas DataFrame."""
        result = generate_epic_medical_history_data(
            num_rows=5,
            entered_list=["P1", "P2"],
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )
        self.assertIsInstance(result, pd.DataFrame)

    def test_fallback_with_empty_entered_list(self):
        """Test fallback implementation with empty entered_list."""
        result = generate_epic_medical_history_data(
            num_rows=5,
            entered_list=[],
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)

    def test_fallback_returns_required_columns(self):
        """Test that fallback returns DataFrame with required columns."""
        result = generate_epic_medical_history_data(
            num_rows=3,
            entered_list=["P1"],
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )

        required_columns = [
            "document_PatientDurableKey",
            "document_CreatedWhen",
            "document_Diagnosis",
            "document_Name",
            "id",
        ]
        for col in required_columns:
            self.assertIn(col, result.columns)

    def test_fallback_num_rows(self):
        """Test that fallback returns correct number of rows."""
        result = generate_epic_medical_history_data(
            num_rows=10,
            entered_list=["P1", "P2"],
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )
        self.assertEqual(len(result), 20)

    def test_fallback_patient_keys(self):
        """Test that fallback returns correct patient keys."""
        client_ids = ["P1", "P2", "P3"]
        result = generate_epic_medical_history_data(
            num_rows=2,
            entered_list=client_ids,
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )
        unique_keys = result["document_PatientDurableKey"].unique()
        self.assertCountEqual(unique_keys, client_ids)

    def test_fallback_with_custom_fields_list(self):
        """Test fallback with custom fields_list."""
        custom_fields = [
            "document_PatientDurableKey",
            "document_CreatedWhen",
            "custom_field_1",
        ]
        result = generate_epic_medical_history_data(
            num_rows=2,
            entered_list=["P1"],
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
            fields_list=custom_fields,
        )
        for col in custom_fields:
            self.assertIn(col, result.columns)

    def test_fallback_date_format(self):
        """Test that fallback returns dates in correct format."""
        result = generate_epic_medical_history_data(
            num_rows=5,
            entered_list=["P1"],
            global_start_year=2020,
            global_start_month=6,
            global_end_year=2023,
            global_end_month=12,
        )
        date_values = result["document_CreatedWhen"].tolist()
        for date_val in date_values:
            self.assertIsInstance(date_val, str)
            self.assertIn("T", date_val)

    def test_fallback_id_format(self):
        """Test that fallback generates UUID-like IDs."""
        result = generate_epic_medical_history_data(
            num_rows=5,
            entered_list=["P1"],
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )
        id_values = result["id"].tolist()
        self.assertEqual(len(id_values), 5)
        for id_val in id_values:
            self.assertIsInstance(id_val, str)
            self.assertLessEqual(len(id_val), 8)

    def test_fallback_diagnosis_format(self):
        """Test that fallback generates diagnosis values."""
        result = generate_epic_medical_history_data(
            num_rows=10,
            entered_list=["P1"],
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )
        diagnosis_values = result["document_Diagnosis"].tolist()
        for diag in diagnosis_values:
            self.assertTrue(diag.startswith("Condition_"))

    def test_fallback_note_format(self):
        """Test that fallback generates note names."""
        result = generate_epic_medical_history_data(
            num_rows=10,
            entered_list=["P1"],
            global_start_year=2020,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )
        note_values = result["document_Name"].tolist()
        for note in note_values:
            self.assertTrue(note.startswith("Note_"))


if __name__ == "__main__":
    unittest.main()
