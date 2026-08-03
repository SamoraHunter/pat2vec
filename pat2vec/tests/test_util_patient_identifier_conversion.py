import unittest
import pandas as pd
from unittest.mock import MagicMock

from pat2vec.util.patient_identifier_conversion import (
    extract_hospital_numbers,
    extract_nhs_number,
    extract_mrn,
    convert_hospital_number_to_durable_key,
    convert_durable_key_to_hospital_numbers,
    convert_nhs_number_to_durable_key,
    convert_durable_key_to_nhs_numbers,
    convert_mrn_to_durable_key,
    convert_durable_key_to_mrn,
    convert_hospital_numbers_to_durable_keys,
    convert_durable_keys_to_hospital_numbers,
    convert_nhs_numbers_to_durable_keys,
    convert_durable_keys_to_nhs_numbers,
    convert_mrns_to_durable_keys,
    convert_durable_keys_to_mrns,
)


class TestPatientIdentifierConversion(unittest.TestCase):
    """Unit tests for patient identifier conversion functions."""

    def test_extract_hospital_numbers_single(self):
        """Test extracting a single hospital number."""
        result = extract_hospital_numbers("123456")
        self.assertEqual(result, ["123456"])

    def test_extract_hospital_numbers_multiple_comma_separated(self):
        """Test extracting multiple comma-separated hospital numbers."""
        result = extract_hospital_numbers("123456, 789012")
        self.assertEqual(result, ["123456", "789012"])

    def test_extract_hospital_numbers_multiple_with_spaces(self):
        """Test extracting hospital numbers with varying spaces."""
        result = extract_hospital_numbers(" 123 , 456 , 789 ")
        self.assertEqual(result, ["123", "456", "789"])

    def test_extract_hospital_numbers_empty_string(self):
        """Test extracting from empty string returns empty list."""
        result = extract_hospital_numbers("")
        self.assertEqual(result, [])

    def test_extract_hospital_numbers_none(self):
        """Test extracting from None returns empty list."""
        result = extract_hospital_numbers(None)
        self.assertEqual(result, [])

    def test_extract_hospital_numbers_nan(self):
        """Test extracting from NaN returns empty list."""
        result = extract_hospital_numbers(float("nan"))
        self.assertEqual(result, [])

    def test_extract_nhs_number_with_formatting(self):
        """Test extracting NHS number with spaces."""
        result = extract_nhs_number("NHS 123 456 7890")
        self.assertEqual(result, "1234567890")

    def test_extract_nhs_number_no_prefix(self):
        """Test that strings without NHS prefix return None."""
        result = extract_nhs_number("123 456 7890")
        self.assertIsNone(result)

    def test_extract_nhs_number_multiple(self):
        """Test extracting first NHS number from multiple."""
        result = extract_nhs_number("NHS 123 456 7890 and NHS 098 765 4321")
        self.assertEqual(result, "1234567890")

    def test_extract_nhs_number_empty(self):
        """Test extracting from empty string returns None."""
        result = extract_nhs_number("")
        self.assertIsNone(result)

    def test_extract_mrn_with_formatting(self):
        """Test extracting MRN with various formats."""
        result1 = extract_mrn("MRN: ABC123")
        self.assertEqual(result1, "ABC123")

        result2 = extract_mrn("MRN ABC456")
        self.assertEqual(result2, "ABC456")

    def test_extract_mrn_no_prefix(self):
        """Test that strings without MRN prefix return None."""
        result = extract_mrn("ABC123")
        self.assertIsNone(result)

    def test_extract_mrn_empty(self):
        """Test extracting from empty string returns None."""
        self.assertIsNone(extract_mrn(""))


class TestConvertWithMockPat2Vec(unittest.TestCase):
    """Tests for conversion functions that use pat2vec object."""

    def setUp(self):
        """Set up mock pat2vec object."""
        self.pat2vec_obj = MagicMock()

    def test_convert_hospital_number_to_durable_key_found(self):
        """Test converting hospital number to durable key when found."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "patient_HospitalNumber": ["123456"],
                    "patient_DurableKey": ["DURABLE123"],
                }
            )
        )

        result, missing = convert_hospital_number_to_durable_key(
            ["123456"], self.pat2vec_obj
        )

        self.assertEqual(result, "DURABLE123")
        self.assertEqual(missing, [])

    def test_convert_hospital_number_to_durable_key_not_found(self):
        """Test converting hospital number to durable key when not found."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame({"patient_HospitalNumber": [], "patient_DurableKey": []})
        )

        result, missing = convert_hospital_number_to_durable_key(
            ["999999"], self.pat2vec_obj
        )

        self.assertIsNone(result)
        self.assertEqual(missing, ["999999"])

    def test_convert_durable_key_to_hospital_numbers(self):
        """Test converting durable key to hospital numbers."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "patient_HospitalNumber": ["123456", "789012"],
                    "patient_DurableKey": ["DURABLE123", "DURABLE123"],
                }
            )
        )

        result, missing = convert_durable_key_to_hospital_numbers(
            "DURABLE123", self.pat2vec_obj
        )

        self.assertEqual(result, ["123456", "789012"])
        self.assertEqual(missing, [])

    def test_convert_nhs_number_to_durable_key_found(self):
        """Test converting NHS number to durable key when found."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "patient_NhsNumber": ["NHS 123 456 7890"],
                    "patient_DurableKey": ["DURABLE123"],
                }
            )
        )

        result, missing = convert_nhs_number_to_durable_key(
            ["NHS 123 456 7890"], self.pat2vec_obj
        )

        self.assertEqual(result, "DURABLE123")
        self.assertEqual(missing, [])

    def test_convert_nhs_number_to_durable_key_not_found(self):
        """Test converting NHS number to durable key when not found."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame({"patient_NhsNumber": [], "patient_DurableKey": []})
        )

        result, missing = convert_nhs_number_to_durable_key(
            ["NHS 999 999 9999"], self.pat2vec_obj
        )

        self.assertIsNone(result)
        self.assertEqual(missing, ["NHS 999 999 9999"])

    def test_convert_durable_key_to_nhs_numbers(self):
        """Test converting durable key to NHS numbers."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "patient_NhsNumber": ["1234567890"],
                    "patient_DurableKey": ["DURABLE123"],
                }
            )
        )

        result, missing = convert_durable_key_to_nhs_numbers(
            "DURABLE123", self.pat2vec_obj
        )

        self.assertEqual(result, ["1234567890"])
        self.assertEqual(missing, [])

    def test_convert_mrn_to_durable_key_found(self):
        """Test converting MRN to durable key when found."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {"Patient_MRN": ["ABC123"], "patient_DurableKey": ["DURABLE123"]}
            )
        )

        result, missing = convert_mrn_to_durable_key(["ABC123"], self.pat2vec_obj)

        self.assertEqual(result, "DURABLE123")
        self.assertEqual(missing, [])

    def test_convert_durable_key_to_mrn(self):
        """Test converting durable key to MRNs."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {"Patient_MRN": ["ABC123"], "patient_DurableKey": ["DURABLE123"]}
            )
        )

        result, missing = convert_durable_key_to_mrn("DURABLE123", self.pat2vec_obj)

        self.assertEqual(result, ["ABC123"])
        self.assertEqual(missing, [])

    def test_convert_hospital_numbers_to_durable_keys(self):
        """Test converting multiple hospital numbers to durable keys."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "patient_HospitalNumber": ["123456", "789012"],
                    "patient_DurableKey": ["DURABLE001", "DURABLE002"],
                }
            )
        )

        result, missing = convert_hospital_numbers_to_durable_keys(
            ["123456", "789012"], self.pat2vec_obj
        )

        self.assertEqual(result, ["DURABLE001", "DURABLE002"])
        self.assertEqual(missing, [])

    def test_convert_nhs_numbers_to_durable_keys(self):
        """Test converting multiple NHS numbers to durable keys."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "patient_NhsNumber": ["NHS 123 456 7890", "NHS 098 765 4321"],
                    "patient_DurableKey": ["DURABLE001", "DURABLE002"],
                }
            )
        )

        result, missing = convert_nhs_numbers_to_durable_keys(
            ["NHS 123 456 7890", "NHS 098 765 4321"], self.pat2vec_obj
        )

        self.assertEqual(result, ["DURABLE001", "DURABLE002"])
        self.assertEqual(missing, [])

    def test_convert_mrns_to_durable_keys(self):
        """Test converting multiple MRNs to durable keys."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "Patient_MRN": ["ABC123", "XYZ789"],
                    "patient_DurableKey": ["DURABLE001", "DURABLE002"],
                }
            )
        )

        result, missing = convert_mrns_to_durable_keys(
            ["ABC123", "XYZ789"], self.pat2vec_obj
        )

        self.assertEqual(result, ["DURABLE001", "DURABLE002"])
        self.assertEqual(missing, [])

    def test_convert_durable_keys_to_hospital_numbers(self):
        """Test converting multiple durable keys to hospital numbers."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "patient_HospitalNumber": ["123456", "789012"],
                    "patient_DurableKey": ["DURABLE001", "DURABLE001"],
                }
            )
        )

        result, missing = convert_durable_keys_to_hospital_numbers(
            ["DURABLE001"], self.pat2vec_obj
        )

        self.assertEqual(result, ["123456", "789012"])
        self.assertEqual(missing, [])

    def test_convert_durable_keys_to_nhs_numbers(self):
        """Test converting multiple durable keys to NHS numbers."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "patient_NhsNumber": ["1234567890"],
                    "patient_DurableKey": ["DURABLE001"],
                }
            )
        )

        result, missing = convert_durable_keys_to_nhs_numbers(
            ["DURABLE001"], self.pat2vec_obj
        )

        self.assertEqual(result, ["1234567890"])
        self.assertEqual(missing, [])

    def test_convert_durable_keys_to_mrns(self):
        """Test converting multiple durable keys to MRNs."""
        self.pat2vec_obj.cohort_searcher_with_terms_and_search.return_value = (
            pd.DataFrame(
                {
                    "Patient_MRN": ["ABC123"],
                    "patient_DurableKey": ["DURABLE001"],
                }
            )
        )

        result, missing = convert_durable_keys_to_mrns(["DURABLE001"], self.pat2vec_obj)

        self.assertEqual(result, ["ABC123"])
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
