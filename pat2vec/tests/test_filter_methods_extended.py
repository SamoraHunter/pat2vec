"""Comprehensive tests for pat2vec/util/filter_methods.py."""

import unittest
from unittest.mock import MagicMock
import pandas as pd
from pat2vec.util.filter_methods import (
    apply_data_type_epic_clinical_notes_filters,
    apply_data_type_epic_clinical_notes_appointments_filters,
    apply_data_type_epic_patients_filters,
    apply_data_type_epic_medical_history_filters,
    apply_data_type_epic_orders_filters,
    apply_data_type_epic_lab_results_filters,
    apply_data_type_epic_imaging_reports_filters,
    apply_data_type_drugs_filters,
    apply_data_type_diagnostics_filters,
    apply_data_type_news_filters,
    apply_data_type_textual_obs_filters,
    apply_data_type_reports_filters,
    apply_data_type_obs_filters,
)


class TestEpicClinicalNotesFilters(unittest.TestCase):
    """Tests for epic clinical notes filters."""

    def test_apply_epic_clinical_notes_filter_basic(self):
        """Test basic filtering of Epic clinical notes."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_clinical_notes": ["Note"]},
        }

        df = pd.DataFrame(
            {
                "document_Name": ["Progress Note", "Discharge Summary", "Letter"],
                "document_Content": [
                    "Patient condition good",
                    "Patient discharged",
                    "Follow up letter",
                ],
            }
        )

        result = apply_data_type_epic_clinical_notes_filters(mock_config, df)

        # Should filter to only rows with 'Note' in document_Name
        self.assertEqual(len(result), 1)
        self.assertIn("Progress Note", result["document_Name"].values)

    def test_apply_epic_clinical_notes_filter_with_regex(self):
        """Test filtering with regex patterns."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {
                "epic_clinical_notes": ["Progress Note"],
                "epic_clinical_notes_term_regex": ["good", "bad"],
            },
        }

        df = pd.DataFrame(
            {
                "document_Name": ["Progress Note", "Discharge Summary"],
                "document_Content": [
                    "Patient condition is good",
                    "Patient condition is bad",
                ],
            }
        )

        result = apply_data_type_epic_clinical_notes_filters(mock_config, df)

        self.assertEqual(len(result), 1)  # Only one matches 'Progress Note'
        # Column names are the regex terms themselves, not with _count suffix
        self.assertIn("good", result.columns)
        self.assertIn("bad", result.columns)

    def test_apply_epic_clinical_notes_filter_empty_df(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_clinical_notes": ["Note"]},
        }
        df = pd.DataFrame(columns=["document_Name", "document_Content"])

        result = apply_data_type_epic_clinical_notes_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestEpicClinicalNotesAppointmentsFilters(unittest.TestCase):
    """Tests for epic clinical notes and appointments filters."""

    def test_apply_epic_clinical_notes_appointments_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {
                "epic_clinical_notes_appointments": ["Outpatient", "Clinic"]
            },
        }

        df = pd.DataFrame(
            {
                "document_Name": [
                    "Outpatient Follow-up",
                    "Inpatient Note",
                    "Clinic Letter",
                ]
            }
        )

        result = apply_data_type_epic_clinical_notes_appointments_filters(
            mock_config, df
        )

        self.assertEqual(
            len(result), 2
        )  # Should filter to only those with 'Outpatient' or 'Clinic'

    def test_apply_epic_clinical_notes_appointments_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_clinical_notes_appointments": ["Outpatient"]},
        }
        df = pd.DataFrame(columns=["document_Name"])

        result = apply_data_type_epic_clinical_notes_appointments_filters(
            mock_config, df
        )
        self.assertTrue(result.empty)


class TestEpicPatientsFilters(unittest.TestCase):
    """Tests for epic patients filters."""

    def test_apply_epic_patients_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_patients": ["Active", "Admitted"]},
        }

        df = pd.DataFrame(
            {
                "patient_Gender": [
                    "Active",
                    "Discharged Patient",
                    "Admitted Case",
                ]
            }
        )

        result = apply_data_type_epic_patients_filters(mock_config, df)

        self.assertEqual(len(result), 2)  # Matches 'Active' and 'Admitted'

    def test_apply_epic_patients_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_patients": ["Active"]},
        }
        df = pd.DataFrame(columns=["patient_Gender"])

        result = apply_data_type_epic_patients_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestEpicMedicalHistoryFilters(unittest.TestCase):
    """Tests for epic medical history filters."""

    def test_apply_epic_medical_history_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {
                "epic_medical_history": ["Diagnosis", "Problem List"]
            },
        }

        df = pd.DataFrame(
            {
                "document_Comment": [
                    "Diabetes Diagnosis",
                    "Medical Problem List",
                    "Lab Results",
                ]
            }
        )

        result = apply_data_type_epic_medical_history_filters(mock_config, df)

        self.assertEqual(len(result), 2)

    def test_apply_epic_medical_history_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_medical_history": ["Diagnosis"]},
        }
        df = pd.DataFrame(columns=["document_Comment"])

        result = apply_data_type_epic_medical_history_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestEpicOrdersFilters(unittest.TestCase):
    """Tests for epic orders filters."""

    def test_apply_epic_orders_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_orders": ["Prescription", "Order"]},
        }

        df = pd.DataFrame(
            {
                "document_Name": [
                    "Medication Prescription",
                    "Lab Order",
                    "Progress Note",
                ]
            }
        )

        result = apply_data_type_epic_orders_filters(mock_config, df)

        self.assertEqual(len(result), 2)  # Matches 'Prescription' and 'Order'

    def test_apply_epic_orders_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_orders": ["Prescription"]},
        }
        df = pd.DataFrame(columns=["document_Name"])

        result = apply_data_type_epic_orders_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestEpicLabResultsFilters(unittest.TestCase):
    """Tests for epic lab results filters."""

    def test_apply_epic_lab_results_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_lab_results": ["Blood", "Lab"]},
        }

        df = pd.DataFrame(
            {
                "document_Name": [
                    "Blood Test Results",
                    "Lab Report",
                    "Imaging Findings",
                ]
            }
        )

        result = apply_data_type_epic_lab_results_filters(mock_config, df)

        self.assertEqual(len(result), 2)  # Matches 'Blood' and 'Lab'

    def test_apply_epic_lab_results_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_lab_results": ["Blood Test"]},
        }
        df = pd.DataFrame(columns=["document_Name"])

        result = apply_data_type_epic_lab_results_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestEpicImagingReportsFilters(unittest.TestCase):
    """Tests for epic imaging reports filters."""

    def test_apply_epic_imaging_reports_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_imaging_reports": ["X-ray", "MRI", "CT"]},
        }

        df = pd.DataFrame(
            {
                "document_Name": [
                    "Chest X-ray",
                    "Brain MRI",
                    "Abdomen CT",
                    "Lab Report",
                ]
            }
        )

        result = apply_data_type_epic_imaging_reports_filters(mock_config, df)

        self.assertEqual(len(result), 3)  # Matches 'X-ray', 'MRI', and 'CT'

    def test_apply_epic_imaging_reports_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_imaging_reports": ["X-ray"]},
        }
        df = pd.DataFrame(columns=["document_Name"])

        result = apply_data_type_epic_imaging_reports_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestDrugsFilters(unittest.TestCase):
    """Tests for drugs filters."""

    def test_apply_drugs_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"drugs": ["Prescription", "Drug"]},
        }

        df = pd.DataFrame(
            {
                "order_name": [
                    "Prescription List",
                    " Drug Order",
                    "Progress Note",
                ]
            }
        )

        result = apply_data_type_drugs_filters(mock_config, df)

        self.assertEqual(len(result), 2)  # Matches 'Prescription' and 'Drug'

    def test_apply_drugs_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"drugs": ["Medication"]},
        }
        df = pd.DataFrame(columns=["order_name"])

        result = apply_data_type_drugs_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestDiagnosticsFilters(unittest.TestCase):
    """Tests for diagnostics filters."""

    def test_apply_diagnostics_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"diagnostics": ["Diagnostic", "Diagnosis"]},
        }

        df = pd.DataFrame(
            {
                "order_name": [
                    "Diagnostic Findings",
                    "Diagnosis Summary",
                    "Procedure Note",
                ]
            }
        )

        result = apply_data_type_diagnostics_filters(mock_config, df)

        self.assertEqual(len(result), 2)  # Matches 'Diagnostic' and 'Diagnosis'

    def test_apply_diagnostics_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"diagnostics": ["Diagnosis"]},
        }
        df = pd.DataFrame(columns=["order_name"])

        result = apply_data_type_diagnostics_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestNewsFilters(unittest.TestCase):
    """Tests for news filters."""

    def test_apply_news_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"news": ["Patient", "Clinical"]},
        }

        df = pd.DataFrame(
            {
                "obscatalogmasteritem_displayname": [
                    "Patient News",
                    "Clinical Update",
                    "Operation Note",
                ]
            }
        )

        result = apply_data_type_news_filters(mock_config, df)

        self.assertEqual(len(result), 2)  # Matches 'Patient' and 'Clinical'

    def test_apply_news_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"news": ["News"]},
        }
        df = pd.DataFrame(columns=["obscatalogmasteritem_displayname"])

        result = apply_data_type_news_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestTextualObsFilters(unittest.TestCase):
    """Tests for textual observations filters."""

    def test_apply_textual_obs_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"textual_obs": ["Observation", " Note"]},
        }

        df = pd.DataFrame(
            {
                "textualObs": [
                    "Clinical Observation",
                    "Progress Note",
                    "Lab Report",
                ]
            }
        )

        result = apply_data_type_textual_obs_filters(mock_config, df)

        self.assertEqual(len(result), 2)

    def test_apply_textual_obs_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"textual_obs": ["Observation"]},
        }
        df = pd.DataFrame(columns=["textualObs"])

        result = apply_data_type_textual_obs_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestReportsFilters(unittest.TestCase):
    """Tests for reports filters."""

    def test_apply_reports_filter_basic(self):
        """Test basic filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"reports": ["Report", "Summary"]},
        }

        df = pd.DataFrame(
            {
                "body_analysed": [
                    "Patient Report",
                    "Discharge Summary",
                    "Procedure Note",
                ]
            }
        )

        result = apply_data_type_reports_filters(mock_config, df)

        self.assertEqual(len(result), 2)

    def test_apply_reports_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"reports": ["Report"]},
        }
        df = pd.DataFrame(columns=["body_analysed"])

        result = apply_data_type_reports_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestObsFilters(unittest.TestCase):
    """Tests for observations filters."""

    def test_apply_obs_filter_basic(self):
        "Test basic filtering."
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"obs": ["Vital", "Nursing"]},
        }

        df = pd.DataFrame(
            {
                "obscatalogmasteritem_displayname": [
                    "Vital Signs Observation",
                    "Nursing Note",
                    "Lab Report",
                ]
            }
        )

        result = apply_data_type_obs_filters(mock_config, df)

        self.assertEqual(len(result), 2)  # Matches 'Vital' and 'Nursing'

    def test_apply_obs_filter_empty(self):
        """Test filtering empty DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"obs": ["Observation"]},
        }
        df = pd.DataFrame(columns=["obscatalogmasteritem_displayname"])

        result = apply_data_type_obs_filters(mock_config, df)
        self.assertTrue(result.empty)


class TestFilterMethodsIntegration(unittest.TestCase):
    """Integration tests for filter methods."""

    def test_empty_filter_dict_returns_original_df(self):
        """Test that empty filter dict returns original DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = {"filter_term_lists": {}}

        df = pd.DataFrame(
            {
                "document_Name": ["Note 1", "Note 2"],
                "content": ["A", "B"],
            }
        )

        result = apply_data_type_epic_clinical_notes_filters(mock_config, df)
        pd.testing.assert_frame_equal(result, df)

    def test_none_filter_dict_returns_original_df(self):
        """Test that None filter dict returns original DataFrame."""
        mock_config = MagicMock()
        mock_config.verbosity = 0
        mock_config.data_type_filter_dict = None

        df = pd.DataFrame(
            {
                "document_Name": ["Note 1", "Note 2"],
                "content": ["A", "B"],
            }
        )

        result = apply_data_type_epic_clinical_notes_filters(mock_config, df)
        pd.testing.assert_frame_equal(result, df)

    def test_verbose_logging_with_filter(self):
        """Test verbose logging with filtering."""
        mock_config = MagicMock()
        mock_config.verbosity = 5
        mock_config.data_type_filter_dict = {
            "filter_term_lists": {"epic_clinical_notes": ["Note"]},
        }

        df = pd.DataFrame(
            {
                "document_Name": ["Progress Note", "Discharge"],
                "content": ["A", "B"],
            }
        )

        # Should not raise and should apply filter
        result = apply_data_type_epic_clinical_notes_filters(mock_config, df)
        self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
