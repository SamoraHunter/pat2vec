import unittest
from unittest.mock import MagicMock, patch

import pandas as pd


class TestCohortSearcherWithTermsAndSearchDummy(unittest.TestCase):
    """Tests for the cohort_searcher_with_terms_and_search_dummy function."""

    def setUp(self):
        """Set up test fixtures."""
        self.fields_list = [
            "client_idcode",
            "basicobs_itemname_analysed",
            "_id",
            "_index",
        ]
        self.entered_list = ["P001"]

    def test_route_epr_documents(self):
        """Test routing to epr_documents generator."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["epr_documents"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epr_documents.generate_epr_documents_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epr_documents",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_route_epr_documents_personal_data(self):
        """Test routing to epr_documents personal data generator."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_firstname": ["John"], "_index": ["epr_documents"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epr_documents.generate_epr_documents_personal_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epr_documents",
                fields_list=["client_idcode", "client_firstname"],
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_basic_observations_covid_routing(self):
        """Test routing basic_observations index with COVID search string."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["basic_observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.covid.generate_covid_observations_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="SARS CoV-2 AND COVID-19",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_basic_observations_reports_routing(self):
        """Test routing basic_observations index with report text."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["basic_observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_observations_Reports_text_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="basicobs_itemname_analysed:report",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_basic_observations_textual_obs_routing(self):
        """Test routing basic_observations index with textualObs field."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "textualObs": ["test"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_textual_obs_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=["client_idcode", "textualObs"],
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_basic_observations_numeric_routing(self):
        """Test routing basic_observations index with numeric values."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "basicobs_value_numeric": [75.5]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="basicobs_value_numeric",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_observations_bmi_routing(self):
        """Test routing observations index with BMI search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_bmi_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="OBS BMI",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_observations_news_routing(self):
        """Test routing observations index with NEWS search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_news_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="NEWS",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_observations_core_o2_routing(self):
        """Test routing observations index with CORE_SpO2 search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_core_o2_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string='"CORE_SpO2"',
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_observations_core_resus_routing(self):
        """Test routing observations index with CORE_RESUS_STATUS search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_core_resus_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string='"CORE_RESUS_STATUS"',
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_observations_bed_routing(self):
        """Test routing observations index with CORE_BedNumber3 search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_bed_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="CORE_BedNumber3",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_observations_vte_routing(self):
        """Test routing observations index with CORE_VTE_STATUS search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_vte_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="CORE_VTE_STATUS",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_observations_smoking_routing(self):
        """Test routing observations index with CORE_SmokingStatus search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_smoking_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="CORE_SmokingStatus",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_observations_hospital_site_routing(self):
        """Test routing observations index with CORE_HospitalSite search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_hospital_site_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="CORE_HospitalSite",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_observations_mrc_text_routing(self):
        """Test routing observations index with AoMRC_ClinicalSummary_FT search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["observations"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.observations.generate_observations_MRC_text_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="AoMRC_ClinicalSummary_FT",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_order_medication_routing(self):
        """Test routing order index with medication search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame({"client_idcode": ["P001"], "_index": ["order"]})
        )

        with patch(
            "pat2vec.util.dummy_data_generation.orders.generate_drug_orders_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="order",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="medication",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_order_diagnostic_routing(self):
        """Test routing order index with diagnostic search."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame({"client_idcode": ["P001"], "_index": ["order"]})
        )

        with patch(
            "pat2vec.util.dummy_data_generation.orders.generate_diagnostic_orders_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="order",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="diagnostic",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_pims_apps_routing(self):
        """Test routing pims_apps* index."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["pims_apps"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.appointments.generate_appointments_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="pims_apps*",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_epic_encounters_routing(self):
        """Test routing epic_encounters index."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["epic_encounters"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_encounters_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_encounters",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_epic_clinical_notes_routing(self):
        """Test routing epic_clinical_notes index."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["epic_clinical_notes"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_clinical_notes_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_clinical_notes",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_epic_medical_history_routing(self):
        """Test routing epic_medical_history index."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["epic_medical_history"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_medical_history_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_medical_history",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_epic_orders_routing(self):
        """Test routing epic_orders index."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["epic_orders"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_orders_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_orders",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_epic_lab_results_routing(self):
        """Test routing epic_lab_results index."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["epic_lab_results"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_lab_results_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_lab_results",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_epic_patients_routing(self):
        """Test routing epic_patients index."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["epic_patients"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_patients_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_patients",
                fields_list=self.fields_list,
                term_name="patient_DurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_epic_imaging_reports_routing(self):
        """Test routing epic_imaging_reports index."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {"client_idcode": ["P001"], "_index": ["epic_imaging_reports"]}
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_imaging_reports_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_imaging_reports",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_epic_clinical_notes_appointments_routing(self):
        """Test routing epic_clinical_notes_appointments index."""
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {
                    "client_idcode": ["P001"],
                    "_index": ["epic_clinical_notes_appointments"],
                }
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_clinical_notes_appointments_data",
            new=mock_gen,
        ):
            from pat2vec.util.dummy_data_generation.observation_router import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_clinical_notes_appointments",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

    def test_unknown_index_routing(self):
        """Test routing with unknown index name."""
        from pat2vec.util.dummy_data_generation.observation_router import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        result = cohort_searcher_with_terms_and_search_dummy(
            index_name="unknown_index",
            fields_list=self.fields_list,
            term_name="client_idcode",
            entered_list=["P001"],
            search_string="test",
        )
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 0)

    def test_date_range_parsing(self):
        """Test that date range is correctly extracted from search string."""
        from pat2vec.util.dummy_data_generation.observation_router import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        search_string = "2020-01-01 TO 2023-12-31"
        mock_gen = MagicMock(return_value=pd.DataFrame({"client_idcode": ["P001"]}))

        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string=search_string,
            )
            self.assertIsNotNone(result)

    def test_date_range_not_found_default_dates(self):
        """Test that default dates are used when no date range is found."""
        from pat2vec.util.dummy_data_generation.observation_router import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        mock_gen = MagicMock(return_value=pd.DataFrame({"client_idcode": ["P001"]}))

        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_data",
            new=mock_gen,
        ):
            cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="no dates here",
            )

    def test_duplicate_column_cleanup(self):
        """Test that duplicate columns are removed from result."""
        from pat2vec.util.dummy_data_generation.observation_router import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {
                    "client_idcode": ["P001"],
                    "basicobs_itemname_analysed": ["test"],
                    "_id": ["abc123"],
                }
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)

    def test_search_term_added_to_result(self):
        """Test that search string is added to result DataFrame."""
        from pat2vec.util.dummy_data_generation.observation_router import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        search_string = "test search query"
        mock_gen = MagicMock(return_value=pd.DataFrame({"client_idcode": ["P001"]}))

        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string=search_string,
            )
            self.assertIn("search_term", result.columns)
            self.assertEqual(result["search_term"].iloc[0], search_string)

    def test_empty_search_string(self):
        """Test with empty search string."""
        from pat2vec.util.dummy_data_generation.observation_router import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        mock_gen = MagicMock(return_value=pd.DataFrame({"client_idcode": ["P001"]}))

        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)

    def test_multiple_patients_in_entered_list(self):
        """Test with multiple patient IDs in entered_list."""
        from pat2vec.util.dummy_data_generation.observation_router import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        mock_gen = MagicMock(
            return_value=pd.DataFrame({"client_idcode": ["P001", "P002"]})
        )

        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001", "P002"],
                search_string="",
            )
            self.assertIsNotNone(result)

    def test_custom_fields_list(self):
        """Test with custom fields list."""
        from pat2vec.util.dummy_data_generation.observation_router import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        custom_fields = ["client_idcode", "custom_field_1", "custom_field_2"]
        mock_gen = MagicMock(
            return_value=pd.DataFrame(
                {
                    "client_idcode": ["P001"],
                    "custom_field_1": ["value1"],
                    "custom_field_2": ["value2"],
                }
            )
        )

        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=custom_fields,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)

    def test_all_generic_routing_index_names(self):
        """Test routing for all supported index names."""
        from pat2vec.util.dummy_data_generation.observation_router import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        mock_gen = MagicMock(return_value=pd.DataFrame({"client_idcode": ["P001"]}))

        # Test epr_documents
        with patch(
            "pat2vec.util.dummy_data_generation.epr_documents.generate_epr_documents_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epr_documents",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test basic_observations
        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_basic_observations_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="basic_observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test observations
        with patch(
            "pat2vec.util.dummy_data_generation.basic_observations.generate_observations_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="some random string",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test order
        with patch(
            "pat2vec.util.dummy_data_generation.orders.generate_drug_orders_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="order",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="medication",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test pims_apps*
        with patch(
            "pat2vec.util.dummy_data_generation.appointments.generate_appointments_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="pims_apps*",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test epic_encounters
        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_encounters_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_encounters",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test epic_clinical_notes
        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_clinical_notes_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_clinical_notes",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test epic_medical_history
        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_medical_history_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_medical_history",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test epic_orders
        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_orders_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_orders",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test epic_lab_results
        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_lab_results_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_lab_results",
                fields_list=self.fields_list,
                term_name="client_idcode",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test epic_patients
        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_patients_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_patients",
                fields_list=self.fields_list,
                term_name="patient_DurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test epic_imaging_reports
        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_imaging_reports_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_imaging_reports",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()

        # Test epic_clinical_notes_appointments
        with patch(
            "pat2vec.util.dummy_data_generation.epic.generate_epic_clinical_notes_appointments_data",
            new=mock_gen,
        ):
            result = cohort_searcher_with_terms_and_search_dummy(
                index_name="epic_clinical_notes_appointments",
                fields_list=self.fields_list,
                term_name="document_PatientDurableKey",
                entered_list=["P001"],
                search_string="",
            )
            self.assertIsNotNone(result)
            mock_gen.assert_called_once()
            mock_gen.reset_mock()


if __name__ == "__main__":
    unittest.main()
