import unittest
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_epr_documents_data,
    generate_observations_MRC_text_data,
    generate_basic_observations_textual_obs_data,
)
from pat2vec.util.helper_functions import save_raw_patient_batch
from pat2vec.util.pre_processing import (
    get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy,
    draw_document_samples,
)


class TestMedCATTrainerSamplingIntegration(unittest.TestCase):
    """
    Integration test for the MedCAT Trainer Input Generation flow:
    1. Synthetic generation of clinical notes with specific "Target" terms.
    2. Ingestion of raw data into the DB to simulate a searchable corpus.
    3. Aggregation of documents via iterative multi-term search.
    4. Verification of Stratified Sampling logic.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "sampling_test_project"
        self.db_connection_string = (
            f"sqlite:///{os.path.join(self.test_dir, 'test.sqlite')}"
        )

        self.test_patient_id = "P_SAMPLING_TEST"
        self.search_terms = ["Asthma", "Diabetes"]

        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            proj_name=self.project_name,
            root_path=self.test_dir,
            all_patient_list=[self.test_patient_id],
            batch_mode=True,  # type: ignore
            main_options={
                "annotations": True,  # EPR Docs
                "annotations_mrc": True,  # MCT Docs
                "textual_obs": True,  # Textual Obs
            },
        )
        self.config.epr_docs_time_field = "updatetime"  # Align with dummy data
        self.config.mct_docs_time_field = (
            "observationdocument_recordeddtm"  # Align with dummy data
        )
        self.config.textual_obs_time_field = "basicobs_entered"  # Align with dummy data
        self.engine = self.config.db_engine

    def tearDown(self):
        self.engine.dispose()
        shutil.rmtree(self.test_dir)

    @patch("pat2vec.util.get_dummy_data_cohort_searcher.pipeline")
    def test_medcat_trainer_sampling_lifecycle(self, mock_pipeline):
        # 0. Mock transformer to produce specific terms for sampling
        mock_gen = MagicMock()
        mock_gen.side_effect = [
            [{"generated_text": "Patient has severe Asthma."}],
            [{"generated_text": "History of Type 2 Diabetes."}],
            [{"generated_text": "Mentioning both Asthma and Diabetes."}],
        ]
        mock_pipeline.return_value = mock_gen

        # 1. Synthetic Data Generation & Ingestion
        # Generate diverse notes across sources to test aggregation
        epr_df = generate_epr_documents_data(
            num_rows=1,
            entered_list=[self.test_patient_id],
            global_start_year=2023,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )
        mct_df = generate_observations_MRC_text_data(
            num_rows=1,
            entered_list=[self.test_patient_id],
            global_start_year=2023,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )
        text_obs_df = generate_basic_observations_textual_obs_data(
            num_rows=1,
            entered_list=[self.test_patient_id],
            global_start_year=2023,
            global_start_month=1,
            global_end_year=2023,
            global_end_month=12,
        )

        save_raw_patient_batch(
            epr_df, self.test_patient_id, "raw_epr_docs", self.config
        )
        save_raw_patient_batch(
            mct_df, self.test_patient_id, "raw_mct_docs", self.config
        )
        save_raw_patient_batch(
            text_obs_df, self.test_patient_id, "raw_textual_obs", self.config
        )

        # 2. Extract documents based on search terms
        # Create a mock pat2vec object for pre_processing routing
        pat2vec_obj = MagicMock()
        pat2vec_obj.config_obj = self.config
        pat2vec_obj.treatment_doc_filename = "treatment_docs.csv"

        # This function aggregates documents from the DB into a CSV file in project root
        treatment_docs_df = (
            get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
                pat2vec_obj, term_list=self.search_terms
            )
        )

        self.assertFalse(
            treatment_docs_df.empty, "Treatment docs pool should not be empty."
        )
        self.assertIn("search_term", treatment_docs_df.columns)

        # 3. Perform Stratified Sampling
        # Target: Sample 1 document per unique search term
        sampled_df = draw_document_samples(treatment_docs_df, n=1)

        # 4. Verifications
        # Ensure we have at least one sample for each term if available
        unique_terms_in_pool = treatment_docs_df["search_term"].unique()
        unique_terms_sampled = sampled_df["search_term"].unique()

        self.assertEqual(
            len(unique_terms_sampled),
            len(unique_terms_in_pool),
            "Sampling should cover all terms found in the document pool.",
        )

        self.assertTrue(
            (sampled_df.groupby("search_term").size() <= 1).all(),
            "Sampling should respect the maximum per-strata count 'n'.",
        )

        # Verify columns required for MedCAT Trainer input
        self.assertIn("body_analysed", sampled_df.columns)
        self.assertIn("client_idcode", sampled_df.columns)


if __name__ == "__main__":
    unittest.main()
