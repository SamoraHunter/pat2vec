import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.filter_methods import apply_data_type_epr_docs_filters
from pat2vec.util.helper_functions import (
    get_all_features,
    get_df_from_db,
    save_patient_features,
    save_raw_patient_batch,
)


class TestRegexExtractionIntegration(unittest.TestCase):
    """Integration test for Regex Feature Extraction lifecycle:
    1. Synthetic generation of EPR documents containing specific keywords ("Asthma").
    2. Ingestion of raw data into the DB.
    3. Feature extraction using the regex applicator.
    4. Verification that regex matches are counted and stored in the final feature vector.
    5. Verify date filtering for temporal windows.
    """

    def setUp(self):

        self.test_patient_id = "P_REGEX_TEST"
        self.base_date = datetime(2023, 6, 15)
        self.target_date_range = (
            self.base_date - timedelta(days=5),
            self.base_date + timedelta(days=5),
        )

        # Config with regex search term enabled
        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            all_patient_list=[self.test_patient_id],
            batch_mode=True,
            main_options={
                "annotations": True,  # EPR Docs index
            },
            data_type_filter_dict={
                "filter_term_lists": {"epr_docs_term_regex": ["Asthma"]},
            },
        )
        self.engine = self.config.db_engine

    def tearDown(self):
        self.engine.dispose()

    @patch("pat2vec.util.get_dummy_data_cohort_searcher.pipeline")
    def test_regex_extraction_lifecycle(self, mock_pipeline):
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            generate_epr_documents_data,
        )

        # Mock transformer to produce text with the target keyword
        mock_gen = MagicMock()
        mock_gen.return_value = [{"generated_text": "Patient with history of Asthma."}]
        mock_pipeline.return_value = mock_gen

        # Use use_GPT=True to ensure the pipeline mock is used (conftest.py defaults to False)
        raw_epr_df = generate_epr_documents_data(
            num_rows=3,
            entered_list=[self.test_patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
            use_GPT=True,  # Explicitly use GPT so the pipeline mock is applied
        )

        # Set specific times: 2 inside window, 1 outside
        raw_epr_df["updatetime"] = [
            self.base_date.strftime("%Y-%m-%dT%H:%M:%S"),
            (self.base_date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S"),
            (self.base_date + timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S"),
        ]

        # Ingestion into raw_data_raw_epr_docs
        save_raw_patient_batch(
            raw_epr_df,
            self.test_patient_id,
            "raw_epr_docs",
            self.config,
        )
        db_raw = get_df_from_db(
            self.config,
            "raw_data",
            "raw_epr_docs",
            patient_ids=[self.test_patient_id],
        )

        # Apply Regex Extraction
        filtered_df = apply_data_type_epr_docs_filters(self.config, db_raw)

        # Verify that the regex column "Asthma" was added and matches found
        assert "Asthma" in filtered_df.columns
        assert filtered_df["Asthma"].sum() == 3  # All 3 docs had the keyword from mock

        # Final Feature Verification (Date filtering check)
        window_mask = (
            pd.to_datetime(filtered_df["updatetime"], utc=True)
            >= pd.to_datetime(self.target_date_range[0], utc=True)
        ) & (
            pd.to_datetime(filtered_df["updatetime"], utc=True)
            <= pd.to_datetime(self.target_date_range[1], utc=True)
        )

        features_df = filtered_df[window_mask]

        # Verify only 2 records remain in window
        assert len(features_df) == 2

        # Create count feature
        final_feature_val = features_df["Asthma"].sum()
        assert (
            final_feature_val == 2
        ), "Should only count occurrences within the temporal window."

        # Verify persistence format
        final_vec = pd.DataFrame(
            {
                "client_idcode": [self.test_patient_id],
                "epr_regex_count_Asthma": [final_feature_val],
            },
        )
        save_patient_features(final_vec, self.test_patient_id, self.config)

        final_from_db = get_all_features(self.config)
        assert "epr_regex_count_Asthma" in final_from_db.columns
        assert final_from_db.iloc[0]["epr_regex_count_Asthma"] == 2


if __name__ == "__main__":
    unittest.main()
