import unittest
from datetime import datetime, timedelta

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_cohort_searcher import generate_news_data
from pat2vec.util.helper_functions import (
    save_raw_patient_batch,
    get_df_from_db,
    save_patient_features,
    get_all_features,
)
from pat2vec.pat2vec_get_methods.get_method_news import get_news


class TestNEWSIntegration(unittest.TestCase):
    """
    Comprehensive integration test for NEWS (National Early Warning Score) lifecycle:
    1. Synthetic data generation (NEWS records).
    2. Ingestion of raw data into DB.
    3. Feature vectorization using the 'get' method.
    4. Storage of feature vectors in the final feature vector table.
    5. Verification of date filtering during feature extraction.
    """

    def setUp(self):
        self.test_patient_id = "P_NEWS_INTEGRATION_TEST"
        self.base_date = datetime(2023, 6, 15)
        self.target_date_range_inclusive = (
            self.base_date - timedelta(days=5),
            self.base_date + timedelta(days=5),
        )
        self.target_date_range_exclusive = (
            self.base_date + timedelta(days=10),
            self.base_date + timedelta(days=20),
        )

        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            global_start_year=self.base_date.year - 1,
            global_start_month=self.base_date.month,
            global_start_day=self.base_date.day,
            global_end_year=self.base_date.year + 1,
            global_end_month=self.base_date.month,
            global_end_day=self.base_date.day,
            all_patient_list=[self.test_patient_id],  # type: ignore
            batch_mode=True,  # type: ignore
            main_options={
                "news": True,
            },
            days=0,
        )
        self.config.news_time_field = (
            "observationdocument_recordeddtm"  # Align with dummy data
        )
        self.engine = self.config.db_engine

    def tearDown(self):
        self.engine.dispose()

    def test_news_full_integration_lifecycle(self):
        # 1. Synthetic Data Generation (Raw NEWS data)
        raw_news_df = generate_news_data(
            num_rows=3,
            entered_list=[self.test_patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
        )

        # Ensure the generated data falls within the inclusive date range.
        # NEWS data typically uses a configured time field or 'updatetime'.
        time_field = self.config.news_time_field
        raw_news_df[time_field] = [
            (self.base_date + timedelta(days=i - 2)).strftime("%Y-%m-%dT%H:%M:%S")
            for i in range(len(raw_news_df))
        ]

        # 2. Ingestion of raw data into DB (raw_data_raw_news)
        save_raw_patient_batch(
            raw_news_df, self.test_patient_id, "raw_news", self.config
        )
        db_raw_news = get_df_from_db(
            self.config, "raw_data", "raw_news", patient_ids=[self.test_patient_id]
        )
        self.assertFalse(db_raw_news.empty, "Raw NEWS data should be in DB.")
        self.assertEqual(len(db_raw_news), 3)

        # 3. Feature Vectorization & Storage (Correct Date Range)
        # Using positional arguments for (ID, DateRange, Batch) to ensure compatibility
        features_inclusive = get_news(
            self.test_patient_id,
            self.target_date_range_inclusive,
            db_raw_news,
            config_obj=self.config,
        )
        self.assertFalse(
            features_inclusive.empty,
            "Features should be extracted for inclusive date range.",
        )
        save_patient_features(features_inclusive, self.test_patient_id, self.config)

        # Verify patient isolation and storage in final features table
        final_features_all = get_all_features(self.config)
        final_features = final_features_all[
            final_features_all["client_idcode"] == self.test_patient_id
        ]
        self.assertEqual(
            len(final_features),
            1,
            "Final features table should contain exactly 1 row for the test patient.",
        )

        # 4. Feature Vectorization (Incorrect Date Range - Date Filtering Check)
        features_exclusive = get_news(
            self.test_patient_id,
            self.target_date_range_exclusive,
            db_raw_news,
            config_obj=self.config,
        )

        # If no rows match the temporal filter, numeric features should be absent, zero, or NaN
        signal_cols = [c for c in features_exclusive.columns if c != "client_idcode"]
        if not features_exclusive.empty:
            is_zero_or_nan = (features_exclusive[signal_cols] == 0) | (
                features_exclusive[signal_cols].isna()
            )
            self.assertTrue(
                is_zero_or_nan.all().all(),
                "No NEWS features should have signal for exclusive date range.",
            )


if __name__ == "__main__":
    unittest.main()
