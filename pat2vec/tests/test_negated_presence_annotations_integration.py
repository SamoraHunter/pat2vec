import unittest
import pandas as pd
from unittest.mock import patch
from datetime import datetime, timedelta

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_epr_documents_data,
)
from pat2vec.util.helper_functions import save_raw_patient_batch
from pat2vec.pat2vec_get_methods.get_method_pat_annotations import (
    get_current_pat_annotations,
)


class TestNegatedPresenceAnnotationsIntegration(unittest.TestCase):
    """
    Comprehensive integration test for Negated Presence Annotations logic:
    1. Synthetic generation of EPR documents.
    2. Manual ingestion of annotations with both 'True' and 'False' presence values.
    3. Verification that feature vectorization correctly filters by Presence_Value.
    4. Verification that toggling negated_presence_annotations handles feature extraction correctly.
    """

    def setUp(self):
        self.test_patient_id = "P_NEGATION_INTEGRATION_TEST"
        self.base_date = datetime(2023, 6, 15)
        self.target_date_range = (
            self.base_date - timedelta(days=5),
            self.base_date + timedelta(days=5),
        )

        # Config with negation filtering enabled
        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            all_patient_list=[self.test_patient_id],
            batch_mode=True,
            main_options={"annotations": True, "negated_presence_annotations": True},
        )
        self.engine = self.config.db_engine
        # Explicitly set filter arguments after initialization
        self.config.filter_arguments = {"Presence_Value": ["True"]}

    def tearDown(self):
        self.engine.dispose()

    @patch("pat2vec.util.get_dummy_data_cohort_searcher.pipeline")
    def test_negation_filtering_lifecycle(self, mock_pipeline):
        # 1. Setup raw documents (to satisfy schema dependencies)
        raw_epr_docs_df = generate_epr_documents_data(
            num_rows=2,
            entered_list=[self.test_patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
        )
        raw_epr_docs_df["updatetime"] = self.base_date.strftime("%Y-%m-%dT%H:%M:%S")
        save_raw_patient_batch(
            raw_epr_docs_df, self.test_patient_id, "raw_epr_docs", self.config
        )

        # 2. Setup Annotation data with mixed presence
        # Entity 1: Fever (Confirmed Present)
        # Entity 2: Cough (Negated - e.g., "No cough")
        annot_df = pd.DataFrame(
            [
                {
                    "client_idcode": self.test_patient_id,
                    "updatetime": self.base_date,
                    "pretty_name": "Fever",
                    "cui": "C_FEVER",
                    "Presence_Value": "True",
                    "Presence_Confidence": 0.95,
                    "annotation_batch_source": "epr",
                },
                {
                    "client_idcode": self.test_patient_id,
                    "updatetime": self.base_date,
                    "pretty_name": "Cough",
                    "cui": "C_COUGH",
                    "Presence_Value": "False",  # Negated mention
                    "Presence_Confidence": 0.95,
                    "annotation_batch_source": "epr",
                },
            ]
        )
        # Persist to database (SQLite schema prefixing)
        annot_df.to_sql("annotations_ann_epr_docs", self.engine, index=False)

        # 3. Vectorize with 'True' filter
        features_present = get_current_pat_annotations(
            self.test_patient_id,
            self.target_date_range,
            annot_df,
            config_obj=self.config,
        )

        # Fever should be counted (1), Cough should NOT be counted (0 or missing)
        self.assertEqual(features_present["pretty_name_count_epr_Fever"].iloc[0], 1)
        self.assertTrue(
            "pretty_name_count_epr_Cough" not in features_present.columns
            or features_present["pretty_name_count_epr_Cough"].iloc[0] == 0
        )

        # 4. Vectorize with 'False' filter (Targeting negated mentions)
        self.config.filter_arguments["Presence_Value"] = ["False"]
        features_negated = get_current_pat_annotations(
            self.test_patient_id,
            self.target_date_range,
            annot_df,
            config_obj=self.config,
        )

        self.assertEqual(
            features_negated["pretty_name_count_epr_Cough"].iloc[0],
            1,
            "Should count negated mentions when filtered for Presence_Value=False.",
        )


if __name__ == "__main__":
    unittest.main()
