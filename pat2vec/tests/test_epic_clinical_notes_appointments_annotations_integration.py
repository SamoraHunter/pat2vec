import unittest
import tqdm
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.methods_get_medcat import get_cat
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_epic_clinical_notes_appointments_data,
    dummy_CAT,
)
from pat2vec.util.helper_functions import save_raw_patient_batch, get_df_from_db, save_patient_features, get_all_features  # type: ignore
from pat2vec.util.methods_annotation import (
    annot_pat_batch_docs,
    multi_annots_to_df_epic_clinical_notes_appointments,
)
from pat2vec.pat2vec_get_methods.get_method_pat_annotations import (
    get_current_pat_annotations,
)


class TestEpicClinicalNotesAppointmentsAnnotationsIntegration(unittest.TestCase):
    """
    Comprehensive integration test for Epic Clinical Notes Appointments Annotations lifecycle:
    1. Synthetic data generation (raw Epic Clinical Notes Appointments).
    2. Ingestion of raw data into DB.
    3. Annotation processing and storage of annotations into DB.
    4. Feature vectorization using the 'get' method.
    5. Storage of feature vectors in the final features table.
    6. Verification of date filtering during feature extraction.
    """

    def setUp(self):
        self.test_patient_id = "P_EPIC_NOTE_APP_ANN_TEST"
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
            all_patient_list=[self.test_patient_id],
            batch_mode=True,
            main_options={
                "epic_clinical_notes_appointments": True,
                "epic_clinical_notes_appointments_annotations": True,
            },
        )
        self.engine = self.config.db_engine

        # Mock MedCAT and CogStack client
        self.mock_cat = dummy_CAT()
        self.mock_initialize_cogstack_client = patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.initialize_cogstack_client",
            return_value=MagicMock(),
        ).start()
        self.mock_get_cat = patch(
            f"{__name__}.get_cat", return_value=self.mock_cat
        ).start()
        self.addCleanup(patch.stopall)

    def tearDown(self):
        self.engine.dispose()

    @patch("pat2vec.util.get_dummy_data_cohort_searcher.pipeline")
    def test_epic_clinical_notes_appointments_annotations_full_integration_lifecycle(
        self, mock_pipeline
    ):
        # 0. Mock transformer pipeline to avoid external network calls
        mock_gen = MagicMock()
        mock_gen.return_value = [
            {
                "generated_text": "Sample clinical appointment note text mentioning Fever."
            }
        ]
        mock_pipeline.return_value = mock_gen

        # 1. Synthetic Data Generation (Raw Epic Clinical Notes Appointments)
        raw_notes_app_df = generate_epic_clinical_notes_appointments_data(
            num_rows=3,
            entered_list=[self.test_patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
        )
        # Ensure the generated data falls within the inclusive date range
        raw_notes_app_df["document_CreatedWhen"] = [
            (self.base_date + timedelta(days=i - 2)).strftime("%Y-%m-%dT%H:%M:%S")
            for i in range(len(raw_notes_app_df))
        ]

        # 2. Ingestion of raw data into DB (raw_data_raw_epic_clinical_notes_appointments)
        save_raw_patient_batch(
            raw_notes_app_df,
            self.test_patient_id,
            "raw_epic_clinical_notes_appointments",
            self.config,
        )
        db_raw_notes_app = get_df_from_db(
            self.config,
            "raw_data",
            "raw_epic_clinical_notes_appointments",
            patient_ids=[self.test_patient_id],
        )
        self.assertFalse(
            db_raw_notes_app.empty,
            "Raw Epic Clinical Notes Appointments data should be in DB.",
        )

        # 3. Annotation processing and storage into DB (annotations_ann_epic_clinical_notes_appointments)
        mock_tqdm = MagicMock(spec=tqdm.tqdm)
        cat = get_cat(self.config)

        multi_annots = annot_pat_batch_docs(
            self.test_patient_id,
            db_raw_notes_app,
            cat,
            self.config,
            mock_tqdm,
            text_column="document_Content",
        )
        annotated_df = multi_annots_to_df_epic_clinical_notes_appointments(
            self.test_patient_id, db_raw_notes_app, multi_annots, self.config, mock_tqdm
        )
        # For SQLite, the schema name is prepended to the table name
        annot_table = "annotations_ann_epic_clinical_notes_appointments"
        annotated_df.to_sql(annot_table, self.engine, index=False, if_exists="replace")

        db_ann_notes_app = get_df_from_db(
            self.config,
            "annotations",
            "ann_epic_clinical_notes_appointments",
            patient_ids=[self.test_patient_id],
        )
        self.assertFalse(
            db_ann_notes_app.empty,
            "Annotated Epic Clinical Notes Appointments data should be in DB.",
        )

        # 4. Feature Vectorization & Storage (Correct Date Range)
        # Using positional arguments for (ID, DateRange, Batch) for consistency # type: ignore
        features_inclusive = get_current_pat_annotations(
            self.test_patient_id,
            self.target_date_range_inclusive,
            db_ann_notes_app,
            config_obj=self.config,
        )
        self.assertFalse(
            features_inclusive.empty,
            "Features should be extracted for inclusive date range.",
        )
        self.assertTrue(
            any(
                col.startswith("pretty_name_count_")
                for col in features_inclusive.columns
            ),
            "Annotation feature columns should be present.",
        )

        save_patient_features(features_inclusive, self.test_patient_id, self.config)

        # Ensure patient isolation in the final features table
        final_features_all = get_all_features(self.config)
        final_features = final_features_all[
            final_features_all["client_idcode"] == self.test_patient_id
        ]
        self.assertEqual(
            len(final_features),
            1,
            "Final features table should contain exactly 1 row for the test patient.",
        )

        # 5. Feature Vectorization (Incorrect Date Range - Date Filtering Check)
        features_exclusive = get_current_pat_annotations(
            self.test_patient_id,
            self.target_date_range_exclusive,
            db_ann_notes_app,
            config_obj=self.config,
        )
        # Verify that no annotation features were produced for the exclusive range
        exclusive_feature_cols = [
            c for c in features_exclusive.columns if c.startswith("pretty_name_count_")
        ]
        self.assertEqual(
            len(exclusive_feature_cols),
            0,
            "No annotation features should be extracted for exclusive date range.",
        )


if __name__ == "__main__":
    unittest.main()
