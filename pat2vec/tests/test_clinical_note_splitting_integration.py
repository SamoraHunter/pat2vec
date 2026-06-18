import unittest
import pandas as pd
from datetime import datetime, timedelta

from pat2vec.util.config_pat2vec import config_class  # type: ignore
from pat2vec.util.clinical_note_splitter import split_clinical_notes
from pat2vec.pat2vec_get_methods.get_method_pat_annotations import (
    get_current_pat_annotations,
)


class TestClinicalNoteSplittingIntegration(unittest.TestCase):
    """
    Integration test for the Clinical Note Splitting logic:
    1. Generate a multi-entry clinical note with different "Entered on" dates.
    2. Ingest document into DB.
    3. Execute the splitting logic to expand the document into individual chunks.
    4. Verify that feature extraction (IPW) correctly filters these chunks using extracted dates.
    """

    def setUp(self):
        self.test_patient_id = "P_SPLIT_TEST"
        self.base_date = datetime(2023, 6, 15)

        # Window: only includes the first entry, excludes the second
        self.target_date_range = (
            self.base_date - timedelta(days=1),
            self.base_date + timedelta(days=1),
        )

        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            all_patient_list=[self.test_patient_id],
            batch_mode=True,
            main_options={"annotations": True},
        )
        self.engine = self.config.db_engine

    def tearDown(self):
        self.engine.dispose()

    def test_clinical_note_splitting_and_temporal_filtering(self):
        # 1. Create a document with two clinical entries
        # Entry A: Inside window (2023-06-15)
        # Entry B: Outside window (2024-01-01)
        entry_a_date = self.base_date.strftime("%d-%b-%Y %H:%M")
        entry_b_date = "01-Jan-2024 10:00"

        body_text = (
            f"Entered on - {entry_a_date} Patient has Fever. "
            f"Entered on - {entry_b_date} Patient has Diabetes."
        )

        raw_doc = pd.DataFrame(
            [
                {
                    "client_idcode": self.test_patient_id,
                    "body_analysed": body_text,
                    "updatetime": self.base_date,  # Document level time
                    "document_guid": "DOC_MULTI",
                    "document_description": "Clinical Note",
                }
            ]
        )

        # 2. Split Document into individual date-stamped chunks (mocked)
        split_df, _ = split_clinical_notes(raw_doc)

        self.assertEqual(
            len(split_df), 2, "Document should be split into 2 rows based on dates."
        )
        self.assertEqual(split_df.iloc[0]["updatetime"], pd.to_datetime(self.base_date))
        self.assertEqual(
            split_df.iloc[1]["updatetime"], pd.to_datetime("2024-01-01 10:00:00")
        )

        # 3. Setup Annotations for these chunks (Mocked logic)
        # Normally MedCAT would run on split_df
        annots_df = pd.DataFrame(
            [
                {
                    "client_idcode": self.test_patient_id,
                    "updatetime": split_df.iloc[0]["updatetime"],
                    "pretty_name": "Fever",
                    "cui": "C_FEVER",
                    "annotation_batch_source": "epr",
                },
                {
                    "client_idcode": self.test_patient_id,
                    "updatetime": split_df.iloc[1]["updatetime"],
                    "pretty_name": "Diabetes",
                    "cui": "C_DIAB",
                    "annotation_batch_source": "epr",
                },
            ]
        )

        # 4. Extract Features using the temporal window
        features = get_current_pat_annotations(
            self.test_patient_id, self.target_date_range, annots_df, self.config
        )

        # Fever (inside window) should be present, Diabetes (outside window) should be filtered out
        self.assertEqual(features["pretty_name_count_epr_Fever"].iloc[0], 1.0)
        self.assertTrue(
            "pretty_name_count_epr_Diabetes" not in features.columns
            or features["pretty_name_count_epr_Diabetes"].iloc[0] == 0
        )


if __name__ == "__main__":
    unittest.main()
