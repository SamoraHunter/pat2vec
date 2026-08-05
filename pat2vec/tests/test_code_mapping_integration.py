import os
import unittest
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import datetime

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.methods_get_medcat import get_cat
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_epr_documents_data,
)
from pat2vec.util.helper_functions import save_raw_patient_batch, get_df_from_db
from pat2vec.util.methods_annotation import (
    annot_pat_batch_docs,
    multi_annots_to_df_reports,
)

original_read_csv = pd.read_csv


class TestCodeMappingIntegration(unittest.TestCase):
    """
    Integration test for SNOMED-to-ICD-10 and SNOMED-to-OPCS-4 mapping logic:
    1. Synthetic document generation.
    2. Mocked MedCAT returns specific SNOMED CUIs.
    3. Verification that mapping tables are joined correctly during document processing.
    4. Verification that mapped codes appear in the final annotation dataframes.
    """

    def setUp(self):
        self.test_patient_id = "P_MAPPING_INTEGRATION_TEST"
        self.base_date = datetime(2023, 6, 15)

        # Initialize config with mapping enabled
        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            all_patient_list=[self.test_patient_id],
            batch_mode=True,
            add_icd10=True,
            add_opc4s=True,
            main_options={
                "annotations": True,
            },
        )
        self.engine = self.config.db_engine

        # Mock MedCAT CAT
        self.mock_cat = MagicMock()
        # Mock entities return (CUIs that match our dummy maps - from test_icd10_map.tsv)
        self.mock_cat.get_entities_multi_texts.return_value = [
            {"entities": {"e1": {"cui": "100", "pretty_name": "Asthma"}}},
            {"entities": {"e2": {"cui": "101", "pretty_name": "Incision"}}},
        ]

        # Patch get_cat in the namespace where it is called
        self.mock_get_cat = patch(
            f"{__name__}.get_cat", return_value=self.mock_cat
        ).start()

        # Define dummy mapping data matching test_icd10_map.tsv format
        # For ICD-10: referencedComponentId -> mapTarget (which becomes icd10)
        self.icd10_map = pd.DataFrame(
            {
                "referencedComponentId": [100, 101],
                "mapTarget": ["J45", "J44"],
                "mapTargetName": ["Asthma", "COPD"],
            }
        )
        # For OPCS-4: conceptId -> targetId (which becomes opcs4), matching test_map.csv format
        self.opcs4_map = pd.DataFrame(
            {
                "conceptId": [100, 101],
                "targetId": ["X10", "X11"],
                "targetName": ["Test Concept 1", "Test Concept 2"],
            }
        )

        # Patch pd.read_csv to serve our dummy mapping tables when requested by join logic
        self.patch_read_csv = patch("pandas.read_csv").start()

        def side_effect(path, **kwargs):
            path_str = str(path)
            filename = os.path.basename(path_str)
            icd10_patterns = [
                "test_icd10",
                "tls_Icd10",
                "Icd10cmHumanReadableMap",
                "icd10_map",
            ]
            # Match ICD-10 test mapping files (including production and fallback patterns)
            for pattern in icd10_patterns:
                if pattern in filename or pattern in path_str:
                    return self.icd10_map
            # Match OPCS-4 mapping file (both test and fallback patterns)
            if "test_map.csv" == filename or "snomed_to_icd10_opcs4" in path_str:
                return self.opcs4_map
            return original_read_csv(path, **kwargs)

        self.patch_read_csv.side_effect = side_effect
        self.addCleanup(patch.stopall)

    def tearDown(self):
        self.engine.dispose()

    @patch("pat2vec.util.get_dummy_data_cohort_searcher.pipeline")
    def test_code_mapping_lifecycle(self, mock_pipeline):
        # 0. Mock transformer pipeline
        mock_pipeline.return_value = MagicMock(
            return_value=[{"generated_text": "Sample clinical text."}]
        )

        # 1. Setup raw data
        raw_df = generate_epr_documents_data(
            num_rows=2,
            entered_list=[self.test_patient_id],
            global_start_year=self.base_date.year,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year,
            global_end_month=self.base_date.month,
        )
        raw_df["updatetime"] = self.base_date.strftime("%Y-%m-%dT%H:%M:%S")
        save_raw_patient_batch(
            raw_df, self.test_patient_id, "raw_epr_docs", self.config
        )
        db_raw = get_df_from_db(
            self.config, "raw_data", "raw_epr_docs", patient_ids=[self.test_patient_id]
        )

        # 2. Annotation processing (This triggers the join logic)
        cat = get_cat(self.config)
        multi_annots = annot_pat_batch_docs(
            self.test_patient_id, db_raw, cat, self.config, MagicMock()
        )

        annotated_df = multi_annots_to_df_reports(
            self.test_patient_id,
            db_raw,
            multi_annots,
            self.config,
            MagicMock(),
            text_column="body_analysed",
            time_column="updatetime",
            guid_column="document_guid",
        )

        # 3. Verification of Mappings
        self.assertIn("icd10", annotated_df.columns)
        self.assertIn("opcs4", annotated_df.columns)

        # Verify CUI 100 mapped to J45
        asthma_row = annotated_df[annotated_df["cui"] == "100"]
        self.assertEqual(asthma_row.iloc[0]["icd10"], "J45")

        # Verify CUI 101 mapped to X11 (OPCS-4)
        proc_row = annotated_df[annotated_df["cui"] == "101"]
        self.assertEqual(proc_row.iloc[0]["opcs4"], "X11")


if __name__ == "__main__":
    unittest.main()
