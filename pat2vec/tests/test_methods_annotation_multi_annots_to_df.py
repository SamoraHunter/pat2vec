import unittest
import pandas as pd
import os
import shutil
from unittest.mock import patch, MagicMock
from datetime import datetime
import numpy as np
import ast

from pat2vec.util.methods_annotation_multi_annots_to_df import multi_annots_to_df


class TestMultiAnnotsToDf(unittest.TestCase):
    # ... setUp, tearDown, and test_basic_dataframe_creation remain the same ...
    def setUp(self):
        """Set up a temporary directory and mock objects for testing."""
        self.test_dir = "temp_test_dir_multi_annots"
        os.makedirs(self.test_dir, exist_ok=True)

        # Mock config object
        self.mock_config = MagicMock()
        self.mock_config.start_time = datetime.now()
        self.mock_config.pre_document_annotation_batch_path = self.test_dir
        self.mock_config.verbosity = 0
        self.mock_config.add_icd10 = False
        self.mock_config.add_opc4s = False
        self.mock_t = MagicMock()

        # Sample data
        self.pat_id = "P12345"
        self.pat_batch = pd.DataFrame(
            {
                "document_guid": ["doc1", "doc2"],
                "updatetime": [datetime(2025, 8, 21), datetime(2025, 8, 22)],
                "body_analysed": ["some text about fever", "another note about cough"],
                "client_idcode": [self.pat_id, self.pat_id],
            }
        )
        self.multi_annots = [
            {"entities": {"ent1": {"cui": "C0015967", "pretty_name": "Fever"}}},
            {"entities": {"ent2": {"cui": "C0010200", "pretty_name": "Cough"}}},
        ]
        self.df_from_json_1 = pd.DataFrame(
            {
                "client_idcode": [self.pat_id],
                "updatetime": [pd.Timestamp("2025-08-21")],
                "pretty_name": ["Fever"],
                "cui": ["C0015967"],
                "type_ids": [["T047"]],
                "types": [["Symptom"]],
                "source_value": ["fever"],
                "detected_name": ["fever"],
                "acc": [0.9],
                "context_similarity": [0.9],
                "start": [16],
                "end": [21],
                "icd10": [np.nan],
                "ontologies": [[]],
                "snomed": [True],
                "id": ["ent1"],
                "Time_Value": ["Present"],
                "Time_Confidence": [0.95],
                "Presence_Value": [True],
                "Presence_Confidence": [0.98],
                "Subject_Value": ["Patient"],
                "Subject_Confidence": [0.99],
                "text_sample": ["some text about fever"],
                "full_doc": [np.nan],
                "document_guid": ["doc1"],
            }
        )
        self.df_from_json_2 = pd.DataFrame(
            {
                "client_idcode": [self.pat_id],
                "updatetime": [pd.Timestamp("2025-08-22")],
                "pretty_name": ["Cough"],
                "cui": ["C0010200"],
                "type_ids": [["T047"]],
                "types": [["Symptom"]],
                "source_value": ["cough"],
                "detected_name": ["cough"],
                "acc": [0.92],
                "context_similarity": [0.91],
                "start": [18],
                "end": [23],
                "icd10": [np.nan],
                "ontologies": [[]],
                "snomed": [True],
                "id": ["ent2"],
                "Time_Value": ["Present"],
                "Time_Confidence": [0.96],
                "Presence_Value": [True],
                "Presence_Confidence": [0.97],
                "Subject_Value": ["Patient"],
                "Subject_Confidence": [0.98],
                "text_sample": ["another note about cough"],
                "full_doc": [np.nan],
                "document_guid": ["doc2"],
            }
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    @patch("pat2vec.util.methods_annotation_multi_annots_to_df.json_to_dataframe")
    def test_basic_dataframe_creation(self, mock_json_to_df):
        """Test that a CSV is created correctly from annotations."""
        # Arrange
        mock_json_to_df.side_effect = [self.df_from_json_1, self.df_from_json_2]
        expected_df = pd.concat(
            [self.df_from_json_1, self.df_from_json_2], ignore_index=True
        )

        # Act
        multi_annots_to_df(
            self.pat_id,
            self.pat_batch,
            self.multi_annots,
            config_obj=self.mock_config,
            t=self.mock_t,
        )

        # Assert
        expected_dest_path = os.path.join(self.test_dir, self.pat_id + ".csv")
        self.assertTrue(os.path.exists(expected_dest_path))

        # 1. Read the clean CSV, which has no index column.
        result_df = pd.read_csv(expected_dest_path)

        # 2. Convert all necessary data types to match the expected DataFrame.
        result_df["updatetime"] = pd.to_datetime(result_df["updatetime"])

        for col in ["type_ids", "types", "ontologies"]:
            if col in result_df.columns:
                result_df[col] = result_df[col].apply(ast.literal_eval)

        for col in ["snomed", "Presence_Value"]:
            if col in result_df.columns:
                result_df[col] = result_df[col].astype(bool)

        # 3. Ensure column order is the same.
        expected_df = expected_df[result_df.columns]

        # 4. Use pandas' built-in testing function for a robust comparison.
        #    It correctly handles NaN values and provides detailed error messages.
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch("pat2vec.util.methods_annotation_multi_annots_to_df.json_to_dataframe")
    def test_empty_annotations(self, mock_json_to_df):
        """Test handling of empty annotations."""
        mock_json_to_df.return_value = pd.DataFrame(columns=self.df_from_json_1.columns)
        empty_annots = [{"entities": {}}, {"entities": {}}]

        multi_annots_to_df(
            self.pat_id,
            self.pat_batch,
            empty_annots,
            config_obj=self.mock_config,
            t=self.mock_t,
        )

        expected_dest_path = os.path.join(self.test_dir, self.pat_id + ".csv")

        # FIX: Assert that the file IS created.
        self.assertTrue(os.path.exists(expected_dest_path))

        # Bonus: Check that the created file is empty (has 0 rows).
        result_df = pd.read_csv(expected_dest_path)
        self.assertEqual(len(result_df), 0)

    @patch("pat2vec.util.methods_annotation_multi_annots_to_df.json_to_dataframe")
    def test_nan_filtering(self, mock_json_to_df):
        """Test that rows with NaN in critical columns are filtered out."""
        df_with_nan = self.df_from_json_1.copy()
        df_with_nan.loc[0, "client_idcode"] = np.nan
        mock_json_to_df.return_value = df_with_nan

        multi_annots_to_df(
            self.pat_id,
            self.pat_batch.head(1),
            [self.multi_annots[0]],
            config_obj=self.mock_config,
            t=self.mock_t,
        )

        expected_dest_path = os.path.join(self.test_dir, self.pat_id + ".csv")

        # FIX: Assert that the file IS created.
        self.assertTrue(os.path.exists(expected_dest_path))

        # Bonus: Check that the created file is empty (has 0 rows).
        result_df = pd.read_csv(expected_dest_path)
        self.assertEqual(len(result_df), 0)

    @patch(
        "pat2vec.util.methods_annotation_multi_annots_to_df.join_icd10_codes_to_annot"
    )
    @patch("pat2vec.util.methods_annotation_multi_annots_to_df.json_to_dataframe")
    def test_icd10_join_logic(self, mock_json_to_df, mock_join_icd10):
        """Test that ICD10 codes are joined when add_icd10 is True."""
        # Arrange
        self.mock_config.add_icd10 = True
        self.mock_config.add_opc4s = False
        mock_json_to_df.return_value = self.df_from_json_1
        mock_join_icd10.return_value = self.df_from_json_1.copy()

        # Act
        multi_annots_to_df(
            self.pat_id,
            self.pat_batch.head(1),
            [self.multi_annots[0]],
            config_obj=self.mock_config,
            t=self.mock_t,
        )

        # Assert
        mock_join_icd10.assert_called_once()

    @patch(
        "pat2vec.util.methods_annotation_multi_annots_to_df.join_icd10_OPC4S_codes_to_annot"
    )
    @patch("pat2vec.util.methods_annotation_multi_annots_to_df.json_to_dataframe")
    def test_icd10_opcs4_join_logic(self, mock_json_to_df, mock_join_opcs4):
        """Test that ICD10 and OPC4S codes are joined when both flags are True."""
        # Arrange
        self.mock_config.add_icd10 = True
        self.mock_config.add_opc4s = True
        mock_json_to_df.return_value = self.df_from_json_1
        mock_join_opcs4.return_value = self.df_from_json_1.copy()

        # Act
        multi_annots_to_df(
            self.pat_id,
            self.pat_batch.head(1),
            [self.multi_annots[0]],
            config_obj=self.mock_config,
            t=self.mock_t,
        )

        # Assert
        mock_join_opcs4.assert_called_once()

    @patch("pat2vec.util.methods_annotation_multi_annots_to_df.json_to_dataframe")
    def test_error_in_one_document_does_not_stop_processing(self, mock_json_to_df):
        """Test that an error in one document doesn't halt processing of others."""
        # Arrange
        # First call raises an error, second call returns a valid DataFrame
        mock_json_to_df.side_effect = [
            Exception("Simulated processing error"),
            self.df_from_json_2,
        ]
        self.mock_config.verbosity = (
            1  # To cover the print statement in the except block
        )

        # Act
        with self.assertLogs(
            "pat2vec.util.methods_annotation_multi_annots_to_df", level="WARNING"
        ) as cm:
            result_df = multi_annots_to_df(
                self.pat_id,
                self.pat_batch,
                self.multi_annots,
                config_obj=self.mock_config,
                t=self.mock_t,
            )

        # Assert
        self.assertIn(
            "Error processing document 0: Simulated processing error", cm.output[0]
        )
