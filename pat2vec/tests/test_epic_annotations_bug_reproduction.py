"""
Unit tests for Epic clinical notes annotations bug reproduction.

This module reproduces the issue where Epic clinical notes annotations
are not generated properly when using database backend with dummy data.
"""

import logging
import os
import shutil
import sys
import tempfile
import unittest

import pandas as pd

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(TESTS_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# SQLAlchemy must be imported after setting up paths
from sqlalchemy import inspect as db_inspect

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_medcat_annotation import dummy_CAT
from pat2vec.util.helper_functions import (
    get_df_from_db,
    save_annotations_to_db,
    save_raw_patient_batch,
)


class TestEpicTablesCreation(unittest.TestCase):
    """Test cases specifically for table creation logic with empty DataFrames."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test_tables.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _get_inspector(self, config):
        """Get SQLAlchemy inspector from config."""
        return db_inspect(config.db_engine)

    def test_save_empty_with_columns_creates_table(self):
        """
        Test that save_raw_patient_batch creates table when DataFrame has columns but no rows.

        This is the key bug: when initial batches are empty (no data from ES),
        tables should still be created so downstream annotation processing can query them.
        """
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        # DataFrame with columns but no rows - simulates what happens when ES returns empty results
        df = pd.DataFrame(columns=["client_idcode", "updatetime", "document_Content"])

        save_raw_patient_batch(df, "TEST_001", "raw_epic_clinical_notes", config)

        # Verify table exists
        inspector = self._get_inspector(config)
        table_names = [
            t
            for s in inspector.get_schema_names()
            for t in inspector.get_table_names(schema=s)
        ]

        print(f"Tables: {table_names}")

        # Check both potential naming conventions for SQLite
        table_found = False
        for table_name in [
            "raw_data_raw_epic_clinical_notes",
            "raw_epic_clinical_notes",
        ]:
            if inspector.has_table(table_name):
                table_found = True
                break

        self.assertTrue(
            table_found,
            f"Table should be created even when DataFrame is empty. Tables: {table_names}",
        )

    def test_save_empty_annotations_creates_annotation_table(self):
        """
        Test that save_annotations_to_db creates annotation table for empty batch.

        Same as above but specifically for annotations which are the final output
        of interest (what build_merged_epr_mct_annot_df needs).
        """
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        # Empty annotation DataFrame with all required columns
        df = pd.DataFrame(
            columns=[
                "client_idcode",
                "pretty_name",
                "cui",
                "type_ids",
                "types",
                "source_value",
                "detected_name",
                "acc",
                "context_similarity",
                "start",
                "end",
                "id",
            ]
        )

        save_annotations_to_db(df, "TEST_001", "ann_epic_clinical_notes", config)

        inspector = self._get_inspector(config)
        table_names = [
            t
            for s in inspector.get_schema_names()
            for t in inspector.get_table_names(schema=s)
        ]

        print(f"Tables: {table_names}")

        table_found = False
        for table_name in [
            "annotations_ann_epic_clinical_notes",
            "ann_epic_clinical_notes",
        ]:
            if inspector.has_table(table_name):
                table_found = True
                break

        self.assertTrue(
            table_found, f"Annotation table should be created. Tables: {table_names}"
        )

    def test_save_non_empty_annotations_creates_table(self):
        """
         Test that save_annotations_to_db works correctly with non-empty DataFrames.

        This verifies the existing functionality still works after our fixes.
        """
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        # Non-empty annotation DataFrame with single patient (save_raw_patient_batch deletes old data for same ID)
        df = pd.DataFrame(
            {
                "client_idcode": ["PAT_001", "PAT_001"],  # Same patient, different rows
                "pretty_name": ["Hypertension", "Diabetes"],
                "cui": ["38341003", "73211009"],
                "types": [["disorder"], ["disorder"]],
                "acc": [0.95, 0.88],
            }
        )

        save_annotations_to_db(df, "PAT_001", "ann_epic_clinical_notes", config)

        inspector = self._get_inspector(config)

        # Verify table was created
        table_found = False
        for table_name in [
            "annotations_ann_epic_clinical_notes",
            "ann_epic_clinical_notes",
        ]:
            if inspector.has_table(table_name):
                table_found = True
                break

        self.assertTrue(
            table_found, "Annotation table should be created for non-empty DataFrame"
        )

        # Verify data was saved
        df_retrieved = get_df_from_db(
            config,
            schema="annotations",
            table="ann_epic_clinical_notes",
            patient_ids=["PAT_001"],
        )

        self.assertEqual(len(df_retrieved), 2)
        self.assertIn("Hypertension", df_retrieved["pretty_name"].values)

    def test_save_raw_with_columns_preserves_schema(self):
        """
        Test that save_raw_patient_batch preserves column schema when creating table.
        """
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        # DataFrame with specific columns
        df = pd.DataFrame(
            {
                "client_idcode": [],
                "updatetime": [],
                "document_Content": [],
                "document_CreatedWhen": [],
            }
        )

        save_raw_patient_batch(df, "TEST_001", "raw_epic_clinical_notes", config)

        inspector = self._get_inspector(config)

        # Check table exists
        table_found = False
        table_name = None
        for tn in ["raw_data_raw_epic_clinical_notes", "raw_epic_clinical_notes"]:
            if inspector.has_table(tn):
                table_found = True
                table_name = tn
                break

        self.assertTrue(table_found, "Table should be created")

        # Check columns were preserved
        columns = [c["name"] for c in inspector.get_columns(table_name)]

        expected_cols = [
            "client_idcode",
            "updatetime",
            "document_Content",
            "document_CreatedWhen",
        ]
        for col in expected_cols:
            self.assertIn(col, columns, f"Expected column {col} not found in table")


class TestMedCATDummyAnnotations(unittest.TestCase):
    """Test cases for dummy MedCAT annotation generation."""

    def test_medcat_annotates_empty_text(self):
        """
        Test that dummy MedCAT model can annotate empty or minimal text.
        """
        cat = dummy_CAT()

        # Test with various text inputs including edge cases
        test_texts = [
            "",  # Empty string
            " ",  # Whitespace only
            "a" * 5,  # Very short text
        ]

        for i, text in enumerate(test_texts):
            with self.subTest(text_length=len(text)):
                results = cat.get_entities_multi_texts([text])

                self.assertIsInstance(results, list)
                self.assertEqual(len(results), 1)

                entities = results[0].get("entities", {})
                # Even empty/short texts should generate some dummy annotations
                self.assertIsInstance(entities, dict)

    def test_medcat_annotates_nonempty_text(self):
        """
        Test that dummy MedCAT model properly annotates non-empty text.
        """
        cat = dummy_CAT()

        text = "The patient has hypertension and diabetes. Blood pressure is elevated."

        results = cat.get_entities_multi_texts([text])

        self.assertEqual(len(results), 1)
        entities = results[0].get("entities", {})

        # Should have at least some entities
        self.assertGreater(len(entities), 0)


if __name__ == "__main__":
    import logging
    import sys

    logging.basicConfig(level=logging.WARNING)

    unittest.main(verbosity=2)
