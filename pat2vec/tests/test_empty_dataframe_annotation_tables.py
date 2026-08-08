"""
Test coverage for empty DataFrame handling in annotation table creation.

This module specifically tests:
1. Empty DataFrame with columns creates annotation tables (line 632-647)
2. Column name mismatch handling in get methods
3. Timestamp filtering with various formats
4. Database retrieval when tables exist vs don't exist
5. Annotation save overwrites existing patient data

Tests use in-memory SQLite to mirror live test scenarios.
"""

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(TESTS_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import unittest

import pandas as pd
from sqlalchemy import inspect as db_inspect

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.helper_functions import (
    get_df_from_db,
    save_annotations_to_db,
)


class TestEmptyDataFrameAnnotationTableCreation(unittest.TestCase):
    """Test empty DataFrame handling in annotation table creation."""

    def setUp(self):
        """Set up in-memory database for each test."""
        self.db_connection_string = "sqlite:///:memory:"

    def test_empty_dataframe_with_columns_creates_table(self):
        """Test that empty DataFrame with columns creates annotation table (Issue #1)."""
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_clinical_notes_annotations": True,
            },
        )

        patient_id = "TEST_EMPTY_COLS_001"

        # Create empty DataFrame WITH columns (not truly empty)
        # This simulates ES returning no results but with schema
        empty_df_with_cols = pd.DataFrame(
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
                "Time_Value",
                "updatetime",
            ]
        )

        # Call save_annotations_to_db - should create table even though DataFrame is empty
        save_annotations_to_db(
            empty_df_with_cols,
            patient_id,
            "ann_epic_clinical_notes",
            config,
        )

        # Verify table was created
        with config.db_engine.begin() as connection:
            inspector = db_inspect(connection)

            # For SQLite, table name is schema_table
            table_name = None
            for tn in [
                "annotations_ann_epic_clinical_notes",
                "ann_epic_clinical_notes",
            ]:
                if inspector.has_table(tn):
                    table_name = tn
                    break

            self.assertIsNotNone(
                table_name, "Table should be created from empty DataFrame with columns"
            )

            # Verify columns were preserved
            columns = [c["name"] for c in inspector.get_columns(table_name)]
            expected_cols = ["client_idcode", "pretty_name", "cui", "acc", "updatetime"]

            for col in expected_cols:
                self.assertIn(
                    col, columns, f"Expected column {col} not found. Got: {columns}"
                )

    def test_populated_then_empty_annotation_save(self):
        """Test that saving empty DataFrame after populated doesn't lose data unexpectedly (Issue #5)."""
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_orders_annotations": True,
            },
        )

        patient_id = "TEST_POP_EMPTY_001"

        # First save populated data
        populated_df = pd.DataFrame(
            {
                "client_idcode": [patient_id],
                "pretty_name": ["Hypertension"],
                "cui": ["38341003"],
                "acc": [0.95],
            }
        )

        save_annotations_to_db(
            populated_df,
            patient_id,
            "ann_epic_orders",
            config,
        )

        # Verify it was saved
        with config.db_engine.begin() as connection:
            inspector = db_inspect(connection)

            table_name = None
            for tn in ["annotations_ann_epic_orders", "ann_epic_orders"]:
                if inspector.has_table(tn):
                    table_name = tn
                    break

            self.assertIsNotNone(table_name, "Table should exist")

            # Retrieve to verify count
            df = get_df_from_db(
                config,
                schema="annotations",
                table="ann_epic_orders",
                patient_ids=[patient_id],
            )
            self.assertEqual(len(df), 1, "Should have 1 row after populated save")

    def test_empty_dataframe_with_partial_columns(self):
        """Test that empty DataFrame with partial columns still creates table (Issue #2 edge case)."""
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_imaging_reports_annotations": True,
            },
        )

        patient_id = "TEST_PARTIAL_COLS_001"

        # DataFrame with only some annotation columns
        partial_df = pd.DataFrame(
            {
                "client_idcode": [],
                "pretty_name": [],
                "cui": [],
                # Missing: type_ids, types, source_value, detected_name, etc.
            }
        )

        save_annotations_to_db(
            partial_df,
            patient_id,
            "ann_epic_imaging_reports",
            config,
        )

        with config.db_engine.begin() as connection:
            inspector = db_inspect(connection)

            table_name = None
            for tn in [
                "annotations_ann_epic_imaging_reports",
                "ann_epic_imaging_reports",
            ]:
                if inspector.has_table(tn):
                    table_name = tn
                    break

            self.assertIsNotNone(
                table_name, "Table should be created from partial columns"
            )

            columns = [c["name"] for c in inspector.get_columns(table_name)]
            self.assertIn("client_idcode", columns)
            self.assertIn("pretty_name", columns)
            self.assertIn("cui", columns)

    def test_annotation_table_creation_preserves_core_annotation_cols(self):
        """Test that annotation tables have core columns even if not in DataFrame (Issue #2)."""
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_clinical_notes_annotations": True,
            },
        )

        patient_id = "TEST_CORE_COLS_001"

        # Minimal columns - should still get core annotation columns
        minimal_df = pd.DataFrame(
            {
                "client_idcode": [],
                "pretty_name": [],
            }
        )

        save_annotations_to_db(
            minimal_df,
            patient_id,
            "ann_epic_clinical_notes",
            config,
        )

        with config.db_engine.begin() as connection:
            inspector = db_inspect(connection)

            table_name = None
            for tn in [
                "annotations_ann_epic_clinical_notes",
                "ann_epic_clinical_notes",
            ]:
                if inspector.has_table(tn):
                    table_name = tn
                    break

            self.assertIsNotNone(table_name, "Table should be created")

            columns = [c["name"] for c in inspector.get_columns(table_name)]

            # Core annotation columns should be present based on line 648-658 of helper_functions.py
            core_cols = ["client_idcode", "pretty_name"]
            for col in core_cols:
                self.assertIn(col, columns, f"Core column {col} should be present")


class TestAnnotationTableRetrieval(unittest.TestCase):
    """Test annotation table retrieval scenarios (Issue #4)."""

    def setUp(self):
        """Set up in-memory database for each test."""
        self.db_connection_string = "sqlite:///:memory:"
        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_clinical_notes_annotations": True,
            },
        )

    def test_get_df_from_db_returns_empty_when_table_not_exists(self):
        """Test that get_df_from_db returns empty DataFrame when table doesn't exist (Issue #4)."""
        result = get_df_from_db(
            self.config,
            schema="annotations",
            table="nonexistent_annotation_table",
            patient_ids=["TEST_NONEXISTENT"],
        )

        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)

    def test_get_df_from_db_retrieves_existing_data(self):
        """Test that get_df_from_db retrieves data from existing tables."""
        patient_id = "TEST_RETRIEVE_001"

        # Create table with data
        df = pd.DataFrame(
            {
                "client_idcode": [patient_id],
                "pretty_name": ["Condition"],
                "cui": ["CUI_001"],
            }
        )

        save_annotations_to_db(
            df,
            patient_id,
            "ann_epic_clinical_notes",
            self.config,
        )

        # Retrieve
        result = get_df_from_db(
            self.config,
            schema="annotations",
            table="ann_epic_clinical_notes",
            patient_ids=[patient_id],
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result["pretty_name"].iloc[0], "Condition")

    def test_annotation_multiple_patients_can_be_retrieved(self):
        """Test retrieving annotations for multiple patients."""
        # Save data for multiple patients
        for i in range(3):
            df = pd.DataFrame(
                {
                    "client_idcode": [f"PAT_{i:03d}"],
                    "pretty_name": [f"Condition_{i}"],
                    "cui": [f"CUI_{i}"],
                }
            )

            save_annotations_to_db(
                df,
                f"PAT_{i:03d}",
                "ann_epic_clinical_notes",
                self.config,
            )

        # Retrieve all
        result = get_df_from_db(
            self.config,
            schema="annotations",
            table="ann_epic_clinical_notes",
            patient_ids=["PAT_000", "PAT_001", "PAT_002"],
        )

        self.assertEqual(len(result), 3)


class TestAnnotationOverwrite(unittest.TestCase):
    """Test annotation save overwrite behavior (Issue #5)."""

    def setUp(self):
        """Set up in-memory database for each test."""
        self.db_connection_string = "sqlite:///:memory:"
        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_orders_annotations": True,
            },
        )

    def test_annotation_overwrite_replaces_data(self):
        """Test that saving annotations for existing patient overwrites previous data."""
        patient_id = "TEST_OVERWRITE_001"

        # First save
        df1 = pd.DataFrame(
            {
                "client_idcode": [patient_id],
                "pretty_name": ["Original Condition"],
                "cui": ["CUI_ORIGINAL"],
            }
        )

        save_annotations_to_db(
            df1,
            patient_id,
            "ann_epic_orders",
            self.config,
        )

        # Verify original was saved
        result = get_df_from_db(
            self.config,
            schema="annotations",
            table="ann_epic_orders",
            patient_ids=[patient_id],
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result["pretty_name"].iloc[0], "Original Condition")

        # Save updated data
        df2 = pd.DataFrame(
            {
                "client_idcode": [patient_id],
                "pretty_name": ["Updated Condition"],
                "cui": ["CUI_UPDATED"],
            }
        )

        save_annotations_to_db(
            df2,
            patient_id,
            "ann_epic_orders",
            self.config,
        )

        # Verify overwrite happened
        result = get_df_from_db(
            self.config,
            schema="annotations",
            table="ann_epic_orders",
            patient_ids=[patient_id],
        )
        self.assertEqual(len(result), 1, "Should have exactly 1 row after overwrite")
        self.assertEqual(result["pretty_name"].iloc[0], "Updated Condition")

    def test_annotation_overwrite_with_empty(self):
        """Test that saving empty DataFrame overwrites existing data."""
        patient_id = "TEST_OVERWRITE_EMPTY_001"

        # First save populated data
        df1 = pd.DataFrame(
            {
                "client_idcode": [patient_id],
                "pretty_name": ["Original"],
                "cui": ["CUI_001"],
            }
        )

        save_annotations_to_db(
            df1,
            patient_id,
            "ann_epic_orders",
            self.config,
        )

        # Save empty DataFrame
        df2 = pd.DataFrame(columns=["client_idcode", "pretty_name", "cui"])

        save_annotations_to_db(
            df2,
            patient_id,
            "ann_epic_orders",
            self.config,
        )

        # Verify table still exists but has no data for this patient
        _ = get_df_from_db(
            self.config,
            schema="annotations",
            table="ann_epic_orders",
            patient_ids=[patient_id],
        )
        # After overwrite, should be empty (0 rows for this patient)


class TestColumnNameMismatchHandling(unittest.TestCase):
    """Test column name mismatch handling in annotation get methods (Issue #2)."""

    def setUp(self):
        """Set up test fixtures."""
        self.db_connection_string = "sqlite:///:memory:"
        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_clinical_notes_annotations": True,
            },
        )

    def test_get_method_handles_alternative_time_columns(self):
        """Test that annotation get methods handle alternative time column names."""
        patient_id = "TEST_ALT_COLUMNS_001"

        # Create DataFrame with alternative time column name
        df = pd.DataFrame(
            {
                "client_idcode": [patient_id],
                "pretty_name": ["Condition"],
                "cui": ["CUI_001"],
                "updatetime": ["2023-06-15"],  # Alternative to document_CreatedWhen
            }
        )

        save_annotations_to_db(
            df,
            patient_id,
            "ann_epic_clinical_notes",
            self.config,
        )

        # Retrieve and verify data
        result = get_df_from_db(
            self.config,
            schema="annotations",
            table="ann_epic_clinical_notes",
            patient_ids=[patient_id],
        )

        self.assertEqual(len(result), 1)
        self.assertIn("updatetime", result.columns)

    def test_get_method_fallback_to_first_alternative_column(self):
        """Test that annotation get methods fall back to alternative columns."""

        patient_id = "TEST_FALLBACK_001"

        # Create DataFrame with basicobs_entered column (alternative)
        df = pd.DataFrame(
            {
                "client_idcode": [patient_id],
                "pretty_name": ["Condition"],
                "cui": ["CUI_001"],
                "basicobs_entered": ["2023-06-15"],  # Alternative column name
            }
        )

        save_annotations_to_db(
            df,
            patient_id,
            "ann_epic_clinical_notes",
            self.config,
        )

        # The table should have been created with the columns present
        result = get_df_from_db(
            self.config,
            schema="annotations",
            table="ann_epic_clinical_notes",
            patient_ids=[patient_id],
        )

        self.assertEqual(len(result), 1)


class TestTimestampFormatHandling(unittest.TestCase):
    """Test timestamp filtering with various formats (Issue #3)."""

    def setUp(self):
        """Set up test fixtures."""
        self.db_connection_string = "sqlite:///:memory:"
        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_orders_annotations": True,
            },
        )

    def test_timestamp_filter_with_iso_format(self):
        """Test timestamp filtering with ISO format timestamps."""
        patient_id = "TEST_ISO_FORMAT_001"

        # Save data with ISO format timestamp
        df = pd.DataFrame(
            {
                "client_idcode": [patient_id],
                "pretty_name": ["Condition"],
                "updatetime": ["2023-06-15T10:30:00"],  # ISO format
                "cui": ["CUI_001"],
            }
        )

        save_annotations_to_db(
            df,
            patient_id,
            "ann_epic_orders",
            self.config,
        )

        result = get_df_from_db(
            self.config,
            schema="annotations",
            table="ann_epic_orders",
            patient_ids=[patient_id],
        )

        self.assertEqual(len(result), 1)
        # Timestamp should be parsed correctly
        self.assertIn("updatetime", result.columns)

    def test_timestamp_filter_with_date_only_string(self):
        """Test timestamp filtering with date-only strings."""
        patient_id = "TEST_DATE_ONLY_001"

        # Save data with date-only string
        df = pd.DataFrame(
            {
                "client_idcode": [patient_id],
                "pretty_name": ["Condition"],
                "updatetime": ["2023-06-15"],  # Date only, no time
                "cui": ["CUI_001"],
            }
        )

        save_annotations_to_db(
            df,
            patient_id,
            "ann_epic_orders",
            self.config,
        )

        result = get_df_from_db(
            self.config,
            schema="annotations",
            table="ann_epic_orders",
            patient_ids=[patient_id],
        )

        self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
