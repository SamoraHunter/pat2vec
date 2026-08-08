"""
Comprehensive integration tests for Epic annotation table creation scenarios.

This module tests both empty DataFrame table creation and full integration
lifecycles with actual annotations for all 4 Epic annotation types:
- Clinical Notes
- Imaging Reports
- Medical History
- Orders

Test scenarios:
1. Empty DataFrame table creation (when ES returns no data)
2. Full integration lifecycle with actual annotations
"""

import os
import shutil
import sys
import tempfile
import unittest
import unittest.mock

import pandas as pd

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(TESTS_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# SQLAlchemy must be imported after setting up paths
from sqlalchemy import inspect as db_inspect

from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_cohort_searcher import dummy_CAT
from pat2vec.util.helper_functions import (
    save_annotations_to_db,
    save_raw_patient_batch,
)


def _get_test_config(base_config, **kwargs):
    """Helper to create test config with base settings."""
    config_dict = {
        "storage_backend": "database",
        "db_connection_string": "sqlite:///:memory:",
        "testing": True,
        "verbosity": 0,
        "batch_mode": True,
    }
    config_dict.update(kwargs)
    return config_class(**config_dict)


class TestEpicTableCreationEmptyDataFrame(unittest.TestCase):
    """Test cases for table creation with empty DataFrames (Scenario 1)."""

    def setUp(self):
        """Set up in-memory database for each test."""
        self.db_connection_string = "sqlite:///:memory:"

        self.config_clinical_notes = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_clinical_notes": True,
                "epic_clinical_notes_annotations": True,
            },
        )

        self.config_imaging = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_imaging_reports": True,
                "epic_imaging_reports_annotations": True,
            },
        )

        self.config_medical_history = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_medical_history": True,
                "epic_medical_history_annotations": True,
            },
        )

        self.config_orders = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_orders": True,
                "epic_orders_annotations": True,
            },
        )

    def _get_table_names(self, config):
        """Get all table names from database."""
        inspector = db_inspect(config.db_engine)
        return [
            t
            for s in inspector.get_schema_names()
            for t in inspector.get_table_names(schema=s)
        ]

    def test_epic_clinical_notes_empty_df_creates_tables(self):
        """Verify raw_data.raw_epic_clinical_notes and annotations.ann_epic_clinical_notes tables created with empty DataFrame."""
        config = self.config_clinical_notes
        patient_id = "TEST_CLIN_NOTE_EMPTY"

        # Empty DataFrame with columns (simulates ES returning no data but with schema)
        raw_df = pd.DataFrame(columns=["client_idcode", "updatetime", "body_analysed"])
        annot_df = pd.DataFrame(
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

        # Save both raw and annotation data
        save_raw_patient_batch(raw_df, patient_id, "raw_epic_clinical_notes", config)
        save_annotations_to_db(annot_df, patient_id, "ann_epic_clinical_notes", config)

        tables = self._get_table_names(config)

        # Verify both raw and annotation tables exist (SQLite schema prefix)
        inspector = db_inspect(config.db_engine)
        raw_table_found = False
        annot_table_found = False

        for table_name in [
            "raw_data_raw_epic_clinical_notes",
            "raw_epic_clinical_notes",
        ]:
            if inspector.has_table(table_name):
                raw_table_found = True
                break

        for table_name in [
            "annotations_ann_epic_clinical_notes",
            "ann_epic_clinical_notes",
        ]:
            if inspector.has_table(table_name):
                annot_table_found = True
                break

        self.assertTrue(
            raw_table_found,
            f"Raw epic clinical notes table should be created. Tables: {tables}",
        )
        self.assertTrue(
            annot_table_found, f"Annotation table should be created. Tables: {tables}"
        )

    def test_epic_imaging_reports_empty_df_creates_tables(self):
        """Verify raw_data.raw_epic_imaging_reports and annotations.ann_epic_imaging_reports tables created with empty DataFrame."""
        config = self.config_imaging
        patient_id = "TEST_IMG_EMPTY"

        raw_df = pd.DataFrame(columns=["client_idcode", "updatetime", "body_analysed"])
        annot_df = pd.DataFrame(
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

        save_raw_patient_batch(raw_df, patient_id, "raw_epic_imaging_reports", config)
        save_annotations_to_db(annot_df, patient_id, "ann_epic_imaging_reports", config)

        tables = self._get_table_names(config)

        inspector = db_inspect(config.db_engine)
        raw_table_found = False
        annot_table_found = False

        for table_name in [
            "raw_data_raw_epic_imaging_reports",
            "raw_epic_imaging_reports",
        ]:
            if inspector.has_table(table_name):
                raw_table_found = True
                break

        for table_name in [
            "annotations_ann_epic_imaging_reports",
            "ann_epic_imaging_reports",
        ]:
            if inspector.has_table(table_name):
                annot_table_found = True
                break

        self.assertTrue(
            raw_table_found,
            f"Raw epic imaging reports table should be created. Tables: {tables}",
        )
        self.assertTrue(
            annot_table_found, f"Annotation table should be created. Tables: {tables}"
        )

    def test_epic_medical_history_empty_df_creates_tables(self):
        """Verify raw_data.raw_epic_medical_history and annotations.ann_epic_medical_history tables created with empty DataFrame."""
        config = self.config_medical_history
        patient_id = "TEST_MED_EMPTY"

        raw_df = pd.DataFrame(columns=["client_idcode", "updatetime", "body_analysed"])
        annot_df = pd.DataFrame(
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

        save_raw_patient_batch(raw_df, patient_id, "raw_epic_medical_history", config)
        save_annotations_to_db(annot_df, patient_id, "ann_epic_medical_history", config)

        tables = self._get_table_names(config)

        inspector = db_inspect(config.db_engine)
        raw_table_found = False
        annot_table_found = False

        for table_name in [
            "raw_data_raw_epic_medical_history",
            "raw_epic_medical_history",
        ]:
            if inspector.has_table(table_name):
                raw_table_found = True
                break

        for table_name in [
            "annotations_ann_epic_medical_history",
            "ann_epic_medical_history",
        ]:
            if inspector.has_table(table_name):
                annot_table_found = True
                break

        self.assertTrue(
            raw_table_found,
            f"Raw epic medical history table should be created. Tables: {tables}",
        )
        self.assertTrue(
            annot_table_found, f"Annotation table should be created. Tables: {tables}"
        )

    def test_epic_orders_empty_df_creates_tables(self):
        """Verify raw_data.raw_epic_orders and annotations.ann_epic_orders tables created with empty DataFrame."""
        config = self.config_orders
        patient_id = "TEST_ORD_EMPTY"

        raw_df = pd.DataFrame(columns=["client_idcode", "updatetime", "body_analysed"])
        annot_df = pd.DataFrame(
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

        save_raw_patient_batch(raw_df, patient_id, "raw_epic_orders", config)
        save_annotations_to_db(annot_df, patient_id, "ann_epic_orders", config)

        tables = self._get_table_names(config)

        inspector = db_inspect(config.db_engine)
        raw_table_found = False
        annot_table_found = False

        for table_name in [
            "raw_data_raw_epic_orders",
            "raw_epic_orders",
        ]:
            if inspector.has_table(table_name):
                raw_table_found = True
                break

        for table_name in [
            "annotations_ann_epic_orders",
            "ann_epic_orders",
        ]:
            if inspector.has_table(table_name):
                annot_table_found = True
                break

        self.assertTrue(
            raw_table_found,
            f"Raw epic orders table should be created. Tables: {tables}",
        )
        self.assertTrue(
            annot_table_found, f"Annotation table should be created. Tables: {tables}"
        )

    def test_truly_empty_dataframe_no_columns_creates_minimal_table(self):
        """Test truly empty DataFrame (no rows, no columns) creates minimal table."""
        config = self.config_clinical_notes
        patient_id = "TEST_TRULY_EMPTY"

        # Completely empty DataFrame with no columns
        raw_df = pd.DataFrame()

        save_raw_patient_batch(raw_df, patient_id, "raw_epic_clinical_notes", config)

        tables = self._get_table_names(config)

        inspector = db_inspect(config.db_engine)
        raw_table_found = False

        for table_name in [
            "raw_data_raw_epic_clinical_notes",
            "raw_epic_clinical_notes",
        ]:
            if inspector.has_table(table_name):
                raw_table_found = True
                break

        self.assertTrue(
            raw_table_found,
            f"Table should be created from truly empty DataFrame. Tables: {tables}",
        )

    def test_empty_dataframe_columns_preserved(self):
        """Test that columns are preserved when creating table from empty DataFrame."""
        config = self.config_clinical_notes
        patient_id = "TEST_COLS_PRESERVED"

        raw_df = pd.DataFrame(
            {
                "client_idcode": [],
                "updatetime": [],
                "body_analysed": [],
                "document_guid": [],
                "document_description": [],
            }
        )

        save_raw_patient_batch(raw_df, patient_id, "raw_epic_clinical_notes", config)

        inspector = db_inspect(config.db_engine)

        table_name = None
        for tn in [
            "raw_data_raw_epic_clinical_notes",
            "raw_epic_clinical_notes",
        ]:
            if inspector.has_table(tn):
                table_name = tn
                break

        self.assertIsNotNone(table_name, "Table should be created")

        columns = [c["name"] for c in inspector.get_columns(table_name)]

        expected_cols = ["client_idcode", "updatetime", "body_analysed"]
        for col in expected_cols:
            self.assertIn(
                col,
                columns,
                f"Expected column {col} not found in table. Got: {columns}",
            )


class TestEpicAnnotationsFullIntegration(unittest.TestCase):
    """Test full integration lifecycle with actual annotations (Scenario 2).

    Tests the complete flow through get methods when:
    1. Database has no existing annotation data
    2. File backend doesn't have batch file
    3. ES returns empty DataFrame
    """

    def setUp(self):
        """Set up in-memory database for each test."""
        self.db_connection_string = "sqlite:///:memory:"

        self.base_date_str = "2023-06-15"
        self.year, self.month, self.day = 2023, 6, 15

    def test_epic_clinical_notes_empty_es_response(self):
        """Test get_pat_batch_epic_clinical_notes_annotations with empty ES response.

        When:
        - Database has no existing annotation data
        - File backend doesn't have batch file
        - ES returns empty DataFrame

        Expected: Table is created and empty DataFrame is returned
        """
        from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_clinical_notes_annotations import (
            get_pat_batch_epic_clinical_notes_annotations,
        )

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            global_start_year=self.year,
            global_start_month=self.month,
            global_start_day=self.day,
            all_patient_list=["TEST_EMPTY_ES"],
            batch_mode=True,
            main_options={
                "epic_clinical_notes": True,
                "epic_clinical_notes_annotations": True,
            },
        )

        cat = dummy_CAT()

        # Mock ES to return empty DataFrame (simulating no matching data)
        with unittest.mock.patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.cohort_searcher_with_terms_and_search",
            return_value=(None, pd.DataFrame()),
        ):
            t = None

            result = get_pat_batch_epic_clinical_notes_annotations(
                current_pat_client_id_code="TEST_EMPTY_ES",
                config_obj=config,
                cat=cat,
                t=t,
                cohort_searcher_with_terms_and_search=None,
            )

            # Should return empty DataFrame with expected columns
            self.assertIsInstance(result, pd.DataFrame)

            # Verify annotation table was created
            inspector = db_inspect(config.db_engine)
            table_found = False

            for tn in [
                "annotations_ann_epic_clinical_notes",
                "ann_epic_clinical_notes",
            ]:
                if inspector.has_table(tn):
                    table_found = True
                    break

            self.assertTrue(
                table_found,
                f"Annotation table should be created even with empty ES response. Tables: {inspector.get_table_names()}",
            )

    def test_epic_imaging_reports_empty_es_response(self):
        """Test get_pat_batch_epic_imaging_reports_annotations with empty ES response."""
        from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_imaging_reports_annotations import (
            get_pat_batch_epic_imaging_reports_annotations,
        )

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            global_start_year=self.year,
            global_start_month=self.month,
            global_start_day=self.day,
            all_patient_list=["TEST_EMPTY_IMG"],
            batch_mode=True,
            main_options={
                "epic_imaging_reports": True,
                "epic_imaging_reports_annotations": True,
            },
        )

        cat = dummy_CAT()

        with unittest.mock.patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.cohort_searcher_with_terms_and_search",
            return_value=(None, pd.DataFrame()),
        ):
            t = None

            result = get_pat_batch_epic_imaging_reports_annotations(
                current_pat_client_id_code="TEST_EMPTY_IMG",
                config_obj=config,
                cat=cat,
                t=t,
                cohort_searcher_with_terms_and_search=None,
            )

            self.assertIsInstance(result, pd.DataFrame)

            inspector = db_inspect(config.db_engine)
            table_found = False

            for tn in [
                "annotations_ann_epic_imaging_reports",
                "ann_epic_imaging_reports",
            ]:
                if inspector.has_table(tn):
                    table_found = True
                    break

            self.assertTrue(
                table_found,
                f"Annotation table should be created. Tables: {inspector.get_table_names()}",
            )

    def test_epic_medical_history_empty_es_response(self):
        """Test get_pat_batch_epic_medical_history_annotations with empty ES response."""
        from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_medical_history_annotations import (
            get_pat_batch_epic_medical_history_annotations,
        )

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            global_start_year=self.year,
            global_start_month=self.month,
            global_start_day=self.day,
            all_patient_list=["TEST_EMPTY_MED"],
            batch_mode=True,
            main_options={
                "epic_medical_history": True,
                "epic_medical_history_annotations": True,
            },
        )

        cat = dummy_CAT()

        with unittest.mock.patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.cohort_searcher_with_terms_and_search",
            return_value=(None, pd.DataFrame()),
        ):
            t = None

            result = get_pat_batch_epic_medical_history_annotations(
                current_pat_client_id_code="TEST_EMPTY_MED",
                config_obj=config,
                cat=cat,
                t=t,
                cohort_searcher_with_terms_and_search=None,
            )

            self.assertIsInstance(result, pd.DataFrame)

            inspector = db_inspect(config.db_engine)
            table_found = False

            for tn in [
                "annotations_ann_epic_medical_history",
                "ann_epic_medical_history",
            ]:
                if inspector.has_table(tn):
                    table_found = True
                    break

            self.assertTrue(
                table_found,
                f"Annotation table should be created. Tables: {inspector.get_table_names()}",
            )

    def test_epic_orders_empty_es_response(self):
        """Test get_pat_batch_epic_orders_annotations with empty ES response."""
        from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_orders_annotations import (
            get_pat_batch_epic_orders_annotations,
        )

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            global_start_year=self.year,
            global_start_month=self.month,
            global_start_day=self.day,
            all_patient_list=["TEST_EMPTY_ORD"],
            batch_mode=True,
            main_options={
                "epic_orders": True,
                "epic_orders_annotations": True,
            },
        )

        cat = dummy_CAT()

        with unittest.mock.patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.cohort_searcher_with_terms_and_search",
            return_value=(None, pd.DataFrame()),
        ):
            t = None

            result = get_pat_batch_epic_orders_annotations(
                current_pat_client_id_code="TEST_EMPTY_ORD",
                config_obj=config,
                cat=cat,
                t=t,
                cohort_searcher_with_terms_and_search=None,
            )

            self.assertIsInstance(result, pd.DataFrame)

            inspector = db_inspect(config.db_engine)
            table_found = False

            for tn in [
                "annotations_ann_epic_orders",
                "ann_epic_orders",
            ]:
                if inspector.has_table(tn):
                    table_found = True
                    break

            self.assertTrue(
                table_found,
                f"Annotation table should be created. Tables: {inspector.get_table_names()}",
            )


class TestEpicTableCreationWithMockedES(unittest.TestCase):
    """Test table creation when mocking ES to return empty responses.

    This scenario tests the full get pattern through main_pat2vec.py methods
    where ES returns an empty DataFrame, triggering the table creation logic.
    """

    def setUp(self):
        self.db_connection_string = "sqlite:///:memory:"

    def test_clinical_notes_full_flow_empty_es(self):
        """Test complete flow for clinical notes with mocked empty ES response."""
        from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_clinical_notes_annotations import (
            get_pat_batch_epic_clinical_notes_annotations,
        )

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            global_start_year=2023,
            global_start_month=6,
            global_start_day=15,
            all_patient_list=["PAT_FULL_FLOW_001"],
            batch_mode=True,
            main_options={
                "epic_clinical_notes": True,
                "epic_clinical_notes_annotations": True,
            },
        )

        cat = dummy_CAT()

        # Mock ES to return empty DataFrame
        with unittest.mock.patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.cohort_searcher_with_terms_and_search",
            return_value=(None, pd.DataFrame()),
        ):
            t = None

            result = get_pat_batch_epic_clinical_notes_annotations(
                current_pat_client_id_code="PAT_FULL_FLOW_001",
                config_obj=config,
                cat=cat,
                t=t,
                cohort_searcher_with_terms_and_search=None,
            )

            # Verify table creation
            inspector = db_inspect(config.db_engine)
            tables = inspector.get_table_names()

            self.assertIsInstance(result, pd.DataFrame)
            self.assertGreater(len(tables), 0, "At least one table should be created")

    def test_imaging_reports_full_flow_empty_es(self):
        """Test complete flow for imaging reports with mocked empty ES response."""
        from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_imaging_reports_annotations import (
            get_pat_batch_epic_imaging_reports_annotations,
        )

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            global_start_year=2023,
            global_start_month=6,
            global_start_day=15,
            all_patient_list=["PAT_FULL_FLOW_002"],
            batch_mode=True,
            main_options={
                "epic_imaging_reports": True,
                "epic_imaging_reports_annotations": True,
            },
        )

        cat = dummy_CAT()

        with unittest.mock.patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.cohort_searcher_with_terms_and_search",
            return_value=(None, pd.DataFrame()),
        ):
            t = None

            result = get_pat_batch_epic_imaging_reports_annotations(
                current_pat_client_id_code="PAT_FULL_FLOW_002",
                config_obj=config,
                cat=cat,
                t=t,
                cohort_searcher_with_terms_and_search=None,
            )

            self.assertIsInstance(result, pd.DataFrame)

            inspector = db_inspect(config.db_engine)
            tables = inspector.get_table_names()

            self.assertGreater(len(tables), 0, "At least one table should be created")

    def test_medical_history_full_flow_empty_es(self):
        """Test complete flow for medical history with mocked empty ES response."""
        from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_medical_history_annotations import (
            get_pat_batch_epic_medical_history_annotations,
        )

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            global_start_year=2023,
            global_start_month=6,
            global_start_day=15,
            all_patient_list=["PAT_FULL_FLOW_003"],
            batch_mode=True,
            main_options={
                "epic_medical_history": True,
                "epic_medical_history_annotations": True,
            },
        )

        cat = dummy_CAT()

        with unittest.mock.patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.cohort_searcher_with_terms_and_search",
            return_value=(None, pd.DataFrame()),
        ):
            t = None

            result = get_pat_batch_epic_medical_history_annotations(
                current_pat_client_id_code="PAT_FULL_FLOW_003",
                config_obj=config,
                cat=cat,
                t=t,
                cohort_searcher_with_terms_and_search=None,
            )

            self.assertIsInstance(result, pd.DataFrame)

            inspector = db_inspect(config.db_engine)
            tables = inspector.get_table_names()

            self.assertGreater(len(tables), 0, "At least one table should be created")

    def test_orders_full_flow_empty_es(self):
        """Test complete flow for orders with mocked empty ES response."""
        from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_orders_annotations import (
            get_pat_batch_epic_orders_annotations,
        )

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            global_start_year=2023,
            global_start_month=6,
            global_start_day=15,
            all_patient_list=["PAT_FULL_FLOW_004"],
            batch_mode=True,
            main_options={
                "epic_orders": True,
                "epic_orders_annotations": True,
            },
        )

        cat = dummy_CAT()

        with unittest.mock.patch(
            "pat2vec.pat2vec_search.cogstack_search_methods.cohort_searcher_with_terms_and_search",
            return_value=(None, pd.DataFrame()),
        ):
            t = None

            result = get_pat_batch_epic_orders_annotations(
                current_pat_client_id_code="PAT_FULL_FLOW_004",
                config_obj=config,
                cat=cat,
                t=t,
                cohort_searcher_with_terms_and_search=None,
            )

            self.assertIsInstance(result, pd.DataFrame)

            inspector = db_inspect(config.db_engine)
            tables = inspector.get_table_names()

            self.assertGreater(len(tables), 0, "At least one table should be created")


class TestEpicTableCreationEdgeCases(unittest.TestCase):
    """Test edge cases for Epic annotation table creation."""

    def setUp(self):
        self.db_connection_string = "sqlite:///:memory:"

    def test_raw_table_creation_columns_preserved(self):
        """Test that raw data table preserves all columns from empty DataFrame."""
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        # Create DataFrame with various column types
        df = pd.DataFrame(
            {
                "client_idcode": pd.Series([], dtype=str),
                "updatetime": pd.Series([], dtype=str),
                "body_analysed": pd.Series([], dtype=str),
                "document_guid": pd.Series([], dtype=str),
                "document_description": pd.Series([], dtype=str),
            }
        )

        save_raw_patient_batch(df, "TEST_RAW_COLS", "raw_epic_clinical_notes", config)

        inspector = db_inspect(config.db_engine)

        table_name = None
        for tn in [
            "raw_data_raw_epic_clinical_notes",
            "raw_epic_clinical_notes",
        ]:
            if inspector.has_table(tn):
                table_name = tn
                break

        self.assertIsNotNone(table_name, "Raw table should be created")

        columns = [c["name"] for c in inspector.get_columns(table_name)]

        expected_cols = ["client_idcode", "updatetime", "body_analysed"]
        for col in expected_cols:
            self.assertIn(col, columns)

    def test_annotation_table_creation_with_explicit_id_column(self):
        """Test annotation table creation with explicit id_column parameter."""
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        df = pd.DataFrame(
            columns=[
                "client_idcode",
                "pretty_name",
                "cui",
                "acc",
            ]
        )

        save_annotations_to_db(
            df,
            "TEST_ANNO_ID",
            "ann_epic_clinical_notes",
            config,
            id_column="client_idcode",
        )

        inspector = db_inspect(config.db_engine)

        table_name = None
        for tn in [
            "annotations_ann_epic_clinical_notes",
            "ann_epic_clinical_notes",
        ]:
            if inspector.has_table(tn):
                table_name = tn
                break

        self.assertIsNotNone(table_name, "Annotation table should be created")

        columns = [c["name"] for c in inspector.get_columns(table_name)]

        # Verify client_idcode column exists (from id_column parameter)
        self.assertIn("client_idcode", columns)


class TestEpicFullPipelineDatabase(unittest.TestCase):
    """Test full pipeline flow with Epic annotations using database backend."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "epic_full_pipeline_db_test"
        self.db_path = os.path.join(self.test_dir, f"{self.project_name}.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_epic_clinical_notes_annotations_full_pipeline(self):
        """Test full pipeline for epic_clinical_notes_annotations with database backend."""
        from pat2vec.main_pat2vec import main
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            generate_patient_timeline_faker,
        )

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            dummy_medcat_model=True,
            verbosity=0,
            proj_name=self.project_name,
            root_path=self.test_dir,
            global_start_year=2020,
            global_start_month=1,
            global_start_day=1,
            global_end_year=2025,
            global_end_month=1,
            global_end_day=1,
            all_patient_list=["TEST_FULL_PIPELINE_001"],
            batch_mode=True,
            main_options={
                "epic_clinical_notes": True,
                "epic_clinical_notes_annotations": True,
            },
        )

        with unittest.mock.patch(
            "pat2vec.util.get_dummy_data_cohort_searcher.get_patient_timeline_dummy",
            side_effect=generate_patient_timeline_faker,
        ):
            pat2vec_obj = main(config_obj=config, cogstack=True)
            pat2vec_obj.pat_maker(0)

            inspector = db_inspect(config.db_engine)
            tables = inspector.get_table_names()

            self.assertTrue(
                any("epic_clinical_notes" in t for t in tables),
                f"Clinical notes tables should exist. Tables: {tables}",
            )

    def test_all_epic_annotation_sources_table_creation(self):
        """Test that all epic annotation sources create tables in full pipeline."""
        from pat2vec.main_pat2vec import main
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            generate_patient_timeline_faker,
        )

        project_name = "epic_all_annotations_test"
        db_path = os.path.join(self.test_dir, f"{project_name}.sqlite")

        config = config_class(
            storage_backend="database",
            db_connection_string=f"sqlite:///{db_path}",
            testing=True,
            dummy_medcat_model=True,
            verbosity=0,
            proj_name=project_name,
            root_path=self.test_dir,
            global_start_year=2020,
            global_start_month=1,
            global_start_day=1,
            global_end_year=2025,
            global_end_month=1,
            global_end_day=1,
            all_patient_list=["TEST_ALL_ANNO_001"],
            batch_mode=True,
            main_options={
                "epic_clinical_notes_annotations": True,
                "epic_imaging_reports_annotations": True,
                "epic_medical_history_annotations": True,
                "epic_orders_annotations": True,
            },
        )

        with unittest.mock.patch(
            "pat2vec.util.get_dummy_data_cohort_searcher.get_patient_timeline_dummy",
            side_effect=generate_patient_timeline_faker,
        ):
            pat2vec_obj = main(config_obj=config, cogstack=True)
            pat2vec_obj.pat_maker(0)

            inspector = db_inspect(config.db_engine)
            tables = inspector.get_table_names()

            expected_annotations = [
                "epic_clinical_notes",
                "epic_imaging_reports",
                "epic_medical_history",
                "epic_orders",
            ]

            for ann_type in expected_annotations:
                self.assertTrue(
                    any(ann_type.replace("_annotations", "") in t for t in tables),
                    f"Table for {ann_type} should exist. Tables: {tables}",
                )


class TestEpicEmptyBatchEdgeCases(unittest.TestCase):
    """Test edge cases related to empty batches and their impact on table creation."""

    def setUp(self):
        self.db_connection_string = "sqlite:///:memory:"

    def test_multiple_empty_batches_create_single_table(self):
        """Test that multiple empty batches for same patient don't create duplicate tables."""
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        empty_df = pd.DataFrame(
            columns=["client_idcode", "updatetime", "body_analysed"]
        )

        # Save same patient with empty batch multiple times
        for i in range(3):
            save_raw_patient_batch(
                empty_df, f"TEST_MULTI_EMPTY_{i}", "raw_epic_clinical_notes", config
            )

        inspector = db_inspect(config.db_engine)

        table_found = False
        for tn in [
            "raw_data_raw_epic_clinical_notes",
            "raw_epic_clinical_notes",
        ]:
            if inspector.has_table(tn):
                table_found = True
                break

        self.assertTrue(table_found, "Table should be created")

    def test_mixed_empty_and_nonempty_batches(self):
        """Test that mixing empty and non-empty batches works correctly."""
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        # Empty batch first
        save_raw_patient_batch(
            pd.DataFrame(columns=["client_idcode", "updatetime", "body_analysed"]),
            "TEST_MIXED_001",
            "raw_epic_clinical_notes",
            config,
        )

        # Non-empty batch for same patient (should overwrite)
        non_empty_df = pd.DataFrame(
            {
                "client_idcode": ["TEST_MIXED_001"],
                "updatetime": ["2023-06-15"],
                "body_analysed": ["Test document"],
            }
        )

        save_raw_patient_batch(
            non_empty_df, "TEST_MIXED_001", "raw_epic_clinical_notes", config
        )

        # Retrieve and verify
        from pat2vec.util.helper_functions import get_df_from_db

        df = get_df_from_db(
            config,
            schema="raw_data",
            table="raw_epic_clinical_notes",
            patient_ids=["TEST_MIXED_001"],
        )

        self.assertEqual(len(df), 1)
        self.assertIn("Test document", df["body_analysed"].values)


if __name__ == "__main__":
    unittest.main(verbosity=2)
