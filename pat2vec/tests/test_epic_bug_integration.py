"""
Integration tests to reproduce the Epic clinical notes annotation bug.

This test module focuses on the exact flow described in the bug report:
1. pat2vec runs in testing mode with `testing=True` and `dummy_medcat_model=True`
2. ES search returns empty data (expected in testing mode)
3. Tables don't get created because batch is empty
4. Annotation retrieval fails because table doesn't exist

The expected behavior after fix:
- Tables should be created even when initial batches are empty
- Annotations can be generated from dummy data for annotation sources
"""

import unittest
import os
import sys
import tempfile
import shutil
import logging

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# SQLAlchemy must be imported after setting up paths
from sqlalchemy import inspect as db_inspect  # noqa: E402

import pandas as pd  # noqa: E402
from pat2vec.util.config_pat2vec import config_class  # noqa: E402
from pat2vec.util.helper_functions import (  # noqa: E402
    get_df_from_db,
    save_raw_patient_batch,
    save_annotations_to_db,
)


class TestEpicBugReproduction(unittest.TestCase):
    """
    Reproduce the exact bug scenario from example_usage_simple_epic.ipynb.

    The notebook:
    1. Creates a pat2vec config with testing=True and dummy_medcat_model=True
    2. Runs main() which should populate raw_epic_clinical_notes table
    3. Calls get_pat_batch_epic_clinical_notes_annotations()
    4. Builds annotation dataframe using build_merged_epr_mct_annot_df()

    Bug: When ES returns empty data, the raw table is never created because:
    - _save_batches_to_db only saves if batch is NOT empty (line 1470)
    - Empty batches from ES return no data
    - Annotation retrieval fails because tables don't exist
    """

    def setUp(self):
        """Set up test environment with temporary database."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test_db.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

    def tearDown(self):
        """Clean up test resources."""
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _get_inspector(self, config):
        """Get SQLAlchemy inspector from config."""
        return db_inspect(config.db_engine)

    def test_bug_scenario_empty_es_batch_creates_table(self):
        """
        Reproduce the bug: empty ES results shouldstill create tables.

        In testing mode with no ES data:
        1. _save_batches_to_db gets empty batches
        2. Empty batches don't save because of `if batches[batch_key].empty and not is_enabled`
        3. Tables never get created
        4. Subsequent annotation retrieval fails

        Expected: Even empty batches should create tables in testing mode.
        """
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_clinical_notes": True,  # Enabled
            },
        )

        # Simulate what _save_batches_to_db does with an empty batch
        # This is the problematic scenario: ES returns empty DataFrame

        # Scenario 1: Empty batch (what happens when ES returns no data)
        empty_batch = pd.DataFrame(
            columns=["client_idcode", "updatetime", "document_Content"]
        )

        save_raw_patient_batch(
            empty_batch, "TEST_PAT_001", "raw_epic_clinical_notes", config
        )

        # After this, the table should exist
        inspector = self._get_inspector(config)
        table_names = [
            t
            for s in inspector.get_schema_names()
            for t in inspector.get_table_names(schema=s)
        ]

        print(f"Tables after empty batch save: {table_names}")

        # The fix ensures table is created even with empty data
        table_found = False
        for tn in ["raw_data_raw_epic_clinical_notes", "raw_epic_clinical_notes"]:
            if inspector.has_table(tn):
                table_found = True
                break

        self.assertTrue(
            table_found,
            f"Table should be created even when batch is empty. Tables: {table_names}",
        )

    def test_annotation_retrieval_after_raw_table_creation(self):
        """
        Test that annotation retrieval works after raw data table is created.

        The notebook flow:
        1. Raw clinical notes are saved to `raw_epic_clinical_notes` table
        2. get_pat_batch_epic_clinical_notes_annotations() reads from this table
        3. Annotations are generated via MedCAT and saved to `ann_epic_clinical_notes`
        4. build_merged_epr_mct_annot_df() retrieve annotations

        Bug: If step 1 creates empty batch, table doesn't exist
             So step 2 fails because get_df_from_db returns empty DataFrame
        """
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_clinical_notes": True,
            },
        )

        # Create the raw data table (simulating _save_batches_to_db with empty batch)
        empty_batch = pd.DataFrame(
            columns=["client_idcode", "updatetime", "document_Content"]
        )
        save_raw_patient_batch(
            empty_batch, "TEST_PAT_001", "raw_epic_clinical_notes", config
        )

        # Now try to retrieve from the table (this is what get_pat_batch_epic_clinical_notes_annotations does)
        df = get_df_from_db(
            config,
            schema="raw_data",
            table="raw_epic_clinical_notes",
            patient_ids=["TEST_PAT_001"],
        )

        # This should NOT raise an error - table exists even though data is empty
        self.assertIsInstance(df, pd.DataFrame)
        # Should be empty because we used empty batch, but no exception

    def test_annotation_table_creation_with_empty_batch(self):
        """
        Test annotation table creation when annotation batch is empty.

        After raw data is read,MedCAT generates annotations.
        If the MedCAT model returns empty annotations (edge case),
        save_annotations_to_db still needs to create the table.
        """
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        # Empty annotation DataFrame with all expected columns
        empty_annot_df = pd.DataFrame(
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

        save_annotations_to_db(
            empty_annot_df, "TEST_PAT_001", "ann_epic_clinical_notes", config
        )

        inspector = self._get_inspector(config)

        # Check if annotation table was created
        table_found = False
        for tn in ["annotations_ann_epic_clinical_notes", "ann_epic_clinical_notes"]:
            if inspector.has_table(tn):
                table_found = True
                break

        self.assertTrue(
            table_found,
            f"Annotation table should be created even with empty batch. Tables: {inspector.get_table_names()}",
        )

    def test_full_workflow_tables_exist(self):
        """
        Test that all required tables exist for the epic clinical notes workflow.

        The notebook needs:
        1. raw_epic_clinical_notes - raw data input
        2. ann_epic_clinical_notes - annotations output

        Both should be created even when initial data is empty.
        """
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
            main_options={
                "epic_clinical_notes": True,
            },
        )

        # Create both tables (simulating what should happen during initialization)

        # 1. Raw data table
        empty_raw = pd.DataFrame(
            columns=["client_idcode", "updatetime", "document_Content"]
        )
        save_raw_patient_batch(
            empty_raw, "TEST_PAT_001", "raw_epic_clinical_notes", config
        )

        # 2. Annotation table
        empty_annot = pd.DataFrame(
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
        save_annotations_to_db(
            empty_annot, "TEST_PAT_001", "ann_epic_clinical_notes", config
        )

        inspector = self._get_inspector(config)
        table_names = inspector.get_table_names()

        # Verify both tables exist
        raw_found = any("raw_epic_clinical_notes" in tn for tn in table_names)
        annot_found = any("ann_epic_clinical_notes" in tn for tn in table_names)

        self.assertTrue(
            raw_found, f"raw_epic_clinical_notes table not found. Tables: {table_names}"
        )
        self.assertTrue(
            annot_found,
            f"ann_epic_clinical_notes table not found. Tables: {table_names}",
        )

    def test_retrieve_annotation_empty_patient_ids(self):
        """
        Test that get_df_from_db handles empty patient_id list correctly.

        This edge case can happen when the all_patient_list is empty or
        when filtering results in no matching patients.
        """
        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        # Create the annotation table first
        empty_annot = pd.DataFrame(
            columns=["client_idcode", "pretty_name", "cui", "type_ids", "types"]
        )
        save_annotations_to_db(
            empty_annot, "TEST_PAT_001", "ann_epic_clinical_notes", config
        )

        # Retrieve with empty patient list - should return empty DataFrame, not crash
        df = get_df_from_db(
            config,
            schema="annotations",
            table="ann_epic_clinical_notes",
            patient_ids=[],  # Empty list
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    def test_get_all_features_empty_database(self):
        """
        Test that get_all_features returns empty DataFrame for fresh database.

        This simulates the state where no features have been generated yet.
        """
        from pat2vec.util.helper_functions import get_all_features

        config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )

        # Get all features from fresh database
        df = get_all_features(config)

        # Should return empty DataFrame, not crash
        self.assertIsInstance(df, pd.DataFrame)


class TestEpicFullPipelineIntegration(unittest.TestCase):
    """Test full pipeline integration with Epic annotations via database backend."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "epic_pipeline_integration_test"
        self.db_path = os.path.join(self.test_dir, f"{self.project_name}.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_epic_clinical_notes_full_pipeline_db_backend(self):
        """Test complete pipeline for epic_clinical_notes_annotations using database backend."""
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
            all_patient_list=["TEST_FULL_PIPELINE_DB_001"],
            batch_mode=True,
            main_options={
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

            raw_found = any("epic_clinical_notes" in t and "raw" in t for t in tables)
            annot_found = any(
                "ann_epic_clinical_notes" in t or "epic_clinical_notes" in t
                for t in tables
            )

            self.assertTrue(raw_found, f"Raw table should exist. Tables: {tables}")
            self.assertTrue(
                annot_found, f"Annotation table should exist. Tables: {tables}"
            )

    def test_epic_imaging_reports_full_pipeline_db(self):
        """Test complete pipeline for epic_imaging_reports_annotations."""
        from pat2vec.main_pat2vec import main
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            generate_patient_timeline_faker,
        )

        project_name = "epic_img_pipeline_test"
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
            all_patient_list=["TEST_IMG_PIPELINE_001"],
            batch_mode=True,
            main_options={
                "epic_imaging_reports_annotations": True,
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
                any("epic_imaging_reports" in t for t in tables),
                f"Imaging reports table should exist. Tables: {tables}",
            )

    def test_epic_medical_history_full_pipeline_db(self):
        """Test complete pipeline for epic_medical_history_annotations."""
        from pat2vec.main_pat2vec import main
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            generate_patient_timeline_faker,
        )

        project_name = "epic_med_pipeline_test"
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
            all_patient_list=["TEST_MED_PIPELINE_001"],
            batch_mode=True,
            main_options={
                "epic_medical_history_annotations": True,
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
                any("epic_medical_history" in t for t in tables),
                f"Medical history table should exist. Tables: {tables}",
            )

    def test_epic_orders_full_pipeline_db(self):
        """Test complete pipeline for epic_orders_annotations."""
        from pat2vec.main_pat2vec import main
        from pat2vec.util.get_dummy_data_cohort_searcher import (
            generate_patient_timeline_faker,
        )

        project_name = "epic_ord_pipeline_test"
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
            all_patient_list=["TEST_ORD_PIPELINE_001"],
            batch_mode=True,
            main_options={
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

            self.assertTrue(
                any("epic_orders" in t for t in tables),
                f"Orders table should exist. Tables: {tables}",
            )


if __name__ == "__main__":
    import logging
    import pandas as pd

    logging.basicConfig(level=logging.WARNING)

    unittest.main(verbosity=2)
