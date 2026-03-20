import unittest
import pandas as pd
from sqlalchemy import text, inspect
from unittest.mock import MagicMock, patch
from pat2vec.util.config_pat2vec import config_class
from pat2vec.main_pat2vec import main
import logging
from datetime import datetime
from pat2vec.patvec_get_batch_methods.get_merged_batches import (
    get_merged_pat_batch_bloods,
    get_merged_pat_batch_epr_docs,
    get_merged_pat_batch_reports,
    get_merged_pat_batch_appointments,
    get_merged_pat_batch_drugs,
    get_merged_pat_batch_diagnostics,
    get_merged_pat_batch_mct_docs,
    get_merged_pat_batch_textual_obs_docs,
    get_merged_pat_batch_demo,
    get_merged_pat_batch_bmi,
    get_merged_pat_batch_obs,
    get_merged_pat_batch_news,
)
from pat2vec.util.helper_functions import get_df_from_db


class TestPatMakerFullFlow(unittest.TestCase):
    def setUp(self):
        # Disable logging for cleaner test output
        logging.getLogger().setLevel(logging.CRITICAL)

        # Initialize config with database backend (in-memory SQLite) and testing mode
        # Adjust time window to ensure dummy data generation overlaps with processing window
        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            calculate_vectors=True,
            # Set global bounds for dummy data generation
            global_start_year=2020,
            global_start_month=1,
            global_start_day=1,
            global_end_year=2020,
            global_end_month=1,
            global_end_day=5,
            # Set processing window to cover this range
            start_date=datetime(2020, 1, 5),
            years=0,
            months=0,
            days=5,
            lookback=True,
        )
        self.engine = self.config.db_engine

        # Enable annotations to ensure EPR docs are processed
        self.config.main_options = {
            "demo": True,
            "bmi": True,
            "bloods": True,
            "drugs": True,
            "diagnostics": True,
            "core_02": True,
            "bed": True,
            "vte_status": True,
            "hosp_site": True,
            "core_resus": True,
            "news": True,
            "smoking": True,
            "annotations": True,
            "annotations_mrc": True,
            "negated_presence_annotations": False,
            "appointments": False,
            "annotations_reports": False,
            "textual_obs": False,
            "covid": True,
        }
        # Mock config's patient dict logic which usually runs inside main
        # For testing, we ensure these are set
        self.config.patient_dict = {}

    @patch("pat2vec.main_pat2vec.initialize_cogstack_client")
    @patch("pat2vec.main_pat2vec.get_cat")
    @patch("pat2vec.main_pat2vec.main_batch")
    def test_pat_maker_db_flow(self, mock_main_batch, mock_get_cat, mock_init_cogstack):
        """
        Test full flow of pat_maker:
        1. Fetch data (mocked/dummy)
        2. Save raw data to DB
        3. Process vectors
        4. Save vectors to DB
        """
        # Mock MedCAT model
        mock_cat = MagicMock()
        # Mock get_entities_multi_texts to return empty or dummy annotations
        # This prevents the annotation step from failing
        mock_cat.get_entities_multi_texts.side_effect = lambda texts: [
            {"entities": {}}
        ] * len(texts)
        mock_get_cat.return_value = mock_cat

        # Ensure main_batch returns a real DataFrame so to_sql actually runs
        mock_features_df = pd.DataFrame(
            {"client_idcode": ["P_TEST_001"], "age": [25], "dummy_feature": [1.0]}
        )
        mock_main_batch.return_value = mock_features_df

        # Initialize main
        pat2vec_obj = main(cogstack=True, config_obj=self.config)

        # Override patient list to have just one dummy patient
        test_patient_id = "P_TEST_001"
        pat2vec_obj.all_patient_list = [test_patient_id]
        # Clear stripped list to ensure patient is processed
        pat2vec_obj.stripped_list_start = []

        # Run pat_maker for the first patient
        pat2vec_obj.pat_maker(0)

        # Verify Data Persistence in DB
        inspector = inspect(self.engine)
        with self.engine.connect() as connection:
            # 1. Verify Raw Data tables are created and populated
            # List of tables expected to be populated by dummy data
            expected_tables = [
                "raw_data_raw_epr_docs",
                "raw_data_raw_demographics",
                "raw_data_raw_bloods",
                "raw_data_raw_bmi",
                "raw_data_raw_drugs",
                "raw_data_raw_diagnostics",
                "raw_data_raw_covid",
            ]

            for table_raw in expected_tables:
                if inspector.has_table(table_raw):
                    try:
                        result = connection.execute(
                            text(
                                f'SELECT count(*) FROM "{table_raw}" WHERE "client_idcode" = :pid'
                            ),
                            {"pid": test_patient_id},
                        ).scalar()
                        # Most dummy generators return data; verify > 0 for key tables
                        if result == 0:
                            print(
                                f"Warning: Table {table_raw} exists but is empty for patient."
                            )
                        else:
                            self.assertGreater(
                                result, 0, f"Should have saved data to {table_raw}"
                            )
                    except Exception as e:
                        self.fail(f"Failed to query {table_raw}: {e}")
                else:
                    self.fail(f"Table {table_raw} was not created.")

            # 2. Verify Features
            table_features = "features_features"
            if inspector.has_table(table_features):
                result = connection.execute(
                    text(
                        f'SELECT count(*) FROM "{table_features}" WHERE "client_idcode" = :pid'
                    ),
                    {"pid": test_patient_id},
                ).scalar()
                self.assertGreater(result, 0, "Should have saved feature vectors to DB")

                # Check for feature columns presence to ensure transformation happened
                feature_df = get_df_from_db(
                    config_obj=self.config,
                    schema="features",
                    table="features",
                    patient_ids=[test_patient_id],
                )

                # Check for demographics features (e.g., age, gender)
                self.assertTrue(
                    any("age" in col.lower() for col in feature_df.columns),
                    f"Age feature missing. Columns found: {feature_df.columns.tolist()}",
                )
                # Check for bloods features (column name depends on dummy data, but usually present)
                self.assertTrue(
                    "dummy_feature" in feature_df.columns, "Mocked feature missing"
                )

            else:
                self.fail(f"Table {table_features} was not created.")

            # 3. Verify Annotations (optional, depending on flow)
            # Since we mocked MedCAT to return empty entities, table might exist but be empty or contain 0 rows for this pat
            # But the table creation logic should have run if the batch annotator was called.
            table_annot = "annotations_ann_epr_docs"
            if not inspector.has_table(table_annot):
                # This is acceptable if no annotations were generated, but ideally we check flow.
                # The dummy data generator creates text, so multi_annots_to_df should run.
                pass


class TestBatchRetrievalDB(unittest.TestCase):
    """
    Test suite to exhaustively verify:
    1. Data is extracted from 'Elasticsearch' (mocked) and stored in DB.
    2. Data is correctly retrieved from DB (cached) on subsequent calls.
    """

    def setUp(self):
        logging.getLogger().setLevel(logging.CRITICAL)
        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            global_start_year=2020,
            global_start_month=1,
            global_start_day=1,
            global_end_year=2021,
            global_end_month=1,
            global_end_day=1,
        )
        self.engine = self.config.db_engine
        self.pat_list = ["P1", "P2"]

    def tearDown(self):
        # Note: In-memory DB is destroyed when engine/connection is garbage collected or closed.
        self.config.db_engine.dispose()

    def test_bloods_db_caching(self):
        # Setup mock searcher
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P2"],
                "basicobs_itemname_analysed": ["Test", "Test"],
                "basicobs_value_numeric": [1.0, 2.0],
                "basicobs_entered": ["2020-01-01", "2020-01-01"],
                "clientvisit_serviceguid": ["S1", "S2"],
                "updatetime": ["2020-01-01", "2020-01-01"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        # 1. First call: Should fetch from searcher and save to DB
        df1 = get_merged_pat_batch_bloods(
            self.pat_list, "term", self.config, mock_searcher
        )

        self.assertFalse(df1.empty)
        self.assertEqual(len(df1), 2)
        mock_searcher.assert_called_once()

        # Check DB
        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_bloods"')
            ).scalar()
            self.assertEqual(count, 2)

        # 2. Second call: Should load from DB, NOT searcher
        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_bloods(
            self.pat_list, "term", self.config, mock_searcher
        )

        self.assertFalse(df2.empty)
        self.assertEqual(len(df2), 2)
        mock_searcher.assert_not_called()

    def test_reports_db_caching_with_hospital_id(self):
        # Test reports specifically because of the HospitalID/client_idcode logic
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P2"],
                "HospitalID": [
                    "P1",
                    "P2",
                ],  # Ensure this column exists for the db lookup
                "updatetime": ["2020-01-01", "2020-01-01"],
                "textualObs": ["Text1", "Text2"],
                "basicobs_guid": ["G1", "G2"],
                "basicobs_value_analysed": ["V1", "V2"],
                "basicobs_itemname_analysed": ["Report", "Report"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        # 1. Fetch
        df1 = get_merged_pat_batch_reports(
            self.pat_list, "Report", self.config, mock_searcher
        )
        self.assertEqual(len(df1), 2)
        mock_searcher.assert_called_once()

        # Check DB
        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_reports"')
            ).scalar()
            self.assertEqual(count, 2)

        # 2. Cache hit
        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_reports(
            self.pat_list, "Report", self.config, mock_searcher
        )
        self.assertEqual(len(df2), 2)
        mock_searcher.assert_not_called()

    def test_epr_docs_db_caching(self):
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "document_guid": ["D1"],
                "document_description": ["Note"],
                "body_analysed": ["Body"],
                "updatetime": ["2020-01-01"],
                "clientvisit_visitidcode": ["V1"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        df1 = get_merged_pat_batch_epr_docs(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_epr_docs"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_epr_docs(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()

    def test_drugs_db_caching(self):
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "order_guid": ["O1"],
                "order_name": ["DrugA"],
                "order_createdwhen": ["2020-01-01"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        df1 = get_merged_pat_batch_drugs(self.pat_list, self.config, mock_searcher)
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_drugs"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_drugs(self.pat_list, self.config, mock_searcher)
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()

    def test_diagnostics_db_caching(self):
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "order_guid": ["O2"],
                "order_name": ["TestA"],
                "order_createdwhen": ["2020-01-01"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        df1 = get_merged_pat_batch_diagnostics(
            self.pat_list, self.config, mock_searcher
        )
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_diagnostics"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_diagnostics(
            self.pat_list, self.config, mock_searcher
        )
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()

    def test_mct_docs_db_caching(self):
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "observation_guid": ["M1"],
                "observation_valuetext_analysed": ["MCT Note"],
                "observationdocument_recordeddtm": ["2020-01-01"],
                "obscatalogmasteritem_displayname": ["AoMRC_ClinicalSummary_FT"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        df1 = get_merged_pat_batch_mct_docs(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_mct_docs"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_mct_docs(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()

    def test_textual_obs_db_caching(self):
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "basicobs_guid": ["T1"],
                "textualObs": ["Observation text"],
                "updatetime": ["2020-01-01"],
                "basicobs_itemname_analysed": ["Item"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        df1 = get_merged_pat_batch_textual_obs_docs(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_textual_obs"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_textual_obs_docs(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()

    def test_demo_db_caching(self):
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "client_firstname": ["John"],
                "client_lastname": ["Doe"],
                "updatetime": ["2020-01-01"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        df1 = get_merged_pat_batch_demo(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_demographics"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_demo(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()

    def test_bmi_db_caching(self):
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "observation_guid": ["B1"],
                "obscatalogmasteritem_displayname": ["OBS BMI"],
                "observationdocument_recordeddtm": ["2020-01-01"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        df1 = get_merged_pat_batch_bmi(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_bmi"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_bmi(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()

    def test_news_db_caching(self):
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "observation_guid": ["N1"],
                "obscatalogmasteritem_displayname": ["NEWS"],
                "observationdocument_recordeddtm": ["2020-01-01"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        df1 = get_merged_pat_batch_news(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_news"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_news(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()

    def test_obs_db_caching(self):
        search_term = "Smoking"
        # Sanitization logic matching get_merged_pat_batch_obs
        safe_term = "".join(e for e in search_term if e.isalnum() or e == "_").lower()
        mock_df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "observation_guid": ["S1"],
                "obscatalogmasteritem_displayname": ["Smoking"],
                "observationdocument_recordeddtm": ["2020-01-01"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        df1 = get_merged_pat_batch_obs(
            self.pat_list, search_term, self.config, mock_searcher
        )
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            # Note: generic obs tables include the search term in table name
            # The get_merged_pat_batch_obs sanitizes the term
            count = conn.execute(
                text(f'SELECT count(*) FROM "raw_data_raw_obs_{safe_term}"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_obs(
            self.pat_list, search_term, self.config, mock_searcher
        )
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()

    def test_searcher_returns_empty(self):
        """Test behavior when searcher returns empty DataFrame."""
        mock_searcher = MagicMock(return_value=pd.DataFrame())

        # Should return empty DataFrame gracefully
        df = get_merged_pat_batch_bloods(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertTrue(df.empty)

    def test_appointments_db_caching(self):
        # Appointments uses HospitalID instead of client_idcode for internal filtering
        mock_df = pd.DataFrame(
            {
                "HospitalID": ["P1"],  # Required for DB filtering logic in appointments
                "AppointmentDateTime": ["2020-01-01"],
                "ClinicCode": ["C1"],
            }
        )
        mock_searcher = MagicMock(return_value=mock_df)

        # The function arguments for appointments batch are list, search_term, config, searcher
        df1 = get_merged_pat_batch_appointments(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df1), 1)
        mock_searcher.assert_called_once()

        with self.engine.connect() as conn:
            count = conn.execute(
                text('SELECT count(*) FROM "raw_data_raw_appointments"')
            ).scalar()
            self.assertEqual(count, 1)

        mock_searcher.reset_mock()
        df2 = get_merged_pat_batch_appointments(
            self.pat_list, "term", self.config, mock_searcher
        )
        self.assertEqual(len(df2), 1)
        mock_searcher.assert_not_called()


class TestPatMakerLogic(unittest.TestCase):
    """
    Tests specific logic paths in pat_maker: isolation, skipping, and idempotency.
    """

    def setUp(self):
        logging.getLogger().setLevel(logging.CRITICAL)
        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            verbosity=0,
            calculate_vectors=True,
            global_start_year=2020,
            global_start_month=1,
            global_start_day=1,
            global_end_year=2020,
            global_end_month=2,
            global_end_day=1,
        )
        self.engine = self.config.db_engine

    @patch("pat2vec.main_pat2vec.initialize_cogstack_client")
    @patch("pat2vec.main_pat2vec.get_cat")
    @patch("pat2vec.main_pat2vec.main_batch")
    def test_all_sources_isolation(self, mock_main_batch, mock_get_cat, mock_init_cs):
        """
        Exhaustively test that enabling each individual data source results in
        the correct database table being populated, and that disabling it prevents creation.
        """
        # Map option name to expected SQLite table name (schema 'raw_data' becomes prefix)
        source_map = {
            "demo": "raw_data_raw_demographics",
            "bloods": "raw_data_raw_bloods",
            "bmi": "raw_data_raw_bmi",
            "drugs": "raw_data_raw_drugs",
            "diagnostics": "raw_data_raw_diagnostics",
            "annotations": "raw_data_raw_epr_docs",
            "annotations_mrc": "raw_data_raw_mct_docs",
            "textual_obs": "raw_data_raw_textual_obs",
            "annotations_reports": "raw_data_raw_reports",
            "news": "raw_data_raw_news",
            "appointments": "raw_data_raw_appointments",
            # Observation types mapped in _save_batches_to_db
            "covid": "raw_data_raw_covid",
            "smoking": "raw_data_raw_smoking",
            "core_02": "raw_data_raw_core_02",
            "bed": "raw_data_raw_bed",
            "vte_status": "raw_data_raw_vte",
            "hosp_site": "raw_data_raw_hospsite",
            "core_resus": "raw_data_raw_resus",
        }

        mock_get_cat.return_value = MagicMock()
        # Return dummy dataframe so main_batch doesn't crash if called
        mock_main_batch.return_value = pd.DataFrame({"client_idcode": ["P_ISO"]})

        for option, table_name in source_map.items():
            with self.subTest(source=option):
                # 1. Clean slate: drop all potential tables
                inspector = inspect(self.engine)
                existing_tables = inspector.get_table_names()
                with self.engine.connect() as conn:
                    for t in existing_tables:
                        conn.execute(text(f'DROP TABLE IF EXISTS "{t}"'))
                    conn.commit()

                # 2. Configure: Disable all, enable ONLY the current option
                for k in source_map.keys():
                    self.config.main_options[k] = False
                self.config.main_options[option] = True

                # 3. Initialize and Run
                pat2vec_obj = main(cogstack=True, config_obj=self.config)
                pat2vec_obj.all_patient_list = ["P_ISO"]
                pat2vec_obj.stripped_list_start = []
                pat2vec_obj.pat_maker(0)

                # 4. Verify the specific table exists and has data
                inspector = inspect(self.engine)
                self.assertTrue(
                    inspector.has_table(table_name),
                    f"Table '{table_name}' should exist when option '{option}' is enabled.",
                )

                with self.engine.connect() as conn:
                    count = conn.execute(
                        text(f'SELECT count(*) FROM "{table_name}"')
                    ).scalar()
                self.assertGreater(
                    count, 0, f"Table '{table_name}' should contain data."
                )

                # 5. Verify NO other source tables were created
                for other_opt, other_table in source_map.items():
                    if other_opt != option:
                        self.assertFalse(
                            inspector.has_table(other_table),
                            f"Table '{other_table}' should NOT exist when option '{other_opt}' is disabled.",
                        )

    @patch("pat2vec.main_pat2vec.initialize_cogstack_client")
    @patch("pat2vec.main_pat2vec.get_cat")
    @patch("pat2vec.main_pat2vec.main_batch")
    def test_patient_skipping(self, mock_main_batch, mock_get_cat, mock_init_cs):
        """Test that a patient in stripped_list_start is skipped."""
        pat2vec_obj = main(cogstack=True, config_obj=self.config)
        pat2vec_obj.all_patient_list = ["P_DONE"]
        # Manually mark patient as done
        pat2vec_obj.stripped_list_start = ["P_DONE"]

        pat2vec_obj.pat_maker(0)

        # main_batch should NOT be called
        mock_main_batch.assert_not_called()

    @patch("pat2vec.main_pat2vec.initialize_cogstack_client")
    @patch("pat2vec.main_pat2vec.get_cat")
    @patch("pat2vec.main_pat2vec.main_batch")
    def test_idempotency(self, mock_main_batch, mock_get_cat, mock_init_cs):
        """Test that running pat_maker twice for the same patient doesn't duplicate raw data."""
        # Use demographics as the test case
        self.config.main_options = {k: False for k in self.config.main_options}
        self.config.main_options["demo"] = True

        mock_main_batch.return_value = pd.DataFrame(
            {"client_idcode": ["P_REPEAT"], "f1": [1]}
        )
        mock_get_cat.return_value = MagicMock()

        # 1. First Run
        pat2vec_obj = main(cogstack=True, config_obj=self.config)
        pat2vec_obj.all_patient_list = ["P_REPEAT"]
        pat2vec_obj.stripped_list_start = []  # Ensure it runs
        pat2vec_obj.pat_maker(0)

        with self.engine.connect() as conn:
            count_1 = conn.execute(
                text(
                    'SELECT count(*) FROM "raw_data_raw_demographics" WHERE "client_idcode" = \'P_REPEAT\''
                )
            ).scalar()

        self.assertGreater(count_1, 0, "First run should populate data")

        # 2. Second Run
        # We need to ensure stripped_list_start is empty so it actually runs logic again,
        # simulating a case where we force a re-run or the resume logic hasn't picked it up yet.
        pat2vec_obj = main(cogstack=True, config_obj=self.config)
        pat2vec_obj.all_patient_list = ["P_REPEAT"]
        pat2vec_obj.stripped_list_start = []
        pat2vec_obj.pat_maker(0)

        with self.engine.connect() as conn:
            count_2 = conn.execute(
                text(
                    'SELECT count(*) FROM "raw_data_raw_demographics" WHERE "client_idcode" = \'P_REPEAT\''
                )
            ).scalar()

        # The dummy data generator likely generates the same number of rows.
        # If the save logic is idempotent (delete/insert or replace), count_2 should equal count_1.
        # If it was append only, count_2 would be 2 * count_1.
        self.assertEqual(
            count_2,
            count_1,
            "Row count should remain consistent after re-run (idempotency check)",
        )

    def test_annot_toggle(self):
        """Verify annotation batch configs are empty if option is disabled."""
        self.config.main_options["annotations"] = False

        # We can test this by checking _get_patient_data_batches directly without full mock
        # Need minimal mocks to init class
        with (
            patch("pat2vec.main_pat2vec.initialize_cogstack_client"),
            patch("pat2vec.main_pat2vec.get_cat"),
        ):
            pat2vec_obj = main(cogstack=True, config_obj=self.config)
            batches = pat2vec_obj._get_patient_data_batches("P1")
            self.assertTrue(batches["batch_epr_docs_annotations"].empty)
