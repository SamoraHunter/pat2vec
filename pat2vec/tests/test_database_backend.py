import unittest
import pandas as pd
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.helper_functions import (
    save_patient_features,
    get_df_from_db,
    get_all_features,
)
from pat2vec.util.post_processing_build_ipw_dataframe import build_ipw_dataframe


class TestDatabaseBackend(unittest.TestCase):
    def setUp(self):
        # Use in-memory SQLite for testing to ensure isolation
        self.db_connection_string = "sqlite:///:memory:"
        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )
        self.engine = self.config.db_engine

    def test_save_and_retrieve_features(self):
        """Test saving and retrieving patient features using the database backend."""
        patient_id = "P001"
        features_data = {
            "client_idcode": [patient_id],
            "feature_1": [1.5],
            "feature_2": [0.8],
        }
        df = pd.DataFrame(features_data)

        # Save features to DB
        save_patient_features(df, patient_id, self.config)

        # Retrieve features using get_all_features
        retrieved_df = get_all_features(self.config)

        self.assertEqual(len(retrieved_df), 1)
        self.assertEqual(retrieved_df.iloc[0]["client_idcode"], patient_id)
        self.assertAlmostEqual(retrieved_df.iloc[0]["feature_1"], 1.5)

        # Retrieve features using get_df_from_db with explicit schema/table
        # Note: In SQLite, schema is often flattened into table name (e.g. features_features)
        # by the helper functions logic or sqlalchemy handling
        retrieved_df_filtered = get_df_from_db(
            self.config, schema="features", table="features", patient_ids=[patient_id]
        )
        self.assertEqual(len(retrieved_df_filtered), 1)
        self.assertEqual(retrieved_df_filtered.iloc[0]["client_idcode"], patient_id)

    def test_get_df_from_db_raw_data(self):
        """Test retrieving raw data simulating what migrate_to_db would populate."""
        # Manually populate a raw data table for testing retrieval
        table_name = "raw_data_raw_bloods"
        df = pd.DataFrame(
            {"client_idcode": ["P001", "P002"], "basicobs_value_numeric": [10.5, 20.0]}
        )
        df.to_sql(table_name, self.engine, index=False)

        # Retrieve specific patient
        result = get_df_from_db(
            self.config, schema="raw_data", table="raw_bloods", patient_ids=["P001"]
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["client_idcode"], "P001")
        self.assertEqual(result.iloc[0]["basicobs_value_numeric"], 10.5)

    def test_overwrite_features(self):
        """Test that saving features for an existing patient overwrites previous data."""
        patient_id = "P001"
        df1 = pd.DataFrame({"client_idcode": [patient_id], "f1": [1]})
        save_patient_features(df1, patient_id, self.config)

        df2 = pd.DataFrame({"client_idcode": [patient_id], "f1": [2]})
        save_patient_features(df2, patient_id, self.config)

        retrieved = get_all_features(self.config)
        self.assertEqual(len(retrieved), 1)
        self.assertEqual(retrieved.iloc[0]["f1"], 2)

    def test_get_df_from_db_non_existent_table(self):
        """Test retrieving data from a non-existent table returns an empty DataFrame."""
        result = get_df_from_db(
            self.config,
            schema="non_existent_schema",
            table="non_existent_table",
            patient_ids=["P001"],
        )
        self.assertTrue(result.empty)

    def test_build_ipw_dataframe_db(self):
        """Test building IPW dataframe from database source."""
        # Setup: Populate raw_epr_docs so patients can be found
        raw_docs_table = (
            "raw_data_raw_epr_docs" if self.engine.name == "sqlite" else "raw_epr_docs"
        )
        raw_docs_data = pd.DataFrame(
            {
                "client_idcode": ["P001", "P002"],
                "updatetime": [
                    pd.to_datetime("2023-01-01"),
                    pd.to_datetime("2023-01-02"),
                ],
            }
        )
        raw_docs_data.to_sql(raw_docs_table, self.engine, index=False)

        # Setup: Populate annotations so get_pat_ipw_record finds something
        annot_table = (
            "annotations_ann_epr_docs"
            if self.engine.name == "sqlite"
            else "ann_epr_docs"
        )
        annot_data = pd.DataFrame(
            {
                "client_idcode": ["P001"],
                "updatetime": [pd.to_datetime("2023-01-01")],
                "cui": [12345],
                "pretty_name": ["Test Disease"],
            }
        )
        annot_data.to_sql(annot_table, self.engine, index=False)

        result_df = build_ipw_dataframe(config_obj=self.config)

        self.assertIn("P001", result_df["client_idcode"].values)
        self.assertIn("P002", result_df["client_idcode"].values)
        self.assertFalse(result_df[result_df["client_idcode"] == "P001"].empty)
