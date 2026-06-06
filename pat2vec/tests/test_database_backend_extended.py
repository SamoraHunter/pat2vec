import unittest
import pandas as pd
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.helper_functions import get_df_from_db


class TestDatabaseBackendExtended(unittest.TestCase):
    def setUp(self):
        self.db_connection_string = "sqlite:///:memory:"
        self.config = config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            verbosity=0,
        )
        self.engine = self.config.db_engine

    def test_get_df_from_db_all_patients(self):
        """Test that passing None for patient_ids retrieves all rows."""
        df = pd.DataFrame({"client_idcode": ["P1", "P2"], "val": [1, 2]})
        df.to_sql("raw_data_test_table", self.engine, index=False)

        result = get_df_from_db(
            self.config, schema="raw_data", table="test_table", patient_ids=None
        )
        self.assertEqual(len(result), 2)

    def test_get_df_from_db_custom_id_column(self):
        """Test filtering by a custom patient ID column name."""
        df = pd.DataFrame({"custom_id": ["P1", "P2"], "val": [1, 2]})
        df.to_sql("raw_data_custom_table", self.engine, index=False)

        result = get_df_from_db(
            self.config,
            schema="raw_data",
            table="custom_table",
            patient_ids=["P1"],
            patient_id_column="custom_id",
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["custom_id"], "P1")

    def test_get_df_from_db_empty_patient_list(self):
        """Test that an empty list of patient IDs returns an empty DataFrame."""
        df = pd.DataFrame({"client_idcode": ["P1"], "val": [1]})
        df.to_sql("raw_data_empty_test", self.engine, index=False)

        result = get_df_from_db(
            self.config, schema="raw_data", table="empty_test", patient_ids=[]
        )
        self.assertTrue(result.empty)
