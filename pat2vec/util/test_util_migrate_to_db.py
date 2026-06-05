import unittest
import os
import shutil
import tempfile
import pandas as pd
from unittest.mock import MagicMock, patch
from pat2vec.util.migrate_to_db import migrate_csv_to_db, create_indexes
from sqlalchemy import create_engine, inspect


class TestMigrateToDb(unittest.TestCase):
    """Unit tests for the data migration utility from CSV to Database."""

    def setUp(self):
        """Set up temporary directory and in-memory database."""
        self.test_dir = tempfile.mkdtemp()
        self.db_url = "sqlite:///:memory:"
        self.engine = create_engine(self.db_url)
        self.config_obj = MagicMock()
        self.config_obj.db_connection_string = self.db_url
        self.bloods_path = os.path.join(self.test_dir, "bloods")
        os.makedirs(self.bloods_path)
        self.config_obj.pre_bloods_batch_path = self.bloods_path
        self.df_bloods = pd.DataFrame(
            {
                "client_idcode": ["P1", "P2"],
                "basicobs_entered": ["2023-01-01", "2023-01-02"],
                "val": [10.5, 20.0],
            }
        )
        self.df_bloods.to_csv(os.path.join(self.bloods_path, "data.csv"), index=False)

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)
        self.engine.dispose()

    @patch("pat2vec.util.migrate_to_db.create_engine")
    def test_migrate_csv_to_db_full_flow(self, mock_create_engine):
        """Test the end-to-end migration of CSV files to database tables."""
        mock_create_engine.return_value = self.engine
        with patch(
            "pat2vec.util.migrate_to_db.mappings",
            [
                (
                    "pre_bloods_batch_path",
                    "raw_data",
                    "raw_bloods",
                    "client_idcode",
                    ["client_idcode"],
                )
            ],
        ):
            migrate_csv_to_db(self.config_obj)
        inspector = inspect(self.engine)
        table_name = "raw_data_raw_bloods"
        self.assertTrue(inspector.has_table(table_name))
        result_df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)
        self.assertEqual(len(result_df), 2)
        self.assertEqual(result_df.iloc[0]["client_idcode"], "P1")

    def test_create_indexes_sqlite(self):
        """Test index creation logic for SQLite backend."""
        pd.DataFrame({"col1": [1]}).to_sql("raw_data_test", self.engine, index=False)
        create_indexes(self.engine, "raw_data", "test", ["col1"])
        inspector = inspect(self.engine)
        indexes = inspector.get_indexes("raw_data_test")
        self.assertTrue(any("idx_raw_data_test_col1" in idx["name"] for idx in indexes))

    @patch("pat2vec.util.migrate_to_db.pd.read_csv")
    @patch("pat2vec.util.migrate_to_db._write_batch")
    def test_migrate_csv_to_db_skips_missing_attr(self, mock_write, mock_read):
        """Test that migration skips tables if the configuration attribute is missing."""
        empty_config = MagicMock()
        empty_config.db_connection_string = self.db_url
        with patch(
            "pat2vec.util.migrate_to_db.mappings",
            [
                (
                    "pre_bloods_batch_path",
                    "raw_data",
                    "raw_bloods",
                    "client_idcode",
                    ["client_idcode"],
                )
            ],
        ):
            migrate_csv_to_db(empty_config)
        mock_read.assert_not_called()
        mock_write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
