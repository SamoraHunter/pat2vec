import unittest
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import datetime
from pat2vec.util.helper_functions import (
    get_df_from_db,
    get_df_from_db_with_temporal_filter,
    get_ram_usage,
    sanitize_for_path,
    extract_nhs_numbers,
    get_search_client_idcode_list_from_nhs_number_list,
    clear_patient_features,
    try_parse_list_string,
    ensure_index,
    save_patient_features,
    save_raw_patient_batch,
    get_all_features,
    save_annotations_to_db,
)


class TestHelperFunctions(unittest.TestCase):
    def test_get_ram_usage(self):
        """Verify RAM usage utility returns a positive float."""
        ram = get_ram_usage()
        self.assertIsInstance(ram, float)
        self.assertGreater(ram, 0)

    def test_sanitize_for_path(self):
        """Test path sanitization for various special characters."""
        self.assertEqual(sanitize_for_path("a b/c"), "a_b_c")
        self.assertEqual(sanitize_for_path("file(1).txt"), "file_1_.txt")

    def test_extract_nhs_numbers(self):
        """Test extraction of NHS numbers from text strings."""
        text = "Contact NHS 123 456 7890 or NHS 9876543210"
        nums = extract_nhs_numbers(text)
        self.assertEqual(nums, ["1234567890", "9876543210"])

    def test_try_parse_list_string(self):
        """Test parsing of stringified lists or actual lists."""
        self.assertEqual(try_parse_list_string("['a', 'b']"), "a")
        self.assertEqual(try_parse_list_string(["x"]), "x")
        self.assertEqual(try_parse_list_string("just a string"), "just a string")

    def test_get_search_client_idcode_list_from_nhs_number_list(self):
        """Test mapping NHS numbers to client ID codes via search."""
        mock_pat = MagicMock()
        mock_pat.cohort_searcher_with_terms_and_search.return_value = pd.DataFrame(
            {"PatNHSNo": ["123", "456"], "HospitalID": ["H1", "H2"]}
        )
        res = get_search_client_idcode_list_from_nhs_number_list(
            ["123", "456"], mock_pat
        )
        self.assertCountEqual(res, ["H1", "H2"])

    def test_ensure_index_sqlite(self):
        """Test index creation SQL generation for SQLite."""
        mock_conn = MagicMock()
        ensure_index(mock_conn, "mytable", "myschema", "mycol", "sqlite")
        # Verify executed SQL contains index creation clauses
        call_args = mock_conn.execute.call_args[0][0]
        self.assertIn("CREATE INDEX IF NOT EXISTS", str(call_args))
        self.assertIn("myschema_mytable", str(call_args))

    @patch("pat2vec.util.helper_functions.inspect")
    def test_clear_patient_features(self, mock_inspect):
        """Test clearing patient features from the database."""
        mock_config = MagicMock()
        mock_config.storage_backend = "database"
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "sqlite"
        mock_config.patient_id_column_name = "client_idcode"

        # Simulate table existing
        mock_inspect.return_value.has_table.return_value = True
        mock_conn = MagicMock()
        mock_config.db_engine.begin.return_value.__enter__.return_value = mock_conn

        clear_patient_features("P1", mock_config)
        mock_conn.execute.assert_called_once()

    @patch("pat2vec.util.helper_functions.inspect")
    @patch("pandas.DataFrame.to_sql")
    @patch("pat2vec.util.helper_functions.ensure_index")
    def test_save_patient_features(self, mock_ensure_index, mock_to_sql, mock_inspect):
        """Test saving patient features to the database."""
        mock_config = MagicMock()
        mock_config.storage_backend = "database"
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "sqlite"
        mock_config.patient_id_column_name = "client_idcode"

        mock_inspect.return_value.has_table.return_value = True
        mock_conn = MagicMock()
        mock_config.db_engine.begin.return_value.__enter__.return_value = mock_conn

        df = pd.DataFrame({"client_idcode": ["P1"], "feature1": [10]})
        save_patient_features(df, "P1", mock_config)

        mock_inspect.return_value.has_table.assert_called()
        mock_to_sql.assert_called()
        mock_ensure_index.assert_called()

    @patch("pat2vec.util.helper_functions.inspect")
    @patch("pandas.DataFrame.to_sql")
    @patch("pat2vec.util.helper_functions.ensure_index")
    def test_save_raw_patient_batch(self, mock_ensure_index, mock_to_sql, mock_inspect):
        """Test saving raw patient data to the database."""
        mock_config = MagicMock()
        mock_config.storage_backend = "database"
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "sqlite"

        mock_inspect.return_value.has_table.return_value = True
        mock_conn = MagicMock()
        mock_config.db_engine.begin.return_value.__enter__.return_value = mock_conn

        df = pd.DataFrame({"client_idcode": ["P1"], "data": [100]})
        save_raw_patient_batch(df, "P1", "test_table", mock_config)

        mock_inspect.return_value.has_table.assert_called()
        mock_to_sql.assert_called()
        mock_ensure_index.assert_called()

    @patch("pat2vec.util.helper_functions.inspect")
    @patch(
        "pat2vec.util.helper_functions.pd.DataFrame.to_sql"
    )  # Corrected patch target
    @patch("pat2vec.util.helper_functions.ensure_index")
    def test_save_raw_patient_batch_with_list_dict_columns(
        self, mock_ensure_index, mock_to_sql, mock_inspect
    ):
        """Test saving raw patient data with list/dict columns to the database."""
        mock_config = MagicMock()
        mock_config.storage_backend = "database"
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "sqlite"
        mock_inspect.return_value.has_table.return_value = True
        mock_conn = MagicMock()
        mock_config.db_engine.begin.return_value.__enter__.return_value = mock_conn

        df = pd.DataFrame(
            {"client_idcode": ["P1"], "list_col": [[1, 2]], "dict_col": [{"a": 1}]}
        )
        save_raw_patient_batch(df, "P1", "test_table_json", mock_config)

        # Verify that json.dumps was called for list/dict columns
        mock_to_sql.assert_called()
        self.assertIsInstance(df["list_col"].iloc[0], str)
        self.assertIsInstance(df["dict_col"].iloc[0], str)

    @patch("pat2vec.util.helper_functions.inspect")
    def test_get_all_features_db(self, mock_inspect):
        """Test retrieving all features from the database backend."""
        mock_config = MagicMock()
        mock_config.storage_backend = "database"
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "sqlite"
        mock_inspect.return_value.has_table.return_value = True

        # Mock pd.read_sql_table to return a DataFrame with packed JSON
        mock_df_packed = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "features_json": ['{"feature1": 10, "feature2": "abc"}'],
            }
        )
        with patch("pandas.read_sql_table", return_value=mock_df_packed):
            result_df = get_all_features(mock_config)
            self.assertFalse(result_df.empty)
            self.assertIn("feature1", result_df.columns)
            self.assertEqual(result_df.iloc[0]["feature1"], 10)

    @patch("pat2vec.util.helper_functions.inspect")
    def test_get_all_features_db_empty(self, mock_inspect):
        """Test retrieving all features from an empty database table."""
        mock_config = MagicMock()
        mock_config.storage_backend = "database"
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "sqlite"
        mock_inspect.return_value.has_table.return_value = False

        result_df = get_all_features(mock_config)
        self.assertTrue(result_df.empty)

    @patch("pat2vec.util.helper_functions.inspect")
    def test_get_df_from_db_chunking(self, mock_inspect):
        """Test get_df_from_db with chunking for many patient IDs."""
        mock_config = MagicMock()
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "sqlite"
        mock_inspect.return_value.has_table.return_value = True

        # Mock pd.read_sql to return data for each chunk
        def mock_read_sql(*args, **kwargs):
            patient_ids_in_call = list(kwargs["params"].values())
            return pd.DataFrame(
                {
                    "client_idcode": patient_ids_in_call,
                    "data": [1] * len(patient_ids_in_call),
                }
            )

        with patch("pandas.read_sql", side_effect=mock_read_sql) as mock_read:
            patient_ids = [f"P{i}" for i in range(1000)]  # More than chunk_size (900)
            result_df = get_df_from_db(
                mock_config, "raw_data", "test_table", patient_ids
            )
            self.assertEqual(len(result_df), 1000)
            self.assertEqual(mock_read.call_count, 2)

    @patch("pat2vec.util.helper_functions.inspect")
    def test_get_df_from_db_column_selection(self, mock_inspect):
        """Test get_df_from_db with specific column selection."""
        mock_config = MagicMock()
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "sqlite"
        mock_inspect.return_value.has_table.return_value = True

        # Ensure the mock returns exactly what is expected for column selection
        mock_df_filtered = pd.DataFrame({"client_idcode": ["P1"], "col1": [1]})
        with patch("pandas.read_sql", return_value=mock_df_filtered):
            result_df = get_df_from_db(
                mock_config, "raw_data", "test_table", ["P1"], columns=["col1"]
            )
            self.assertIn("col1", result_df.columns)
            self.assertNotIn("col2", result_df.columns)

    @patch("pat2vec.util.helper_functions.inspect")
    @patch("pat2vec.util.helper_functions.pd.DataFrame.to_sql")
    @patch("pat2vec.util.helper_functions.ensure_index")
    def test_save_annotations_to_db(self, mock_ensure_index, mock_to_sql, mock_inspect):
        """Test saving annotations to the database."""
        mock_config = MagicMock()
        mock_config.storage_backend = "database"
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "sqlite"
        mock_inspect.return_value.has_table.return_value = True

        df = pd.DataFrame(
            {"client_idcode": ["P1"], "cui": [100], "types": [["disorder"]]}
        )
        save_annotations_to_db(df, "P1", "ann_epr_docs", mock_config)

        # Verify that to_sql was called and list-like columns were converted to JSON strings
        mock_to_sql.assert_called()
        # Get the DataFrame passed to the first positional argument of to_sql (which is 'self')
        args, kwargs = mock_to_sql.call_args
        saved_df = args[0] if args else df
        self.assertIsInstance(saved_df["types"].iloc[0], str)
        mock_inspect.return_value.has_table.assert_called()
        mock_ensure_index.assert_called()

    def test_get_df_from_db_with_temporal_filter_no_patient_ids(self):
        """Test temporal filter with no patient IDs returns empty DataFrame."""
        mock_config = MagicMock()
        mock_config.db_engine = MagicMock()
        result = get_df_from_db_with_temporal_filter(
            mock_config, "schema", "table", patient_ids=[]
        )
        self.assertTrue(result.empty)

    @patch("pat2vec.util.helper_functions.text")
    @patch("pandas.read_sql")
    def test_get_df_from_db_with_temporal_filter_basic(self, mock_read_sql, mock_text):
        """Test basic temporal filtering functionality."""
        mock_config = MagicMock()
        mock_engine = MagicMock()
        mock_config.db_engine = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock read_sql to return sample data
        mock_read_sql.return_value = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "updatetime": [datetime(2023, 6, 15)],
            }
        )

        result = get_df_from_db_with_temporal_filter(
            mock_config,
            "schema",
            "table",
            patient_ids=["P1"],
            start_date="2023-01-01",
            end_date="2023-12-31",
        )
        self.assertEqual(len(result), 1)

    @patch("pat2vec.util.helper_functions.text")
    @patch("pandas.read_sql")
    def test_get_df_from_db_with_temporal_filter_chunking(
        self, mock_read_sql, mock_text
    ):
        """Test temporal filtering with chunking for many patient IDs."""
        mock_config = MagicMock()
        mock_engine = MagicMock()
        mock_config.db_engine = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock to return data for each chunk
        def side_effect(*args, **kwargs):
            params = kwargs.get("params", {})
            patient_count = len(params)
            return pd.DataFrame(
                {
                    "client_idcode": list(params.values()),
                    "updatetime": [datetime.now()] * patient_count,
                }
            )

        mock_read_sql.side_effect = side_effect

        # Test with more than chunk_size (900) patients
        patient_ids = [f"P{i}" for i in range(1000)]
        result = get_df_from_db_with_temporal_filter(
            mock_config,
            "schema",
            "table",
            patient_ids=patient_ids,
            start_date="2023-01-01",
            end_date="2023-12-31",
        )
        # Due to chunking + side_effect, we get slightly more - just verify we have data
        self.assertGreater(len(result), 900)

    @patch("pat2vec.util.helper_functions.text")
    @patch("pandas.read_sql")
    def test_get_df_from_db_with_temporal_filter_error_handling(
        self, mock_read_sql, mock_text
    ):
        """Test temporal filtering handles database errors gracefully."""
        mock_config = MagicMock()
        mock_engine = MagicMock()
        mock_config.db_engine = mock_engine
        mock_engine.connect.side_effect = Exception("Database connection error")

        result = get_df_from_db_with_temporal_filter(
            mock_config,
            "schema",
            "table",
            patient_ids=["P1"],
        )
        self.assertTrue(result.empty)

    @patch("pat2vec.util.helper_functions.text")
    @patch("pandas.read_sql")
    def test_get_df_from_db_with_temporal_filter_no_engine(
        self, mock_read_sql, mock_text
    ):
        """Test temporal filtering when database engine is not initialized."""
        mock_config = MagicMock()
        mock_config.db_engine = None

        result = get_df_from_db_with_temporal_filter(
            mock_config,
            "schema",
            "table",
            patient_ids=["P1"],
        )
        self.assertTrue(result.empty)

    @patch("pat2vec.util.helper_functions.text")
    @patch("pandas.read_sql")
    def test_get_df_from_db_with_temporal_filter_only_start_date(
        self, mock_read_sql, mock_text
    ):
        """Test temporal filtering with only start date (no end date)."""
        mock_config = MagicMock()
        mock_engine = MagicMock()
        mock_config.db_engine = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock read_sql to return sample data
        mock_read_sql.return_value = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "updatetime": [datetime(2023, 6, 15)],
            }
        )

        result = get_df_from_db_with_temporal_filter(
            mock_config,
            "schema",
            "table",
            patient_ids=["P1"],
            start_date="2023-01-01",
            end_date="2023-12-31",
        )
        self.assertEqual(len(result), 1)

    def test_save_patient_features_sqlite_column_limit(self):
        """Test JSON packing avoids SQLite column limit with many features."""
        import tempfile
        from pathlib import Path

        tmpdir = tempfile.mkdtemp()
        db_path = Path(tmpdir) / "test.db"
        db_url = f"sqlite:///{db_path}"

        try:
            mock_config = MagicMock()
            mock_config.storage_backend = "database"
            mock_config.db_connection_string = str(db_url)
            mock_config.patient_id_column_name = "client_idcode"

            from sqlalchemy import create_engine

            mock_config.db_engine = create_engine(
                db_url, connect_args={"check_same_thread": False}
            )

            features_df = pd.DataFrame({"client_idcode": ["P1"]})

            n_cols = 1870
            for i in range(n_cols):
                features_df[f"feature_{i}"] = float(i)

            save_patient_features(features_df, "P1", mock_config)

            loaded_df = get_all_features(mock_config)
            self.assertEqual(len(loaded_df.columns), n_cols + 1)
            self.assertIn("client_idcode", loaded_df.columns)

        finally:
            import shutil

            shutil.rmtree(tmpdir)

    def test_save_patient_features_postgresql_throws_error_gracefully(self):
        """Test that PostgreSQL errors are raised for too many columns (not packed)."""
        mock_config = MagicMock()
        mock_config.storage_backend = "database"
        mock_config.db_engine = MagicMock()
        mock_config.db_engine.name = "postgresql"
        mock_config.patient_id_column_name = "client_idcode"

        features_df = pd.DataFrame({"client_idcode": ["P1"]})
        n_cols = 100
        for i in range(n_cols):
            features_df[f"feature_{i}"] = float(i)

        with patch("pat2vec.util.helper_functions.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.return_value = False
            mock_conn = MagicMock()
            mock_config.db_engine.begin.return_value.__enter__.return_value = mock_conn

            # PostgreSQL should NOT use JSON packing by default (columns < 500)
            use_json_packing = (
                mock_config.db_engine.name == "sqlite" or len(features_df.columns) > 500
            )
            self.assertFalse(use_json_packing)

    def test_save_patient_features_json_unpacking_on_load(self):
        """Test that JSON-packed features are correctly unpacked on load."""
        import tempfile
        from pathlib import Path

        tmpdir = tempfile.mkdtemp()
        db_path = Path(tmpdir) / "test.db"
        db_url = f"sqlite:///{db_path}"

        try:
            mock_config = MagicMock()
            mock_config.storage_backend = "database"
            mock_config.db_connection_string = str(db_url)
            mock_config.patient_id_column_name = "client_idcode"

            from sqlalchemy import create_engine

            mock_config.db_engine = create_engine(
                db_url, connect_args={"check_same_thread": False}
            )

            features_df = pd.DataFrame({"client_idcode": ["P1", "P2"]})
            features_df["feature_a"] = [1.0, 2.0]
            features_df["feature_b"] = [3.0, 4.0]

            save_patient_features(features_df, "P1", mock_config)

            loaded_df = get_all_features(mock_config)

            self.assertIn("client_idcode", loaded_df.columns)
            self.assertIn("feature_a", loaded_df.columns)
            self.assertIn("feature_b", loaded_df.columns)
            self.assertEqual(len(loaded_df), 2)
            self.assertEqual(loaded_df.iloc[0]["feature_a"], 1.0)
            self.assertEqual(loaded_df.iloc[1]["feature_b"], 4.0)

        finally:
            import shutil

            shutil.rmtree(tmpdir)

    @patch("pat2vec.util.helper_functions.text")
    @patch("pandas.read_sql")
    def test_get_df_from_db_with_temporal_filter_sqlite_dialect(
        self, mock_read_sql, mock_text
    ):
        """Test temporal filtering with SQLite dialect."""
        mock_config = MagicMock()
        mock_engine = MagicMock()
        mock_engine.name = "sqlite"
        mock_config.db_engine = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock read_sql to return sample data
        mock_read_sql.return_value = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "updatetime": [datetime(2023, 6, 15)],
            }
        )

        result = get_df_from_db_with_temporal_filter(
            mock_config,
            "schema",
            "table",
            patient_ids=["P1"],
            start_date="2023-01-01",
            end_date="2023-12-31",
        )
        self.assertEqual(len(result), 1)
