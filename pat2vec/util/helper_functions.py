import ast
import re
import warnings
from typing import Any, List, Optional  # Keep typing imports together
import os
import psutil
import logging
import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.schema import CreateSchema
from tqdm import tqdm
import json

# Moved from pat2vec.util.post_processing_build_methods to break circular import
logger = logging.getLogger(__name__)  # Ensure logger is defined at module level


def get_ram_usage():
    """Returns current RAM usage in GB."""
    return psutil.Process(os.getpid()).memory_info().rss / (1024**3)


HELPER_FUNCTIONS_VERSION = "2.5-validation-refactor"  # Logging cleanup


def sanitize_for_path(text: str) -> str:
    """Sanitizes a string to be safe for use in a file/directory path."""
    # Replace invalid characters with an underscore
    return re.sub(r'[\\/*?:"<>|()\s]', "_", text)


def extract_nhs_numbers(input_string: str) -> List[str]:
    """Extracts all occurrences of "NHS" followed by a 10-digit number.

    The function searches for the pattern "NHS" followed by a 10-digit number,
    which may contain spaces. It then cleans the extracted numbers by removing
    any spaces.

    Args:
        input_string: The string to search for NHS numbers.

    Returns:
        A list of all extracted 10-digit NHS numbers as strings.

    Examples:
        >>> extract_nhs_numbers("NHS 123 456 7890")
        ['1234567890']
        >>> extract_nhs_numbers("NHS 123 456 7890 and NHS 098 765 4321")
        ['1234567890', '0987654321']
    """
    # Find all occurrences of "NHS" followed by a 10-digit number
    matches = re.findall(r"NHS\s*(\d{3}\s*\d{3}\s*\d{4})", input_string)
    # Remove spaces from each extracted number
    cleaned_numbers = [re.sub(r"\s+", "", number) for number in matches]
    return cleaned_numbers


def get_search_client_idcode_list_from_nhs_number_list(
    nhs_numbers: List[str], pat2vec_obj: Any
) -> List[str]:
    """Retrieves a unique list of hospital IDs from a list of NHS numbers.

    This function uses a `pat2vec_obj` to perform a cohort search against an
    index (e.g., 'pims_apps*') to find the corresponding 'HospitalID' for each
    'PatNHSNo' in the provided list.

    Args:
        nhs_numbers: A list of NHS numbers to search for.
        pat2vec_obj: An object with a `cohort_searcher_with_terms_and_search`
            method for querying the data source.

    Returns:
        A unique list of hospital IDs found for the given NHS numbers.
    """
    # Perform cohort search
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="pims_apps*",
        fields_list=["PatNHSNo", "HospitalID"],
        term_name="PatNHSNo",
        entered_list=nhs_numbers,
        search_string="*",
    )

    # Extract unique hospital IDs
    unique_hospital_ids = list(df["HospitalID"].unique())

    # Check for missing hospital IDs
    missing_ids = df[df["HospitalID"].isna() | (df["HospitalID"] == "")][
        "PatNHSNo"
    ].tolist()
    if missing_ids:
        warnings.warn(
            f"The following NHS numbers do not have associated Hospital IDs: {missing_ids}"
        )

    return unique_hospital_ids


def ensure_index(
    connection: Any,
    table_name: str,
    schema_name: Optional[str],
    column_name: str,
    engine_name: str,
) -> None:
    """Ensures a database index exists for a given column."""
    try:
        idx_name = f"idx_{table_name}_{column_name}"

        if engine_name == "sqlite":
            # SQLite flattening logic matching other functions
            target_table = f"{schema_name}_{table_name}" if schema_name else table_name
            sql = f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{target_table}" ("{column_name}")'
        else:
            # Standard SQL with schema support
            if schema_name:
                target_table = f'"{schema_name}"."{table_name}"'
            else:
                target_table = f'"{table_name}"'
            sql = f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON {target_table} ("{column_name}")'

        connection.execute(text(sql))
    except Exception as e:
        logging.warning(f"Could not create index {idx_name} on {table_name}: {e}")


def clear_patient_features(patient_id: str, config_obj: Any) -> None:
    """Deletes all features for a specific patient from the database."""
    if config_obj.storage_backend != "database":
        return

    try:
        engine = config_obj.db_engine
        if not engine:
            return

        table_name = "features"
        schema_name = "features"
        id_column = config_obj.patient_id_column_name

        with engine.begin() as connection:
            if engine.name == "sqlite":
                target_table = f"{schema_name}_{table_name}"
                del_query = text(
                    f'DELETE FROM "{target_table}" WHERE "{id_column}" = :pat_id'
                )
                if inspect(connection).has_table(target_table):
                    connection.execute(del_query, {"pat_id": patient_id})
            else:
                # Postgres/others
                if inspect(connection).has_table(table_name, schema=schema_name):
                    del_query = text(
                        f'DELETE FROM "{schema_name}"."{table_name}" WHERE "{id_column}" = :pat_id'
                    )
                    connection.execute(del_query, {"pat_id": patient_id})
    except Exception as e:
        logging.error(f"Failed to clear features for patient {patient_id}: {e}")


def try_parse_list_string(val: Any) -> Any:
    """Handles stringified list types like \"['procedure']\" or actual Python lists."""
    if isinstance(val, list):
        return str(val[0]) if len(val) > 0 else "Unknown"

    if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list) and len(parsed) > 0:
                return str(parsed[0])
            return str(parsed)
        except (ValueError, SyntaxError):
            return val.strip("[]'\" ")
    return val


def save_patient_features(
    features_df: pd.DataFrame, patient_id: str, config_obj: Any, overwrite: bool = True
) -> None:
    """Saves the feature vector(s) for a single patient to the configured backend.

    If `storage_backend` is 'database', it appends/overwrites the features in a
    'features' table within a 'features' schema.

    If `storage_backend` is 'file', it saves the features to a CSV file in the
    `current_pat_lines_path` directory, preserving the original behavior.

    Args:
        features_df: The DataFrame containing one or more feature vectors for the patient.
        patient_id: The unique identifier for the patient.
        config_obj: The configuration object containing backend settings and paths.
        overwrite: If True, delete existing features for the patient before saving. Defaults to True.

    Raises:
        ValueError: If an unknown `storage_backend` is specified.
        Exception: Propagates exceptions from database operations.
    """
    if features_df.empty:
        logging.debug(f"features_df is empty for patient {patient_id}, skipping save.")
        return

    if config_obj.storage_backend == "database":
        try:
            engine = config_obj.db_engine
            if not engine:
                raise ValueError("Database engine not initialized in config_obj.")

            table_name = "features"
            schema_name = "features"
            id_column = config_obj.patient_id_column_name

            # Ensure the patient ID column exists in the DataFrame
            if id_column not in features_df.columns:
                features_df[id_column] = patient_id

            # For database storage, keep individual columns for proper querying
            # (JSON packing was previously used to avoid DB column limits)

            with engine.begin() as connection:
                # Handle SQLite differences
                if engine.name == "sqlite":
                    target_table = f"{schema_name}_{table_name}"
                    target_schema = None
                    del_query_str = (
                        f'DELETE FROM "{target_table}" WHERE "{id_column}" = :pat_id'
                    )
                else:
                    target_table = table_name
                    target_schema = schema_name
                    del_query_str = f'DELETE FROM "{schema_name}"."{table_name}" WHERE "{id_column}" = :pat_id'

                # Ensure the schema exists
                if engine.name != "sqlite" and not connection.dialect.has_schema(
                    connection, schema_name
                ):
                    connection.execute(CreateSchema(schema_name))

                # Check if table exists before trying to delete
                if overwrite:
                    inspector = inspect(connection)
                    if inspector.has_table(target_table, schema=target_schema):
                        del_query = text(del_query_str)
                        connection.execute(del_query, {"pat_id": patient_id})

                # Check for missing columns and update schema
                inspector = inspect(connection)
                if inspector.has_table(target_table, schema=target_schema):
                    existing_cols = {
                        c["name"]
                        for c in inspector.get_columns(
                            target_table, schema=target_schema
                        )
                    }
                    missing_cols = [
                        c for c in features_df.columns if c not in existing_cols
                    ]

                    if missing_cols:
                        logging.info(
                            f"Schema evolution: Adding {len(missing_cols)} new columns to table '{target_table}'"
                        )
                        for col in missing_cols:
                            dtype = features_df[col].dtype
                            if pd.api.types.is_integer_dtype(dtype):
                                sql_type = "INTEGER"
                            elif pd.api.types.is_float_dtype(dtype):
                                sql_type = "FLOAT"
                            else:
                                sql_type = "TEXT"

                            # Quote identifiers to handle special characters in column names
                            col_clean = col.replace('"', '""')
                            quoted_col = f'"{col_clean}"'
                            if engine.name == "sqlite":
                                table_ref = f'"{target_table}"'
                            else:
                                table_ref = (
                                    f'"{target_schema}"."{target_table}"'
                                    if target_schema
                                    else f'"{target_table}"'
                                )

                            try:
                                connection.execute(
                                    text(
                                        f"ALTER TABLE {table_ref} ADD COLUMN {quoted_col} {sql_type}"
                                    )
                                )
                            except Exception as e:
                                logging.error(
                                    f"Failed to add column {col} to table: {e}"
                                )
                                raise e

                logging.info(
                    f"Inserting {len(features_df)} rows for patient {patient_id} into {target_table} (cols: {len(features_df.columns)})"
                )
                # Append the new features
                features_df.to_sql(
                    name=target_table,
                    con=connection,
                    schema=target_schema,
                    if_exists="append",
                    index=False,
                )

                # Ensure index on ID column for performance
                ensure_index(
                    connection, table_name, schema_name, id_column, engine.name
                )
        except Exception as e:
            logging.error(
                f"Failed to save features for patient {patient_id} to database: {e}"
            )
            raise

    elif config_obj.storage_backend == "file":
        output_dir = os.path.join(config_obj.current_pat_lines_path, str(patient_id))
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{patient_id}.csv")
        if overwrite and os.path.exists(output_file):
            os.remove(output_file)
            logging.debug(f"Overwriting existing feature file: {output_file}")
        features_df.to_csv(
            output_file, index=False, mode="a", header=not os.path.exists(output_file)
        )
        logging.debug(f"Saved features for patient {patient_id} to {output_file}")
    else:
        raise ValueError(f"Unknown storage_backend: {config_obj.storage_backend}")


def save_raw_patient_batch(
    df: pd.DataFrame,
    patient_id: str,
    table_name: str,
    config_obj: Any,
    id_column: str = "client_idcode",
) -> None:
    """Saves a raw data batch for a patient to the database.

    Args:
        df: The DataFrame containing the raw data.
        patient_id: The patient identifier.
        table_name: The target table name (without schema prefix).
        config_obj: The configuration object.
        id_column: The column name for the patient ID in this table.
    """
    if config_obj.storage_backend != "database":
        return

    try:
        engine = config_obj.db_engine
        if not engine:
            return

        schema_name = "raw_data"

        # Ensure ID column is present
        if id_column not in df.columns:
            df[id_column] = patient_id

        with engine.begin() as connection:
            if engine.name == "sqlite":
                target_table = f"{schema_name}_{table_name}"
                target_schema = None
                del_query = text(
                    f'DELETE FROM "{target_table}" WHERE "{id_column}" = :pat_id'
                )
            else:
                target_table = table_name
                target_schema = schema_name
                del_query = text(
                    f'DELETE FROM "{schema_name}"."{table_name}" WHERE "{id_column}" = :pat_id'
                )

                if not connection.dialect.has_schema(connection, schema_name):
                    connection.execute(CreateSchema(schema_name))

            inspector = inspect(connection)
            if inspector.has_table(target_table, schema=target_schema):
                connection.execute(del_query, {"pat_id": patient_id})

            # Debug output - show columns before and after drop
            print(
                f"DEBUG helper_functions: Before drop - columns: {df.columns.tolist()}"
            )

            # Drop Elasticsearch/MongoDB metadata columns and index column that conflict with SQLite
            # Note: updatetime is preserved for demographics table as it's required by get_demographics3_batch
            cols_to_drop = ["_id", "_index", "_score", "search_term", "index"]
            if table_name != "raw_demographics":
                cols_to_drop.append("updatetime")
            for col in cols_to_drop:
                if col in df.columns:
                    print(f"DEBUG helper_functions: Dropping column {col}")
                    df.drop(columns=col, inplace=True)

            print(
                f"DEBUG helper_functions: After drop - columns: {df.columns.tolist()}"
            )

            # Fix problematic backslashes in text columns that cause SQLite parameter binding issues
            text_cols = [
                "observation_valuetext_analysed",
                "obscatalogmasteritem_displayname",
            ]
            for col in text_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace("\\", "", regex=False)

            # Convert any list/dict/tuple columns to JSON strings for database compatibility
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (list, dict, tuple))).any():
                    df[col] = df[col].apply(
                        lambda x: (
                            json.dumps(x) if isinstance(x, (list, dict, tuple)) else x
                        )
                    )

            if not df.empty:
                df.to_sql(
                    name=target_table,
                    con=connection,
                    schema=target_schema,
                    if_exists="append",
                    index=False,
                )

                # Ensure index on ID column
                ensure_index(
                    connection, table_name, schema_name, id_column, engine.name
                )
    except Exception as e:
        logging.error(f"Failed to save raw batch {table_name} for {patient_id}: {e}")


def save_annotations_to_db(
    df: pd.DataFrame,
    patient_id: str,
    table_name: str,
    config_obj: Any,
    id_column: str = "client_idcode",
) -> None:
    """Saves an annotation batch for a patient to the database.

    Args:
        df: The DataFrame containing the annotations.
        patient_id: The patient identifier.
        table_name: The target table name (without schema prefix).
        config_obj: The configuration object.
        id_column: The column name for the patient ID in this table.
    """
    if config_obj.storage_backend != "database":
        return

    try:
        engine = config_obj.db_engine
        if not engine:
            return

        schema_name = "annotations"

        # Ensure ID column is present
        if id_column not in df.columns:
            df[id_column] = patient_id

        with engine.begin() as connection:
            if engine.name == "sqlite":
                target_table = f"{schema_name}_{table_name}"
                target_schema = None
                del_query = text(
                    f'DELETE FROM "{target_table}" WHERE "{id_column}" = :pat_id'
                )
            else:
                target_table = table_name
                target_schema = schema_name
                del_query = text(
                    f'DELETE FROM "{schema_name}"."{table_name}" WHERE "{id_column}" = :pat_id'
                )

                if not connection.dialect.has_schema(connection, schema_name):
                    connection.execute(CreateSchema(schema_name))

            inspector = inspect(connection)
            if inspector.has_table(target_table, schema=target_schema):
                connection.execute(del_query, {"pat_id": patient_id})

            # Convert any list/dict/tuple columns to JSON strings for database compatibility
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (list, dict, tuple))).any():
                    df[col] = df[col].apply(
                        lambda x: (
                            json.dumps(x) if isinstance(x, (list, dict, tuple)) else x
                        )
                    )

            if not df.empty:
                df.to_sql(
                    name=target_table,
                    con=connection,
                    schema=target_schema,
                    if_exists="append",
                    index=False,
                )

                # Ensure index on ID column
                ensure_index(
                    connection, table_name, schema_name, id_column, engine.name
                )
    except Exception as e:
        logging.error(
            f"Failed to save annotation batch {table_name} for {patient_id}: {e}"
        )


def get_all_features(config_obj: Any) -> pd.DataFrame:
    """Retrieves all patient features from the configured backend.

    If storage_backend is 'database', it reads the entire 'features' table.

    If storage_backend is 'file', it reads and concatenates all individual
    patient CSV files from the `current_pat_lines_path` directory.
    """
    logging.info(f"get_all_features called with backend: {config_obj.storage_backend}")
    if config_obj.storage_backend == "database":
        try:
            engine = config_obj.db_engine
            if not engine:
                logging.error("Database engine not initialized in config_obj.")
                return pd.DataFrame()

            table_name = "features"
            schema_name = "features"

            # Determine actual table name for inspection/reading
            if engine.name == "sqlite":
                target_table = f"{schema_name}_{table_name}"
                target_schema = None
            else:
                target_table = table_name
                target_schema = schema_name

            with engine.connect() as connection:
                inspector = inspect(connection)
                if not inspector.has_table(target_table, schema=target_schema):
                    logging.warning(
                        f"Table '{target_table}' not found in database. Returning empty DataFrame."
                    )
                    return pd.DataFrame()

                df = pd.read_sql_table(target_table, connection, schema=target_schema)

                # Check for packed JSON features and unpack if present
                if "features_json" in df.columns:
                    logging.debug("Unpacking 'features_json' column...")
                    # Only unpack non-null rows
                    json_mask = df["features_json"].notna()
                    if json_mask.any():
                        unpacked = pd.json_normalize(
                            df.loc[json_mask, "features_json"]
                            .apply(json.loads)
                            .tolist()
                        )
                        unpacked.index = df.loc[json_mask].index
                        df = df.drop(columns=["features_json"]).combine_first(unpacked)

                logging.debug(
                    f"Loaded DataFrame from DB table {target_table}. Shape: {df.shape}"
                )
                return df
        except Exception as e:
            logging.error(f"Error in get_all_features reading from database: {e}")
            return pd.DataFrame()

    elif config_obj.storage_backend == "file":
        path = config_obj.current_pat_lines_path
        all_files = [
            os.path.join(path, f) for f in os.listdir(path) if f.endswith(".csv")
        ]
        if not all_files:
            return pd.DataFrame()
        df_from_each_file = (
            pd.read_csv(f) for f in tqdm(all_files, desc="Loading feature files")
        )
        return pd.concat(df_from_each_file, ignore_index=True)

    raise ValueError(f"Unknown storage_backend: {config_obj.storage_backend}")


def get_df_from_db(
    config_obj: Any,
    schema: str,
    table: str,
    patient_ids: Optional[List[str]] = None,
    patient_id_column: str = "client_idcode",
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Generic helper to retrieve a DataFrame from the database backend.

    This function handles database connections, dialect-specific table naming
    (e.g., for SQLite), and filtering by a list of patient IDs.

    Args:
        config_obj: The configuration object.
        schema: The database schema name (e.g., 'raw_data').
        table: The database table name (e.g., 'raw_drugs').
        patient_ids: An optional list of patient IDs to filter the DataFrame.
        patient_id_column: The name of the patient ID column.
        columns: An optional list of columns to select.

    Returns:
        A pandas DataFrame with the requested data, or an empty DataFrame on error.
    """
    try:
        engine = config_obj.db_engine
        if not engine:
            logger.error("Database engine not initialized in config_obj.")
            return pd.DataFrame()

        with engine.connect() as connection:
            # Determine actual table name for inspection/reading
            if engine.name == "sqlite":
                target_table = f"{schema}_{table}"
                target_schema = None
            else:
                target_table = table
                target_schema = schema

            inspector = inspect(connection)
            if not inspector.has_table(target_table, schema=target_schema):
                logger.warning(
                    f"Table '{target_table}' not found in database. Returning empty DataFrame."
                )
                return pd.DataFrame()

            chunk_size = 900
            all_data = []

            if patient_ids is None:
                query_str = f"SELECT {', '.join(columns) if columns else '*'} FROM {target_table}"
                df = pd.read_sql(text(query_str), connection, params={})
                return df

            if len(patient_ids) == 0:
                return pd.DataFrame()

            for i in range(0, len(patient_ids), chunk_size):
                chunk = patient_ids[i : i + chunk_size]

                query_str_chunk = f"SELECT {', '.join(columns) if columns else '*'} FROM {target_table}"

                placeholders = ", ".join([f":id_{j}" for j in range(len(chunk))])
                query_str_chunk += f' WHERE "{patient_id_column}" IN ({placeholders})'
                params = {f"id_{j}": pid for j, pid in enumerate(chunk)}

                df_chunk = pd.read_sql(text(query_str_chunk), connection, params=params)
                all_data.append(df_chunk)

            return (
                pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
            )

    except Exception as e:
        logger.error(
            f"Error in get_df_from_db reading from database for {schema}.{table}: {e}"
        )
        return pd.DataFrame()


def get_df_from_db_with_temporal_filter(
    config_obj: Any,
    schema: str,
    table: str,
    patient_ids: Optional[List[str]] = None,
    time_column: str = "updatetime",
    start_date: Optional[Any] = None,
    end_date: Optional[Any] = None,
    patient_id_column: str = "client_idcode",
) -> pd.DataFrame:
    """Retrieves DataFrame from database with optional temporal filtering.

    Args:
        config_obj: Configuration object with database engine.
        schema: Database schema name.
        table: Table name.
        patient_ids: List of patient IDs to filter.
        time_column: Name of the datetime column to filter on.
        start_date: Start date for temporal filter (inclusive).
        end_date: End date for temporal filter (inclusive).
        patient_id_column: Name of the patient ID column.

    Returns:
        DataFrame with filtered data, or empty DataFrame on error.
    """
    try:
        engine = config_obj.db_engine
        if not engine:
            logging.error("Database engine not initialized in config_obj.")
            return pd.DataFrame()

        with engine.connect() as connection:
            # Chunking for SQLite parameter limits
            chunk_size = 900
            all_data = []

            if patient_ids is None or len(patient_ids) == 0:
                return pd.DataFrame()

            for i in range(0, len(patient_ids), chunk_size):
                chunk = patient_ids[i : i + chunk_size]

                # Build table reference based on dialect
                if engine.name == "sqlite":
                    full_table_name = f'"{schema}_{table}"'
                else:
                    full_table_name = f'"{schema}"."{table}"'

                # Build WHERE clause
                where_clauses = []
                params = {}

                # Patient ID filter
                patient_where = f'"{patient_id_column}" IN ({", ".join([f":p{i}" for i in range(len(chunk))])})'
                where_clauses.append(patient_where)
                params.update({f"p{i}": str(pid) for i, pid in enumerate(chunk)})

                # Temporal filter if dates provided
                if start_date is not None and end_date is not None:

                    def _format_datetime_for_sql(dt):
                        """Convert datetime to string suitable for SQL comparison.

                        For timezone-aware datetimes, extracts just the date portion.
                        For naive datetimes, uses the date part only.
                        Ensures proper handling of the full day by using date-only strings
                        which SQLite will compare as 'string <= date'.
                        """
                        if pd.isna(dt):
                            return None
                        # Convert to string representation
                        dt_str = str(dt)
                        # Extract just the date portion (YYYY-MM-DD)
                        # Handle both ISO format (2023-01-01T00:00:00+00:00) and space-separated formats
                        if "T" in dt_str:
                            # ISO format like '2023-01-01T00:00:00+00:00'
                            date_part = dt_str.split("T")[0]
                        elif " " in dt_str:
                            # Space-separated like '2023-01-01 00:00:00' or '2023-01-01 00:00:00+00:00'
                            parts = dt_str.split(" ")
                            date_part = parts[0]
                        else:
                            # Already just the date part
                            date_part = dt_str
                        return date_part

                    params["start_date"] = _format_datetime_for_sql(start_date)

                    # For end_date, we need to ensure it includes the entire day.
                    # The issue: '2023-01-01T00:00:00' <= '2023-01-01' is FALSE in SQLite
                    # because string comparison treats '2023-01-01...' > '2023-01-01'.
                    # The solution: use date + 1 day for end, and use < instead of <=
                    import datetime as dt_module

                    end_date_obj = pd.to_datetime(end_date)
                    if isinstance(end_date_obj, pd.Timestamp) and hasattr(
                        end_date_obj, "date"
                    ):
                        end_date_only = end_date_obj.date()
                        # Add one day to include the entire end date
                        end_date_next = (
                            end_date_only + dt_module.timedelta(days=1)
                        ).isoformat()
                        params["end_date"] = (
                            end_date_next.split("T")[0]
                            if "T" in end_date_next
                            else end_date_next
                        )
                    else:
                        # Fallback to original behavior
                        params["end_date"] = _format_datetime_for_sql(end_date)

                    where_clauses.append(
                        f'"{time_column}" >= :start_date AND "{time_column}" < :end_date'
                    )

                where_clause = " WHERE " + " AND ".join(where_clauses)

                query_str = f"SELECT * FROM {full_table_name} {where_clause}"
                query = text(query_str)

                df_chunk = pd.read_sql(query, connection, params=params)
                all_data.append(df_chunk)

            result_df = (
                pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
            )
            return result_df

    except Exception as e:
        logging.error(f"Error with temporal database filter for {table}: {e}")
        return pd.DataFrame()
