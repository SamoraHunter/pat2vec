import os
import logging
import pandas as pd
import time

from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema
from typing import Any
from tqdm import tqdm
from sqlalchemy import inspect

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Move mappings to module level to allow testing and patching
MAPPINGS = [
    # Raw Data
    (
        "pre_bloods_batch_path",
        "raw_data",
        "raw_bloods",
        "client_idcode",
        ["client_idcode", "basicobs_entered"],
    ),
    (
        "pre_drugs_batch_path",
        "raw_data",
        "raw_drugs",
        "client_idcode",
        ["client_idcode", "order_entered"],
    ),
    (
        "pre_diagnostics_batch_path",
        "raw_data",
        "raw_diagnostics",
        "client_idcode",
        ["client_idcode", "order_entered"],
    ),
    (
        "pre_news_batch_path",
        "raw_data",
        "raw_news",
        "client_idcode",
        ["client_idcode", "observationdocument_recordeddtm"],
    ),
    (
        "pre_bmi_batch_path",
        "raw_data",
        "raw_bmi",
        "client_idcode",
        ["client_idcode", "observationdocument_recordeddtm"],
    ),
    (
        "pre_demo_batch_path",
        "raw_data",
        "raw_demographics",
        "client_idcode",
        ["client_idcode", "updatetime"],
    ),
    (
        "pre_document_batch_path",
        "raw_data",
        "raw_epr_docs",
        "client_idcode",
        ["client_idcode", "updatetime"],
    ),
    (
        "pre_document_batch_path_mct",
        "raw_data",
        "raw_mct_docs",
        "client_idcode",
        ["client_idcode", "observationdocument_recordeddtm"],
    ),
    (
        "pre_textual_obs_document_batch_path",
        "raw_data",
        "raw_textual_obs",
        "client_idcode",
        ["client_idcode", "basicobs_entered"],
    ),
    (
        "pre_document_batch_path_reports",
        "raw_data",
        "raw_reports",
        "client_idcode",
        ["client_idcode", "updatetime"],
    ),
    (
        "pre_appointments_batch_path",
        "raw_data",
        "raw_appointments",
        "HospitalID",
        ["HospitalID", "AppointmentDateTime"],
    ),
    # Annotations
    (
        "pre_document_annotation_batch_path",
        "annotations",
        "ann_epr_docs",
        "client_idcode",
        ["client_idcode", "updatetime"],
    ),
    (
        "pre_document_annotation_batch_path_mct",
        "annotations",
        "ann_mct_docs",
        "client_idcode",
        ["client_idcode", "observationdocument_recordeddtm"],
    ),
    (
        "pre_textual_obs_annotation_batch_path",
        "annotations",
        "ann_textual_obs",
        "client_idcode",
        ["client_idcode", "basicobs_entered"],
    ),
    (
        "pre_document_annotation_batch_path_reports",
        "annotations",
        "ann_reports",
        "client_idcode",
        ["client_idcode", "updatetime"],
    ),
    # Features
    (
        "current_pat_lines_path",
        "features",
        "features",
        "client_idcode",
        ["client_idcode"],
    ),
]


def create_indexes(engine, schema_name, table_name, index_columns):
    """Creates an index on the specified table and columns, if it doesn't already exist."""
    inspector = inspect(engine)

    # Determine the actual table name used in the DB
    if engine.name == "sqlite":
        target_table = f"{schema_name}_{table_name}"
        quoted_target_table = f'"{target_table}"'
        target_schema = None
    else:
        target_table = table_name
        quoted_target_table = (
            f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
        )
        target_schema = schema_name

    index_name = f"idx_{target_table}_{'_'.join(index_columns)}"

    # Check if index exists using inspector
    if not inspector.has_table(target_table, schema=target_schema):
        return  # Table does not exist, so no need to create an index

    # Now check if index already exists
    existing_indexes = inspector.get_indexes(target_table, schema=target_schema)
    if any(idx["name"] == index_name for idx in existing_indexes):
        return

    with engine.begin() as connection:
        # Construct the CREATE INDEX statement directly
        columns_quoted = [f'"{col}"' for col in index_columns]
        create_index_sql = f'CREATE INDEX "{index_name}" ON {quoted_target_table} ({", ".join(columns_quoted)})'
        connection.execute(text(create_index_sql))


def _write_batch(dfs, engine, schema, table):
    if not dfs:
        return
    try:
        combined = pd.concat(dfs, ignore_index=True)
        if "Unnamed: 0" in combined.columns:
            combined.drop(columns=["Unnamed: 0"], inplace=True)

        # Handle SQLite flattening
        if engine.name == "sqlite":
            target_table = f"{schema}_{table}"
            target_schema = None
        else:
            target_table = table
            target_schema = schema

        combined.to_sql(
            name=target_table,
            con=engine,
            schema=target_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )
    except Exception as e:
        logger.error(f"Failed to write batch to {schema}.{table}: {e}")
        time.sleep(5)


def migrate_csv_to_db(config_obj: Any):
    """
    Migrates data from CSV files (file-based backend) to the Database backend..
    It iterates over known directories in the config, reads CSVs and pushes them to the DB.
    """
    if not config_obj.db_connection_string:
        raise ValueError("db_connection_string not set in config object.")

    engine = create_engine(config_obj.db_connection_string)

    # 1. Create Schemas
    # SQLite does not support CREATE SCHEMA
    if engine.name != "sqlite":
        with engine.begin() as connection:
            for schema in ["raw_data", "annotations", "features"]:
                if not connection.dialect.has_schema(connection, schema):
                    connection.execute(CreateSchema(schema))

    # 2. Iterate Mappings
    for dir_attr, schema, table, id_col, index_columns in MAPPINGS:
        if not hasattr(config_obj, dir_attr):
            logger.warning(
                f"Config object missing attribute {dir_attr}, skipping {table}"
            )
            continue

        dir_path = getattr(config_obj, dir_attr, None)
        # Safety check: ensure dir_path is a string and not a MagicMock (for tests)
        if not isinstance(dir_path, str):
            continue
        if not os.path.exists(dir_path):
            logger.info(f"Directory {dir_path} does not exist, skipping {table}")
            continue

        files = [f for f in os.listdir(dir_path) if f.endswith(".csv")]
        if not files:
            continue

        logger.info(
            f"Migrating {len(files)} files from {dir_path} to {schema}.{table}..."
        )

        # Check if table exists to decide on append/replace behavior or just appending

        batch_size = 100
        dfs = []

        for i, f in enumerate(tqdm(files, desc=f"Reading {table}")):
            try:
                df = pd.read_csv(os.path.join(dir_path, f))
                # Ensure ID column is present if not in CSV (e.g. inferred from filename)
                # But usually pat2vec saves ID in CSV.
                # Just in case for features which might strictly use filename as ID sometimes?
                # Helper save_patient_features ensures column exists.
                # Raw batch files usually have the ID column.

                # Appointment special case: hospitalID might be the filename, but column is HospitalID
                if table == "raw_appointments" and id_col not in df.columns:
                    pass

                dfs.append(df)

                if len(dfs) >= batch_size:
                    _write_batch(dfs, engine, schema, table)
                    dfs = []
            except Exception as e:
                logger.error(f"Failed to read/process {f}: {e}")

        if dfs:
            _write_batch(dfs, engine, schema, table)

    # 3. Create Indexes
    for dir_attr, schema, table, id_col, index_columns in MAPPINGS:
        create_indexes(engine, schema, table, index_columns)

    logger.info("Migration completed.")


if __name__ == "__main__":
    """Example usage: This script is intended to be imported and run with a valid config_obj."""

    # User needs to provide config, likely via importing their setup
    print("\nThis script is intended to be imported and run with a valid config_obj.")
    print("  from pat2vec.util.config_pat2vec import config_class")
    print(
        "  conf = config_class(storage_backend='database', db_connection_string='postgresql://user:pass@host/db')"
    )
    print("  # ensure you import and call migrate_csv_to_db(conf)")

    print("  # Ensure paths in conf point to existing CSV directories")
