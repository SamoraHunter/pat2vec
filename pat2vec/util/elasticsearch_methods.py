from getpass import getpass
import pandas as pd
from typing import Any, Dict, List, Optional
from .credentials import *
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from IPython.display import display
from tqdm import tqdm
from elasticsearch.helpers import BulkIndexError
from elasticsearch import Elasticsearch, helpers


def ingest_data_to_elasticsearch(
    temp_df: pd.DataFrame,
    index_name: str,
    index_mapping: Optional[Dict[str, Any]] = None,
    replace_index: bool = False,
) -> Dict[str, int]:
    """Ingests data from a DataFrame into Elasticsearch with error handling.

    Args:
        temp_df: The DataFrame containing the data to be ingested.
        index_name: Name of the Elasticsearch index.
        index_mapping: Optional mapping for the index.
        replace_index: Whether to replace the index if it exists.

    Returns:
        A summary containing the number of successful and failed operations.

    Raises:
        ConnectionError: If the Elasticsearch server is not reachable.
    """
    # Set default index mapping if none is provided
    index_mapping = index_mapping or {  # type: ignore
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "index.mapping.ignore_malformed": True,
            "index.mapping.total_fields.limit": 100000,
        }
    }

    # Initialize Elasticsearch client
    es = Elasticsearch(
        [{"host": host_name, "port": port, "scheme": scheme}],
        verify_certs=False,
        http_auth=(username, password),
    )

    # Check connection
    try:
        if not es.ping():
            raise ConnectionError("Elasticsearch server not reachable.")
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        raise

    # Replace index if requested
    if es.indices.exists(index=index_name) and replace_index:
        response = es.indices.delete(index=index_name)
        print(f"Index {index_name} deleted successfully.")
        print(response)

    # Create the index if it does not exist
    if not es.indices.exists(index=index_name):
        try:
            es.indices.create(index=index_name, body=index_mapping)
            print(f"Index {index_name} created successfully.")
        except Exception as e:
            print(f"Error creating index: {e}")
            raise

    # Prepare documents for bulk indexing
    docs = temp_df.to_dict(orient="records")
    actions = [
        {"_op_type": "index", "_index": index_name, "_source": doc} for doc in docs
    ]

    success_count = 0
    failed_docs = []
    problematic_fields = {}

    try:
        for ok, result in helpers.streaming_bulk(es, actions):
            if not ok:
                failed_docs.append(result)

                # Extract error details
                error_info = result.get("index", {}).get("error", {})
                doc_id = result.get("index", {}).get("_id", "N/A")
                field_name = (
                    error_info.get("reason", "").split("field [")[1].split("]")[0]
                    if "field [" in error_info.get("reason", "")
                    else "Unknown"
                )

                # Log problematic field
                if field_name not in problematic_fields:
                    problematic_fields[field_name] = []
                problematic_fields[field_name].append(
                    error_info.get("reason", "Unknown")
                )

            else:
                success_count += 1

        print(f"Successfully ingested {success_count} documents.")
        print(f"Failed to ingest {len(failed_docs)} documents.")

        # Log details of failed documents
        if failed_docs:
            print("\nDetails of failed documents:")
            for fail in failed_docs:
                error_info = fail.get("index", {}).get("error", {})
                doc_id = fail.get("index", {}).get("_id", "N/A")
                print(f"Failed Document ID: {doc_id}")
                print(f"Error Type: {error_info.get('type', 'Unknown')}")
                print(f"Error Reason: {error_info.get('reason', 'Unknown')}")

            # Log problematic fields summary
            print("\nProblematic fields summary:")
            for field, issues in problematic_fields.items():
                print(f"Field: {field}")
                for issue in set(issues):
                    print(f"  - {issue}")

        return {"success": success_count, "failed": len(failed_docs)}

    except BulkIndexError as bulk_error:
        # Handle BulkIndexError and log detailed information
        print(f"BulkIndexError: {bulk_error}")
        failed_docs = bulk_error.errors

        # Log failed documents
        print("\nDetailed failure report:")
        for error in failed_docs:
            error_info = error.get("index", {}).get("error", {})
            doc_id = error.get("index", {}).get("_id", "N/A")
            print(f"Failed Document ID: {doc_id}")
            print(f"Error Type: {error_info.get('type', 'Unknown')}")
            print(f"Error Reason: {error_info.get('reason', 'Unknown')}")

        return {"success": success_count, "failed": len(failed_docs)}

    except Exception as e:
        print(f"Unexpected error during bulk ingestion: {e}")
        raise


# Example usage:
# ingest_data_to_elasticsearch(temp_df, "annotations_myeloma")


def handle_inconsistent_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Handles inconsistent data types in a DataFrame's columns.

    Iterates through each column, determines the majority data type (datetime,
    string, int, or float), and casts the entire column to that type. This is
    useful for cleaning data before ingestion into systems with strict schemas
    like Elasticsearch.

    Args:
        df: The DataFrame to process.

    Returns:
        The DataFrame with columns cast to their majority data type.
    """
    for column in tqdm(df.columns, desc="Processing columns"):
        non_null_values = df[column].dropna()
        dt_count = (
            non_null_values.apply(pd.to_datetime, errors="coerce").notnull().sum()
        )
        str_count = non_null_values.apply(type).eq(str).sum()
        int_count = non_null_values.apply(type).eq(int).sum()
        float_count = non_null_values.apply(type).eq(float).sum()

        total_valid = dt_count + str_count + int_count + float_count
        if total_valid == 0:
            print(f"No valid data types found in column '{column}'")
            continue

        dt_percent = dt_count / total_valid
        str_percent = str_count / total_valid
        int_percent = int_count / total_valid
        float_percent = float_count / total_valid

        majority_dtype = max(dt_percent, str_percent, int_percent, float_percent)
        if dt_percent > 0.5:
            display(f"Casting column '{column}' to datetime...")
            df[column] = pd.to_datetime(df[column], errors="ignore")
        else:
            display(f"Casting column '{column}' to majority datatype...")
            if majority_dtype == dt_percent:
                df[column] = pd.to_datetime(df[column], errors="ignore")
            elif majority_dtype == str_percent:
                df[column] = df[column].astype(str, errors="ignore")
            elif majority_dtype == int_percent:
                df[column] = df[column].astype(int, errors="ignore")
            elif majority_dtype == float_percent:
                df[column] = df[column].astype(float, errors="ignore")

    return df


def guess_datetime_columns(df: pd.DataFrame, threshold: float = 0.5) -> List[str]:
    """Guesses which columns in a DataFrame are datetime columns.

    It iterates through each column and attempts to parse its values as datetimes.
    If the percentage of parsable values exceeds a given threshold, the column
    is considered a datetime column.

    Args:
        df: The DataFrame to analyze.
        threshold: The minimum percentage of values in a column that must be
            parsable as datetime for it to be considered a datetime column.
            Defaults to 0.5.

    Returns:
        A list of column names that are likely to be datetime columns.
    """
    datetime_columns = []
    for column in tqdm(df.columns, desc="Processing Columns"):
        parse_count = 0
        total_count = 0
        for value in df[column]:
            total_count += 1
            # Skip parsing if the value is NaN
            if pd.isna(value):
                continue
            # Check if the value is a string, int, or float before attempting to parse as datetime
            if isinstance(value, str):
                try:
                    pd.to_datetime(value)
                    parse_count += 1
                except ValueError:
                    continue
        if parse_count / total_count >= threshold:
            datetime_columns.append(column)
    return datetime_columns


def get_guess_datetime_column(
    df: pd.DataFrame, threshold: float = 0.2
) -> Optional[str]:
    """Finds the single column most likely to be a datetime column.

    This function iterates through all columns and calculates the ratio of values
    that can be parsed as a datetime. It returns the name of the column with the
    highest ratio, provided that ratio is above the specified threshold.

    Args:
        df: The DataFrame to analyze.
        threshold: The minimum percentage of values that must be parsable as
            datetime for a column to be considered. Defaults to 0.2.

    Returns:
        The name of the column most likely to contain datetimes, or None if no
        column meets the threshold.
    """
    highest_ratio = 0
    highest_column = None

    for column in tqdm(df.columns, desc="Processing Columns"):
        parse_count = 0
        total_count = 0
        for value in df[column]:
            total_count += 1
            # Skip parsing if the value is NaN
            if pd.isna(value):
                continue
            # Check if the value is a string, int, or float before attempting to parse as datetime
            if isinstance(value, str):
                try:
                    pd.to_datetime(value)
                    parse_count += 1
                except ValueError:
                    continue
        if total_count > 0:
            parse_ratio = parse_count / total_count
            if parse_ratio >= threshold and parse_ratio > highest_ratio:
                highest_ratio = parse_ratio
                highest_column = column

    return highest_column
