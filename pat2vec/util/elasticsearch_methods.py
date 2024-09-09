from getpass import getpass

import pandas as pd
from credentials import *
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from IPython.display import display
from tqdm import tqdm
from elasticsearch.helpers import BulkIndexError


def ingest_data_to_elasticsearch(temp_df, index_name, index_mapping=None):
    """
    Function to ingest data from a DataFrame into Elasticsearch.

    Parameters:
        temp_df (DataFrame): The DataFrame containing the data to be ingested.
        index_name (str): Name of the Elasticsearch index.

    Returns:
        tuple: A tuple containing the number of successfully ingested documents and the number of failed documents.
    """

    print(host_name, port, scheme, username)

    # Elastic cannot handle nan
    temp_df.fillna("", inplace=True)

    if "updatetime" in temp_df.columns:
        temp_df["updatetime"] = pd.to_datetime(temp_df["updatetime"], format="ISO8601")

    if "observationdocument_recordeddtm" in temp_df.columns:
        temp_df["observationdocument_recordeddtm"] = pd.to_datetime(
            temp_df["observationdocument_recordeddtm"], format="ISO8601"
        )

    for column in temp_df.columns:
        if pd.api.types.is_datetime64_any_dtype(temp_df[column]):
            # If it's a datetime column, convert it to ISO 8601 format
            temp_df[column] = (
                temp_df[column].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                if temp_df[column].dt.tz is not None
                else temp_df[column].dt.strftime("%Y-%m-%d")
            )

    # Connect to Elasticsearch
    es = Elasticsearch(
        [{"host": host_name, "port": port, "scheme": scheme}],
        verify_certs=False,
        http_auth=(username, password),
    )

    try:
        if not es.ping():
            raise ConnectionError("Elasticsearch server not reachable.")
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        raise

    # Create the index if it does not exist
    if not es.indices.exists(index=index_name):
        try:
            es.indices.create(index=index_name, body=mapping, ignore=400)
        except Exception as e:
            print(f"Error creating index: {e}")
            raise

    # Convert DataFrame to JSON format
    docs = temp_df.to_dict(orient="records")

    # Ingest data into Elasticsearch
    actions = [
        {"_op_type": "index", "_index": index_name, "_source": doc} for doc in docs
    ]

    try:
        success, failed = bulk(es, actions)
        print(
            f"Successfully ingested {success} documents, failed to ingest {failed} documents."
        )

        # If there are failed documents, print detailed error information
        if failed:
            print("\nDetails of failed documents:")
            for doc_info in failed:
                print(f"Error for document: {doc_info['index']}")
                print(f"Reason: {doc_info['indexing']['error']}")

        return success, failed

    except BulkIndexError as e:
        print(e.errors)

    except Exception as e:
        print(f"Error bulk indexing: {e}")
        raise


# Example usage:
# ingest_data_to_elasticsearch(temp_df, "annotations_myeloma")


def handle_inconsistent_dtypes(df):
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


def guess_datetime_columns(df, threshold=0.5):
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


def get_guess_datetime_column(df, threshold=0.2):
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
