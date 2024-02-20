from getpass import getpass

import pandas as pd
from credentials import *
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def ingest_data_to_elasticsearch(temp_df, index_name):
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

    if 'updatetime' in temp_df.columns:
        temp_df['updatetime'] = pd.to_datetime(
            temp_df['updatetime'], format='ISO8601')

    if 'observationdocument_recordeddtm' in temp_df.columns:
        temp_df['observationdocument_recordeddtm'] = pd.to_datetime(
            temp_df['observationdocument_recordeddtm'], format='ISO8601')

    for column in tempdf.columns:
        if pd.api.types.is_datetime64_any_dtype(tempdf[column]):
            # If it's a datetime column, convert it to ISO 8601 format
            tempdf[column] = tempdf[column].dt.strftime('%Y-%m-%dT%H:%M:%S%z') if tempdf[column].dt.tz is not None else tempdf[column].dt.strftime('%Y-%m-%d')

    # Connect to Elasticsearch
    es = Elasticsearch(
        [{'host': host_name, 'port': port, 'scheme': scheme}],
        verify_certs=False,
        http_auth=(username, password)
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
            es.indices.create(index=index_name, ignore=400)
        except Exception as e:
            print(f"Error creating index: {e}")
            raise

    # Convert DataFrame to JSON format
    docs = temp_df.to_dict(orient='records')

    # Ingest data into Elasticsearch
    actions = [
        {
            "_op_type": "index",
            "_index": index_name,
            "_source": doc
        }
        for doc in docs
    ]

    try:
        success, failed = bulk(es, actions)
        print(
            f"Successfully ingested {success} documents, failed to ingest {failed} documents.")

        # If there are failed documents, print detailed error information
        if failed:
            print("\nDetails of failed documents:")
            for doc_info in failed:
                print(f"Error for document: {doc_info['index']}")
                print(f"Reason: {doc_info['indexing']['error']}")

        return success, failed

    except Exception as e:
        print(f"Error bulk indexing: {e}")
        raise

# Example usage:
# ingest_data_to_elasticsearch(temp_df, "annotations_myeloma")
