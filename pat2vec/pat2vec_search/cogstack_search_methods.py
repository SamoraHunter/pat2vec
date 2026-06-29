import os
from os.path import exists
from pathlib import Path
import sys
from tqdm.notebook import tqdm

import eland as ed
import elasticsearch
import elasticsearch.helpers
import pandas as pd
import importlib.util
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import getpass

from pat2vec.util.get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
    generate_uuid_list,
)
from pat2vec.util.get_method_index_map import get_index_for_method

import random
import warnings
import logging

warnings.filterwarnings("ignore", category=DeprecationWarning)  # Keep this line

# Suppress Elasticsearch logger
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# add one level up to path with sys.path for importing actual credentials

random_state = 42
random.seed(random_state)

# Global CogStack client instance
cs = None

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)


def create_credentials_file() -> None:
    """Creates a template credentials.py file.

    This function creates a `credentials.py` file three levels up from the
    current file's directory. This file contains placeholder variables for
    Elasticsearch connection details (hosts, username, password, api_key).
    It is intended to be filled out by the user with their actual credentials.
    """
    base_dir = (
        Path(__file__).resolve().parent.parent.parent.parent
    )  # Go up three levels
    credentials_dir = base_dir
    credentials_file = credentials_dir / "credentials.py"

    # Create the directory if it doesn't exist
    credentials_dir.mkdir(parents=True, exist_ok=True)

    # Define the content for the credentials file
    content = """# Elasticsearch credentials
hosts = [
    "https://your-actual-elasticsearch-host:9200"
]  # List of real Elasticsearch URLs

# Choose either HTTP auth or API key (comment out what you're not using)
username = "your_real_username"
password = "your_real_password"
# api_key = "your_real_api_key"
"""

    # Write the content to the file
    with open(credentials_file, "w") as f:
        f.write(content)

    import sys

    sys.path.append(str(credentials_dir))

    logging.info(f"Credentials file created at: {credentials_file}")
    logging.info("Please update the file with your actual credentials.")


class CogStack(object):
    logging.debug("CogStack class refreshed.")

    def __init__(
        self,
        hosts: List[str],
        username: Optional[str] = None,
        password: Optional[str] = None,
        api: bool = True,
        api_key: Optional[str] = None,
    ):
        """Initializes the CogStack client for Elasticsearch interaction.

        This class provides an interface to interact with CogStack/Elasticsearch
        instances, supporting both API key and basic authentication methods.

        Args:
            hosts: A list of CogStack host URLs.
            username: The username for basic authentication. Required if using
                basic auth (api=False).
            password: The password for basic authentication. Required if using
                basic auth (api=False).
            api: If True, use API key authentication. If False, use basic
                authentication. Defaults to True.
            api_key: The API key for authentication. Required if using API
                key auth (api=True).

        Returns:
            CogStack: Returns self instance connected to the Elasticsearch cluster.

        Raises:
            Exception: If Elasticsearch connection fails during initialization.
        """
        if api:
            self.elastic = elasticsearch.Elasticsearch(
                hosts=hosts, api_key=api_key, verify_certs=False
            )
        else:
            username, password = self._check_auth_details(username, password)  # type: ignore
            self.elastic = elasticsearch.Elasticsearch(
                hosts=hosts, basic_auth=(username, password), verify_certs=False
            )

    def _check_api_auth_details(
        self, api_username: Optional[str] = None, api_password: Optional[str] = None
    ) -> Tuple[str, str]:
        """Prompts for API credentials if they are not provided.

        Args:
            api_username: The API username. If not provided, prompts the user
                interactively.
            api_password: The API password. If not provided, prompts the user
                interactively using getpass.

        Returns:
            Tuple[str, str]: A tuple containing (api_username, api_password).
        """
        if api_username is None:
            api_username = input("API Username: ")
        if api_password is None:
            api_password = getpass.getpass("API Password: ")
        return api_username, api_password  # type: ignore

    def _check_auth_details(
        self, username: Optional[str] = None, password: Optional[str] = None
    ) -> Tuple[str, str]:
        """Prompts for basic authentication credentials if they are not provided.

        Args:
            username: The username. If not provided, prompts the user
                interactively.
            password: The password. If not provided, prompts the user
                interactively using getpass.

        Returns:
            Tuple[str, str]: A tuple containing (username, password).
        """
        if username is None:
            username = input("Username: ")
        if password is None:
            password = getpass.getpass("Password: ")
        return username, password  # type: ignore

    def get_docs_generator(
        self,
        index: List[str],
        query: Dict[str, Any],
        es_gen_size: int = 800,
        request_timeout: int = 300,
    ) -> Generator[Dict[str, Any], None, None]:
        """Returns a generator that yields documents from an Elasticsearch search.

        This method uses `elasticsearch.helpers.scan` to efficiently scroll
        through all results of a query.

        Args:
            index: A list of Elasticsearch indices to search.
            query: The Elasticsearch query dictionary.
            es_gen_size: The number of documents to retrieve per shard in each
                scroll. Defaults to 800.
            request_timeout: The timeout in seconds for the request. Defaults to 300.

        Returns:
            Generator[Dict[str, Any], None, None]: A generator that yields search
                hits as dictionaries.

        Raises:
            elasticsearch.ElasticsearchException: If the Elasticsearch query fails.
        """
        docs_generator = elasticsearch.helpers.scan(
            self.elastic,
            query=query,
            index=index,
            size=es_gen_size,
            request_timeout=request_timeout,
        )
        return docs_generator

    def cogstack2df(
        self,
        query: Dict[str, Any],
        index: str,
        column_headers: Optional[List[str]] = None,
        es_gen_size: int = 800,
        request_timeout: int = 300,
    ) -> pd.DataFrame:
        """Executes a search query and returns the results as a pandas DataFrame.

        This method fetches documents from Elasticsearch using the scan API
        and converts them into a pandas DataFrame with metadata fields.

        Args:
            query: The Elasticsearch query dictionary.
            index: The name of the index or a list of indices to search.
            column_headers: An optional list of specific columns to include in
                the DataFrame. If provided, adds these to default metadata fields.
            es_gen_size: The number of documents per scroll request. Defaults to 800.
            request_timeout: The timeout in seconds for each scroll request.
                Defaults to 300.

        Returns:
            pd.DataFrame: A DataFrame containing the search results with columns
                _index, _id, _score, and the fields from _source.

        Raises:
            elasticsearch.ElasticsearchException: If the Elasticsearch query fails.
        """
        docs_generator = elasticsearch.helpers.scan(
            self.elastic,
            query=query,
            index=index,
            size=es_gen_size,
            request_timeout=request_timeout,
        )
        temp_results = []
        self.elastic.count(index=index, query=query["query"], request_timeout=30)
        for hit in docs_generator:
            row = dict()
            row["_index"] = hit["_index"]
            # row['_type'] = hit['_type']
            row["_id"] = hit["_id"]
            row["_score"] = hit["_score"]
            row.update(hit["_source"])
            temp_results.append(row)
        if column_headers:
            df_headers = [
                "_index",
                "_id",
                "_score",
            ]  # ['_index', '_type', '_id', '_score']
            df_headers.extend(column_headers)
            df = pd.DataFrame(temp_results, columns=df_headers)
        else:
            df = pd.DataFrame(temp_results)
        return df

    def get_index_fields(self, index_name: str) -> List[str]:
        """Retrieves a list of all unique field names for a given
        Elasticsearch index or index pattern.

        This method uses the get_mapping API to extract all fields defined
        in the index mapping.

        Args:
            index_name: The name of the index or an index pattern (e.g., 'my-index-*').

        Returns:
            List[str]: A sorted list of unique field names found across the
                matching indices. Returns an empty list if the index is not found.
        """
        try:
            # Get the mapping for the given index or index pattern
            mapping = self.elastic.indices.get_mapping(index=index_name)

            all_fields = set()

            # Iterate through each index returned in the mapping
            for index_data in mapping.values():
                # The properties dictionary contains the field mappings
                properties = index_data.get("mappings", {}).get("properties", {})
                all_fields.update(properties.keys())

            return sorted(list(all_fields))

        except elasticsearch.exceptions.NotFoundError:
            logging.error(f"Index or pattern '{index_name}' not found.")
            return []
        except Exception as e:
            logging.error(
                f"An error occurred while fetching fields for index '{index_name}': {e}"
            )
            return []

    def get_available_indices(self) -> List[str]:
        """Retrieves a list of all available index names from Elasticsearch.

        Uses the cat.indices API to fetch a list of indices.

        Returns:
            List[str]: A sorted list of unique index names. Returns an empty
                list if an error occurs.
        """
        try:
            # Use the cat API to get a list of indices
            indices = self.elastic.cat.indices(format="json", h="index")

            # Extract the index name from each dictionary in the list
            index_names = [index["index"] for index in indices]

            # Return a sorted list of unique index names
            return sorted(list(set(index_names)))

        except Exception as e:
            logging.error(f"An error occurred while fetching indices: {e}")
            return []

    def DataFrame(self, index: str) -> ed.DataFrame:
        """Returns an Eland DataFrame for the specified index.

        Eland provides a pandas-like API for data stored in Elasticsearch.

        Args:
            index: The name of the index or index pattern to query.

        Returns:
            ed.DataFrame: An Eland DataFrame object configured with the
                Elasticsearch client and index pattern.
        """
        return ed.DataFrame(es_client=self.elastic, es_index_pattern=index)


def get_all_fields_for_method(
    method_name: str, cs: Optional["CogStack"] = None
) -> List[str]:
    """Retrieves all available fields from the Elasticsearch index
    associated with a given `get` method.

    Uses the cogstack client's get_index_fields method to fetch field names.

    Args:
        method_name: The name of the `get` method used to look up the index.
        cs: An initialized CogStack client. If not provided or None, one will be
            initialized by calling initialize_cogstack_client().

    Returns:
        List[str]: A list of all fields in the index, or an empty list if no
            index is found or the client initialization fails.
    """
    if cs is None:
        cs = initialize_cogstack_client()

    index_name = get_index_for_method(method_name)
    if not index_name:
        logging.warning(f"No index found for method '{method_name}'")
        return []

    if not cs:
        logging.error("Could not initialize CogStack client.")
        return []

    return cs.get_index_fields(index_name)


def list_chunker(entered_list: List[Any]) -> List[List[Any]]:
    """Splits a list into smaller chunks of up to 10,000 elements.

    Useful for processing large lists in batches to avoid overwhelming
    systems with too much data at once.

    Args:
        entered_list: The list to be split into chunks.

    Returns:
        List[List[Any]]: A list of sublists, each containing up to 10,000
            elements from the original list.
    """
    return [entered_list[x : x + 10000] for x in range(0, len(entered_list), 10000)]


def dataframe_generator(
    list_of_dfs: List[pd.DataFrame],
) -> Generator[pd.DataFrame, None, None]:
    """A generator that yields DataFrames from a list of DataFrames.

    This utility function allows iterating over a list of pandas DataFrames
    one at a time, which can help manage memory usage when processing large
    datasets.

    Args:
        list_of_dfs: A list of pandas DataFrames to iterate over.

    Yields:
        pd.DataFrame: The next DataFrame in the list.
    """
    for df in list_of_dfs:
        yield df


def cohort_searcher_with_terms_and_search(
    index_name: str,
    fields_list: List[str],
    term_name: str,
    entered_list: List[str],
    search_string: str,
) -> pd.DataFrame:
    """Searches a cohort using a term filter and a query string.

    This function performs a boolean search in Elasticsearch that combines
    a terms-level filter (for exact value matching) with a query string
    (for full-text search). For large entered_list (>10000), it processes
    the list in chunks.

    Args:
        index_name: The name of the Elasticsearch index to search.
        fields_list: The list of fields to return from each document.
        term_name: The name of the field to use for the term-level filter.
        entered_list: The list of values to filter for in the `term_name` field.
        search_string: The query string to apply to the search.

    Returns:
        pd.DataFrame: A DataFrame containing the search results with all
            specified fields and document metadata. Empty DataFrame if
            no results found or CogStack client not initialized. When
            entered_list >= 10000, returns a list of DataFrames due to
            chunked processing.

    Raises:
        Exception: If merging results fails (re-raises exceptions from
            dataframe operations).
    """
    global cs
    if cs is None:
        initialize_cogstack_client()
    if cs is None:
        logging.error("CogStack client is not initialized. Returning empty DataFrame.")
        return pd.DataFrame()
    if len(entered_list) >= 10000:

        results = []
        chunked_list = list_chunker(entered_list)
        for mini_list in chunked_list:
            query = {
                "from": 0,
                "size": 10000,
                "query": {
                    "bool": {
                        "filter": {"terms": {term_name: mini_list}},
                        "must": [{"query_string": {"query": search_string}}],
                    }
                },
                "_source": fields_list,
            }
            df = cs.cogstack2df(
                query=query, index=index_name, column_headers=fields_list
            )
            results.append(df)
        try:
            merged_df = [df.set_index("_id") for df in results]
        except Exception as e:
            logging.error(e)
            raise e
            return results

        try:
            # Concatenate DataFrames using the generator
            merged_df = pd.concat(dataframe_generator(results), ignore_index=True)
            merged_df = merged_df.set_index("_id")
        except Exception as e:
            raise e

        return merged_df
    else:
        query = {
            "from": 0,
            "size": 10000,
            "query": {
                "bool": {
                    "filter": {"terms": {term_name: entered_list}},
                    "must": [{"query_string": {"query": search_string}}],
                }
            },
            "_source": fields_list,
        }
        df = cs.cogstack2df(query=query, index=index_name, column_headers=fields_list)
        return df


def set_index_safe_wrapper(df: pd.DataFrame) -> pd.DataFrame:
    """Safely attempts to set the DataFrame index to 'id', ignoring errors.

    This wrapper function tries to set the 'id' column as the index and
    returns the original DataFrame if it fails, logging a warning.

    Note:
        The current implementation has a bug where df.set_index("id") does not
        assign the result back to df. Consider using df = df.set_index("id")
        instead.

    Args:
        df: The pandas DataFrame to modify.

    Returns:
        pd.DataFrame: Either the DataFrame with 'id' set as index, or the
            original DataFrame if setting the index fails.
    """
    try:
        df.set_index("id")
        return df
    except Exception as e:
        logging.warning(f"Could not set index 'id': {e}")
        return df


def cohort_searcher_with_terms_no_search(
    index_name: str,
    fields_list: List[str],
    term_name: str,
    entered_list: List[str],
) -> pd.DataFrame:
    """Searches a cohort using only a term-level filter.

    This function performs an Elasticsearch search with only a terms filter
    and no query string. For large entered_list (>10000), it processes the
    list in chunks.

    Args:
        index_name: The name of the index to search.
        fields_list: A list of fields to return from each document.
        term_name: The field to filter on with exact value matching.
        entered_list: The list of values to search for in the `term_name` field.

    Returns:
        pd.DataFrame or List[pd.DataFrame]: A DataFrame containing the search
            results. For large lists (>=10000), returns a list of DataFrames
            where each chunk has been processed through set_index_safe_wrapper.
            When entry_list < 10000, returns a single DataFrame.

    Raises:
        Exception: Re-raises exceptions from Elasticsearch queries or dataframe
            operations during chunked processing.
    """
    global cs
    if cs is None:
        initialize_cogstack_client()
    if cs is None:
        logging.error("CogStack client is not initialized. Returning empty DataFrame.")
        return pd.DataFrame()
    if len(entered_list) >= 10000:
        results = []
        chunked_list = list_chunker(entered_list)
        for mini_list in chunked_list:
            query = {
                "from": 0,
                "size": 10000,
                "query": {"bool": {"filter": {"terms": {term_name: mini_list}}}},
                "_source": fields_list,
            }
            df = cs.cogstack2df(
                query=query, index=index_name, column_headers=fields_list
            )
            results.append(df)
        merged_df = [set_index_safe_wrapper(df) for df in results]
        return merged_df
    else:
        query = {
            "from": 0,
            "size": 10000,
            "query": {"bool": {"filter": {"terms": {term_name: entered_list}}}},
            "_source": fields_list,
        }
        df = cs.cogstack2df(query=query, index=index_name, column_headers=fields_list)
        return df


def cohort_searcher_no_terms(
    index_name: str, fields_list: List[str], search_string: str
) -> pd.DataFrame:
    """Searches an index using only a query string.

    This function performs a full-text search without any terms filter.

    Args:
        index_name: The name of the Elasticsearch index to search.
        fields_list: A list of fields to return from each document.
        search_string: The query string to use for the full-text search.

    Returns:
        pd.DataFrame: A DataFrame containing the search results. Empty
            DataFrame if CogStack client not initialized or no matches found.
    """
    global cs
    if cs is None:
        initialize_cogstack_client()
    if cs is None:
        logging.error("CogStack client is not initialized. Returning empty DataFrame.")
        return pd.DataFrame()
    query = {
        "from": 0,
        "size": 10000,
        "query": {"bool": {"must": [{"query_string": {"query": search_string}}]}},
        "_source": fields_list,
    }
    df = cs.cogstack2df(query=query, index=index_name, column_headers=fields_list)
    return df


def cohort_searcher_no_terms_fuzzy(
    index_name: str,
    fields_list: List[str],
    search_string: str,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
) -> pd.DataFrame:
    """Searches an index using different query string methods.

    Supports multiple search modes including fuzzy matching (for typos),
    exact term matching, and phrase matching with configurable word
    proximity.

    Args:
        index_name: The name of the Elasticsearch index to search.
        fields_list: List of fields to retrieve from each document.
        search_string: The search string to query.
        method: The search method. Options are 'fuzzy' (default), 'exact',
            or 'phrase'.
        fuzzy: The fuzziness level for fuzzy matching. Only applies when
            method='fuzzy'. Defaults to 2.
        slop: The slop value for phrase searches, controlling word proximity.
            Only applies when method='phrase'. Defaults to 1.

    Returns:
        pd.DataFrame: A DataFrame containing the search results. Empty
            DataFrame if CogStack client not initialized or no matches found.

    Raises:
        ValueError: If an invalid `method` is provided (not 'fuzzy', 'exact',
            or 'phrase').
    """
    global cs
    if cs is None:
        initialize_cogstack_client()
    if cs is None:
        logging.error("CogStack client is not initialized. Returning empty DataFrame.")
        return pd.DataFrame()
    if method == "fuzzy":
        # Fuzzy query
        query = {
            "from": 0,
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "fields": ["*"],  # Search across all fields by default
                                "query": search_string,
                                "fuzziness": fuzzy,  # Set fuzziness level
                            }
                        }
                    ]
                }
            },
            "_source": fields_list,
        }
    elif method == "exact":
        # Exact match query using keyword fields
        query = {
            "from": 0,
            "size": 10000,
            "query": {
                "term": {
                    f"{fields_list[0]}.keyword": search_string  # Exact match on the first field in the list
                }
            },
            "_source": fields_list,
        }
    elif method == "phrase":
        # Phrase match query with slop and fuzziness for typos
        query = {
            "from": 0,
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "_all": {  # Fuzzy matching to allow typos
                                    "query": search_string,
                                    "fuzziness": fuzzy,  # Allow typos
                                }
                            }
                        },
                        {
                            "match_phrase": {
                                "_all": {  # Ensure phrase-like behavior with word proximity
                                    "query": search_string,
                                    "slop": slop,  # Allow slight reordering of words
                                }
                            }
                        },
                    ]
                }
            },
            "_source": fields_list,
        }
    else:
        raise ValueError("Invalid method. Choose from 'fuzzy', 'exact', or 'phrase'.")

    # Execute the query and return the results as a DataFrame
    df = cs.cogstack2df(query=query, index=index_name, column_headers=fields_list)
    return df


def iterative_multi_term_cohort_searcher_no_terms_fuzzy(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = False,
    uuid_column_name: str = "client_idcode",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for EPR documents matching multiple search terms.

    This function performs a series of fuzzy searches across multiple search
    terms and combines the results. It reads/writes to CSV files to allow
    incremental data collection.

    Args:
        terms_list: The list of search terms to search for in document content.
        treatment_doc_filename: The name of the file to store the results in.
            If append=True and file exists, new results are appended.
        start_year: The start year of the date range (inclusive).
        start_month: The start month of the date range (1-12).
        start_day: The start day of the date range (1-31).
        end_year: The end year of the date range (inclusive).
        end_month: The end month of the date range (1-12).
        end_day: The end day of the date range (1-31).
        append: Whether to append results to an existing file. If False and
            file exists, overwrites the file. Defaults to True.
        debug: Whether to print debug logging information. Defaults to False.
        uuid_column_name: The name of the column containing patient identifiers.
            Defaults to 'client_idcode'.
        additional_filters: A list of additional filter strings to append to
            searches (e.g., "AND status:true").
        all_fields: If True, retrieves all available fields. If False, uses a
            default minimal field set. Defaults to False.
        method: The search method for fuzzy matching. Options are 'fuzzy',
            'exact', or 'phrase'. Defaults to 'fuzzy'.
        fuzzy: The fuzziness level for fuzzy matching (0-2 recommended).
            Defaults to 2.
        slop: The slop value for phrase searches, allowing adjacent word
            reordering. Defaults to 1.
        testing: If True, uses a dummy data generator instead of Elasticsearch.
            Defaults to False.
        testing_elastic: If True and testing=True, still uses the real ES (not
            dummy). Used for partial testing scenarios. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing all search results with an added
            'search_term' column indicating which term matched. Also saves to
            `treatment_doc_filename` if specified.
    """

    global cs
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    if cs is None:
        # Skip ES connection in testing mode to avoid credential errors and warnings
        if testing:
            from pat2vec.util.get_dummy_data_cohort_searcher import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            search_string = f"updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            return cohort_searcher_with_terms_and_search_dummy(
                index_name="epr_documents",
                fields_list=[
                    "client_idcode",
                    "updatetime",
                    "body_analysed",
                    "document_guid",
                    "document_description",
                ],
                term_name=uuid_column_name,
                entered_list=terms_list,
                global_start_day=start_day,
                global_end_day=end_day,
                search_string=search_string,
            )

        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            # Modify the search string for each term
            search_string = f'"{term}" AND updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]'

            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("search_string: %s", search_string)

            all_field_list = [
                "client_dob",
                "body_analysed",
                "client_firstname",
                "client_gendercode",
                "client_idcode",
                "clientvisit_currentlocation_analysed",
                "clientvisit_serviceguid",
                "document_dateadded",
                "document_description",
                "document_guid",
                "updatetime",
                # "_id",
                # "_index",
                # "_score",
                "client_applicsource",
                "client_build",
                "client_cityofbirth",
                "client_createdby",
                "client_createdwhen",
                "client_deceaseddtm",
                "client_displayname",
                "client_guid",
                "client_languagecode",
                "client_lastname",
                "client_maritalstatuscode",
                "client_middlename",
                "client_racecode",
                "client_religioncode",
                "client_siteid",
                "client_title",
                "client_touchedby",
                "client_touchedwhen",
                "client_universalnumber",
                "clientaddress_city",
                "clientaddress_line1",
                "clientaddress_line2",
                "clientaddress_line3",
                "clientaddress_postalcode",
                "clientaddress_typecode",
                "clientvisit_admitdtm",
                "clientvisit_applicsource",
                "clientvisit_build",
                "clientvisit_carelevelcode",
                "clientvisit_chartguid",
                "clientvisit_clientdisplayname_analysed",
                "clientvisit_closedtm",
                "clientvisit_createdby",
                "clientvisit_createdwhen",
                "clientvisit_currentlocationguid",
                "clientvisit_dischargedisposition",
                "clientvisit_dischargedtm",
                "clientvisit_dischargelocation",
                "clientvisit_guid",
                "clientvisit_idcode",
                "clientvisit_internalvisitstatus",
                "clientvisit_providerdisplayname_analysed",
                "clientvisit_siteid",
                "clientvisit_touchedby",
                "clientvisit_touchedwhen",
                "clientvisit_typecode",
                "clientvisit_visitidcode",
                "clientvisit_visitstatus",
                "clientvisit_visittypecarelevelguid",
                "document_clientguid",
                "document_clientvisitguid",
                "document_datecreated",
                "document_definitionguid",
                "document_filename",
                "documentoutput_doc_dob",
                "primarykeyfieldvalue",
            ]
            all_field_list = list(set(all_field_list))

            if all_fields:
                field_list = all_field_list
            else:
                field_list = "client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode".split()

            # method="fuzzy", fuzzy=2, slop=1
            # Perform the search
            term_docs = cohort_searcher_no_terms_fuzzy(
                index_name="epr_documents",
                fields_list=field_list,
                search_string=search_string,
                method=method,
                fuzzy=fuzzy,
                slop=slop,
            )

            term_docs["search_term"] = term

            if debug:
                logging.debug("%s: %d docs", term, len(term_docs))

            all_docs.append(term_docs)

        # Concatenate the results for all terms
        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)
            logging.info(f"Loaded existing data from: {treatment_doc_filename}")

            # Drop any duplicate columns before reindexing to avoid ValueError
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]

            # Align the columns by using the union of both the existing and new columns
            combined_columns = existing_data.columns.union(docs.columns)

            # Reindex both the existing data and new data to have the same columns
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)

            # Append the new data to the existing data
            docs = pd.concat([existing_data, docs], ignore_index=True)

        docs = docs.drop_duplicates().reset_index(drop=True)

        if treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"Data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )

    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "client_idcode",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for MCT documents matching multiple search terms.

    This function searches the 'observations' index for clinical summary
    documents (type: AoMRC_ClinicalSummary_FT) that contain specified terms.

    Args:
        terms_list: A list of terms to search for in document content.
        treatment_doc_filename: The filename to load or save the results.
        start_year, start_month, start_day: The start date range (inclusive).
        end_year, end_month, end_day: The end date range (inclusive).
        append: Whether to append results to an existing file. Defaults to True.
        debug: Whether to print debug logging information. Defaults to True.
        uuid_column_name: The name of the patient identifier column. Defaults
            to 'client_idcode'.
        additional_filters: Additional filter strings to append to queries.
        all_fields: If True, retrieves all available fields. Defaults to False.
        method: Search method ('fuzzy', 'exact', or 'phrase'). Defaults to 'fuzzy'.
        fuzzy: Fuzziness level for fuzzy search (0-2). Defaults to 2.
        slop: Slop value for phrase search word proximity. Defaults to 1.
        testing: If True, uses dummy data generator instead of ES. Defaults False.
        testing_elastic: If True and testing=True, still uses real ES. Defaults False.

    Returns:
        pd.DataFrame: A DataFrame containing the search results.
    """
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return (
            pd.DataFrame()
        )  # Ensure it returns an empty DataFrame if terms_list is empty

    global cs
    if cs is None:
        # Skip ES connection in testing mode to avoid credential errors and warnings
        if testing:
            from pat2vec.util.get_dummy_data_cohort_searcher import (
                cohort_searcher_with_terms_and_search_dummy,
            )

            search_string = f"updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            return cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=[
                    "client_idcode",
                    "updatetime",
                    "body_analysed",
                    "document_guid",
                    "document_description",
                ],
                term_name=uuid_column_name,
                entered_list=terms_list,
                global_start_day=start_day,
                global_end_day=end_day,
                search_string=search_string,
            )

        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs  # Ensure the function returns the loaded data

    else:
        if file_exists and append:
            docs_prev = pd.read_csv(treatment_doc_filename)
            logging.info(f"Loaded existing file and append: {treatment_doc_filename}")

        all_docs = []

        for term in tqdm(terms_list):
            # Modify the search string for each term

            search_string = f'obscatalogmasteritem_displayname:("AoMRC_ClinicalSummary_FT") AND observation_valuetext_analysed:("{term}") AND observationdocument_recordeddtm:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]'

            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            all_field_list = [
                "client_dob",
                "observation_valuetext_analysed",
                # "_id",
                "client_idcode",
                "clientvisit_admitdtm",
                "clientvisit_typecode",
                "obscatalogmasteritem_displayname",
                "observation_analysed",
                "observationdocument_displaysequence",
                "observationdocument_obsmasteritemguid",
                "observationdocument_recordeddtm",
                "scmobsfslistvalues_value_analysed",
                # "_index",
                # "_score",
                "client_applicsource",
                "client_build",
                "client_cityofbirth",
                "client_createdby",
                "client_createdwhen",
                "client_deceaseddtm",
                "client_displayname",
                "client_firstname",
                "client_gendercode",
                "client_guid",
                "client_languagecode",
                "client_lastname",
                "client_maritalstatuscode",
                "client_middlename",
                "client_racecode",
                "client_religioncode",
                "client_siteid",
                "client_title",
                "client_touchedby",
                "client_touchedwhen",
                "client_universalnumber",
                "clientaddress_city",
                "clientaddress_line1",
                "clientaddress_line2",
                "clientaddress_line3",
                "clientaddress_postalcode",
                "clientdocument_chartguid",
                "clientdocument_clientguid",
                "clientdocument_clientvisitguid",
                "clientvisit_applicsource",
                "clientvisit_build",
                "clientvisit_carelevelcode",
                "clientvisit_chartguid",
                "clientvisit_clientdisplayname_analysed",
                "clientvisit_closedtm",
                "clientvisit_createdby",
                "clientvisit_createdwhen",
                "clientvisit_currentlocation_analysed",
                "clientvisit_currentlocationguid",
                "clientvisit_dischargedisposition",
                "clientvisit_dischargedtm",
                "clientvisit_dischargelocation",
                "clientvisit_guid",
                "clientvisit_idcode",
                "clientvisit_internalvisitstatus",
                "clientvisit_planneddischargedtm",
                "clientvisit_providerdisplayname_analysed",
                "clientvisit_serviceguid",
                "clientvisit_siteid",
                "clientvisit_touchedby",
                "clientvisit_touchedwhen",
                "clientvisit_visitidcode",
                "clientvisit_visitstatus",
                "clientvisit_visittypecarelevelguid",
                "obscatalogmasteritem_calculationtype",
                "obscatalogmasteritem_datatype",
                "obscatalogmasteritem_fluidbalancetype",
                "obscatalogmasteritem_hasnumericequiv",
                "obscatalogmasteritem_includeintotals",
                "obscatalogmasteritem_isoutcome",
                "obscatalogmasteritem_numdecimalsout",
                "obscatalogmasteritem_showabsolutevalue",
                "obscatalogmasteritem_unitofmeasure",
                "obscatalogmasteritem_usenumericseparator",
                "observation_guid",
                "observation_isclientcharacteristic",
                "observation_isgenericitem",
                "observation_obsitemguid",
                "observation_recordedproviderguid",
                "observation_statustype",
                "observation_userguid",
                "observationdocument_active",
                "observationdocument_createdwhen",
                "observationdocument_entered",
                "observationdocument_hascomment",
                "observationdocument_historyseqnum",
                "observationdocument_obssetguid",
                "observationdocument_originalobsguid",
                "observationdocument_ownerguid",
                "observationdocument_ownertype",
                "observationdocument_siteid",
            ]

            all_field_list = list(set(all_field_list))

            if all_fields:
                field_list = all_field_list
            else:
                field_list = [
                    "observation_guid",
                    "client_idcode",
                    "obscatalogmasteritem_displayname",
                    "observation_valuetext_analysed",
                    "observationdocument_recordeddtm",
                    "clientvisit_visitidcode",
                ]

            if testing and not testing_elastic:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="observations",
                    fields_list=field_list,
                    term_name="client_idcode",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            else:

                # Perform the search
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="observations",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )

            # Check if term_docs is empty and log if necessary
            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        # If no documents were found for any term, return an empty DataFrame
        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if treatment_doc_filename and file_exists and append:
                docs_prev = pd.read_csv(treatment_doc_filename)
                logging.info(
                    f"Loaded existing file and no docs found: {treatment_doc_filename}"
                )
                return docs_prev  # Return docs from previous step
            else:
                return (
                    pd.DataFrame()
                )  # Return an empty DataFrame explicitly if nothing was found

        # Concatenate the results for all terms
        docs = pd.concat(all_docs, ignore_index=True)
        logging.info(f"Total documents found: {len(docs)}")

        # Drop duplicate rows
        docs = docs.drop_duplicates()

        if treatment_doc_filename and os.path.exists(treatment_doc_filename):
            # Load the existing CSV
            existing_data = pd.read_csv(treatment_doc_filename)
            logging.info(f"Loaded existing data from: {treatment_doc_filename}")

            # Drop any duplicate columns before reindexing to avoid ValueError
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]

            # Align the columns by using the union of both the existing and new columns
            combined_columns = existing_data.columns.union(docs.columns)

            # Reindex both the existing data and new data to have the same columns
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)

            # Append the new data to the existing data
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)

            # Save the updated data back to the CSV
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            # If the file does not exist, save the new data as a new CSV
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",  # Set backslash as escape character
                doublequote=True,  # Use double quotes to escape quotes
                encoding="utf-8",
            )  # Explicitly set encoding)
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )

    return docs  # Return the final docs DataFrame


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_imaging_reports(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "document_PatientDurableKey",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for Epic imaging reports documents matching multiple search terms.

    Searches the 'epic_imaging_reports' index for documents where `document_Content`
    contains the specified terms and renames fields to a standard schema.

    Args:
        terms_list: A list of terms to search for in document content.
        treatment_doc_filename: The filename to load or save results.
        start_year, start_month, start_day: Start date range (inclusive).
        end_year, end_month, end_day: End date range (inclusive).
        append: Whether to append to existing file. Defaults True.
        debug: Whether to print debug logging. Defaults True.
        uuid_column_name: Patient ID column name. Defaults 'document_PatientDurableKey'.
        additional_filters: Extra filter strings appended to queries.
        all_fields: If True, retrieves all fields. Defaults False.
        method: Search method ('fuzzy', 'exact', 'phrase'). Defaults 'fuzzy'.
        fuzzy: Fuzziness level (0-2). Defaults 2.
        slop: Slop value for phrase search. Defaults 1.
        testing: If True, uses dummy data generator. Defaults False.
        testing_elastic: If True and testing=True, still uses real ES. Defaults False.

    Returns:
        pd.DataFrame: A DataFrame with standard column names (renamed from Epic-specific fields).
    """
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_imaging_reports from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f"document_Content:({term}) AND "
                + f"document_CreatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            search_string = str(search_string)

            if additional_filters:  # This was incorrect, should be join
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "document_PatientDurableKey",
                "document_CreatedWhen",
                "document_Content",
                "document_Name",
                "document_AccessionNumber",
                "id",
                "_index",
                "_score",
            ]
            if all_fields:
                pass

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="epic_imaging_reports",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epic_imaging_reports",
                    fields_list=field_list,
                    term_name="document_PatientDurableKey",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if "document_PatientDurableKey" in docs.columns:
            docs.rename(
                columns={"document_PatientDurableKey": "client_idcode"}, inplace=True
            )
            if uuid_column_name == "document_PatientDurableKey":
                uuid_column_name = "client_idcode"
        if "document_CreatedWhen" in docs.columns:
            docs.rename(columns={"document_CreatedWhen": "updatetime"}, inplace=True)
        if "document_Content" in docs.columns:
            docs.rename(columns={"document_Content": "body_analysed"}, inplace=True)
        if "id" in docs.columns:
            docs.rename(columns={"id": "document_guid"}, inplace=True)
        if "document_Name" in docs.columns:
            docs.rename(columns={"document_Name": "document_description"}, inplace=True)

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)

            # Drop any duplicate columns before reindexing to avoid ValueError
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]

            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_medical_history(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "document_PatientDurableKey",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for Epic medical history documents matching multiple search terms.

    Searches the 'epic_medical_history' index for documents where `document_Diagnosis`
    or `document_Name` fields contain specified terms, with field renaming to standard schema.

    Args:
        terms_list: A list of terms to search for in document content.
        treatment_doc_filename: The filename to load or save results.
        start_year, start_month, start_day: Start date range (inclusive).
        end_year, end_month, end_day: End date range (inclusive).
        append: Whether to append to existing file. Defaults True.
        debug: Whether to print debug logging. Defaults True.
        uuid_column_name: Patient ID column name. Defaults 'document_PatientDurableKey'.
        additional_filters: Extra filter strings appended to queries.
        all_fields: If True, retrieves all fields. Defaults False.
        method: Search method ('fuzzy', 'exact', 'phrase'). Defaults 'fuzzy'.
        fuzzy: Fuzziness level (0-2). Defaults 2.
        slop: Slop value for phrase search. Defaults 1.
        testing: If True, uses dummy data generator. Defaults False.
        testing_elastic: If True and testing=True, still uses real ES. Defaults False.

    Returns:
        pd.DataFrame: A DataFrame with standard column names (renamed from Epic-specific fields).
    """
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_medical_history from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f"(document_Comment:({term}) OR document_Name:({term})) AND "
                + f"document_CreatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            search_string = str(search_string)

            if additional_filters:  # This was incorrect, should be join
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "document_PatientDurableKey",
                "document_CreatedWhen",
                "document_Diagnosis",
                "document_Name",
                "document_Comment",
                "id",
                "_index",
                "_score",
            ]
            if all_fields:
                pass

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="epic_medical_history",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epic_medical_history",
                    fields_list=field_list,
                    term_name="document_PatientDurableKey",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if "document_PatientDurableKey" in docs.columns:
            docs.rename(
                columns={"document_PatientDurableKey": "client_idcode"}, inplace=True
            )
            if uuid_column_name == "document_PatientDurableKey":
                uuid_column_name = "client_idcode"
        if "document_CreatedWhen" in docs.columns:
            docs.rename(columns={"document_CreatedWhen": "updatetime"}, inplace=True)
        if "document_Comment" in docs.columns:
            docs.rename(
                columns={"document_Comment": "body_analysed"}, inplace=True
            )  # Use comment as primary text
        if "id" in docs.columns:
            docs.rename(columns={"id": "document_guid"}, inplace=True)
        if "document_Name" in docs.columns:
            docs.rename(columns={"document_Name": "document_description"}, inplace=True)

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)

            # Drop any duplicate columns before reindexing to avoid ValueError
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]

            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_clinical_notes(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "document_PatientDurableKey",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for Epic clinical notes documents matching multiple search terms.

    Searches the 'epic_clinical_notes' index for documents where `document_Content`
    contains specified terms, with field renaming to standard schema.

    Args:
        terms_list: A list of terms to search for in document content.
        treatment_doc_filename: The filename to load or save results.
        start_year, start_month, start_day: Start date range (inclusive).
        end_year, end_month, end_day: End date range (inclusive).
        append: Whether to append to existing file. Defaults True.
        debug: Whether to print debug logging. Defaults True.
        uuid_column_name: Patient ID column name. Defaults 'document_PatientDurableKey'.
        additional_filters: Extra filter strings appended to queries.
        all_fields: If True, retrieves all fields. Defaults False.
        method: Search method ('fuzzy', 'exact', 'phrase'). Defaults 'fuzzy'.
        fuzzy: Fuzziness level (0-2). Defaults 2.
        slop: Slop value for phrase search. Defaults 1.
        testing: If True, uses dummy data generator. Defaults False.
        testing_elastic: If True and testing=True, still uses real ES. Defaults False.

    Returns:
        pd.DataFrame: A DataFrame with standard column names (renamed from Epic-specific fields).
    """
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_clinical_notes from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f"document_Content:({term}) AND "
                + f"document_CreatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            search_string = str(search_string)

            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "document_PatientDurableKey",
                "document_CreatedWhen",
                "document_Content",
                "document_Name",
                "document_EncounterEpicCsn",
                "id",
                "_index",
                "_score",
            ]
            if all_fields:
                pass

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="epic_clinical_notes",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epic_clinical_notes",
                    fields_list=field_list,
                    term_name="document_PatientDurableKey",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if "document_PatientDurableKey" in docs.columns:
            docs.rename(
                columns={"document_PatientDurableKey": "client_idcode"}, inplace=True
            )
            if uuid_column_name == "document_PatientDurableKey":
                uuid_column_name = "client_idcode"
        if "document_CreatedWhen" in docs.columns:
            docs.rename(columns={"document_CreatedWhen": "updatetime"}, inplace=True)
        if "document_Content" in docs.columns:
            docs.rename(columns={"document_Content": "body_analysed"}, inplace=True)
        if "id" in docs.columns:
            docs.rename(columns={"id": "document_guid"}, inplace=True)
        if "document_Name" in docs.columns:
            docs.rename(columns={"document_Name": "document_description"}, inplace=True)

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)

            # Drop any duplicate columns before reindexing to avoid ValueError
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]

            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_clinical_notes_appointments(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "document_PatientDurableKey",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for Epic clinical notes appointments documents matching multiple search terms.

    This function searches the 'epic_clinical_notes_appointments' index for documents where
    the `document_Content` field contains the specified terms.

    Args:
        terms_list: A list of terms to search for.
        treatment_doc_filename: The filename to load or save the results.
        start_year, start_month, start_day: The start of the date range.
        end_year, end_month, end_day: The end of the date range.
        append: Whether to append results to an existing file.
        debug: Whether to print debug information.
        uuid_column_name: The name of the UUID column.
        additional_filters: Additional filters to apply to the search.
        all_fields: Whether to retrieve all fields.
        method: The search method ('fuzzy', 'exact', 'phrase').
        fuzzy: The fuzziness level for fuzzy search.
        slop: The slop value for phrase search.
        testing: Whether to use a dummy searcher for testing.
        testing_elastic: If True, uses the real searcher against the configured ES
                         instance even if `testing` is True.

    Returns:
        A DataFrame containing the search results.
    """
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_clinical_notes_appointments from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f"document_Content:({term}) AND "
                + f"document_CreatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            search_string = str(search_string)

            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "document_PatientDurableKey",
                "document_CreatedWhen",
                "document_Content",
                "document_Name",
                "document_EncounterEpicCsn",
                "id",
                "_index",
                "_score",
            ]
            if all_fields:
                pass

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="epic_clinical_notes_appointments",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epic_clinical_notes_appointments",
                    fields_list=field_list,
                    term_name="document_PatientDurableKey",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if "document_PatientDurableKey" in docs.columns:
            docs.rename(
                columns={"document_PatientDurableKey": "client_idcode"}, inplace=True
            )
            if uuid_column_name == "document_PatientDurableKey":
                uuid_column_name = "client_idcode"
        if "document_CreatedWhen" in docs.columns:
            docs.rename(columns={"document_CreatedWhen": "updatetime"}, inplace=True)
        if "document_Content" in docs.columns:
            docs.rename(columns={"document_Content": "body_analysed"}, inplace=True)
        if "id" in docs.columns:
            docs.rename(columns={"id": "document_guid"}, inplace=True)
        if "document_Name" in docs.columns:
            docs.rename(columns={"document_Name": "document_description"}, inplace=True)

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)

            # Drop any duplicate columns before reindexing to avoid ValueError
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]

            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_drugs(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "client_idcode",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for drug order documents matching multiple search terms."""
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_drugs from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f'order_typecode:"medication" AND "{term}" AND '
                f"order_createdwhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "client_idcode",
                "order_guid",
                "order_name",
                "order_summaryline",
                "order_holdreasontext",
                "order_entered",
                "order_createdwhen",
                "clientvisit_visitidcode",
            ]
            if all_fields:
                pass  # Use all fields if requested

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="order",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="order",
                    fields_list=field_list,
                    term_name="client_idcode",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]
            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_diagnostics(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "client_idcode",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for diagnostic order documents matching multiple search terms."""
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_diagnostics from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f'order_typecode:"diagnostic" AND "{term}" AND '
                f"order_createdwhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "client_idcode",
                "order_guid",
                "order_name",
                "order_summaryline",
                "order_holdreasontext",
                "order_entered",
                "order_createdwhen",
                "clientvisit_visitidcode",
            ]
            if all_fields:
                pass  # Use all fields if requested

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="order",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="order",
                    fields_list=field_list,
                    term_name="client_idcode",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]
            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_reports(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "client_idcode",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for reports documents matching multiple search terms."""
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_reports from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f'body_analysed:("{term}") AND '
                f"updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "client_idcode",
                "document_guid",
                "document_description",
                "body_analysed",
                "updatetime",
                "clientvisit_visitidcode",
            ]
            if all_fields:
                pass  # Use all fields if requested

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="reports",  # Assuming 'reports' is the index name
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="reports",
                    fields_list=field_list,
                    term_name="client_idcode",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]
            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_encounters(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "activity_PatientDurableKey",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for Epic encounters documents matching multiple search terms."""
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_encounters from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f'(activity_Department:("{term}") OR activity_Type:("{term}") OR activity_VisitClass:("{term}")) AND '
                f"activity_AdmissionDate:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "activity_PatientDurableKey",
                "activity_AdmissionDate",
                "activity_DischargeDate",
                "activity_Department",
                "activity_Type",
                "activity_VisitClass",
                "activity_HospitalService",
                "id",
            ]
            if all_fields:
                pass  # Use all fields if requested

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="epic_encounters",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epic_encounters",
                    fields_list=field_list,
                    term_name="activity_PatientDurableKey",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if "activity_PatientDurableKey" in docs.columns:
            docs.rename(
                columns={"activity_PatientDurableKey": "client_idcode"}, inplace=True
            )
            if uuid_column_name == "activity_PatientDurableKey":
                uuid_column_name = "client_idcode"
        if "activity_AdmissionDate" in docs.columns:
            docs.rename(columns={"activity_AdmissionDate": "updatetime"}, inplace=True)

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]
            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_orders(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "document_PatientDurableKey",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for Epic orders documents matching multiple search terms."""
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_orders from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f'(document_Name:("{term}") OR document_Content:("{term}")) AND '
                f"document_OrderDate:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "document_PatientDurableKey",
                "document_CreatedWhen",
                "document_UpdatedWhen",
                "document_Name",
                "document_Content",
                "document_OrderClass",
                "document_OrderDate",
                "document_OrderStatus",
                "id",
            ]
            if all_fields:
                pass  # Use all fields if requested

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="epic_orders",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epic_orders",
                    fields_list=field_list,
                    term_name="document_PatientDurableKey",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if "document_PatientDurableKey" in docs.columns:
            docs.rename(
                columns={"document_PatientDurableKey": "client_idcode"}, inplace=True
            )
            if uuid_column_name == "document_PatientDurableKey":
                uuid_column_name = "client_idcode"
        if "document_CreatedWhen" in docs.columns:
            docs.rename(columns={"document_CreatedWhen": "updatetime"}, inplace=True)
        if "document_Content" in docs.columns:
            docs.rename(columns={"document_Content": "body_analysed"}, inplace=True)
        if "id" in docs.columns:
            docs.rename(columns={"id": "document_guid"}, inplace=True)
        if "document_Name" in docs.columns:
            docs.rename(columns={"document_Name": "document_description"}, inplace=True)

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]
            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_lab_results(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "document_PatientDurableKey",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for Epic lab results documents matching multiple search terms."""
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_epic_lab_results from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs

    else:
        all_docs = []

        for term in tqdm(terms_list):
            search_string = (
                f'(document_Name:("{term}") OR document_Fields.valueText:("{term}")) AND '
                f"document_CollectedDate:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
            )
            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            field_list = [
                "document_PatientDurableKey",
                "document_CreatedWhen",
                "document_CollectedDate",
                "document_UpdatedWhen",
                "document_Name",
                "document_Content",
                "document_AbnormalLevel",
                "document_LabResultEpicId",
                "document_Fields.valueText",
                "id",
            ]
            if all_fields:
                pass  # Use all fields if requested

            if not testing or (testing and testing_elastic):
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="epic_lab_results",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:
                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epic_lab_results",
                    fields_list=field_list,
                    term_name="document_PatientDurableKey",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if file_exists and append:
                return pd.read_csv(treatment_doc_filename)
            return pd.DataFrame()

        docs = pd.concat(all_docs, ignore_index=True)
        docs = docs.drop_duplicates()

        if "document_PatientDurableKey" in docs.columns:
            docs.rename(
                columns={"document_PatientDurableKey": "client_idcode"}, inplace=True
            )
            if uuid_column_name == "document_PatientDurableKey":
                uuid_column_name = "client_idcode"
        if "document_CollectedDate" in docs.columns:
            docs.rename(columns={"document_CollectedDate": "updatetime"}, inplace=True)
        if "document_Content" in docs.columns:
            docs.rename(columns={"document_Content": "body_analysed"}, inplace=True)
        if "id" in docs.columns:
            docs.rename(columns={"id": "document_guid"}, inplace=True)
        if "document_Name" in docs.columns:
            docs.rename(columns={"document_Name": "document_description"}, inplace=True)

        if treatment_doc_filename and os.path.exists(treatment_doc_filename) and append:
            existing_data = pd.read_csv(treatment_doc_filename)
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]
            combined_columns = existing_data.columns.union(docs.columns)
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",
                doublequote=True,
                encoding="utf-8",
            )
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )
    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_obs(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "client_idcode",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches the general 'observations' index for multiple terms."""
    logging.info("Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_obs")
    if not terms_list:
        return pd.DataFrame()

    global cs
    if cs is None:
        initialize_cogstack_client()

    all_docs = []
    for term in tqdm(terms_list):
        search_string = (
            f'obscatalogmasteritem_displayname:("{term}") AND '
            f"observationdocument_recordeddtm:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
        )
        if additional_filters:
            search_string += " " + " ".join(additional_filters)

        field_list = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
        ]

        if not testing or (testing and testing_elastic):
            term_docs = cohort_searcher_no_terms_fuzzy(
                index_name="observations",
                fields_list=field_list,
                search_string=search_string,
                method=method,
                fuzzy=fuzzy,
                slop=slop,
            )
        else:
            term_docs = cohort_searcher_with_terms_and_search_dummy(
                index_name="observations",
                fields_list=field_list,
                term_name="client_idcode",
                entered_list=generate_uuid_list(5, "P"),
                search_string=search_string,
            )

        if not term_docs.empty:
            term_docs["search_term"] = term
            all_docs.append(term_docs)

    if not all_docs:
        return pd.DataFrame()

    docs = pd.concat(all_docs, ignore_index=True).drop_duplicates()
    if treatment_doc_filename:
        docs.to_csv(
            treatment_doc_filename,
            mode="a",
            index=False,
            header=not os.path.exists(treatment_doc_filename),
        )

    return docs


def initialize_cogstack_client(config_obj=None):
    """Initializes the global CogStack Elasticsearch client instance.

    Loads credentials from multiple sources with a fallback chain:
    1. If config_obj has credentials_path attribute, loads from that file
    2. Falls back to default 'credentials.py' in project root
    3. Creates template credentials file and retries if possible
    4. Returns None if all credential loading attempts fail

    Args:
        config_obj: An optional configuration object that may have a
            `credentials_path` attribute specifying the path to the
            credentials file.

    Returns:
        CogStack or None: The initialized CogStack client instance, or None
            if credentials could not be loaded or connection failed.
    """
    global cs

    credentials_path = None
    if (
        config_obj
        and hasattr(config_obj, "credentials_path")
        and config_obj.credentials_path
    ):
        credentials_path = config_obj.credentials_path

    # If cs is already initialized and no new path is given, do nothing.
    if cs is not None and not credentials_path:
        return cs

    creds = {}
    if credentials_path:
        try:
            spec = importlib.util.spec_from_file_location(
                "credentials", credentials_path
            )
            credentials_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(credentials_module)
            creds["username"] = getattr(credentials_module, "username", None)
            creds["password"] = getattr(credentials_module, "password", None)
            creds["api_key"] = getattr(credentials_module, "api_key", None)
            creds["hosts"] = getattr(credentials_module, "hosts", [])
            logging.info(f"Loaded credentials from: {credentials_path}")
        except (ImportError, FileNotFoundError, TypeError) as e:
            logging.warning(
                "Could not load credentials from %s. Error: %s. Attempting to create one.",
                credentials_path,
                e,
            )
            credentials_path = None  # Force fallback

    if not creds:
        try:
            from credentials import username, password, api_key, hosts

            creds = {
                "username": username,
                "password": password,
                "api_key": api_key,
                "hosts": hosts,
            }
        except ImportError:
            logging.warning("No credentials file found. Attempting to create one.")
            try:
                create_credentials_file()
                from credentials import username, password, api_key, hosts

                importlib.reload(sys.modules["credentials"])
                creds = {
                    "username": username,
                    "password": password,
                    "api_key": api_key,
                    "hosts": hosts,
                }
            except (PermissionError, OSError, ImportError):
                logging.warning(
                    "Failed to import credentials after creation. CogStack client will not be initialized."
                )
                return None

    logging.info("Initializing CogStack client...")
    logging.info(f"Username: {creds.get('username')}")

    if creds.get("api_key"):
        logging.info("Using API key authentication")
        cs = CogStack(creds["hosts"], api_key=creds["api_key"], api=True)
    else:
        logging.info(f"Using basic authentication, username: {creds.get('username')}")
        cs = CogStack(
            creds["hosts"], creds.get("username"), creds.get("password"), api=False
        )

    try:
        cs.elastic.info()
        logging.info("CogStack connection successful.")
    except Exception as e:
        logging.error(f"CogStack connection failed: {e}")

    return cs


def check_patients_existence(
    patient_ids: List[str],
    index_name: Union[str, List[Tuple[str, str]]] = "epr_documents",
    id_field: str = "client_idcode.keyword",
    config_obj: Optional[Any] = None,
) -> List[str]:
    """Checks which patient IDs exist in Elasticsearch using terms aggregation.

    Performs efficient existence checks by batching patient IDs and using
    either terms aggregations (preferred) or fallback search queries if
    the field doesn't support aggregations.

    Args:
        patient_ids: A list of patient identifiers to check for existence.
        index_name: The name of the index, or a list of tuples (index_name,
            id_field) for checking multiple indices with fallback behavior.
            Defaults to 'epr_documents'.
        id_field: The field name containing patient IDs in the index. Only
            used if index_name is a string. Defaults to 'client_idcode.keyword'.
        config_obj: An optional configuration object to initialize CogStack
            client. Can have testing/testing_elastic attributes.

    Returns:
        List[str]: A list of patient IDs that exist in the specified indices.
    """
    # Bypassing ES check during non-elastic testing to allow dummy data generators to work.
    # This fixes the "invalid codes" warning and prevents patient filtering in tests.
    if (
        config_obj
        and getattr(config_obj, "testing", False)
        and not getattr(config_obj, "testing_elastic", False)
    ):
        return patient_ids

    global cs
    # Always attempt to initialize/update cs if config is provided.
    cs = initialize_cogstack_client(config_obj)

    if cs is None:
        logging.error(
            "Failed to initialize CogStack client for patient existence check."
        )
        return []

    # Normalize input to list of configs
    if isinstance(index_name, str):
        indices_to_check = [(index_name, id_field)]
    else:
        # index_name is already a list of tuples
        indices_to_check = index_name

    existing_ids = set()
    chunk_size = 1000  # Safe chunk size for terms query

    # Remove duplicates from input to avoid redundant checks
    unique_ids = list(set(patient_ids))
    ids_to_check = set(unique_ids)

    for idx_name, idx_field in indices_to_check:
        if not ids_to_check:
            break

        current_batch_list = list(ids_to_check)
        logging.info(
            f"Checking existence for {len(current_batch_list)} patients in index '{idx_name}' using field '{idx_field}'..."
        )

        for i in range(0, len(current_batch_list), chunk_size):
            if not ids_to_check:
                break
            chunk = current_batch_list[i : i + chunk_size]
            body = {
                "query": {"terms": {idx_field: chunk}},
                "size": 0,
                "aggs": {
                    "existing_ids": {
                        "terms": {"field": idx_field, "size": len(chunk) + 50}
                    }
                },
            }
            try:
                res = cs.elastic.search(index=idx_name, body=body, request_timeout=60)
                buckets = (
                    res.get("aggregations", {})
                    .get("existing_ids", {})
                    .get("buckets", [])
                )
                for bucket in buckets:
                    found_id = str(bucket["key"])
                    existing_ids.add(found_id)
                    if found_id in ids_to_check:
                        ids_to_check.remove(found_id)
            except Exception as e:
                # Handle fielddata error for text fields where aggregations are disabled.
                if "fielddata" in str(e).lower():
                    try:
                        # Fallback: search and retrieve field from _source instead of aggregation
                        search_res = cs.elastic.search(
                            index=idx_name,
                            body={"query": {"terms": {idx_field: chunk}}, "size": 1000},
                            _source=[idx_field],
                            request_timeout=60,
                        )
                        for hit in search_res.get("hits", {}).get("hits", []):
                            val = hit.get("_source", {}).get(idx_field)
                            if val:
                                found_id = str(val)
                                existing_ids.add(found_id)
                                if found_id in ids_to_check:
                                    ids_to_check.remove(found_id)
                    except Exception as e_inner:
                        logging.error(
                            f"Fallback existence check failed for {idx_name}: {e_inner}"
                        )
                else:
                    logging.error(
                        f"Error checking patient existence for chunk in {idx_name}: {e}"
                    )

    return list(existing_ids)


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs(
    terms_list: List[str],
    treatment_doc_filename: str,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    append: bool = True,
    debug: bool = True,
    uuid_column_name: str = "client_idcode",
    bloods_time_field: str = "basicobs_entered",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
    testing: bool = False,
    testing_elastic: bool = False,
) -> pd.DataFrame:
    """Iteratively searches for textual observations matching multiple terms.

    Searches the 'basic_observations' index for documents where the `textualObs`
    field contains specified terms and applies post-filtering to remove empty
    or null text values.

    Args:
        terms_list: A list of terms to search for in textual content.
        treatment_doc_filename: The filename to load or save results.
        start_year, start_month, start_day: Start date range (inclusive).
        end_year, end_month, end_day: End date range (inclusive).
        append: Whether to append to existing file. Defaults True.
        debug: Whether to print debug logging. Defaults True.
        uuid_column_name: Patient ID column name. Defaults 'client_idcode'.
        bloods_time_field: Timestamp field for date filtering. Defaults
            'basicobs_entered'.
        additional_filters: Extra filter strings appended to queries.
        all_fields: If True, return all fields. Defaults False.
        method: Search method ('fuzzy', 'exact', 'phrase'). Defaults 'fuzzy'.
        fuzzy: Fuzziness level for fuzzy search (0-2). Defaults 2.
        slop: Slop value for phrase searches. Defaults 1.
        testing: If True, use dummy data generator. Defaults False.
        testing_elastic: If True and testing=True, still use real ES. Defaults False.

    Returns:
        pd.DataFrame: A DataFrame with search results including a 'body_analysed'
            column derived from 'textualObs'.
    """
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs from %s-%s-%s to %s-%s-%s",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return (
            pd.DataFrame()
        )  # Ensure it returns an empty DataFrame if terms_list is empty

    global cs
    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename) if treatment_doc_filename else False

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs  # Ensure the function returns the loaded data

    else:
        if file_exists and append:
            docs_prev = pd.read_csv(treatment_doc_filename)
            logging.info(f"Loaded existing file and append: {treatment_doc_filename}")

        all_docs = []

        for term in tqdm(terms_list):
            # Modify the search string for each term

            search_string = (
                f"{bloods_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
            )

            search_string = f"textualObs:({term})"

            search_string = (
                f"textualObs:({term}) AND "
                + f"{bloods_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
            )
            search_string = str(search_string)

            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            logging.info("Search String: %s", search_string)

            all_field_list = [
                "client_dob",
                "basicobs_createdwhen",
                "basicobs_entered",
                "basicobs_guid",
                "basicobs_itemname_analysed",
                "basicobs_masterguid",
                "basicobs_orderguid",
                "basicobs_value_analysed",
                "basicobs_value_numeric",
                "client_idcode",
                "textualObs",
                # "_id",
                # "_index",
                # "_score",
                "basicobs_abnormalitycode",
                "basicobs_arrivaldtm",
                "basicobs_build",
                "basicobs_chartguid",
                "basicobs_createdby",
                "basicobs_referencelowerlimit",
                "basicobs_referenceupperlimit",
                "basicobs_resultitemguid",
                "basicobs_siteid",
                "basicobs_touchedby",
                "basicobs_touchedwhen",
                "basicobs_typecode",
                "basicobs_unitofmeasure",
                "client_applicsource",
                "client_build",
                "client_cityofbirth",
                "client_createdby",
                "client_createdwhen",
                "client_deceaseddtm",
                "client_displayname",
                "client_firstname",
                "client_gendercode",
                "client_guid",
                "client_languagecode",
                "client_lastname",
                "client_maritalstatuscode",
                "client_middlename",
                "client_racecode",
                "client_religioncode",
                "client_siteid",
                "client_title",
                "client_touchedby",
                "client_touchedwhen",
                "client_universalnumber",
                "clientvisit_admitdtm",
                "clientvisit_applicsource",
                "clientvisit_build",
                "clientvisit_carelevelcode",
                "clientvisit_chartguid",
                "clientvisit_clientdisplayname_analysed",
                "clientvisit_closedtm",
                "clientvisit_createdby",
                "clientvisit_createdwhen",
                "clientvisit_currentlocation_analysed",
                "clientvisit_currentlocationguid",
                "clientvisit_dischargedisposition",
                "clientvisit_dischargedtm",
                "clientvisit_dischargelocation",
                "clientvisit_guid",
                "clientvisit_idcode",
                "clientvisit_internalvisitstatus",
                "clientvisit_planneddischargedtm",
                "clientvisit_providerdisplayname_analysed",
                "clientvisit_serviceguid",
                "clientvisit_siteid",
                "clientvisit_touchedby",
                "clientvisit_touchedwhen",
                "clientvisit_typecode",
                "clientvisit_visitidcode",
                "clientvisit_visitstatus",
                "clientvisit_visittypecarelevelguid",
                "document_age",
                "updatetime",
            ]

            all_field_list = list(set(all_field_list))

            if all_fields:
                field_list = all_field_list
            else:
                field_list = [
                    "client_idcode",
                    "basicobs_itemname_analysed",
                    "basicobs_value_numeric",
                    "basicobs_value_analysed",
                    "basicobs_entered",
                    "clientvisit_serviceguid",
                    "basicobs_guid",
                    "updatetime",
                    "textualObs",
                ]

            if not testing or (testing and testing_elastic):
                # Perform the search
                term_docs = cohort_searcher_no_terms_fuzzy(
                    index_name="basic_observations",
                    fields_list=field_list,
                    search_string=search_string,
                    method=method,
                    fuzzy=fuzzy,
                    slop=slop,
                )
            else:

                term_docs = cohort_searcher_with_terms_and_search_dummy(
                    index_name="basic_observations",
                    fields_list=field_list,
                    term_name="client_idcode",
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )

            # Check if term_docs is empty and log if necessary
            if term_docs is None or term_docs.empty:
                logging.info(f"No results found for term: {term}")
            else:
                logging.info(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        # If no documents were found for any term, return an empty DataFrame
        if not all_docs:
            logging.warning("No documents were found for any of the terms.")
            if treatment_doc_filename and file_exists:
                docs_prev = pd.read_csv(treatment_doc_filename)
                logging.info(
                    f"Loaded existing file and no docs found: {treatment_doc_filename}"
                )
                return docs_prev  # Return docs from previous step
            else:
                return (
                    pd.DataFrame()
                )  # Return an empty DataFrame explicitly if nothing was found

        # Concatenate the results for all terms
        docs = pd.concat(all_docs, ignore_index=True)
        logging.info(f"Total documents found: {len(docs)}")

        # Drop duplicate rows
        docs = docs.drop_duplicates()

        # Handle textual obs filtering

        # Drop rows with no textualObs
        docs = docs.dropna(subset=["textualObs"])

        # Drop rows with empty string in textualObs
        docs = docs[docs["textualObs"] != ""]

        docs["body_analysed"] = docs["textualObs"].astype(str)

        if treatment_doc_filename and os.path.exists(treatment_doc_filename):
            # Load the existing CSV
            existing_data = pd.read_csv(treatment_doc_filename)
            logging.info(f"Loaded existing data from: {treatment_doc_filename}")

            # Drop any duplicate columns before reindexing to avoid ValueError
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]

            # Align the columns by using the union of both the existing and new columns
            combined_columns = existing_data.columns.union(docs.columns)

            # Reindex both the existing data and new data to have the same columns
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)

            # Append the new data to the existing data
            docs = pd.concat([existing_data, docs], ignore_index=True)
            docs = docs.drop_duplicates().reset_index(drop=True)

            # Save the updated data back to the CSV
            docs.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        elif treatment_doc_filename:
            # If the file does not exist, save the new data as a new CSV
            docs.to_csv(
                treatment_doc_filename,
                mode="w",
                index=False,
                escapechar="\\",  # Set backslash as escape character
                doublequote=True,  # Use double quotes to escape quotes
                encoding="utf-8",
            )  # Explicitly set encoding))
            logging.info(f"New data saved to: {treatment_doc_filename}")

        if debug:
            logging.debug(
                "n_unique %s: %d/%d",
                uuid_column_name,
                len(docs[uuid_column_name].unique()),
                len(docs),
            )

    return docs  # Return the final docs DataFrame
