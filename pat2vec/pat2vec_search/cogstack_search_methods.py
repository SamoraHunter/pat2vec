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
from typing import Any, Dict, Generator, List, Optional, Tuple

import getpass

from pat2vec.util.get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
    generate_uuid_list,
)

import random
import warnings
import logging

warnings.filterwarnings("ignore")

# Suppress Elasticsearch logger
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# add one level up to path with sys.path for importing actual credentials

random_state = 42
random.seed(random_state)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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

        Args:
            hosts: A list of CogStack host URLs.
            username: The username for basic authentication.
            password: The password for basic authentication.
            api: If True, use API key authentication. Defaults to True.
            api_key: The API key for authentication.
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
        """Prompts for API credentials if they are not provided."""
        if api_username is None:
            api_username = input("API Username: ")
        if api_password is None:
            api_password = getpass.getpass("API Password: ")
        return api_username, api_password  # type: ignore

    def _check_auth_details(
        self, username: Optional[str] = None, password: Optional[str] = None
    ) -> Tuple[str, str]:
        """Prompts for basic authentication credentials if they are not provided."""
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
            es_gen_size: The number of documents to retrieve per shard in each scroll.
            request_timeout: The timeout in seconds for the request.

        Returns:
            A generator object that yields search hits.
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

        Args:
            query: The Elasticsearch query dictionary.
            index: The name of the index or a list of indices to search.
            column_headers: A specific list of columns for the DataFrame.
            es_gen_size: The number of documents per scroll request.
            request_timeout: The timeout in seconds for the request.

        Returns:
            A pandas DataFrame containing the search results.
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

    def DataFrame(self, index: str) -> ed.DataFrame:
        """Returns an Eland DataFrame for the specified index.

        Eland provides a pandas-like API for data in Elasticsearch.

        Args:
            index: The name of the index or index pattern.

        Returns:
            An Eland DataFrame object.
        """
        return ed.DataFrame(es_client=self.elastic, es_index_pattern=index)


def list_chunker(entered_list: List[Any]) -> List[List[Any]]:
    """Splits a list into smaller chunks of up to 10,000 elements.

    Args:
        entered_list: The list to be split into chunks.

    Returns:
        A list of lists, where each sublist is a chunk of the original list.
    """
    if len(entered_list) >= 10000:
        chunks = [
            entered_list[x : x + 10000] for x in range(0, len(entered_list), 10000)
        ]
    return chunks


def dataframe_generator(
    list_of_dfs: List[pd.DataFrame],
) -> Generator[pd.DataFrame, None, None]:
    """A generator that yields DataFrames from a list of DataFrames."""
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

    Args:
        index_name: The name of the Elasticsearch index to search.
        fields_list: The list of fields to return from each document.
        term_name: The name of the field to use for the term-level filter.
        entered_list: The list of values to filter for in the `term_name` field.
        search_string: The query string to apply to the search.

    Returns:
        A pandas DataFrame containing the search results.
    """
    if cs is None:
        initialize_cogstack_client()
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
    """Safely sets the DataFrame index to 'id', ignoring errors."""
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

    Args:
        index_name: The name of the index to search.
        fields_list: A list of fields to return.
        term_name: The field to filter on.
        entered_list: The list of values to search for in the `term_name` field.

    Returns:
        A pandas DataFrame containing the search results.
    """
    if cs is None:
        initialize_cogstack_client()
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

    Args:
        index_name: The name of the Elasticsearch index to search.
        fields_list: A list of fields to return.
        search_string: The query string to use for the search.

    Returns:
        A pandas DataFrame containing the search results.
    """
    if cs is None:
        initialize_cogstack_client()
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

    Args:
        index_name: The name of the Elasticsearch index.
        fields_list: List of fields to retrieve.
        search_string: The search string to query.
        method: The search method ("fuzzy", "exact", or "phrase").
        fuzzy: The fuzziness level for fuzzy matching.
        slop: The slop value for phrase searches (word proximity).

    Returns:
        A DataFrame containing the search results.
    """
    if cs is None:
        initialize_cogstack_client()
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
    overwrite: bool = True,
    debug: bool = False,
    uuid_column_name: str = "client_idcode",
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
) -> pd.DataFrame:
    """Iteratively searches for EPR documents matching multiple search terms.

    Args:
        terms_list: The list of search terms to search for.
        treatment_doc_filename: The name of the file to store the results in.
        start_year: The start year of the date range.
        start_month: The start month of the date range.
        start_day: The start day of the date range.
        end_year: The end year of the date range.
        end_month: The end month of the date range.
        end_day: The end day of the date range.
        overwrite: Whether to overwrite the existing file.
        debug: Whether to print debug information.
        uuid_column_name: The name of the column containing the UUIDs.
        additional_filters: A list of additional filters to apply.
        all_fields: Whether to retrieve all fields.
        method: The search method to use ('fuzzy', 'exact', or 'phrase').
        fuzzy: The fuzziness level for fuzzy matching.
        slop: The slop value for phrase searches.

    Returns:
        A DataFrame containing the search results.
    """
    if not terms_list:
        logging.warning("Terms list is empty. Exiting.")
        return

    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename)

    if file_exists and not overwrite:
        logging.info(
            f"file_exists and not overwrite, reading docs from {treatment_doc_filename}"
        )
        docs = pd.read_csv(treatment_doc_filename)
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

            if all_fields == True:
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
        docs = pd.concat(all_docs, ignore_index=True)

        docs = docs.drop_duplicates()

        # Save the results to a temporary CSV
        docs.to_csv(
            treatment_doc_filename,
            index=False,
            escapechar="\\",  # Set backslash as escape character
            doublequote=True,  # Use double quotes to escape quotes
            encoding="utf-8",
        )  # Explicitly set encoding)
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
) -> pd.DataFrame:
    """Iteratively searches for MCT documents matching multiple search terms.

    This function searches the 'observations' index for documents of type
    'AoMRC_ClinicalSummary_FT' that contain the specified terms.

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

    Returns:
        A DataFrame containing the search results.
    """
    logging.info(
        "Running iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct from %s-%s-%s to %s-%s-%s",
        start_day,
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

    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename)

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs  # Ensure the function returns the loaded data

    else:
        if file_exists and not append:
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

            if all_fields == True:
                field_list = all_field_list
            else:
                field_list = (
                    """observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split()
                )

            if testing == True:
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
            docs_prev = pd.read_csv(treatment_doc_filename)
            logging.info(
                f"Loaded existing file and no docs found: {treatment_doc_filename}"
            )

            return docs_prev  # Return docs from previous step
            # return pd.DataFrame()  # Return an empty DataFrame explicitly if nothing was found

        # Concatenate the results for all terms
        docs = pd.concat(all_docs, ignore_index=True)
        logging.info(f"Total documents found: {len(docs)}")

        # Drop duplicate rows
        docs = docs.drop_duplicates()

        if os.path.exists(treatment_doc_filename):
            # Load the existing CSV
            existing_data = pd.read_csv(treatment_doc_filename)
            logging.info(f"Loaded existing data from: {treatment_doc_filename}")

            # Align the columns by using the union of both the existing and new columns
            combined_columns = existing_data.columns.union(docs.columns)

            # Reindex both the existing data and new data to have the same columns
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)

            # Drop any duplicate columns (if any)
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]

            # Append the new data to the existing data
            updated_data = pd.concat([existing_data, docs], ignore_index=True)

            # Save the updated data back to the CSV
            updated_data.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        else:
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


cs = None


def initialize_cogstack_client(config_obj=None):
    """Initializes the global CogStack client `cs`.

    This function sets up the connection to Elasticsearch. It can be
    configured to load credentials from a specific file path by passing a
    config object. If a client instance already exists, it will not
    re-initialize unless a config object with a new credentials path is
    provided.

    The credential loading priority is:
    1. `credentials_path` from the `config_obj`.
    2. Default `credentials.py` in the project's root.
    3. If not found, it creates a template `credentials.py` and tries again.
    4. Falls back to dummy credentials if all else fails.

    Args:
        config_obj: A configuration object that may have a
            `credentials_path` attribute.

    Returns:
        The initialized CogStack client instance.
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
        return

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
                "Could not load credentials from %s. Error: %s. Falling back to default.",
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
            create_credentials_file()
            try:
                from credentials import username, password, api_key, hosts

                creds = {
                    "username": username,
                    "password": password,
                    "api_key": api_key,
                    "hosts": hosts,
                }
            except ImportError:
                logging.warning(
                    "Still no credentials file found. Using dummy credentials."
                )
                creds = {
                    "username": "dummy_user",
                    "password": "dummy_password",
                    "api_key": "",
                    "hosts": ["https://your-actual-elasticsearch-host:9200"],
                }

    logging.info("Imported cogstack_v8_lite from pat2vec.util .")
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
) -> pd.DataFrame:
    """Iteratively searches for textual observations matching multiple terms.

    This function searches the 'basic_observations' index for documents where
    the `textualObs` field contains the specified terms.

    Args:
        terms_list: A list of terms to search for.
        treatment_doc_filename: The filename to load or save the results.
        start_year, start_month, start_day: The start of the date range.
        end_year, end_month, end_day: The end of the date range.
        append: Whether to append results to an existing file.
        debug: Whether to print debug information.
        uuid_column_name: The name of the UUID column.
        bloods_time_field: The timestamp field to use for date filtering.
        additional_filters: Additional filters to apply to the search.
        all_fields: Whether to retrieve all fields.
        method: The search method ('fuzzy', 'exact', 'phrase').
        fuzzy: The fuzziness level for fuzzy search.
        slop: The slop value for phrase search.
        testing: Whether to use a dummy searcher for testing.

    Returns:
        A DataFrame containing the search results.
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

    if cs is None:
        initialize_cogstack_client()
    file_exists = exists(treatment_doc_filename)

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        logging.info(f"Loaded existing file: {treatment_doc_filename}")
        return docs  # Ensure the function returns the loaded data

    else:
        if file_exists and not append:
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

            if all_fields == True:
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

            if testing == False:
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
            docs_prev = pd.read_csv(treatment_doc_filename)
            logging.info(
                f"Loaded existing file and no docs found: {treatment_doc_filename}"
            )

            return docs_prev  # Return docs from previous step
            # return pd.DataFrame()  # Return an empty DataFrame explicitly if nothing was found

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

        if os.path.exists(treatment_doc_filename):
            # Load the existing CSV
            existing_data = pd.read_csv(treatment_doc_filename)
            logging.info(f"Loaded existing data from: {treatment_doc_filename}")

            # Align the columns by using the union of both the existing and new columns
            combined_columns = existing_data.columns.union(docs.columns)

            # Reindex both the existing data and new data to have the same columns
            existing_data = existing_data.reindex(columns=combined_columns)
            docs = docs.reindex(columns=combined_columns)

            # Drop any duplicate columns (if any)
            existing_data = existing_data.loc[:, ~existing_data.columns.duplicated()]
            docs = docs.loc[:, ~docs.columns.duplicated()]

            # Append the new data to the existing data
            updated_data = pd.concat([existing_data, docs], ignore_index=True)

            # Save the updated data back to the CSV
            updated_data.to_csv(treatment_doc_filename, index=False)
            logging.info(f"Updated data saved to: {treatment_doc_filename}")
        else:
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
