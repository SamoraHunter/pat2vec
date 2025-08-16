import getpass
import random
import warnings
from typing import Dict, List
import eland as ed
import elasticsearch
import elasticsearch.helpers
import pandas as pd
from tqdm.notebook import tqdm


warnings.filterwarnings("ignore")
from os.path import exists
import os
from pathlib import Path

# add one level up to path with sys.path for importing actual credentials
import sys

random_state = 42
random.seed(random_state)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
    generate_uuid_list,
)


def create_credentials_file():
    # Define the path to the credentials file (two levels up)

    """
    Creates a credentials file for Elasticsearch configuration.

    This function defines the path for the credentials file, creates the
    necessary directory if it doesn't exist, and writes default content
    for Elasticsearch credentials into the file. The content includes
    placeholders for Elasticsearch hosts, username, password, and API key.
    After creating the file, it appends the directory to the system path
    for module import purposes.

    Note:
    - The function assumes a specific directory structure and creates
      directories as necessary up to three levels above the current file.
    - The credentials file is created with default content that should be
      updated with actual credentials by the user.
    - The directory name 'gloabl_files' is used.

    Prints:
    - Confirmation message with the path to the created credentials file
      and a prompt to update it with actual credentials.
    """

    base_dir = (
        Path(__file__).resolve().parent.parent.parent.parent
    )  # Go up three levels
    credentials_dir = base_dir / "gloabl_files"  # Note: Spelling matches your example
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

    sys.path.append(credentials_dir)

    print(f"Credentials file created at: {credentials_file}")
    print("Please update the file with your actual credentials.")


try:
    from credentials import *
except ImportError as e:
    print(e)
    print(
        "WARNING: No credentials file found, place credentials in gloabl_files/credentials.py"
    )
    # Run the routine
    create_credentials_file()
    try:
        from credentials import *
    except ImportError as e:
        print(e)
        print(
            "WARNING: No credentials file found, place credentials in gloabl_files/credentials.py"
        )
        username = "dummy username"
        password = "dummy password"
        api_key = ""
        hosts = ["https://your-actual-elasticsearch-host:9200"]


print(f"Imported cogstack_v8_lite from pat2vec.util .")
print(f"Username: %s" % username)


class CogStack(object):
    print("refreshed .")
    """
    :param hosts: List of CogStack host names
    :param username: basic_auth username
    :param password: basic_auth password
    :param api_username: api username
    :param api_password: api password
    :param api: bool
        If True then api credentials will be used
    """

    def __init__(
        self,
        hosts: List,
        username: str = None,
        password: str = None,
        api=True,
        api_key: str = None,
    ):

        if api:
            self.elastic = elasticsearch.Elasticsearch(
                hosts=hosts, api_key=api_key, verify_certs=False
            )
        else:
            username, password = self._check_auth_details(username, password)
            self.elastic = elasticsearch.Elasticsearch(
                hosts=hosts, basic_auth=(username, password), verify_certs=False
            )

    def _check_api_auth_details(self, api_username=None, api_password=None):
        if api_username is None:
            api_username = input("API Username: ")
        if api_password is None:
            api_password = getpass.getpass("API Password: ")
        return api_username, api_password

    def _check_auth_details(self, username=None, password=None):
        if username is None:
            username = input("Username: ")
        if password is None:
            password = getpass.getpass("Password: ")
        return username, password

    def get_docs_generator(
        self,
        index: List,
        query: Dict,
        es_gen_size: int = 800,
        request_timeout: int = 300,
    ):
        """

        :param query: search query
        :param index: List of ES indices to search
        :param es_gen_size:
        :param request_timeout:
        :return: search generator object
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
        query: Dict,
        index: str,
        column_headers=None,
        es_gen_size: int = 800,
        request_timeout: int = 300,
    ):
        """
        Returns DataFrame from a CogStack search

        :param query: search query
        :param index: index or list of indices
        :param column_headers: specify column headers
        :param es_gen_size:
        :param request_timeout:
        :return: DataFrame
        """
        docs_generator = elasticsearch.helpers.scan(
            self.elastic,
            query=query,
            index=index,
            size=es_gen_size,
            request_timeout=request_timeout,
        )
        temp_results = []
        results = self.elastic.count(
            index=index, query=query["query"], request_timeout=30
        )
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

    def DataFrame(self, index: str):
        """
        Fast method to return a pandas dataframe from a CogStack search.
        :param index: List of indices
        :return: A dataframe object
        """
        return ed.DataFrame(es_client=self.elastic, es_index_pattern=index)


# if api_key defined (from credentials import *) then we assume user wants to use API key authentication

if "api_key" in locals() and api_key:
    print("Using API key authentication")
    cs = CogStack(hosts, api_key=api_key, api=True)
else:
    print(
        f"Using basic authentication (active directory or local user), username: {username}"
    )

    cs = CogStack(hosts, username, password, api=False)

# authentication check
try:
    cs.elastic.info()
except Exception as e:
    print(e)


def list_chunker(entered_list):
    """
    Splits a list into smaller chunks of a specified size.

    Parameters:
    entered_list (list): The list to be split into chunks.

    Returns:
    list: A list of lists, where each sublist contains up to 10000 elements from the original list.
    """

    if len(entered_list) >= 10000:
        chunks = [
            entered_list[x : x + 10000] for x in range(0, len(entered_list), 10000)
        ]
    return chunks


# Use a generator to yield DataFrames one by one
def dataframe_generator(list_of_dfs):
    for df in list_of_dfs:
        yield df


def cohort_searcher_with_terms_and_search(
    index_name, fields_list, term_name, entered_list, search_string
):
    """
    Searches for a cohort of documents in the specified index, using the specified search string,
    and filters the results using the specified term name and list of values.

    Parameters:
    - index_name (str): The name of the Elasticsearch index to search in.
    - fields_list (list): The list of fields to return from each document.
    - term_name (str): The name of the field to filter the results on.
    - entered_list (list): The list of values to filter the results on.
    - search_string (str): The search string to apply to the results.

    Returns:
    - pandas.DataFrame: A DataFrame containing the results of the search, with the specified fields and filtered by the term name and list of values.
    """
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
            print(e)
            raise e
            print(e)
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


def set_index_safe_wrapper(df):

    try:
        df.set_index("id")
        return df
    except Exception as e:
        print(e)
        # pass

        return df


def cohort_searcher_with_terms_no_search(
    index_name, fields_list, term_name, entered_list
):
    """
    Searches a cohort based on specified terms without a search string and returns the results.

    Parameters:
    index_name (str): The name of the index to search in.
    fields_list (list): List of fields to be included in the results.
    term_name (str): The term used for filtering the search.
    entered_list (list): List of terms to search for within the index.

    Returns:
    list or pd.DataFrame: If the number of entered terms is greater than or equal to 10000, returns a list of
    DataFrames with each DataFrame corresponding to a chunk of results. Otherwise, returns a single DataFrame
    with the results.
    """

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


def cohort_searcher_no_terms(index_name, fields_list, search_string):
    """
    Searches the specified Elasticsearch index using a provided search string and returns the results.

    Parameters:
    - index_name (str): The name of the Elasticsearch index to search in.
    - fields_list (list): A list of fields to include in the search results.
    - search_string (str): The search string to use in the query.

    Returns:
    - pandas.DataFrame: A DataFrame containing the search results with the specified fields.
    """

    query = {
        "from": 0,
        "size": 10000,
        "query": {"bool": {"must": [{"query_string": {"query": search_string}}]}},
        "_source": fields_list,
    }
    df = cs.cogstack2df(query=query, index=index_name, column_headers=fields_list)
    return df


def iterative_multi_term_cohort_searcher_no_terms_fuzzy(
    terms_list,
    treatment_doc_filename,
    start_year,
    start_month,
    start_day,
    end_year,
    end_month,
    end_day,
    overwrite=True,
    debug=False,
    uuid_column_name="client_idcode",
    additional_filters=None,
    all_fields=False,
    method="fuzzy",
    fuzzy=2,
    slop=1,
):
    """
    Search Elasticsearch index for EPR documents matching multiple search terms.

    Parameters
    ----------
    terms_list : list
        The list of search terms to search for.
    treatment_doc_filename : str
        The name of the file to store the results in.
    start_year : int
        The start year of the date range to search.
    start_month : int
        The start month of the date range to search.
    start_day : int
        The start day of the date range to search.
    end_year : int
        The end year of the date range to search.
    end_month : int
        The end month of the date range to search.
    end_day : int
        The end day of the date range to search.
    overwrite : bool
        Whether to overwrite the existing file.
    debug : bool
        Whether to print debug information.
    uuid_column_name : str
        The name of the column containing the UUIDs.
    additional_filters : list
        The list of additional filters to apply.
    all_fields : bool
        Whether to retrieve all fields.
    method : str
        The search method to use (fuzzy, exact, or phrase).
    fuzzy : int
        The fuzziness level for fuzzy matching.
    slop : int
        The slop value for phrase searches.

    Returns
    -------
    pd.DataFrame
        The DataFrame containing the results of the search.
    """
    if not terms_list:
        print("Terms list is empty. Exiting.")
        return

    file_exists = exists(treatment_doc_filename)

    if file_exists and not overwrite:
        print(
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

            print("search_string", search_string)

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
                print(term, len(term_docs))

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
            print(
                f"n_unique {uuid_column_name} : {len(docs[uuid_column_name].unique())}/{len(docs)}"
            )

    return docs


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct(
    terms_list,
    treatment_doc_filename,
    start_year,
    start_month,
    start_day,
    end_year,
    end_month,
    end_day,
    append=True,
    debug=True,
    uuid_column_name="client_idcode",
    additional_filters=None,
    all_fields=False,
    method="fuzzy",
    fuzzy=2,
    slop=1,
    testing=False,
):
    """
    Perform an iterative fuzzy search of the given terms in the patient list mct documents.
    Searches observations for AoMRC_ClinicalSummary_FT in field.

    :param terms_list: List of terms to search for
    :param treatment_doc_filename: The filename of the patient list treatment doc CSV
    :param start_year: The start year of the date range to search
    :param start_month: The start month of the date range to search
    :param start_day: The start day of the date range to search
    :param end_year: The end year of the date range to search
    :param end_month: The end month of the date range to search
    :param end_day: The end day of the date range to search
    :param append: Whether to append the new results to the existing file
        (default: True)
    :param debug: Whether to print debug statements (default: True)
    :param uuid_column_name: The column name for the UUIDs (default: 'client_idcode')
    :param additional_filters: Additional filters to apply to the search
        (default: None)
    :param all_fields: Whether to retrieve all fields from the search (default: False)
    :param method: The search method to use (default: 'fuzzy')
    :param fuzzy: The fuzziness parameter for the fuzzy search (default: 2)
    :param slop: The slop parameter for the fuzzy search (default: 1)
    :param testing: Whether to perform a dummy search (default: False)

    :return: The resulting DataFrame containing the search results
    """
    print(
        "iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        print("Terms list is empty. Exiting.")
        return (
            pd.DataFrame()
        )  # Ensure it returns an empty DataFrame if terms_list is empty

    file_exists = exists(treatment_doc_filename)

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        print(f"Loaded existing file: {treatment_doc_filename}")
        return docs  # Ensure the function returns the loaded data

    else:
        if file_exists and not append:
            docs_prev = pd.read_csv(treatment_doc_filename)
            print(f"Loaded existing file and append: {treatment_doc_filename}")

        all_docs = []

        for term in tqdm(terms_list):
            # Modify the search string for each term

            search_string = f'obscatalogmasteritem_displayname:("AoMRC_ClinicalSummary_FT") AND observation_valuetext_analysed:("{term}") AND observationdocument_recordeddtm:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]'

            if additional_filters:
                search_string += " " + " ".join(additional_filters)

            print("Search String", search_string)

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
                field_list = """observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split()

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
                print(f"No results found for term: {term}")
            else:
                print(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        # If no documents were found for any term, return an empty DataFrame
        if not all_docs:
            print("No documents were found for any of the terms.")
            docs_prev = pd.read_csv(treatment_doc_filename)
            print(f"Loaded existing file and no docs found: {treatment_doc_filename}")

            return docs_prev  # Return docs from previous step
            # return pd.DataFrame()  # Return an empty DataFrame explicitly if nothing was found

        # Concatenate the results for all terms
        docs = pd.concat(all_docs, ignore_index=True)
        print(f"Total documents found: {len(docs)}")

        # Drop duplicate rows
        docs = docs.drop_duplicates()

        if os.path.exists(treatment_doc_filename):
            # Load the existing CSV
            existing_data = pd.read_csv(treatment_doc_filename)
            print(f"Loaded existing data from: {treatment_doc_filename}")

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
            print(f"Updated data saved to: {treatment_doc_filename}")
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
            print(f"New data saved to: {treatment_doc_filename}")

        if debug:
            print(
                f"n_unique {uuid_column_name} : {len(docs[uuid_column_name].unique())}/{len(docs)}"
            )

    return docs  # Return the final docs DataFrame


def iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs(
    terms_list,
    treatment_doc_filename,
    start_year,
    start_month,
    start_day,
    end_year,
    end_month,
    end_day,
    append=True,
    debug=True,
    uuid_column_name="client_idcode",
    bloods_time_field="basicobs_entered",
    additional_filters=None,
    all_fields=False,
    method="fuzzy",
    fuzzy=2,
    slop=1,
    testing=False,
):
    """
    Performs a cohort search for textual observations based on multiple terms within a specified date range.
    Searches textualObs field in basic_observations.

    Parameters:
        terms_list (list): List of terms to search for.
        treatment_doc_filename (str): Filename to load or save the cohort search results.
        start_year (int): Start year for the date range.
        start_month (int): Start month for the date range.
        start_day (int): Start day for the date range.
        end_year (int): End year for the date range.
        end_month (int): End month for the date range.
        end_day (int): End day for the date range.
        append (bool): Whether to append results to existing file if it exists. Defaults to True.
        debug (bool): Whether to print debug information. Defaults to True.
        uuid_column_name (str): Name of the column for UUIDs. Defaults to "client_idcode".
        bloods_time_field (str): Field name for bloods time. Defaults to "basicobs_entered".
        additional_filters (list): Additional filter strings to apply to the search. Defaults to None.
        all_fields (bool): Whether to include all fields in the search results. Defaults to False.
        method (str): Search method to use. Defaults to "fuzzy".
        fuzzy (int): Fuzziness level for the search. Defaults to 2.
        slop (int): Slop value for the search. Defaults to 1.
        testing (bool): Whether to run in testing mode with dummy data. Defaults to False.

    Returns:
        pandas.DataFrame: DataFrame containing the search results, or an empty DataFrame if no results are found.
    """

    print(
        "iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs",
        start_day,
        start_month,
        start_year,
        end_day,
        end_month,
        end_year,
    )
    if not terms_list:
        print("Terms list is empty. Exiting.")
        return (
            pd.DataFrame()
        )  # Ensure it returns an empty DataFrame if terms_list is empty

    file_exists = exists(treatment_doc_filename)

    if file_exists and not append:
        docs = pd.read_csv(treatment_doc_filename)
        print(f"Loaded existing file: {treatment_doc_filename}")
        return docs  # Ensure the function returns the loaded data

    else:
        if file_exists and not append:
            docs_prev = pd.read_csv(treatment_doc_filename)
            print(f"Loaded existing file and append: {treatment_doc_filename}")

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

            print("Search String", search_string)

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
                print(f"No results found for term: {term}")
            else:
                print(f"Found {len(term_docs)} documents for term: {term}")
                term_docs["search_term"] = term
                all_docs.append(term_docs)

        # If no documents were found for any term, return an empty DataFrame
        if not all_docs:
            print("No documents were found for any of the terms.")
            docs_prev = pd.read_csv(treatment_doc_filename)
            print(f"Loaded existing file and no docs found: {treatment_doc_filename}")

            return docs_prev  # Return docs from previous step
            # return pd.DataFrame()  # Return an empty DataFrame explicitly if nothing was found

        # Concatenate the results for all terms
        docs = pd.concat(all_docs, ignore_index=True)
        print(f"Total documents found: {len(docs)}")

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
            print(f"Loaded existing data from: {treatment_doc_filename}")

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
            print(f"Updated data saved to: {treatment_doc_filename}")
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
            print(f"New data saved to: {treatment_doc_filename}")

        if debug:
            print(
                f"n_unique {uuid_column_name} : {len(docs[uuid_column_name].unique())}/{len(docs)}"
            )

    return docs  # Return the final docs DataFrame


def cohort_searcher_no_terms_fuzzy(
    index_name, fields_list, search_string, method="fuzzy", fuzzy=2, slop=1
):
    """
    Search Elasticsearch using different query methods: fuzzy, exact, or phrase (with slop and fuzziness).

    Parameters:
    - index_name (str): The name of the Elasticsearch index.
    - fields_list (list): List of fields to retrieve in the response.
    - search_string (str): The search string to query.
    - method (str): The search method ("fuzzy", "exact", or "phrase"). Defaults to "fuzzy".
    - fuzzy (int): The fuzziness level for fuzzy matching. Only used if method="fuzzy" or "phrase".
    - slop (int): The slop value for phrase searches, allowing word reordering. Only used if method="phrase".

    Returns:
    - DataFrame: A DataFrame containing the search results.
    """
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
