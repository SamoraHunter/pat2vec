import getpass
import random
import warnings
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List
import eland as ed
import elasticsearch
import elasticsearch.helpers
import pandas as pd
import regex
from tqdm.notebook import tqdm

warnings.filterwarnings("ignore")
import csv
import multiprocessing
from multiprocessing import Pool
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


print(
    """******Watcher connected to ES Cluster!******

Cogstack toolbox functions:
cohort_searcher_with_terms_and_search(index_name, fields_list, term_name, entered_list, search_string) = Search with terms and search string
cohort_searcher_with_terms_no_search(index_name, fields_list, term_name, entered_list) = Search with terms only
cohort_searcher_no_terms(index_name, fields_list, search_string) = Search with search string only
matcher(data_template_df, lab_results_df, source_patid_colname, source_date_colname, result_date_colname, result_testname, result_resultname, before, after) = match template with dataset
**NOTE: matcher throws up an error if dates are not converted to datetime**
stringlist2searchlist(string_list, output_name) = convert a list of strings to a lucene search string
pylist2searchlist(list_name, output_name) = convert a list of strings to a python list
stringlist2pylist(string_list, var_name) = convert a python list to a lucene search string
date_cleaner(dfs, cols, date_format) = specify the df(s) and columns to convert them to the correct datatype
bulk_str_extract(target_colname_regex_pairs, source_colname, df_name) = target_colname_regex_pairs = {"col_title":r'regex_string'}
bulk_str_findall(target_colname_regex_pairs, source_colname, df_name)
demo_columns = "client_idcode", "client_firstname", "client_lastname", "client_dob", "client_gendercode", "client_racecode", "client_deceaseddtm", "updatetime"

******Watcher connected to ES Cluster!******"""
)


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


def catch(func, handle=lambda e: e, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        pass
        # return handle(e)


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


def nearest(
    date: datetime,
    lookup_dates_and_values: pd.DataFrame,
    date_col: str,
    value_col: str,
    max_time_before: timedelta = timedelta(weeks=6),
    max_time_after: timedelta = timedelta(weeks=50),
):
    """
    Finds the nearest date and its corresponding value within a specified time range.

    Parameters:
    - date (datetime): The reference date to find the nearest date around.
    - lookup_dates_and_values (pd.DataFrame): DataFrame containing dates and their corresponding values.
    - date_col (str): The column name for dates in the DataFrame.
    - value_col (str): The column name for values in the DataFrame.
    - max_time_before (timedelta, optional): Maximum time before the reference date to consider. Defaults to 6 weeks.
    - max_time_after (timedelta, optional): Maximum time after the reference date to consider. Defaults to 50 weeks.

    Returns:
    - The value corresponding to the nearest date within the specified range, or None if no date is found.
    """

    timebefore = date - max_time_before
    timeafter = date + max_time_after
    lookup_dates_and_values = lookup_dates_and_values[
        (lookup_dates_and_values[date_col] > timebefore)
        & (lookup_dates_and_values[date_col] < timeafter)
    ]
    if lookup_dates_and_values.shape[0] == 0:
        return None
    min_date = min(
        lookup_dates_and_values.iterrows(), key=lambda x: abs(x[1][date_col] - date)
    )
    return min_date[1][value_col]


def matcher(
    data_template_df,
    lab_results_df,
    source_patid_colname,
    source_date_colname,
    result_date_colname,
    result_testname,
    result_resultname,
    before,
    after,
):
    """
    Function to match a patient template dataframe with a lab results dataframe.
    For each patient in the template dataframe, it finds the closest lab test result
    in the specified time range (before and after) for each unique lab test name.
    The results are then added as new columns to the template dataframe.

    Parameters:
    - data_template_df (pd.DataFrame): Template dataframe containing patient IDs and dates.
    - lab_results_df (pd.DataFrame): Lab results dataframe containing patient IDs, dates, test names, and test results.
    - source_patid_colname (str): Column name for patient IDs in the template dataframe.
    - source_date_colname (str): Column name for dates in the template dataframe.
    - result_date_colname (str): Column name for dates in the lab results dataframe.
    - result_testname (str): Column name for test names in the lab results dataframe.
    - result_resultname (str): Column name for test results in the lab results dataframe.
    - before (int): Number of days before the target date to consider.
    - after (int): Number of days after the target date to consider.

    Returns:
    - pd.DataFrame: The template dataframe with the added lab test results as new columns.
    """
    data_template = data_template_df  # Upload the template and inspect then rename cols, remove nulls and reset index
    data_template = data_template.dropna(subset=[source_date_colname]).reset_index(
        drop=True
    )
    data_template[source_date_colname] = pd.to_datetime(
        data_template[source_date_colname], utc=True
    )
    lab_results = lab_results_df  # Import the test results
    lab_results[result_date_colname] = pd.to_datetime(
        lab_results[result_date_colname], utc=True
    )

    bloods_filter = list(lab_results[result_testname].unique())
    bloods_values = defaultdict(
        list
    )  # Function that searches for all tests per patient and then returns the closest result to the date range of each patient in the template file
    for indx, row in data_template.iterrows():
        h_id = row[source_patid_colname]  # Patient ID from the template
        target_time = row[source_date_colname]  # Date from the template
        vals = {}
        max_time_before = timedelta(days=before)  # Time before
        max_time_after = timedelta(days=after)  # Time after
        h_id_bloods = lab_results[
            lab_results[source_patid_colname] == h_id
        ]  # Patient ID in results table
        for blood_code_type, sub_df in h_id_bloods[
            h_id_bloods[result_testname].isin(bloods_filter)
        ].groupby(
            result_testname
        ):  # Groups the results by blood test name
            date_val_idx = sub_df.columns.tolist().index(
                result_date_colname
            )  # Organizes them by blood test date
            vals[blood_code_type] = nearest(
                target_time,
                sub_df,
                result_date_colname,
                result_resultname,  # Selects the nearest blood test to display
                max_time_before,
                max_time_after,
            )
        missing_blood_types = [k for k in bloods_filter if k not in vals.keys()]
        for k in missing_blood_types:
            vals[k] = None
        for k, v in vals.items():
            bloods_values[k].append(v)
    out_file = pd.concat([data_template, pd.DataFrame(bloods_values)], axis=1)
    # globals()[output_name] = out_file
    return out_file


def stringlist2searchlist(string_list, output_name):
    list_string = string_list.replace("\n", '" OR "')
    textfile = open(output_name + ".txt", "w")
    textfile.write(f'"{list_string}"')
    textfile.close()
    print("List processed!")


def pylist2searchlist(list_name, output_name):
    test_str = '" OR "'.join(list_name)
    textfile = open(output_name + ".txt", "w")
    textfile.write(f'"{test_str}"')
    textfile.close()


def stringlist2pylist(string_list, var_name):
    globals()[var_name] = string_list.replace("\n", ",").split(",")
    print("List generated!")


def date_cleaner(df, cols, date_format):
    for col in cols:
        df[col] = pd.to_datetime(df[col], utc=True).dt.strftime(date_format)
    print("dates formatted!")


def bulk_str_findall(target_colname_regex_pairs, source_colname, df_name):
    for key, value in target_colname_regex_pairs.items():
        df_name[key] = (
            df_name[source_colname].str.lower().str.findall(value).str.join(",\n")
        )


def bulk_str_extract(target_colname_regex_pairs, source_colname, df_name, expand):
    for key, value in target_colname_regex_pairs.items():
        df_name[key] = (
            df_name[source_colname]
            .str.lower()
            .str.extract(pat=value, expand=expand, flags=re.IGNORECASE)
        )


def without_keys(d, keys):
    return {k: v for k, v in d.items() if k not in keys}


def bulk_str_extract_round_robin(target_dict, df_name, source_colname, expand):
    for key, value in target_dict.items():
        remaining_dict = without_keys(target_dict, key)
        remaining_strings = "|".join(list(remaining_dict.values()))
        df_name[key] = (
            df_name[source_colname]
            .str.lower()
            .str.extract(
                pat=f"{value}(.*?)({remaining_strings})",
                expand=expand,
                flags=re.IGNORECASE,
            )[0]
        )


def appendAge(dataFrame):
    """Creates a current age column and adds to dataframe supplied"""

    def age(born):
        born = born.split(".")[0]
        born = datetime.strptime(born, "%Y-%m-%dT%H:%M:%S").date()
        today = date.today()
        return (
            today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        )

    dataFrame["age"] = dataFrame["client_dob"].apply(age)

    return dataFrame


def appendAgeAtRecord(dataFrame):
    """Creates an age at update time column 'ageAtRecord' and computes using clients date of birth and update time"""

    def ageAtRecord(row):
        born = datetime.strptime(
            row["client_dob"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
        ).date()
        updateTime = datetime.strptime(
            row["updatetime"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
        ).date()

        today = updateTime
        return (
            today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        )

    dataFrame["ageAtRecord"] = dataFrame.apply(ageAtRecord, axis=1)
    return dataFrame


def append_age_at_record_series(series):
    def age_at_record(row):
        try:
            born = datetime.strptime(
                row["client_dob"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
            ).date()
        except Exception as e:
            #         print(e)
            born = datetime.strptime(
                row["client_dob"].iloc[0].split(".")[0], "%Y-%m-%dT%H:%M:%S"
            ).date()

        try:
            updateTime = row["updatetime"].date()
        except:
            updateTime = row["updatetime"].iloc[0].date()

        today = updateTime
        return (
            today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        )

    series["age"] = age_at_record(series)

    return series


def df_column_uniquify(df):
    df_columns = df.columns
    new_columns = []
    for item in df_columns:
        counter = 0
        newitem = item
        while newitem in new_columns:
            counter += 1
            newitem = "{}_{}".format(item, counter)
        new_columns.append(newitem)
    df.columns = new_columns
    return df


# # extract dates from clinical notes

# looks like "Entered on - 10-Mar-2020 09:38" will find relevant entries
# the date after "entered on - " is the data for the PRECEDING text

# endpoint is primary outcome of ITU or death within 7 days of diagnosis or symptom onset (whichever is longer).
# index date is min(symptom date, diagnosis date)


def find_date(txt):

    ## it's confusing that the chunks now start with the previous date

    reg = "Entered on -"
    window = 20
    m = regex.finditer(reg, txt)
    text_start = 0
    chunks = []
    for match in m:
        # print("end",match.span()[1])
        date_window_start = match.span()[1]
        date_window_end = date_window_start + window
        dw = txt[date_window_start:date_window_end]
        dw = dw.strip()
        # print(dw)

        ts = regex.findall(r"[\d]{2}-\w{3}-[\d]{4} [\d]{2}:[\d]{2}", dw)
        date_l = 0  # used to find start of next section
        date_found = False
        if len(ts) == 1:
            # timestamp found ok
            date_found = True
            date = pd.to_datetime(ts[0])
            date_l = len(ts[0])
        else:
            if len(ts) == 0:
                # no timestamp found
                print("no timestamp found in ", dw)
            else:
                # multiple matches
                print("too many timestamps found in ", dw)
            date = None

        text_end = match.span()[1] + date_l + 1  # +1 as all seem to be 1 char short
        chunk_t = txt[text_start:text_end]
        chunks.append(
            {
                "text": chunk_t,
                "date_text": dw,
                "date": date,
                "date_found": date_found,
                "text_start": text_start,
                "text_end": text_end,
            }
        )

        # next window starts at end of this one, try
        if date_found:
            text_start = match.span()[1] + date_l + 1
        else:
            text_start = match.span()[0]

    return chunks


def split_clinical_notes(clin_note):
    """
    Split clinical notes into chunks based on timestamp.

    Parameters
    ----------
    clin_note : pandas.DataFrame
        A dataframe of clinical notes with a column "body_analysed" containing the text of the note.

    Returns
    -------
    pandas.DataFrame
        A dataframe of the processed clinical notes, with each row containing a chunk of a note with a timestamp.
    """
    extracted = []
    none_found = []
    for index, row in clin_note.iterrows():
        d = row["body_analysed"]
        ch = find_date(d)
        extracted.append(
            {"id": row["id"], "client_idcode": row["client_idcode"], "chunks": ch}
        )
        if len(ch) == 0:
            none_found.append(d)

    new_docs = []
    for ex in extracted:
        counter = 0
        for ch in ex["chunks"]:
            nd = {
                "client_idcode": ex["client_idcode"],
                "body_analysed": ch["text"],
                "updatetime": ch["date"],
            }
            nd["document_description"] = f"clinical note chunk_{counter}"
            nd["source_file"] = ex["id"]
            new_docs.append(nd)
            counter += 1
    processed = pd.DataFrame(new_docs)
    return processed


def get_demographics(patlist):
    """
    Retrieve demographics information for a list of patients.

    Parameters
    ----------
    patlist : list
        List of patient IDs.

    Returns
    -------
    pandas.DataFrame
        A dataframe of demographics information for the specified patients.
    """
    demo = cohort_searcher_with_terms_no_search(
        index_name="epr_documents",
        fields_list=[
            "client_idcode",
            "client_firstname",
            "client_lastname",
            "client_dob",
            "client_gendercode",
            "client_racecode",
            "client_deceaseddtm",
            "updatetime",
        ],
        term_name="client_idcode.keyword",
        entered_list=patlist,
    )
    demo["updatetime"] = pd.to_datetime(demo["updatetime"], utc=True)
    demo = demo.sort_values(["client_idcode", "updatetime"]).drop_duplicates(
        subset=["client_idcode"], keep="last", inplace=True
    )
    return demo


def get_demographics2(patlist):
    """
    Retrieve demographics information for a list of patients.

    Parameters
    ----------
    patlist : list
        List of patient IDs.

    Returns
    -------
    pandas.Series
        A series of demographics information for the specified patients.
    """
    demo = cohort_searcher_with_terms_no_search(
        index_name="epr_documents",
        fields_list=[
            "client_idcode",
            "client_firstname",
            "client_lastname",
            "client_dob",
            "client_gendercode",
            "client_racecode",
            "client_deceaseddtm",
            "updatetime",
        ],
        term_name="client_idcode.keyword",
        entered_list=patlist,
    )
    demo["updatetime"] = pd.to_datetime(demo["updatetime"], utc=True)
    demo = demo.sort_values(
        ["client_idcode", "updatetime"]
    )  # .drop_duplicates(subset = ["client_idcode"], keep = "last", inplace = True)
    try:
        return demo.iloc[-1]
    except Exception as e:
        print(e)


def pull_and_write(index_name, fields_list, term_name, entered_list, search_string):
    """
    Pull data from elasticsearch and write to a file.

    Parameters
    ----------
    index_name : str
        The name of the index to search.
    fields_list : list
        The list of fields to retrieve.
    term_name : str
        The name of the term to search.
    entered_list : list
        The list of values to search for.
    search_string : str
        The search string to use.

    Notes
    -----
    The file is written in append mode, so if the file already exists, data will be appended to it.
    The header is not written to the file, so if you want a header, you need to add it manually.
    """
    print(f"running...{len(entered_list)}")
    file_name = "temp_search_store.csv"

    df_write = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_list,
        term_name=term_name,
        entered_list=entered_list,
        search_string=search_string,
    )

    df_write.to_csv(file_name, mode="a", index=False, header=False)


def cohort_searcher_with_terms_and_search_multi(
    index_name, fields_list, term_name, entered_list, search_string
):
    """
    Search a cohort with terms and search string, in parallel.

    Parameters
    ----------
    index_name : str
        The name of the index to search.
    fields_list : list
        The list of fields to retrieve.
    term_name : str
        The name of the term to search.
    entered_list : list
        The list of values to search for.
    search_string : str
        The search string to use.

    Returns
    -------
    pd.DataFrame
        The DataFrame containing the results of the search.
    """
    file_name = "temp_search_store.csv"
    file = open(file_name, "w", newline="")

    with open(file_name, "w", newline="") as outcsv:
        writer = csv.writer(outcsv)
        writer.writerow(fields_list)

    print(pd.read_csv(file_name))
    n = multiprocessing.cpu_count()
    l = entered_list

    # using list comprehension
    pat_list_master = [l[i : i + n] for i in range(0, len(l), n)]
    print(len(pat_list_master))

    pool = Pool()

    if __name__ == "__main__":
        print(f"starting..")
        for _ in tqdm(
            pool.imap(pull_and_write, pat_list_master), total=len(pat_list_master)
        ):
            pass

        pool.close()

    return pd.read_csv("temp_search_store.csv")


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
