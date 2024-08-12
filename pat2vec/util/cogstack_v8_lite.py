import getpass
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

from credentials import *

print("refreshed")


class CogStack(object):
    print("refreshed!")
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
        api_username: str = None,
        api_password: str = None,
        api=False,
    ):

        if api:
            api_username, api_password = self._check_api_auth_details(
                api_username, api_password
            )
            self.elastic = elasticsearch.Elasticsearch(
                hosts=hosts, api_key=(api_username, api_password), verify_certs=False
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

cs = CogStack(hosts, username, password, api=False)


def list_chunker(entered_list):
    if len(entered_list) >= 10000:
        chunks = [
            entered_list[x : x + 10000] for x in range(0, len(entered_list), 10000)
        ]
    return chunks


def cohort_searcher_with_terms_and_search(
    index_name, fields_list, term_name, entered_list, search_string
):
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
            return results

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


# +
# def cohort_searcher_with_terms_no_search(index_name, fields_list, term_name, entered_list):
#     if len(entered_list) >= 10000:
#         results = []
#         chunked_list = list_chunker(entered_list)
#         for mini_list in chunked_list:
#             query = {"from": 0, "size": 10000, "query": {"bool": {"filter": {"terms": {term_name: mini_list}}}},"_source": fields_list}
#             df = cs.cogstack2df(query=query, index=index_name, column_headers=fields_list)
#             results.append(df)
#         merged_df = [df.set_index('id') for df in results]
#         return merged_df
#     else:
#         query = {"from": 0, "size": 10000, "query": {"bool": {"filter": {"terms": {term_name: entered_list}}}},"_source": fields_list}
#         df = cs.cogstack2df(query=query, index=index_name, column_headers=fields_list)
#         return df
# -


def set_index_safe_wrapper(df):

    try:
        df.set_index("id")
        return df
    except Exception as e:
        print(e)
        # pass

        return df


def cohort_searcher_no_terms_fuzzy(index_name, fields_list, search_string, fuzzy=2):
    # Construct a fuzzy query by modifying the "query_string" section
    query = {
        "from": 0,
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {
                        "query_string": {
                            "query": search_string,
                            "fuzziness": fuzzy,  # Set the fuzziness value
                        }
                    }
                ]
            }
        },
        "_source": fields_list,
    }
    df = cs.cogstack2df(query=query, index=index_name, column_headers=fields_list)
    return df


def cohort_searcher_with_terms_no_search(
    index_name, fields_list, term_name, entered_list
):
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


# +
# def append_age_at_record_series(series):
#     """Creates an age at update time column 'ageAtRecord' and computes using clients date of birth and update time"""
#     def age_at_record(row):
#         born = datetime.strptime(row['client_dob'].split(".")[0], "%Y-%m-%dT%H:%M:%S").date()
#         updateTime = row['updatetime'].date()

#         today = updateTime
#         return today.year - born.year - ((today.month,
#                                           today.day) < (born.month,
#                                                         born.day))
#     #series['age'] = series.apply(age_at_record)
#     series['age'] = age_at_record(series)

#     return series
# -


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


# +
# def age_at_record(row):
#     try:
#         born = datetime.strptime(row['client_dob'].split(".")[0], "%Y-%m-%dT%H:%M:%S").date()
#     except Exception as e:
# #         print(e)
#         born = datetime.strptime(row['client_dob'].iloc[0].split(".")[0], "%Y-%m-%dT%H:%M:%S").date()


#     try:
#         updateTime = row['updatetime'].date()
#     except:
#         updateTime = row['updatetime'].iloc[0].date()


#     today = updateTime
#     return today.year - born.year - ((today.month,
#                                       today.day) < (born.month,
#                                                     born.day))
# -


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
    end_year,
    end_month,
    overwrite=True,
    debug=False,
    uuid_column_name="client_idcode",
):
    if not terms_list:
        print("Terms list is empty. Exiting.")
        return

    file_exists = exists(treatment_doc_filename)

    if file_exists and not overwrite:
        docs = pd.read_csv(treatment_doc_filename)
    else:
        all_docs = []

        for term in tqdm(terms_list):
            # Modify the search string for each term
            search_string = f'"{term}" AND updatetime:[{start_year}-{start_month} TO {end_year}-{end_month}]'

            # Perform the search
            term_docs = cohort_searcher_no_terms_fuzzy(
                index_name="epr_documents",
                fields_list="client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode".split(),
                search_string=search_string,
            )

            if debug:
                print(term, len(term_docs))

            all_docs.append(term_docs)

        # Concatenate the results for all terms
        docs = pd.concat(all_docs, ignore_index=True)

        docs = docs.drop_duplicates()

        # Save the results to a temporary CSV
        docs.to_csv(treatment_doc_filename, index=False)
        if debug:
            print(
                f"n_unique {uuid_column_name} : {len(docs[uuid_column_name].unique())}/{len(docs)}"
            )

    return docs
