# import iterative_multi_term_cohort_searcher_no_terms_fuzzy
# create function that takes a list of terms, runs iterative_multi_term_cohort_searcher_no_terms_fuzzy and returns terms
# takes pat2vec_obj

import os
from cogstack_search_methods.cogstack_v8_lite import *
from pat2vec.util.get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
)
import pandas as pd
import numpy as np

import random
import string


def generate_uuid(prefix, length=7):
    """Generate a UUID-like string."""
    if prefix not in ("P", "V"):
        raise ValueError("Prefix must be 'P' or 'V'")

    # Generate random characters for the rest of the string
    chars = string.ascii_uppercase + string.digits
    random_chars = "".join(random.choices(chars, k=length))

    return f"{prefix}{random_chars}"


def generate_uuid_list(n, prefix, length=7):
    """Generate a list of n UUID-like strings."""
    uuid_list = [generate_uuid(prefix, length) for _ in range(n)]
    return uuid_list


def get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
    pat2vec_obj,
    term_list,
    overwrite=False,
    overwrite_search_term=None,
    append=False,
    verbose=0,
    mct=True,
    textual_obs=True,
    additional_filters=None,
    all_fields=False,
):
    """
    This function takes a list of terms, runs iterative_multi_term_cohort_searcher_no_terms_fuzzy
    and returns the search results as a dataframe.

    pat2vec_obj:
        A pat2vec object with the necessary attributes set.

    term_list:
        A list of terms to search for.

    overwrite:
        Whether to overwrite the output file if it already exists. Default is False.

    additional_filters:
        A list of additional filters to apply to the search.

    all_fields:
        Whether to include and return all fields in the search.

    """
    if verbose >= 1:
        print(
            "pat2vec_obj.treatment_doc_filename: ", pat2vec_obj.treatment_doc_filename
        )
    # output_path = os.join(pat2vec_obj.proj_name, pat2vec_obj.treatment_doc_filename)
    output_path = os.path.join(pat2vec_obj.treatment_doc_filename)

    # create function that takes a list of terms, runs iterative_multi_term_cohort_searcher_no_terms_fuzzy and returns terms

    if pat2vec_obj.config_obj.lookback == False:
        if verbose >= 1:
            print("Using global start date.")
        global_start_day = pat2vec_obj.config_obj.global_start_day
        global_start_month = pat2vec_obj.config_obj.global_start_month
        global_start_year = pat2vec_obj.config_obj.global_start_year
        global_end_day = pat2vec_obj.config_obj.global_end_day
        global_end_month = pat2vec_obj.config_obj.global_end_month
        global_end_year = pat2vec_obj.config_obj.global_end_year
    else:
        if verbose >= 1:
            print("Using global end date. as start")
        global_start_day = pat2vec_obj.config_obj.global_end_day
        global_start_month = pat2vec_obj.config_obj.global_end_month
        global_start_year = pat2vec_obj.config_obj.global_start_year
        global_end_day = pat2vec_obj.config_obj.global_start_day
        global_end_month = pat2vec_obj.config_obj.global_start_month
        global_end_year = pat2vec_obj.config_obj.global_start_year

    if verbose >= 1:
        # Printing results
        print("Lookback", pat2vec_obj.config_obj.lookback)
        print(
            "Global Start Date:",
            global_start_day,
            global_start_month,
            global_start_year,
        )
        print("Global End Date:", global_end_day, global_end_month, global_end_year)

    if pat2vec_obj.config_obj.testing == True:

        if verbose >= 1:
            print("Running in testing mode, doing dummy search.")
        results_holder = []
        for i in range(0, len(term_list)):
            for j in range(0, random.randint(1, 3)):
                if overwrite_search_term is None:
                    term_to_search = f'body_analysed:"{term_list[i]}"'
                    if verbose >= 1:
                        print("term_to_search: ", term_to_search)
                else:
                    term_to_search = overwrite_search_term
                    if verbose >= 1:
                        print("overwrite_search_term: ", overwrite_search_term)

                search_string = (
                    term_to_search
                    + " AND "
                    + " "
                    + f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]"
                )

                if additional_filters:
                    search_string += " " + " ".join(additional_filters)

                print("search_string", search_string)

                search_results = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epr_documents",
                    fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                    term_name=pat2vec_obj.config_obj.client_idcode_term_name,
                    entered_list=generate_uuid_list(
                        random.randint(0, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )
                results_holder.append(search_results)

                if verbose >= 1:
                    print("i: ", i)
                    print("search_results: ")
                    print(search_results)

        search_results = pd.concat(results_holder, ignore_index=True)

    else:
        if verbose >= 1:
            print("Running in live mode, doing real search.")

        print(
            "epr:",
            global_start_day,
            global_start_month,
            global_start_year,
            global_end_day,
            global_end_month,
            global_end_year,
            "lookback: ",
            pat2vec_obj.config_obj.lookback,
        )

        search_results = iterative_multi_term_cohort_searcher_no_terms_fuzzy(
            term_list,
            output_path,
            start_day=global_start_day,
            start_month=global_start_month,
            start_year=global_start_year,
            end_day=global_end_day,
            end_month=global_end_month,
            end_year=global_end_year,
            debug=False,
            overwrite=overwrite,
            additional_filters=additional_filters,
            all_fields=all_fields,
        )

    if (os.path.exists(output_path) and overwrite) or os.path.exists(
        output_path
    ) == False:
        output_directory = os.path.dirname(output_path)
        # Check if the directory exists, if not, create it
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Save the DataFrame to CSV
        search_results.to_csv(output_path, index=False, escapechar="\\")
    elif os.path.exists(output_path) and append:
        if verbose >= 1:
            print("treatment docs already exist, appending...")
            search_results.to_csv(
                output_path, index=False, mode="a", header=False, escapechar="\\"
            )

    elif os.path.exists(output_path) and not overwrite:
        if verbose >= 1:
            print("treatment docs already exist")

    elif os.path.exists(output_path) and overwrite == False:
        if verbose >= 1:
            print("treatment docs already exist, reading and returning")

        search_results = pd.read_csv(output_path)

    if mct:
        print(
            "mct:",
            global_start_day,
            global_start_month,
            global_start_year,
            global_end_day,
            global_end_month,
            global_end_year,
        )

        docs = iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct(
            term_list,
            output_path,
            start_day=global_start_day,
            start_month=global_start_month,
            start_year=global_start_year,
            end_day=global_end_day,
            end_month=global_end_month,
            end_year=global_end_year,
            append=True,
            additional_filters=additional_filters,
            all_fields=all_fields,
            # debug=debug,
            # uuid_column_name=uuid_column_name
        )

        search_results = pd.concat([search_results, docs], axis=0)

        # merge document column to fill body_analysed nan with observation_valuetext_analysed
        if "observation_valuetext_analysed" in search_results.columns:
            search_results["body_analysed"] = search_results["body_analysed"].fillna(
                search_results["observation_valuetext_analysed"]
            )

        if not textual_obs:

            return search_results

    if textual_obs:
        docs = iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs(
            term_list,
            output_path,
            start_day=global_start_day,
            start_month=global_start_month,
            start_year=global_start_year,
            end_day=global_end_day,
            end_month=global_end_month,
            end_year=global_end_year,
            append=True,
            additional_filters=additional_filters,
            all_fields=all_fields,
            # debug=debug,
            # uuid_column_name=uuid_column_name
        )

        search_results = pd.concat([search_results, docs], axis=0)

        # merge document column to fill body_analysed nan with textualObs
        if "textualObs" in search_results.columns:
            search_results["body_analysed"] = search_results["body_analysed"].fillna(
                search_results["textualObs"]
            )

        return search_results

    return search_results


def draw_document_samples(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Draw n samples for each search_term from the given DataFrame.

    Parameters:
        df (pd.DataFrame): DataFrame containing search_term and document_description columns.
        n (int): Number of samples to draw for each search_term.

    Returns:
        pd.DataFrame: DataFrame containing the sampled entries.
    """
    sampled_df = pd.DataFrame(columns=df.columns)
    for term in df["search_term"].unique():
        term_df = df[df["search_term"] == term]
        term_size = len(term_df)
        if term_size <= n:
            sampled_df = pd.concat([sampled_df, term_df])
        else:
            weights = pd.Series(np.ones(term_size) / term_size)
            sampled_indices = np.random.choice(
                term_size, size=n, replace=False, p=weights
            )
            sampled_df = pd.concat([sampled_df, term_df.iloc[sampled_indices]])
    return sampled_df


def demo_to_latest(demo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the latest record for each unique 'client_idcode' in the DataFrame.
    """
    demo_df["updatetime"] = pd.to_datetime(demo_df["updatetime"], utc=True)
    latest_demo_df = demo_df.loc[
        demo_df.groupby("client_idcode")["updatetime"].idxmax()
    ]
    return latest_demo_df


def calculate_age_append(df):
    """
    Calculate the age of clients in the given DataFrame. #input demo_df

    Parameters:
    df (pd.DataFrame): DataFrame containing client data.

    Returns:
    pd.DataFrame: DataFrame with an additional 'age' column.
    """
    # Drop rows with missing 'client_dob' values
    df.dropna(subset=["client_dob"], inplace=True)

    # Ensure 'client_dob' is in datetime format and remove timezone
    df["client_dob"] = pd.to_datetime(df["client_dob"], errors="coerce", utc=True)
    df.dropna(subset=["client_dob"], inplace=True)
    df["client_dob"] = df["client_dob"].dt.tz_localize(None)

    # Drop duplicates based on 'client_idcode'
    # df = df.drop_duplicates(subset=["client_idcode"])

    # Get the current date as a timezone-naive datetime object
    time_now = datetime.now()

    # Calculate the age by subtracting the date of birth from the current date
    df["age"] = (time_now - df["client_dob"]).dt.days // 365

    return df


def search_cohort(
    patlist,
    pat2vec_obj,
    start_year,
    start_month,
    start_day,
    end_year,
    end_month,
    end_day,
    additional_filters=None,
):
    """
    Searches for a cohort of patients based on the given parameters.

    Parameters:
    patlist (list): List of patient IDs.
    pat2vec_obj pat2vec object with config obj configured.
    start_year (str): Start year for the search.
    start_month (str): Start month for the search.
    start_day (str): Start day for the search.
    end_year (str): End year for the search.
    end_month (str): End month for the search.
    end_day (str): End day for the search.
    additional_filters (list): List of additional filter strings to append to the search string.

    Returns:
    pd.DataFrame: DataFrame containing the search results.
    """
    search_string = f"updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    if additional_filters:
        search_string += " " + " ".join(additional_filters)

    print("search_string", search_string)

    demo_df = pat2vec_obj.cohort_searcher_with_terms_and_search(
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
        term_name=pat2vec_obj.config_obj.client_idcode_term_name,
        entered_list=patlist,
        search_string=search_string,
    )
    return demo_df


# start_year = '1995'
# start_month = '01'
# start_day = '01'
# end_year = '2024'
# end_month = '01'
# end_day = '01'

# additional_filters = ["AND client_dob: {now-18y TO *}"]

# demo_df = search_cohort(patlist, start_year, start_month, start_day, end_year, end_month, end_day, additional_filters)
