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

                search_results = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epr_documents",
                    fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                    term_name=pat2vec_obj.config_obj.client_idcode_term_name,
                    entered_list=generate_uuid_list(
                        random.randint(0, 10), random.choice(["P", "V"])
                    ),
                    search_string=term_to_search
                    + " AND "
                    + " "
                    + f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
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
        )

    if (os.path.exists(output_path) and overwrite) or os.path.exists(
        output_path
    ) == False:
        output_directory = os.path.dirname(output_path)
        # Check if the directory exists, if not, create it
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Save the DataFrame to CSV
        search_results.to_csv(output_path, index=False)
    elif os.path.exists(output_path) and append:
        if verbose >= 1:
            print("treatment docs already exist, appending...")
            search_results.to_csv(output_path, index=False, mode="a", header=False)

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
            # debug=debug,
            # uuid_column_name=uuid_column_name
        )

        search_results = pd.concat([search_results, docs], axis=0)

        # merge document column to fill body_analysed nan with observation_valuetext_analysed

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
            # debug=debug,
            # uuid_column_name=uuid_column_name
        )

        search_results = pd.concat([search_results, docs], axis=0)

        # merge document column to fill body_analysed nan with textualObs

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
