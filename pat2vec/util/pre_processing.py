# import iterative_multi_term_cohort_searcher_no_terms_fuzzy
# create function that takes a list of terms, runs iterative_multi_term_cohort_searcher_no_terms_fuzzy and returns terms
# takes pat2vec_obj

import os

from cogstack_search_methods.cogstack_v8_lite import *
from pat2vec.util.get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
)
import pandas as pd


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

    # output_path = os.join(pat2vec_obj.proj_name, pat2vec_obj.treatment_doc_filename)
    output_path = os.path.join(pat2vec_obj.treatment_doc_filename)

    # create function that takes a list of terms, runs iterative_multi_term_cohort_searcher_no_terms_fuzzy and returns terms

    if pat2vec_obj.config_obj.lookback == False:

        global_start_day = pat2vec_obj.config_obj.global_start_day
        global_start_month = pat2vec_obj.config_obj.global_start_month
        global_start_year = pat2vec_obj.config_obj.global_start_year
        global_end_day = pat2vec_obj.config_obj.global_end_day
        global_end_month = pat2vec_obj.config_obj.global_end_month
        global_end_year = pat2vec_obj.config_obj.global_end_year
    else:
        global_start_day = pat2vec_obj.config_obj.global_end_day
        global_start_month = pat2vec_obj.config_obj.global_end_month
        global_start_year = pat2vec_obj.config_obj.global_start_year
        global_end_day = pat2vec_obj.config_obj.global_start_day
        global_end_month = pat2vec_obj.config_obj.global_start_month
        global_end_year = pat2vec_obj.config_obj.global_end_year

    if pat2vec_obj.config_obj.testing == True:

        results_holder = []
        for i in range(0, len(term_list)):
            search_results = cohort_searcher_with_terms_and_search_dummy(
                index_name="epr_documents",
                fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                term_name="client_idcode.keyword",
                entered_list=generate_uuid_list(
                    random.randint(0, 10), random.choice(["P", "V"])
                ),
                search_string=term_list[i]
                + str(global_start_year)
                + "-"
                + str(global_start_month).zfill(2)
                + "-"
                + str(global_start_day).zfill(2)
                + " TO "
                + str(global_end_year)
                + "-"
                + str(global_end_month).zfill(2)
                + "-"
                + str(global_end_day).zfill(2),
            )
            results_holder.append(search_results)

        search_results = pd.concat(results_holder, ignore_index=True)
        # search_results = cohort_searcher_with_terms_and_search_dummy(
        #     "epr_documents",
        #     pat2vec_obj.fields_list,
        #     pat2vec_obj.term_name,
        #     pat2vec_obj.entered_list,
        #     pat2vec_obj.search_string,
        #     global_start_day,
        #     global_start_month,
        #     global_start_year,
        #     global_end_day,
        #     global_end_month,
        #     global_end_year,
        #     search_string=f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
        # )
    else:

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

        search_results.to_csv(output_path, index=False)

    elif os.path.exists(output_path) and not overwrite:
        print("treatment docs already exist")

    elif os.path.exists(output_path) and overwrite == False:
        print("treatment docs already exist, reading and returning")

        search_results = pd.read_csv(output_path)

    return search_results