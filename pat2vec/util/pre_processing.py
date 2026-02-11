# import iterative_multi_term_cohort_searcher_no_terms_fuzzy
# create function that takes a list of terms, runs iterative_multi_term_cohort_searcher_no_terms_fuzzy and returns terms
# takes pat2vec_obj

import os
import random
from datetime import datetime
import logging
from typing import Any, List, Optional

import numpy as np
import pandas as pd

from pat2vec.pat2vec_search.cogstack_search_methods import (
    iterative_multi_term_cohort_searcher_no_terms_fuzzy,
    iterative_multi_term_cohort_searcher_no_terms_fuzzy_mct,
    iterative_multi_term_cohort_searcher_no_terms_fuzzy_textual_obs,
)
from pat2vec.util.get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
    generate_uuid_list,
)

logger = logging.getLogger(__name__)

random_state = 42

random.seed(random_state)


def get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(
    pat2vec_obj: Any,
    term_list: List[str],
    overwrite: bool = False,
    overwrite_search_term: Optional[str] = None,
    append: bool = False,
    verbose: int = 0,
    mct: bool = True,
    textual_obs: bool = True,
    additional_filters: Optional[List[str]] = None,
    all_fields: bool = False,
    method: str = "fuzzy",
    fuzzy: int = 2,
    slop: int = 1,
) -> pd.DataFrame:
    """Searches for documents using a list of terms and returns the results.

    This function takes a list of terms, runs an iterative fuzzy search across
    multiple data sources (EPR, MCT, Textual Observations), and returns the
    combined search results as a pandas DataFrame. It also handles saving the
    results to a CSV file.

    Args:
        pat2vec_obj: A pat2vec object with necessary attributes set.
        term_list: A list of terms to search for.
        overwrite: Whether to overwrite the output file if it already exists.
        overwrite_search_term: A term to override the search terms in
            `term_list`. Used for testing.
        append: Whether to append to the output file if it exists.
        verbose: Verbosity level.
        mct: If True, includes results from the MCT source.
        textual_obs: If True, includes results from the textual observations
            source.
        additional_filters: A list of additional filters to apply to the search.
        all_fields: Whether to include and return all fields in the search.
        method: The search method to use ('fuzzy', 'phrase', 'exact').
            Defaults to "fuzzy".
        fuzzy: The fuzzy matching tolerance. Defaults to 2.
        slop: The slop for phrase matching. Defaults to 1.

    Returns:
        A DataFrame containing the search results.
    """
    if verbose >= 1:
        logger.info(
            f"pat2vec_obj.treatment_doc_filename: {pat2vec_obj.treatment_doc_filename}"
        )

    # output_path = os.join(pat2vec_obj.proj_name, pat2vec_obj.treatment_doc_filename)
    output_path = os.path.join(pat2vec_obj.treatment_doc_filename)

    # create function that takes a list of terms, runs iterative_multi_term_cohort_searcher_no_terms_fuzzy and returns terms

    if not pat2vec_obj.config_obj.lookback:
        if verbose >= 1:
            logger.info("Using global start date.")
        global_start_day = pat2vec_obj.config_obj.global_start_day
        global_start_month = pat2vec_obj.config_obj.global_start_month
        global_start_year = pat2vec_obj.config_obj.global_start_year
        global_end_day = pat2vec_obj.config_obj.global_end_day
        global_end_month = pat2vec_obj.config_obj.global_end_month
        global_end_year = pat2vec_obj.config_obj.global_end_year
    else:
        if verbose >= 1:
            logger.info("Using global end date as start.")
        global_start_day = pat2vec_obj.config_obj.global_end_day
        global_start_month = pat2vec_obj.config_obj.global_end_month
        global_start_year = pat2vec_obj.config_obj.global_start_year
        global_end_day = pat2vec_obj.config_obj.global_start_day
        global_end_month = pat2vec_obj.config_obj.global_start_month
        global_end_year = pat2vec_obj.config_obj.global_start_year

    if verbose >= 1:
        # Printing results
        logger.info(f"Lookback: {pat2vec_obj.config_obj.lookback}")
        logger.info(
            f"Global Start Date: {global_start_day}/{global_start_month}/{global_start_year}"
        )
        logger.info(
            f"Global End Date: {global_end_day}/{global_end_month}/{global_end_year}"
        )

    if pat2vec_obj.config_obj.testing:
        random.seed(random_state)
        if verbose >= 1:
            logger.info("Running in testing mode, doing dummy search.")
        results_holder = []
        for i in range(0, len(term_list)):
            for j in range(0, random.randint(1, 3)):
                if overwrite_search_term is None:
                    term_to_search = f'body_analysed:"{term_list[i]}"'
                    if verbose >= 1:
                        logger.info(f"term_to_search: {term_to_search}")
                else:
                    term_to_search = overwrite_search_term
                    if verbose >= 1:
                        logger.info(f"overwrite_search_term: {overwrite_search_term}")

                search_string = (
                    term_to_search
                    + " AND "
                    + " "
                    + f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]"
                )

                if additional_filters:
                    search_string += " " + " ".join(additional_filters)

                logger.info(f"search_string: {search_string}")

                search_results = cohort_searcher_with_terms_and_search_dummy(
                    index_name="epr_documents",
                    fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                    term_name=pat2vec_obj.config_obj.client_idcode_term_name,
                    entered_list=generate_uuid_list(
                        random.randint(2, 10), random.choice(["P", "V"])
                    ),
                    search_string=search_string,
                )
                results_holder.append(search_results)

                if verbose >= 1:
                    logger.info(f"i: {i}")
                    logger.info("search_results: ")
                    logger.info(search_results)

        search_results = pd.concat(results_holder, ignore_index=True)

    else:
        if verbose >= 1:
            logger.info("Running in live mode, doing real search.")

        logger.info(
            f"epr: {global_start_day}/{global_start_month}/{global_start_year} to "
            f"{global_end_day}/{global_end_month}/{global_end_year}, "
            f"lookback: {pat2vec_obj.config_obj.lookback}"
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
            method=method,
            fuzzy=fuzzy,
            slop=slop,
        )
    if verbose > 8:
        logger.debug(f"search_results: {search_results.head()}")

    if (os.path.exists(output_path) and overwrite) or not os.path.exists(output_path):
        output_directory = os.path.dirname(output_path)
        # Check if the directory exists, if not, create it
        try:
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)
        except Exception as e:
            logger.error(e)

        # Save the DataFrame to CSV
        search_results.to_csv(output_path, index=False, escapechar="\\")
    elif os.path.exists(output_path) and append:
        if verbose >= 1:
            logger.info("treatment docs already exist, appending...")
            search_results.to_csv(
                output_path, index=False, mode="a", header=False, escapechar="\\"
            )

    elif os.path.exists(output_path) and not overwrite:
        if verbose >= 1:
            logger.info("treatment docs already exist")

    elif os.path.exists(output_path) and not overwrite:
        if verbose >= 1:
            logger.info("treatment docs already exist, reading and returning")

        search_results = pd.read_csv(output_path)

    if mct:
        logger.info(
            f"mct: {global_start_day}/{global_start_month}/{global_start_year} to "
            f"{global_end_day}/{global_end_month}/{global_end_year}"
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
            method=method,
            fuzzy=fuzzy,
            slop=slop,
            testing=pat2vec_obj.config_obj.testing,
        )

        search_results = pd.concat([search_results, docs], axis=0)

        # merge document column to fill body_analysed nan with observation_valuetext_analysed
        if "observation_valuetext_analysed" in search_results.columns:
            search_results["body_analysed"] = search_results["body_analysed"].fillna(
                search_results["observation_valuetext_analysed"]
            )

        # merge time column to fill updatetime nan with observation_datetime
        if "basicobs_entered" in search_results.columns:
            search_results["updatetime"] = search_results["updatetime"].fillna(
                search_results["basicobs_entered"]  # bloods time field
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
            method=method,
            fuzzy=fuzzy,
            slop=slop,
            testing=pat2vec_obj.config_obj.testing,
        )

        search_results = pd.concat([search_results, docs], axis=0)

        # merge document column to fill body_analysed nan with textualObs
        if "textualObs" in search_results.columns:
            search_results["body_analysed"] = search_results["body_analysed"].fillna(
                search_results["textualObs"]
            )

        # merge time column to fill updatetime nan with observation_datetime
        if "basicobs_entered" in search_results.columns:
            search_results["updatetime"] = search_results["updatetime"].fillna(
                # bloods time field
                search_results["observationdocument_recordeddtm"]
            )

        return search_results

    return search_results


def draw_document_samples(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Draws n random samples for each unique 'search_term' in a DataFrame.

    Args:
        df: DataFrame containing a 'search_term' column.
        n: The number of samples to draw for each unique search term. If a
            term has fewer than `n` rows, all its rows are returned.

    Returns:
        A new DataFrame containing the sampled entries.
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
    """Filters a demographics DataFrame to keep only the latest record per patient.

    Based on the 'updatetime' column, this function finds and returns the
    most recent entry for each unique 'client_idcode'.

    Args:
        demo_df: A DataFrame with patient demographic data, including
            'client_idcode' and 'updatetime' columns.

    Returns:
        A DataFrame containing only the latest record for each patient.
    """
    demo_df["updatetime"] = pd.to_datetime(demo_df["updatetime"], utc=True)
    latest_demo_df = demo_df.loc[
        demo_df.groupby("client_idcode")["updatetime"].idxmax()
    ]
    return latest_demo_df


def calculate_age_append(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the age of clients and appends it as a new column.

    This function takes a DataFrame with a 'client_dob' (date of birth)
    column, calculates the current age for each client, and adds it as a
    new 'age' column. Rows with invalid or missing 'client_dob' are dropped.

    Args:
        df: DataFrame containing client data with a 'client_dob' column.

    Returns:
        The input DataFrame with an additional 'age' column.
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
    patlist: List[str],
    pat2vec_obj: Any,
    start_year: str,
    start_month: str,
    start_day: str,
    end_year: str,
    end_month: str,
    end_day: str,
    additional_filters: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Searches for a cohort of patients' demographic data within a date range.

    Args:
        patlist: List of patient IDs to search for.
        pat2vec_obj: The main pat2vec object with a configured cohort searcher.
        start_year: Start year for the search.
        start_month: Start month for the search.
        start_day: Start day for the search.
        end_year: End year for the search.
        end_month: End month for the search.
        end_day: End day for the search.
        additional_filters: List of additional filter strings to append to
            the search query.

    Returns:
        A DataFrame containing the demographic data for the specified cohort.

    Raises:
        ValueError: If `pat2vec_obj` is not provided.
    """
    search_string = f"updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    if additional_filters:
        search_string += " " + " ".join(additional_filters)

    logger.info(f"search_string: {search_string}")

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
