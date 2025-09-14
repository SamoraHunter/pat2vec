import os
import random
from typing import List, Optional, Union

import pandas as pd
from fuzzywuzzy import fuzz

from pat2vec.pat2vec_search.cogstack_search_methods import \
    cohort_searcher_no_terms_fuzzy
from pat2vec.util.get_dummy_data_cohort_searcher import \
    cohort_searcher_with_terms_and_search_dummy
from pat2vec.util.pre_processing import generate_uuid_list

# Functions to get cohort by drug order name.
# Example: I want the records of everyone who has an order of these three drugs...


def get_treatment_records_by_drug_order_name(
    pat2vec_obj,
    term: str,  # Single search term
    verbose: int = 0,
    all_fields: bool = False,
    column_fields_to_match: List[str] = [
        "order_summaryline",
        "order_name",
        "order_holdreasontext",
    ],
) -> pd.DataFrame:
    """Retrieves treatment records from the 'order' index that match a search term.

    Uses fuzzy matching to check against multiple columns and stores matched
    columns in the output.

    Args:
        pat2vec_obj (object): The main pat2vec object, which contains
            configuration settings.
        term (str): A single drug name or keyword to search for.
        verbose (int, optional): Verbosity level for logging/debugging.
            - 0: No output
            - 1: Basic logs
            - 5: Moderate logs
            - 9: Full debug logs
            Defaults to 0.
        all_fields (bool, optional): If True, retrieves all available fields
            from the database. If False, retrieves only essential fields.
            Defaults to False.
        column_fields_to_match (List[str], optional): List of columns to check
            for fuzzy matching. Defaults to `['order_summaryline', 'order_name',
            'order_holdreasontext']`.

    Returns:
        pd.DataFrame: A DataFrame containing treatment records with an
            additional column indicating which fields matched the search term.
            The matching column is named 'matched_{term}' where {term} is the
            lowercase, underscore-separated version of the search term.

    Notes:
        - The function retrieves records using `cohort_searcher_no_terms_fuzzy()`.
        - Applies fuzzy string matching with a threshold of 80 across selected
          columns.
        - An additional column (`matched_{term}`) stores which fields matched
          the term.
        - Only rows with at least one match are returned.
        - For medication orders only (filtered by order_typecode="medication").
    """

    if pat2vec_obj is None:
        raise ValueError("pat2vec_obj cannot be None")

    if not isinstance(term, str):
        raise ValueError("term must be a string")

    # Extract configuration settings from pat2vec_obj
    config_obj = pat2vec_obj.config_obj

    # Define the start and end date range for filtering
    start_date = f"{config_obj.global_start_year}-{config_obj.global_start_month}-{config_obj.global_start_day}"
    end_date = f"{config_obj.global_end_year}-{config_obj.global_end_month}-{config_obj.global_end_day}"

    drug_time_field = config_obj.drug_time_field

    # Essential fields to retrieve from the database
    field_list = [
        "client_idcode",
        "order_guid",
        "order_name",
        "order_summaryline",
        "order_holdreasontext",
        "order_entered",
        "clientvisit_visitidcode",
        "order_performeddtm",
    ]

    all_fields_list = [
        "client_applicsource",
        "client_build",
        "client_cityofbirth",
        "client_countryofbirth",
        "client_createdby",
        "client_createdwhen",
        "client_deceaseddtm",
        "client_displayname",
        "client_dob",
        "client_firstname",
        "client_gendercode",
        "client_guid",
        "client_idcode",
        "client_languagecode",
        "client_lastname",
        "client_maritalstatuscode",
        "client_middlename",
        "client_occupationcode",
        "client_racecode",
        "client_religioncode",
        "client_siteid",
        "client_title",
        "client_touchedby",
        "client_touchedwhen",
        "client_universalnumber",
        "clientaddress_city",
        "clientaddress_countrycode",
        "clientaddress_iscurrent",
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
        "order_activatedatereference",
        "order_activatedaysbefore",
        "order_activatehoursbefore",
        "order_activatestatus",
        "order_ancillaryreferencecode",
        "order_applicsource",
        "order_arrivaldtm",
        "order_build",
        "order_careproviderguid",
        "order_chartguid",
        "order_completedatereference",
        "order_completedaysafter",
        "order_completehoursafter",
        "order_completestatus",
        "order_conditionstext",
        "order_createdby",
        "order_createdwhen",
        "order_duration",
        "order_entered",
        "order_enterrole",
        "order_frequencycode",
        "order_guid",
        "order_hasbeenmodified",
        "order_holdreasontext",
        "order_idcode",
        "order_isapplyprotection",
        "order_isautoactivatable",
        "order_isautocompletable",
        "order_isautogenerated",
        "order_iscompletetemplate",
        "order_isconditional",
        "order_isdurationchangeable",
        "order_isfordischarge",
        "order_isgenericset",
        "order_isheld",
        "order_isincluded",
        "order_ispartofset",
        "order_isprn",
        "order_issuspended",
        "order_istrackvariance",
        "order_minimumstatusforactivation",
        "order_minimumstatusforcompletion",
        "order_modifieddtm",
        "order_modifier",
        "order_modifyuserguid",
        "order_name",
        "order_name_analysed",
        "order_ordercatalogmasteritemguid",
        "order_orderentryformguid",
        "order_orderprioritycode",
        "order_ordersetguid",
        "order_ordersetheading",
        "order_ordersetname",
        "order_ordersettype",
        "order_orderstatuscode",
        "order_orderstatuslevelnum",
        "order_ordertemplateguid",
        "order_pathwaycolumnheaderguid",
        "order_pathwayrowheaderguid",
        "order_performeddtm",
        "order_repeatorder",
        "order_reqcodedtime",
        "order_reqtimeeventmodifier",
        "order_reqtimevalue",
        "order_requesteddate",
        "order_requesteddtm",
        "order_requestedtime",
        "order_scheduleddtm",
        "order_sequencenum",
        "order_significantdtm",
        "order_significanttime",
        "order_sourcecode",
        "order_stopdate",
        "order_stopdtm",
        "order_stoptime",
        "order_subsequencenum",
        "order_summaryline",
        "order_systemorderprioritycode",
        "order_touchedby",
        "order_touchedwhen",
        "order_transpmethodcode",
        "order_typecode",
        "order_userguid",
        "order_variancetype",
        "orderuserdata_value_analysed",
        "orderv4",
        "updatetime",
    ]

    if verbose >= 5:
        print(f"[INFO] Searching for term: {term}")
        print(f"[INFO] Searching in columns: {column_fields_to_match}")
        print(f"[INFO] Date range: {start_date} to {end_date}")

    if pat2vec_obj.config_obj.testing == False:
        # Perform search in the 'order' index
        drug_treatment_docs = cohort_searcher_no_terms_fuzzy(
            index_name="order",
            fields_list=all_fields_list if all_fields else field_list,
            search_string=f'order_typecode:"medication" AND "{term}" '
            f"AND {drug_time_field}:[{start_date} TO {end_date}]",
        )
    else:

        drug_treatment_docs = cohort_searcher_with_terms_and_search_dummy(
            index_name="order",
            fields_list=field_list,
            term_name=pat2vec_obj.config_obj.client_idcode_term_name,
            entered_list=generate_uuid_list(
                random.randint(0, 10), random.choice(["P", "V"])
            ),
            search_string=f'order_typecode:"medication" AND "{term}" '
            f"AND {drug_time_field}:[{start_date} TO {end_date}]",
        )

    if drug_treatment_docs.empty:
        if verbose >= 1:
            print("[WARNING] No treatment records found.")
        return pd.DataFrame()

    if verbose >= 9:
        print(
            f"[DEBUG] Retrieved {len(drug_treatment_docs)} records from database.")

    # Function to find matching columns for the search term
    def find_matching_columns(row, search_term):
        matched_cols = []
        for field in column_fields_to_match:
            if field in row and pd.notna(row[field]):
                match_score = fuzz.partial_ratio(
                    str(row[field]).lower(), search_term.lower()
                )
                if match_score >= 80:
                    matched_cols.append(field)
                    if verbose >= 20:
                        print(
                            f"[DEBUG] Match found! Term: '{search_term}' | Column: '{field}' | Score: {match_score}"
                        )
        return matched_cols if matched_cols else None

    # Apply fuzzy matching and store results
    column_name = f"matched_{term.lower().replace(' ', '_')}"
    drug_treatment_docs[column_name] = drug_treatment_docs.apply(
        lambda row: find_matching_columns(row, term), axis=1
    )

    if verbose >= 5:
        print(f"[INFO] Processed matching for term: '{term}'")

    # Filter out rows where the term didn't match
    filtered_drug_records = drug_treatment_docs[
        drug_treatment_docs[column_name].notna()
    ]

    if verbose >= 5:
        print(
            f"[INFO] Filtered dataset contains {len(filtered_drug_records)} records after fuzzy matching."
        )

    return filtered_drug_records


# treatment_docs = get_treatment_records_by_drug_order_name(
#     pat2vec_obj=pat2vec_obj,
#     term="biktarvy",
#     verbose=9,  # Adjust verbosity for debugging
#     all_fields=False
# )

# treatment_docs


def iterative_drug_treatment_search(
    pat2vec_obj: object,
    search_terms: List[str],
    output_file_path: str,
    verbose: int = 0,
    all_fields: bool = False,
    column_fields_to_match: List[str] = [
        "order_summaryline",
        "order_name",
        "order_holdreasontext",
    ],
    drop_duplicates: bool = True,
    overwrite=False,
) -> None:
    """Iteratively search for drug treatment records and save them.

    Searches for treatment records using a list of search terms, appends
    results to a CSV file, includes matched fields, and provides an option to
    drop duplicate records based on 'order_guid'.

    Args:
        pat2vec_obj (object): The main pat2vec object containing configuration
            settings.
        search_terms (List[str]): A list of drug names or keywords to search for.
        output_file_path (str): Path to the CSV file where results will be
            stored. Appends to the file if it exists.
        verbose (int, optional): Verbosity level for logging/debugging.
            Defaults to 0.
        all_fields (bool, optional): Whether to retrieve all available fields.
            Defaults to False.
        column_fields_to_match (List[str], optional): List of columns to check
            for fuzzy matching. Defaults to `['order_summaryline', 'order_name',
            'order_holdreasontext']`.
        drop_duplicates (bool, optional): If True, drops duplicate records
            based on `order_guid`, while merging search term info. Defaults to True.
        overwrite (bool, optional): If True, overwrite the output file if it
            exists. Defaults to False.

    Returns:
        pd.DataFrame: A merged dataframe of the results.
    """

    if overwrite:
        # check output_file_path exists:

        if os.path.exists(output_file_path):

            if verbose >= 1:
                print("output_file_path exists: %s" % output_file_path)
                print("removing existing file: %s" % output_file_path)

            # remove file
            os.remove(output_file_path)

    all_results = []  # Store all results for optional deduplication

    for term in search_terms:
        if verbose >= 1:
            print(f"[INFO] Searching for term: {term}")

        # Retrieve treatment records for the current search term
        treatment_records = get_treatment_records_by_drug_order_name(
            pat2vec_obj=pat2vec_obj,
            term=term,
            verbose=verbose,
            all_fields=all_fields,
            column_fields_to_match=column_fields_to_match,
        )

        if treatment_records.empty:
            if verbose >= 1:
                print(f"[WARNING] No results found for term: {term}")
            continue

        # Add columns for search term and matched fields
        treatment_records["searched_term"] = term
        treatment_records["matched_fields"] = treatment_records[
            column_fields_to_match
        ].apply(
            lambda row: [
                col
                for col in column_fields_to_match
                if isinstance(row[col], str) and term.lower() in row[col].lower()
            ],
            axis=1,
        )

        # Store results for potential deduplication
        all_results.append(treatment_records)

        if verbose >= 1:
            print(
                f"[INFO] Retrieved {len(treatment_records)} records for term: {term}")

    if not all_results:
        if verbose >= 1:
            print("[WARNING] No records found for any search terms.")
        return

    # Combine all results into a single DataFrame
    final_results = pd.concat(all_results, ignore_index=True)

    if drop_duplicates:
        before_dedup = len(final_results)

        # Aggregate search terms and matched fields per order_guid
        final_results = final_results.groupby("order_guid", as_index=False).agg(
            {
                **{
                    col: "first"
                    for col in final_results.columns
                    if col not in ["searched_term", "matched_fields"]
                },
                "searched_term": lambda x: ", ".join(sorted(set(x))),
                "matched_fields": lambda x: list(
                    set(field for fields in x for field in fields)
                ),
            }
        )

        after_dedup = len(final_results)

        if verbose >= 1:
            print(
                f"[INFO] Dropped {before_dedup - after_dedup} duplicate records by order_guid."
            )

    # Append to CSV
    final_results.to_csv(
        output_file_path,
        mode="a",
        index=False,
        header=not pd.io.common.file_exists(output_file_path),
    )

    if verbose >= 1:
        print(
            f"[INFO] Final dataset contains {len(final_results)} records. Appended to {output_file_path}"
        )

    merged_df = pd.read_csv(output_file_path)

    return merged_df


# # Example usage
# search_terms_list = ["biktarvy", "Tenofovir", "Emtricitabine", "Mepacrine"]
# output_file_path = "drug_treatment_records.csv"


# iterative_drug_treatment_search(
#     pat2vec_obj=pat2vec_obj,
#     search_terms=search_terms_list,
#     output_file_path=output_file_path,
#     verbose=5,  # Adjust verbosity for logging
#     drop_duplicates=True, # Search terms can produce duplicates, remove by order guid.
#     overwrite = True # Overwrite initial output file
# )
