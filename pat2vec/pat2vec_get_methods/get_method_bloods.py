import os
from datetime import datetime, timezone
from typing import Union, Optional, List
import logging

import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates
from pat2vec.pat2vec_get_methods.get_method_epic_lab_results import (
    search_epic_lab_results,
)

logger = logging.getLogger(__name__)

BLOODS_FIELDS = [
    "client_idcode",
    "basicobs_itemname_analysed",
    "basicobs_value_numeric",
    "basicobs_entered",
    "clientvisit_serviceguid",
    "updatetime",
]


def search_bloods_data(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    client_idcode_name="client_idcode.keyword",
    bloods_time_field="basicobs_entered",
    fields_override: Optional[List[str]] = None,
    start_year: Union[int, str] = 1995,
    start_month: Union[int, str] = 1,
    start_day: Union[int, str] = 1,
    end_year: Union[int, str] = 2025,
    end_month: Union[int, str] = 12,
    end_day: Union[int, str] = 12,
    additional_custom_search_string=None,
    index_name: str = "basic_observations",
    output_filename: Optional[str] = "bloods_search_results.csv",
    overwrite: bool = False,
    config_obj: Optional[object] = None,
):
    """Searches for bloods data for patients within a date range.

    Uses a cohort searcher to find basic observation data that has a numeric value,
    within a specified time window.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        client_idcode_name (str): The name of the client ID code field in the
            index. Defaults to "client_idcode.keyword".
        bloods_time_field (str): The timestamp field for filtering bloods data.
            Defaults to 'basicobs_entered'.
        fields_override (Optional[List[str]]): A list of fields to override the
            default `BLOODS_FIELDS`. Defaults to None.
        start_year (Union[int, str]): Start year for the search. Defaults to 1995.
        start_month (Union[int, str]): Start month for the search. Defaults to 1.
        start_day (Union[int, str]): Start day for the search. Defaults to 1.
        end_year (Union[int, str]): End year for the search. Defaults to 2025.
        end_month (Union[int, str]): End month for the search. Defaults to 12.
        end_day (Union[int, str]): End day for the search. Defaults to 12.
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.
        index_name (str): The name of the Elasticsearch index to search.
            Defaults to "basic_observations".
        output_filename (Optional[str]): The filename or path to a CSV file to
            load from or save to. Defaults to "bloods_search_results.csv".
        overwrite (bool): If True, perform the search even if `output_filename`
            exists. Defaults to False.
        config_obj (Optional[object]): Configuration object containing root_path.
            Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the raw bloods data.

    Raises:
        ValueError: If `cohort_searcher_with_terms_and_search` or `client_id_codes`
            is None.
    """
    if (
        output_filename
        and config_obj
        and hasattr(config_obj, "root_path")
        and hasattr(config_obj, "proj_name")
    ):
        output_filename = os.path.join(
            config_obj.root_path, config_obj.proj_name, output_filename
        )

    if output_filename and os.path.exists(output_filename) and not overwrite:
        print(f"Loading existing bloods data from {output_filename}")
        return pd.read_csv(output_filename)

    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")

    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = (
        validate_input_dates(
            start_year, start_month, start_day, end_year, end_month, end_day
        )
    )

    search_string = f"basicobs_value_numeric:* AND {bloods_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    fields_to_use = BLOODS_FIELDS
    if fields_override:
        fields_to_use = fields_override

    results = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_to_use,
        term_name=client_idcode_name,
        entered_list=client_id_codes,
        search_string=search_string,
    )

    if output_filename:
        if os.path.dirname(output_filename):
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        print(f"Saving bloods data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def get_current_pat_bloods(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    batch_mode=False,
    cohort_searcher_with_terms_and_search=None,
    config_obj=None,
):
    """Retrieves and engineers features from blood test data for a patient.

    This function fetches blood test data for a patient within a specified date
    range, either from a pre-loaded batch or by searching. It then calculates
    a wide range of statistical features for each type of blood test found,
    such as mean, median, standard deviation, counts, and time-based features.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (Tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        batch_mode (bool): Indicates if batch mode is enabled. This is controlled
            by `config_obj.batch_mode`. Defaults to False.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        config_obj (Optional[object]): Configuration object with settings like
            `batch_mode` and `bloods_time_field`. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated blood test features
            for the specified patient.
    """
    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    bloods_time_field = config_obj.bloods_time_field

    if pat_batch.empty and batch_mode:
        if config_obj.verbosity >= 1:
            logger.info(
                f"pat_batch is empty for {current_pat_client_id_code}. Returning empty DataFrame."
            )
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    if batch_mode:
        current_pat_bloods = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            bloods_time_field,
        )
        if config_obj.verbosity >= 1:
            logger.info(
                f"After filter_dataframe_by_timestamp (batch_mode): {len(current_pat_bloods)} rows for {current_pat_client_id_code}"
            )
    else:
        current_pat_bloods = search_bloods_data(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            client_idcode_name=config_obj.client_idcode_term_name,
            bloods_time_field=bloods_time_field,
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            output_filename=None,
            config_obj=config_obj,
        )
        if config_obj.verbosity >= 1:
            logger.info(
                f"After search_bloods_data (non-batch_mode): {len(current_pat_bloods)} rows for {current_pat_client_id_code}"
            )

    # --- Integrate Epic Lab Results if enabled ---
    if config_obj.main_options.get("epic_lab_results", False):
        if config_obj.verbosity >= 1:
            print("Fetching Epic Lab Results for bloods.")

        epic_lab_data = search_epic_lab_results(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            patient_durable_keys=current_pat_client_id_code,
            id_field_name="document_PatientDurableKey",
            time_field="document_CollectedDate",  # Use CollectedDate as primary time for labs
            fields_override=[
                "document_PatientDurableKey",
                "document_CollectedDate",
                "document_Name",
                "document_Fields.valueText",
                "id",  # Use Epic's 'id' as a guid
            ],
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            output_filename=None,
            config_obj=config_obj,
        )

        if not epic_lab_data.empty:
            # Standardize column names to match basic_observations for bloods processing
            epic_lab_data.rename(
                columns={
                    "document_PatientDurableKey": "client_idcode",
                    "document_CollectedDate": "basicobs_entered",
                    "document_Name": "basicobs_itemname_analysed",
                    "document_Fields.valueText": "basicobs_value_numeric",
                    "id": "basicobs_guid",  # Use Epic's 'id' as the guid
                },
                inplace=True,
            )

            # Add/ensure other expected columns from basic_observations, filling with NaN if not present
            for col in ["clientvisit_serviceguid", "updatetime"]:
                if col not in epic_lab_data.columns:
                    epic_lab_data[col] = np.nan

            # Ensure basicobs_value_numeric is numeric
            epic_lab_data["basicobs_value_numeric"] = pd.to_numeric(
                epic_lab_data["basicobs_value_numeric"], errors="coerce"
            )

            # Concatenate Epic lab data with existing bloods data
            current_pat_bloods = pd.concat(
                [current_pat_bloods, epic_lab_data], ignore_index=True
            )

    # Ensure 'datetime' column is always a proper datetime object for calculations.
    # This handles both batch mode (where it might be a string copy) and non-batch mode.
    current_pat_bloods["datetime"] = pd.to_datetime(
        current_pat_bloods[bloods_time_field], errors="coerce"
    )
    if config_obj.verbosity >= 1:
        logger.info(
            f"After datetime conversion: {len(current_pat_bloods)} rows for {current_pat_client_id_code}"
        )

    basicobs_itemname_analysed_list = list(
        current_pat_bloods["basicobs_itemname_analysed"].unique()
    )

    basicobs_itemname_analysed_df_dict = {
        elem: current_pat_bloods[current_pat_bloods.basicobs_itemname_analysed == elem]
        for elem in basicobs_itemname_analysed_list
    }
    # Initialize the DataFrame that will hold the features for the current patient
    # It will have one row for the current_pat_client_id_code
    df_unique_filtered = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    # Ensure 'today' is UTC aware to match filtered data for duration calculations
    today = datetime.now(timezone.utc)

    # The index for the single patient row in df_unique_filtered will always be 0
    patient_row_index = 0

    for col_name in basicobs_itemname_analysed_list:
        filtered_df = basicobs_itemname_analysed_df_dict.get(col_name)

        # Filter out rows where basicobs_value_numeric is NaN or cannot be converted to numeric
        # and ensure datetime is valid for sorting.
        cleaned_df = filtered_df.copy()
        if config_obj.verbosity >= 1:
            logger.info(
                f"cleaned_df (before numeric conversion and dropna) for {col_name}: {len(cleaned_df)} rows"
            )
        cleaned_df["basicobs_value_numeric"] = pd.to_numeric(
            cleaned_df["basicobs_value_numeric"], errors="coerce"
        )
        cleaned_df = cleaned_df.dropna(subset=["basicobs_value_numeric", "datetime"])

        df_len = len(cleaned_df)
        if config_obj.verbosity >= 1:
            logger.info(
                f"cleaned_df (after numeric conversion and dropna) for {col_name}: {df_len} rows"
            )
            if df_len > 0:
                logger.info(
                    f"Sample values: {cleaned_df['basicobs_value_numeric'].tolist()}"
                )

        if df_len >= 1:
            # Mean
            df_unique_filtered.at[patient_row_index, col_name + "_mean"] = cleaned_df[
                "basicobs_value_numeric"
            ].mean()

            # Min
            df_unique_filtered.at[patient_row_index, col_name + "_min"] = cleaned_df[
                "basicobs_value_numeric"
            ].min()

            # Max
            df_unique_filtered.at[patient_row_index, col_name + "_max"] = cleaned_df[
                "basicobs_value_numeric"
            ].max()

            # Number of tests
            df_unique_filtered.at[patient_row_index, col_name + "_num-tests"] = df_len

            # Most recent value
            df_unique_filtered.at[patient_row_index, col_name + "_most-recent"] = (
                cleaned_df.sort_values(by="datetime").iloc[-1]["basicobs_value_numeric"]
            )

            # Earliest test value
            df_unique_filtered.at[patient_row_index, col_name + "_earliest-test"] = (
                cleaned_df.sort_values(by="datetime").iloc[0]["basicobs_value_numeric"]
            )

            # Days since last test (using the latest datetime from cleaned_df)
            latest_date_object = cleaned_df.sort_values(by="datetime").iloc[-1][
                "datetime"
            ]
            if pd.notna(latest_date_object):
                latest_date_object = (
                    latest_date_object.tz_localize("UTC")
                    if latest_date_object.tzinfo is None
                    else latest_date_object.astimezone(timezone.utc)
                )
            delta_days_since_last = (today - latest_date_object).days
            df_unique_filtered.at[
                patient_row_index, col_name + "_days-since-last-test"
            ] = delta_days_since_last

            # Days between earliest and last
            if df_len >= 2:
                oldest_date_object = cleaned_df.sort_values(by="datetime").iloc[0][
                    "datetime"
                ]
                if pd.notna(oldest_date_object):
                    oldest_date_object = (
                        oldest_date_object.tz_localize("UTC")
                        if oldest_date_object.tzinfo is None
                        else oldest_date_object.astimezone(timezone.utc)
                    )
                delta_between_first_last = (
                    latest_date_object - oldest_date_object
                ).days
                df_unique_filtered.at[
                    patient_row_index, col_name + "_days-between-first-last"
                ] = delta_between_first_last

            # Median (requires at least 1 value, but more meaningful with >=2)
            df_unique_filtered.at[patient_row_index, col_name + "_median"] = cleaned_df[
                "basicobs_value_numeric"
            ].median()

            # Mode (requires at least 1 value)
            if not cleaned_df["basicobs_value_numeric"].mode().empty:
                df_unique_filtered.at[patient_row_index, col_name + "_mode"] = (
                    cleaned_df["basicobs_value_numeric"].mode().iloc[0]
                )

        if df_len >= 2:  # Standard deviation requires at least 2 values
            # Std
            df_unique_filtered.at[patient_row_index, col_name + "_std"] = cleaned_df[
                "basicobs_value_numeric"
            ].std()

            # contains extreme low
            col_name_mean = cleaned_df["basicobs_value_numeric"].mean()
            col_name_std = cleaned_df["basicobs_value_numeric"].std()

            col_name_low = col_name_mean - (col_name_std * 3)

            df_unique_filtered.at[
                patient_row_index, col_name + "_contains-extreme-low"
            ] = int(cleaned_df["basicobs_value_numeric"].min() < col_name_low)

            # contains extreme high
            col_name_high = col_name_mean + (col_name_std * 3)

            df_unique_filtered.at[
                patient_row_index, col_name + "_contains-extreme-high"
            ] = int(cleaned_df["basicobs_value_numeric"].max() > col_name_high)

    if config_obj.verbosity >= 6:
        display(df_unique_filtered)

    return df_unique_filtered
