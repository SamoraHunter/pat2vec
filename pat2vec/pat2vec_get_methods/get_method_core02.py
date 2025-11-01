from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

CORE_O2_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]


def search_core_o2_observations(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    observations_time_field="observationdocument_recordeddtm",
    start_year="1995",
    start_month="01",
    start_day="01",
    end_year="2025",
    end_month="12",
    end_day="12",
    search_term="CORE_SpO2",
    additional_custom_search_string=None,
):
    """Searches for CORE_SpO2 observation data within a date range.

    Uses a cohort searcher to find CORE_SpO2 observation data for specified
    patients within a given date range.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        observations_time_field (str): The timestamp field for filtering
            observations. Defaults to 'observationdocument_recordeddtm'.
        start_year (str): Start year for the search. Defaults to '1995'.
        start_month (str): Start month for the search. Defaults to '01'.
        start_day (str): Start day for the search. Defaults to '01'.
        end_year (str): End year for the search. Defaults to '2025'.
        end_month (str): End month for the search. Defaults to '12'.
        end_day (str): End day for the search. Defaults to '12'.
        search_term (str): The observation type to search for.
            Defaults to "CORE_SpO2".
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the raw CORE_SpO2 observation data.

    Raises:
        ValueError: If essential arguments are None.
    """
    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")
    if observations_time_field is None:
        raise ValueError("observations_time_field cannot be None.")
    if any(
        x is None
        for x in [start_year, start_month, start_day, end_year, end_month, end_day]
    ):
        raise ValueError("Date components cannot be None.")

    # Ensure client_id_codes is a list for the search function
    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = (
        validate_input_dates(
            start_year, start_month, start_day, end_year, end_month, end_day
        )
    )

    # Base search string for CORE_SpO2 observations
    search_string = (
        f'obscatalogmasteritem_displayname:("{search_term}") AND '
        + f"{observations_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
    )

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    return cohort_searcher_with_terms_and_search(
        index_name="observations",
        fields_list=CORE_O2_FIELDS,
        # Note: using default, can be made configurable
        term_name="client_idcode.keyword",
        entered_list=client_id_codes,
        search_string=search_string,
    )


def clean_observation_value(value):
    """Cleans an observation value to be used as a feature name.

    Replaces characters that are invalid in column names.

    Args:
        value (str): The original observation value.

    Returns:
        Optional[str]: The cleaned value suitable for use as a column name,
            or None if the input is NaN.
    """
    if pd.isna(value):
        return None
    return str(value).replace("-", "_").replace("%", "pct")


def calculate_core_o2_features(features_data, search_term="CORE_SpO2"):
    """Calculates O2 saturation features from CORE_SpO2 observations.

    Creates binary features for each unique observation value found in the data.

    Args:
        features_data (pd.DataFrame): DataFrame containing CORE_SpO2 observations.
        search_term (str): The observation type being processed. Defaults to "CORE_SpO2".

    Returns:
        Dict[str, int]: A dictionary of calculated binary features.
    """
    features = {}

    if len(features_data) > 0:
        # Get all unique observation values, excluding NaN values
        all_terms = features_data["observation_valuetext_analysed"].dropna().unique()

        # Create binary features for each unique observation value
        for term in all_terms:
            cleaned_term = clean_observation_value(term)
            if cleaned_term:  # Only add if cleaning was successful
                features[cleaned_term] = 1

    return features


def get_core_02(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieves CORE_SpO2 features for a patient within a date range.

    This function fetches CORE_SpO2 (oxygen saturation) data, either from a
    pre-loaded batch or by searching, and then creates binary features for each
    unique observation value.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (Tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object containing batch_mode
            and other settings. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing CORE_SpO2 features for the
            specified patient. If no data is found, a DataFrame with only the
            'client_idcode' is returned.

    Raises:
        ValueError: If `config_obj` is None, or if
            `cohort_searcher_with_terms_and_search` is None when not in batch mode.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration."
        )

    if not config_obj.batch_mode and cohort_searcher_with_terms_and_search is None:
        raise ValueError(
            "cohort_searcher_with_terms_and_search cannot be None when not in batch mode."
        )

    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    search_term = "CORE_SpO2"

    if batch_mode:
        current_pat_raw = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "observationdocument_recordeddtm",
        )
    else:
        current_pat_raw = search_core_o2_observations(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            observations_time_field="observationdocument_recordeddtm",
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            search_term=search_term,
        )

    # Initialize features DataFrame
    features = pd.DataFrame(
        data=[current_pat_client_id_code], columns=["client_idcode"]
    )

    if len(current_pat_raw) == 0:
        # Return base DataFrame with just client_idcode if no data found
        return features

    # Filter for the specific observation type and clean data
    features_data = current_pat_raw[
        current_pat_raw["obscatalogmasteritem_displayname"] == search_term
    ].copy()

    # Remove rows with NaN values in the observation_valuetext_analysed column
    features_data = features_data.dropna(subset=["observation_valuetext_analysed"])

    # Calculate features
    o2_stats = calculate_core_o2_features(features_data, search_term)

    # Add calculated features to the DataFrame
    for key, value in o2_stats.items():
        features[key] = value

    if config_obj.verbosity >= 6:
        display(features)

    return features
