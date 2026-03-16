from typing import Callable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

COVID_FIELDS = [
    "observation_guid",
    "client_idcode",
    "basicobs_itemname_analysed",
    "basicobs_value_analysed",
    "basicobs_entered",
    "clientvisit_visitidcode",
]

SEARCH_TERM_ES = r"SARS CoV-2 \(COVID-19\) RNA"
SEARCH_TERM_PLAIN = "SARS CoV-2 (COVID-19) RNA"


def search_covid(
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
    client_id_codes: Optional[Union[str, List[str]]] = None,
    observations_time_field: str = "basicobs_entered",
    fields_override: Optional[List[str]] = None,
    start_year: str = "1995",
    start_month: str = "01",
    start_day: str = "01",
    end_year: str = "2025",
    end_month: str = "12",
    end_day: str = "12",
    additional_custom_search_string: Optional[str] = None,
    client_idcode_term_name: str = "client_idcode.keyword",
    index_name: str = "basic_observations",
) -> pd.DataFrame:
    """Searches for COVID-19 test observations.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        observations_time_field (str): The timestamp field for filtering
            observations. Defaults to 'basicobs_entered'.
        fields_override (Optional[List[str]]): A list of fields to override the
            default `COVID_FIELDS`. Defaults to None.
        start_year (str): Start year for the search. Defaults to '1995'.
        start_month (str): Start month for the search. Defaults to '01'.
        start_day (str): Start day for the search. Defaults to '01'.
        end_year (str): End year for the search. Defaults to '2025'.
        end_month (str): End month for the search. Defaults to '12'.
        end_day (str): End day for the search. Defaults to '12'.
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.
        client_idcode_term_name (str): The name of the client ID code field in
            the index. Defaults to "client_idcode.keyword".
        index_name (str): The name of the Elasticsearch index to search.
            Defaults to "basic_observations".

    Returns:
        pd.DataFrame: A DataFrame containing the raw COVID-19 test observation data.

    Raises:
        ValueError: If `cohort_searcher_with_terms_and_search` or `client_id_codes`
            is None.
    """
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

    search_string = (
        f'basicobs_itemname_analysed:("{SEARCH_TERM_ES}") AND '
        f"{observations_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
    )
    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    fields_to_use = COVID_FIELDS
    if fields_override:
        fields_to_use = fields_override

    return cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_to_use,
        term_name=client_idcode_term_name,
        entered_list=client_id_codes,
        search_string=search_string,
    )


def calculate_covid_features(
    features_data: pd.DataFrame,
    current_pat_client_id_code: str,
    negate_biochem: bool = False,
) -> pd.DataFrame:
    """Generates a binary feature for COVID-19 test results.

    Creates a binary flag `covid_positive` which is 1 if any 'positive'
    result is found, 0 if only 'negative' results are found, and NaN
    if no results are found (or 0 if negate_biochem is True).

    Args:
        features_data (pd.DataFrame): The prepared COVID-19 test data.
        current_pat_client_id_code (str): The patient's client ID.
        negate_biochem (bool): If True, returns 0 when no data is available.
            Defaults to False.

    Returns:
        pd.DataFrame: A single-row DataFrame with the `covid_positive` feature.
    """
    features = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    if not features_data.empty:
        value_array = features_data["basicobs_value_analysed"].dropna().str.lower()

        is_positive = value_array.str.contains("positive").any()
        is_negative = value_array.str.contains("negative").any()

        if is_positive:
            features["covid_positive"] = 1
        elif is_negative:
            features["covid_positive"] = 0
        else:
            features["covid_positive"] = np.nan  # No clear positive or negative results
    elif negate_biochem:
        features["covid_positive"] = 0
    else:
        features["covid_positive"] = np.nan

    return features


def get_covid(
    current_pat_client_id_code: str,
    target_date_range: Tuple,
    pat_batch: pd.DataFrame,
    config_obj: Optional[object] = None,
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
) -> pd.DataFrame:
    """Retrieves COVID-19 test features for a patient within a date range.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (Tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the COVID-19 test feature for the patient.
    """
    if config_obj is None:
        raise ValueError("config_obj cannot be None. Provide a valid configuration.")

    batch_mode = config_obj.batch_mode
    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    if batch_mode:
        raw_data = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "basicobs_entered",
        )
    else:
        raw_data = search_covid(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            observations_time_field="basicobs_entered",
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            client_idcode_term_name=config_obj.client_idcode_term_name,
        )

    if raw_data.empty:
        features = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})
        features["covid_positive"] = 0 if config_obj.negate_biochem else np.nan
        return features

    features_data = raw_data[
        raw_data["basicobs_itemname_analysed"] == SEARCH_TERM_PLAIN
    ].copy()

    if features_data.empty:
        features = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})
        features["covid_positive"] = 0 if config_obj.negate_biochem else np.nan
        return features

    features = calculate_covid_features(
        features_data,
        current_pat_client_id_code,
        negate_biochem=config_obj.negate_biochem,
    )

    if config_obj.verbosity >= 6:
        display(features)

    return features
