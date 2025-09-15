from typing import Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import \
    filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

SMOKING_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]

SEARCH_TERM = "CORE_SmokingStatus"


def search_smoking(
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
    client_id_codes: Optional[Union[str, List[str]]] = None,
    observations_time_field: str = "observationdocument_recordeddtm",
    start_year: str = "1995",
    start_month: str = "01",
    start_day: str = "01",
    end_year: str = "2025",
    end_month: str = "12",
    end_day: str = "12",
    additional_custom_search_string: Optional[str] = None,
    client_idcode_term_name: str = "client_idcode.keyword",
) -> pd.DataFrame:
    """Searches for CORE_SmokingStatus observations.

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
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.
        client_idcode_term_name (str): The name of the client ID code field in
            the index. Defaults to "client_idcode.keyword".

    Returns:
        pd.DataFrame: A DataFrame containing the raw smoking status observation data.

    Raises:
        ValueError: If `cohort_searcher_with_terms_and_search` or `client_id_codes`
            is None.
    """
    if cohort_searcher_with_terms_and_search is None:
        raise ValueError(
            "cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")

    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = validate_input_dates(
        start_year, start_month, start_day, end_year, end_month, end_day
    )

    search_string = (
        f'obscatalogmasteritem_displayname:("{SEARCH_TERM}") AND '
        f"{observations_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
    )
    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    return cohort_searcher_with_terms_and_search(
        index_name="observations",
        fields_list=SMOKING_FIELDS,
        term_name=client_idcode_term_name,
        entered_list=client_id_codes,
        search_string=search_string,
    )


def prepare_smoking_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Filters for valid CORE_SmokingStatus records and drops NAs.

    Args:
        raw_data (pd.DataFrame): The raw observation data.

    Returns:
        pd.DataFrame: A cleaned DataFrame containing only valid smoking status records.
    """
    data = raw_data[raw_data["obscatalogmasteritem_displayname"]
                    == SEARCH_TERM].copy()
    data.dropna(inplace=True)
    return data


def calculate_smoking_features(
    features_data: pd.DataFrame,
    current_pat_client_id_code: str,
    negate_biochem: bool = False,
) -> pd.DataFrame:
    """Generates binary smoking status features from observation values.

    Creates binary flags indicating if a patient has records for being a
    'Current Smoker' or 'Non-Smoker'.

    Args:
        features_data (pd.DataFrame): The prepared smoking status data.
        current_pat_client_id_code (str): The patient's client ID.
        negate_biochem (bool): If True, returns features with NaN values when
            no data is available. Defaults to False.

    Returns:
        pd.DataFrame: A single-row DataFrame with binary features for smoking status.
    """
    term = "smoking_status"
    categories = {
        "current": "Current Smoker",
        "non": "Non-Smoker",
    }

    features = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    if len(features_data) > 0:
        value_array = features_data["observation_valuetext_analysed"].dropna()
        for suffix, match_str in categories.items():
            features[f"{term}_{suffix}"] = value_array.str.contains(
                match_str).astype(int)
    elif negate_biochem:
        for suffix in categories:
            features[f"{term}_{suffix}"] = np.nan

    return features


def get_smoking(
    current_pat_client_id_code: str,
    target_date_range: Tuple,
    pat_batch: pd.DataFrame,
    config_obj: Optional[object] = None,
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
) -> pd.DataFrame:
    """Retrieves CORE_SmokingStatus features for a patient within a date range.

    This function fetches smoking status observation data, either from a pre-loaded
    batch or by searching, and then creates binary features indicating the presence
    of records for different smoking statuses.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (Tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object with settings like
            `batch_mode` and `negate_biochem`. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing smoking status features for the patient.

    Raises:
        ValueError: If `config_obj` is None.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Provide a valid configuration.")

    batch_mode = config_obj.batch_mode
    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(
        target_date_range, config_obj=config_obj
    )

    # Retrieve data
    if batch_mode:
        raw_data = filter_dataframe_by_timestamp(
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
        raw_data = search_smoking(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            observations_time_field="observationdocument_recordeddtm",
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            client_idcode_term_name=config_obj.client_idcode_term_name,
        )

    if len(raw_data) == 0:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    # Prepare & calculate features
    features_data = prepare_smoking_data(raw_data)
    features = calculate_smoking_features(
        features_data,
        current_pat_client_id_code,
        negate_biochem=config_obj.negate_biochem,
    )

    if config_obj.verbosity >= 6:
        display(features)

    return features
