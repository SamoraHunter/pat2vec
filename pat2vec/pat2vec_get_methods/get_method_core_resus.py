import os
import pandas as pd
from IPython.display import display
from typing import List, Optional

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

CORE_RESUS_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]


def search_core_resus_observations(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    observations_time_field="observationdocument_recordeddtm",
    fields_override: Optional[List[str]] = None,
    start_year="1995",
    start_month="01",
    start_day="01",
    end_year="2025",
    end_month="12",
    end_day="12",
    additional_custom_search_string=None,
    index_name: str = "observations",
    output_filename: Optional[str] = "core_resus_search_results.csv",
    overwrite: bool = False,
    config_obj: Optional[object] = None,
):
    """Searches for CORE_RESUS_STATUS observation data within a date range.

    Uses a cohort searcher to find CORE_RESUS_STATUS observation data for
    specified patients.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        observations_time_field (str): The timestamp field for filtering
            observations. Defaults to 'observationdocument_recordeddtm'.
        fields_override (Optional[List[str]]): A list of fields to override the
            default `CORE_RESUS_FIELDS`. Defaults to None.
        start_year (str): Start year for the search. Defaults to '1995'.
        start_month (str): Start month for the search. Defaults to '01'.
        start_day (str): Start day for the search. Defaults to '01'.
        end_year (str): End year for the search. Defaults to '2025'.
        end_month (str): End month for the search. Defaults to '12'.
        end_day (str): End day for the search. Defaults to '12'.
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.
        index_name (str): The name of the Elasticsearch index to search.
            Defaults to "observations".
        output_filename (Optional[str]): The filename or path to a CSV file to
            load from or save to. Defaults to "core_resus_search_results.csv".
        overwrite (bool): If True, perform the search even if `output_filename`
            exists. Defaults to False.
        config_obj (Optional[object]): Configuration object containing root_path.
            Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the raw CORE_RESUS_STATUS observation data.

    Raises:
        ValueError: If essential arguments are None.
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
        print(f"Loading existing core resus data from {output_filename}")
        return pd.read_csv(output_filename)

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

    # Base search string for CORE_RESUS_STATUS observations
    search_string = (
        'obscatalogmasteritem_displayname:("CORE_RESUS_STATUS") AND '
        + f"{observations_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
    )

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    fields_to_use = CORE_RESUS_FIELDS
    if fields_override:
        fields_to_use = fields_override

    results = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_to_use,
        # Note: using default, can be made configurable
        term_name="client_idcode.keyword",
        entered_list=client_id_codes,
        search_string=search_string,
    )

    if output_filename:
        if os.path.dirname(output_filename):
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        print(f"Saving core resus data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def calculate_core_resus_features(
    features_data, term_prefix="core_resus_status", negate_biochem=False
):
    """Calculates resuscitation status features from observations.

    Counts the occurrences of "For cardiopulmonary resuscitation" and "Not for
    cardiopulmonary resuscitation" statuses.

    Args:
        features_data (pd.DataFrame): DataFrame containing CORE_RESUS_STATUS observations.
        term_prefix (str): Prefix for feature column names. Defaults to "core_resus_status".
        negate_biochem (bool): If True, returns features with a value of 0 when no
            data is available. Defaults to False.

    Returns:
        Dict[str, int]: A dictionary of calculated features.
    """
    features = {}

    if len(features_data) > 0:
        # Count occurrences of each resuscitation status
        features[f"{term_prefix}_For cardiopulmonary resuscitation"] = len(
            features_data[
                features_data["observation_valuetext_analysed"]
                == "For cardiopulmonary resuscitation"
            ]
        )
        features[f"{term_prefix}_Not for cardiopulmonary resuscitation"] = len(
            features_data[
                features_data["observation_valuetext_analysed"]
                == "Not for cardiopulmonary resuscitation"
            ]
        )
    elif negate_biochem:
        # Set 0 values when negate_biochem is True and no data available
        features[f"{term_prefix}_For cardiopulmonary resuscitation"] = 0
        features[f"{term_prefix}_Not for cardiopulmonary resuscitation"] = 0
    # If negate_biochem is False and no data, don't add features (pass)

    return features


def get_core_resus(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    batch_mode=False,
    cohort_searcher_with_terms_and_search=None,
    config_obj=None,
):
    """Retrieves CORE_RESUS_STATUS features for a patient within a date range.

    This function fetches CORE_RESUS_STATUS data, either from a pre-loaded
    batch or by searching, and then counts the occurrences of each status type.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (Tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        batch_mode (bool): Indicates if batch mode is enabled. This is controlled
            by `config_obj.batch_mode`. Defaults to False.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        config_obj (Optional[object]): Configuration object containing batch_mode
            and other settings. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing CORE_RESUS_STATUS features for the
            specified patient.

    Raises:
        ValueError: If `config_obj` is None.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration object."
        )

    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    if pat_batch.empty:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

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
        current_pat_raw = search_core_resus_observations(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            observations_time_field="observationdocument_recordeddtm",
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            output_filename=None,
            config_obj=config_obj,
        )

    # Initialize features DataFrame
    features = pd.DataFrame(
        data=[current_pat_client_id_code], columns=["client_idcode"]
    )

    if len(current_pat_raw) == 0:
        # Return base DataFrame with just client_idcode if no data found
        return features

    # Filter for CORE_RESUS_STATUS observations
    features_data = current_pat_raw[
        current_pat_raw["obscatalogmasteritem_displayname"] == "CORE_RESUS_STATUS"
    ].copy()

    # Calculate features
    term = "CORE_RESUS_STATUS".lower()
    resus_stats = calculate_core_resus_features(
        features_data, term, config_obj.negate_biochem
    )

    # Add calculated features to the DataFrame
    for key, value in resus_stats.items():
        features[key] = value

    if config_obj.verbosity >= 6:
        display(features)

    return features


CORE_RESUS_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]
