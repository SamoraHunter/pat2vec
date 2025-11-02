
import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

BMI_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]


def search_bmi_observations(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    observations_time_field="observationdocument_recordeddtm",
    start_year="1995",
    start_month="01",
    start_day="01",
    end_year="2025",
    end_month="12",
    end_day="12",
    additional_custom_search_string=None,
):
    """Searches for BMI-related observation data within a date range.

    Uses a cohort searcher to find observations related to BMI, weight, and height.

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

    Returns:
        pd.DataFrame: A DataFrame containing the raw BMI observation data.

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

    # Base search string for BMI-related observations
    search_string = (
        'obscatalogmasteritem_displayname:("OBS BMI" OR "OBS Weight" OR "OBS Height") AND '
        + f"{observations_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
    )

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    return cohort_searcher_with_terms_and_search(
        index_name="observations",
        fields_list=BMI_FIELDS,
        # Note: using default, can be made configurable
        term_name="client_idcode.keyword",
        entered_list=client_id_codes,
        search_string=search_string,
    )


def calculate_bmi_features(bmi_sample, term_prefix="bmi", negate_biochem=False):
    """Calculate statistical features from BMI, weight, or height observations.

    Computes mean, median, standard deviation, and other specific features
    based on the term prefix.

    Args:
        bmi_sample (pd.DataFrame): DataFrame containing the observation data.
        term_prefix (str): Prefix for feature column names (e.g., 'bmi',
            'weight', 'height'). Defaults to "bmi".
        negate_biochem (bool): If True, returns features with NaN values when
            no data is available. Defaults to False.

    Returns:
        Dict[str, Union[float, int]]: A dictionary of calculated features.
    """
    features = {}

    if len(bmi_sample) > 0:
        value_array = bmi_sample["observation_valuetext_analysed"].astype(float)

        features[f"{term_prefix}_mean"] = value_array.mean()
        features[f"{term_prefix}_median"] = value_array.median()
        features[f"{term_prefix}_std"] = value_array.std()

        if term_prefix == "bmi":
            # BMI-specific features
            features[f"{term_prefix}_high"] = int(bool(value_array.median() > 24.9))
            features[f"{term_prefix}_low"] = int(bool(value_array.median() < 18.5))
            features[f"{term_prefix}_extreme"] = int(bool(value_array.median() > 30))
            features[f"{term_prefix}_max"] = max(value_array)
            features[f"{term_prefix}_min"] = min(value_array)
        elif term_prefix in ["weight"]:
            # Weight-specific features (max/min)
            features[f"{term_prefix}_max"] = max(value_array)
            features[f"{term_prefix}_min"] = min(value_array)

    elif negate_biochem:
        # Set NaN values for all features
        base_features = [
            f"{term_prefix}_mean",
            f"{term_prefix}_median",
            f"{term_prefix}_std",
        ]

        if term_prefix == "bmi":
            base_features.extend(
                [
                    f"{term_prefix}_high",
                    f"{term_prefix}_low",
                    f"{term_prefix}_extreme",
                    f"{term_prefix}_max",
                    f"{term_prefix}_min",
                ]
            )
        elif term_prefix == "weight":
            base_features.extend([f"{term_prefix}_max", f"{term_prefix}_min"])

        for feature in base_features:
            features[feature] = np.nan

    return features


def get_bmi_features(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieves BMI-related features for a patient within a specified date range.

    This function fetches BMI, weight, and height data, either from a pre-loaded
    batch or by searching, and then calculates statistical features for each.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (Tuple[int, int, int, int, int, int]): A tuple
            representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object containing batch_mode
            and other settings. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing BMI-related features for the
            specified patient.
    """
    if config_obj is None:
        raise ValueError("config_obj cannot be None. Please provide a valid configuration object.")

    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    if batch_mode:
        current_pat_raw_bmi = filter_dataframe_by_timestamp(
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
        current_pat_raw_bmi = search_bmi_observations(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            observations_time_field="observationdocument_recordeddtm",
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
        )

    # Check if we have BMI calculation data
    bmi_calculation_data = current_pat_raw_bmi[
        current_pat_raw_bmi["obscatalogmasteritem_displayname"] == "OBS BMI Calculation"
    ]

    if len(bmi_calculation_data) == 0:
        bmi_features = pd.DataFrame(
            data={"client_idcode": [current_pat_client_id_code]}
        )
    else:
        # Initialize features DataFrame
        bmi_features = pd.DataFrame(
            data={"client_idcode": [current_pat_client_id_code]}
        )

        # Get BMI features
        bmi_sample = bmi_calculation_data[
            (bmi_calculation_data["observation_valuetext_analysed"].astype(float) < 200)
            & (bmi_calculation_data["observation_valuetext_analysed"].astype(float) > 6)
        ]

        bmi_stats = calculate_bmi_features(bmi_sample, "bmi", config_obj.negate_biochem)
        for key, value in bmi_stats.items():
            bmi_features[key] = value

        # Get height features
        height_sample = current_pat_raw_bmi[
            current_pat_raw_bmi["obscatalogmasteritem_displayname"] == "OBS Height"
        ]
        height_sample = height_sample[
            (height_sample["observation_valuetext_analysed"].astype(float) < 300)
            & (height_sample["observation_valuetext_analysed"].astype(float) > 30)
        ]

        height_stats = calculate_bmi_features(
            height_sample, "height", config_obj.negate_biochem
        )
        for key, value in height_stats.items():
            bmi_features[key] = value

        # Get weight features
        weight_sample = current_pat_raw_bmi[
            current_pat_raw_bmi["obscatalogmasteritem_displayname"] == "OBS Weight"
        ]
        weight_sample = weight_sample[
            (weight_sample["observation_valuetext_analysed"].astype(float) < 800)
            & (weight_sample["observation_valuetext_analysed"].astype(float) > 1)
        ]

        weight_stats = calculate_bmi_features(
            weight_sample, "weight", config_obj.negate_biochem
        )
        for key, value in weight_stats.items():
            bmi_features[key] = value

    if config_obj.verbosity >= 6:
        display(bmi_features)

    return bmi_features
