import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
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
    client_idcode_term_name="client_idcode.keyword",
):
    """Search CORE_SmokingStatus observations."""
    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
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


def prepare_smoking_data(raw_data):
    """Filter only valid CORE_SmokingStatus records and drop NAs."""
    data = raw_data[raw_data["obscatalogmasteritem_displayname"] == SEARCH_TERM].copy()
    data.dropna(inplace=True)
    return data


def calculate_smoking_features(features_data, current_pat_client_id_code, negate_biochem=False):
    """Generate binary smoking status features."""
    term = "smoking_status"
    categories = {
        "current": "Current Smoker",
        "non": "Non-Smoker",
    }

    features = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    if len(features_data) > 0:
        value_array = features_data["observation_valuetext_analysed"].dropna()
        for suffix, match_str in categories.items():
            features[f"{term}_{suffix}"] = value_array.str.contains(match_str).astype(int)
    elif negate_biochem:
        for suffix in categories:
            features[f"{term}_{suffix}"] = np.nan

    return features


def get_smoking(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieve CORE_SmokingStatus features for a patient within a date range."""
    if config_obj is None:
        raise ValueError("config_obj cannot be None. Provide a valid configuration.")

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
