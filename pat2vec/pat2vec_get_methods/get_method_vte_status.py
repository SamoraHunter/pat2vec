import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

VTE_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]

SEARCH_TERM = "CORE_VTE_STATUS"


def search_vte(
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
    """Search CORE_VTE_STATUS observations."""
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
        fields_list=VTE_FIELDS,
        term_name=client_idcode_term_name,
        entered_list=client_id_codes,
        search_string=search_string,
    )


def prepare_vte_data(raw_data):
    """Filter only valid CORE_VTE_STATUS records and drop NAs."""
    data = raw_data[raw_data["obscatalogmasteritem_displayname"] == SEARCH_TERM].copy()
    data.dropna(inplace=True)
    return data


def calculate_vte_features(features_data, current_pat_client_id_code, negate_biochem=False):
    """Generate numeric summary features from VTE observations."""
    term = "vte_status"
    mapping = {
        "High risk of VTE High risk of bleeding": 1,
        "High risk of VTE Low risk of bleeding": 0,
    }

    features = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    value_array = features_data["observation_valuetext_analysed"].map(mapping).dropna().astype(float)

    if len(value_array) > 0:
        features[f"{term}_mean"] = value_array.mean()
        features[f"{term}_median"] = value_array.median()
        features[f"{term}_std"] = value_array.std()
        features[f"{term}_max"] = value_array.max()
        features[f"{term}_min"] = value_array.min()
        features[f"{term}_n"] = value_array.shape[0]
    elif negate_biochem:
        for stat in ["mean", "median", "std", "max", "min", "n"]:
            features[f"{term}_{stat}"] = np.nan

    return features


def get_vte_status(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieve CORE_VTE_STATUS features for a patient within a date range."""
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
        raw_data = search_vte(
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
    features_data = prepare_vte_data(raw_data)
    features = calculate_vte_features(
        features_data,
        current_pat_client_id_code,
        negate_biochem=config_obj.negate_biochem,
    )

    if config_obj.verbosity >= 6:
        display(features)

    return features
