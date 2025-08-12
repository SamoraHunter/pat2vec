import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import (
    get_start_end_year_month,
)


def get_vte_status(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieves CORE_VTE_STATUS features for a given patient within a specified date range.

    Parameters:
    - current_pat_client_id_code (str): The client ID code of the patient.
    - target_date_range (tuple): A tuple representing the target date range.
    - pat_batch (pd.DataFrame): The DataFrame containing patient data.
    - batch_mode (bool, optional): Indicates whether batch mode is enabled. Defaults to False.
    - cohort_searcher_with_terms_and_search (callable, optional): The function for cohort searching. Defaults to None.

    Returns:
    - pd.DataFrame: A DataFrame containing CORE_VTE_STATUS features for the specified patient.
    """
    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )
    search_term = "CORE_VTE_STATUS"

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
        current_pat_raw = cohort_searcher_with_terms_and_search(
            index_name="observations",
            fields_list=[
                "observation_guid",
                "client_idcode",
                "obscatalogmasteritem_displayname",
                "observation_valuetext_analysed",
                "observationdocument_recordeddtm",
                "clientvisit_visitidcode",
            ],
            term_name=config_obj.client_idcode_term_name,
            entered_list=[current_pat_client_id_code],
            search_string=f'obscatalogmasteritem_displayname:("{search_term}") AND observationdocument_recordeddtm:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]',
        )

    if len(current_pat_raw) == 0:

        features = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        )

    features_data = current_pat_raw[
        current_pat_raw["obscatalogmasteritem_displayname"] == search_term
    ].copy()

    features_data.dropna(inplace=True)

    features_data = current_pat_raw[
        current_pat_raw["obscatalogmasteritem_displayname"] == search_term
    ].copy()

    term = "VTE_Status".lower()

    if len(features_data) > 0:
        features = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        ).copy()

        di = {
            "High risk of VTE High risk of bleeding": 1,
            "High risk of VTE Low risk of bleeding": 0,
        }

        value_array = features_data["observation_valuetext_analysed"].map(di)

        value_array = value_array.astype(float)

        features[f"{term}_mean"] = value_array.mean()
        features[f"{term}_median"] = value_array.median()
        features[f"{term}_std"] = value_array.std()
        features[f"{term}_max"] = max(value_array)
        features[f"{term}_min"] = min(value_array)
        features[f"{term}_n"] = value_array.shape[0]

    elif config_obj.negate_biochem:

        features = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        ).copy()
        features[f"{term}_mean"] = np.nan
        features[f"{term}_median"] = np.nan
        features[f"{term}_std"] = np.nan
        features[f"{term}_max"] = np.nan
        features[f"{term}_min"] = np.nan
        features[f"{term}_n"] = np.nan

    else:
        features = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        ).copy()

    if config_obj.verbosity >= 6:
        display(features)

    return features
