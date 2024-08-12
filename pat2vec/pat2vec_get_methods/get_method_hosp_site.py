import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.methods_get import (
    filter_dataframe_by_timestamp,
    get_start_end_year_month,
)


def get_hosp_site(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieves CORE_HospitalSite features for a given patient within a specified date range.

    Parameters:
    - current_pat_client_id_code (str): The client ID code of the patient.
    - target_date_range (tuple): A tuple representing the target date range.
    - pat_batch (pd.DataFrame): The DataFrame containing patient data.
    - batch_mode (bool, optional): Indicates whether batch mode is enabled. Defaults to False.
    - cohort_searcher_with_terms_and_search (callable, optional): The function for cohort searching. Defaults to None.

    Returns:
    - pd.DataFrame: A DataFrame containing CORE_HospitalSite features for the specified patient.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration.",
            get_hosp_site,
        )

    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )
    search_term = "CORE_HospitalSite"

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

    # screen and purge dud values
    # features_data =  features_data[(features_data['observation_valuetext_analysed'].astype(float)<20)& (features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    features_data.dropna(inplace=True)

    # -----------------------------------------------------------------

    features_data = current_pat_raw[
        current_pat_raw["obscatalogmasteritem_displayname"] == search_term
    ].copy()
    # features_data =  features_data[(features_data['observation_valuetext_analysed'].astype(float)<20)& (features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    # features_data.dropna(inplace=True)

    term = "hosp_site".lower()

    if len(features_data) > 0:
        features = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        ).copy()
        # value_array = features_data['observation_valuetext_analysed'].astype(float)
        value_array = features_data["observation_valuetext_analysed"].dropna()

        features[f"{term}_dh"] = value_array.str.contains("DH").astype(int)
        features[f"{term}_ph"] = value_array.str.contains("PRUH").astype(int)

    elif config_obj.negate_biochem:
        features = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        ).copy()
        features[f"{term}_dh"] = np.nan
        features[f"{term}_ph"] = np.nan
    else:
        features = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        ).copy()
        pass

    if config_obj.verbosity >= 6:
        display(features)

    return features
