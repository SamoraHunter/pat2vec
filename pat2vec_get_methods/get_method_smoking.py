import numpy as np
import pandas as pd
from IPython.display import display

from util.methods_get import (filter_dataframe_by_timestamp,
                              get_start_end_year_month)


def get_smoking(current_pat_client_id_code, target_date_range, pat_batch, config_obj=None, cohort_searcher_with_terms_and_search=None):
    """
    Retrieves smoking status features for a given patient within a specified date range.

    Parameters:
    - current_pat_client_id_code (str): The client ID code of the patient.
    - target_date_range (tuple): A tuple representing the target date range.
    - pat_batch (pd.DataFrame): The DataFrame containing patient data.
    - batch_mode (bool, optional): Indicates whether batch mode is enabled. Defaults to False.
    - cohort_searcher_with_terms_and_search (callable, optional): The function for cohort searching. Defaults to None.

    Returns:
    - pd.DataFrame: A DataFrame containing smoking status features for the specified patient.
    """
    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(
        target_date_range)
    search_term = 'CORE_SmokingStatus'

    # Filtering data based on batch mode or cohort search
    if batch_mode:
        current_pat_raw = filter_dataframe_by_timestamp(
            pat_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'observationdocument_recordeddtm')
    else:
        current_pat_raw = cohort_searcher_with_terms_and_search(
            index_name="observations",
            fields_list=["observation_guid", "client_idcode", "obscatalogmasteritem_displayname",
                         "observation_valuetext_analysed", "observationdocument_recordeddtm", "clientvisit_visitidcode"],
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f"obscatalogmasteritem_displayname:(\"{search_term}\") AND observationdocument_recordeddtm:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
        )

    if (len(current_pat_raw) == 0):

        features = pd.DataFrame(
            data=[current_pat_client_id_code], columns=['client_idcode'])

    features_data = current_pat_raw[current_pat_raw['obscatalogmasteritem_displayname'] == search_term].copy(
    )

    # screen and purge dud values
    features_data.dropna(inplace=True)

    # -----------------------------------------------------------------

    features_data = current_pat_raw[current_pat_raw['obscatalogmasteritem_displayname'] == search_term].copy(
    )

    term = 'smoking_status'.lower()

    if (len(features_data['observation_valuetext_analysed'].dropna()) > 0):
        features = pd.DataFrame(data=[current_pat_client_id_code], columns=[
                                'client_idcode']).copy()

        value_array = features_data['observation_valuetext_analysed'].dropna()

        features[f'{term}_current'] = value_array.str.contains(
            "Current Smoker").astype(int)
        features[f'{term}_non'] = value_array.str.contains(
            "Non-Smoker").astype(int)

    elif (config_obj.negate_biochem):
        features = pd.DataFrame(data=[current_pat_client_id_code], columns=[
                                'client_idcode']).copy()
        features[f'{term}_current'] = np.nan
        features[f'{term}_non'] = np.nan
    else:
        features = pd.DataFrame(data=[current_pat_client_id_code], columns=[
                                'client_idcode']).copy()
        pass

    if config_obj.verbosity >= 6:
        display(features)

    return features
