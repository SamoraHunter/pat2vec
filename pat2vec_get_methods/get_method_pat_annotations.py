
import os
import pickle
import time

import numpy as np
import pandas as pd
import paramiko
from IPython.display import display
from IPython.utils import io

from util.methods_annotation import (calculate_pretty_name_count_features,
                                     check_pat_document_annotation_complete,
                                     filter_annot_dataframe,
                                     get_pat_document_annotation_batch)
from util.methods_get import (dump_results, exist_check,
                              filter_dataframe_by_timestamp,
                              get_start_end_year_month, update_pbar)


def get_current_pat_annotations(current_pat_client_id_code, target_date_range, batch_epr_docs_annotations, config_obj=None, t=None, cohort_searcher_with_terms_and_search=None, cat=None):
    """
    Retrieves and processes annotations for a specific patient within a given date range.

    Parameters:
    - current_pat_client_id_code (str): The unique identifier for the patient.
    - target_date_range (str): The date range in the format '(YYYY,MM,DD)'.
    - batch_epr_docs_annotations (pd.DataFrame): DataFrame containing EPR document annotations.
    - config_obj (ConfigObject): Configuration object with settings and parameters.
    - t (obj, optional): Placeholder for a progress bar object.
    - cohort_searcher_with_terms_and_search (obj, optional): Placeholder for a cohort searcher object from cogstack search functions.
    - cat (obj, optional): Placeholder for a medcat object with the model used to annotate.

    Returns:
    - pd.DataFrame: DataFrame containing processed annotations for the specified patient.

    Raises:
    - ValueError: If config_obj is None, a valid configuration must be provided.

    Notes:
    - The function uses the provided configuration to set up parameters such as start time and verbosity level.
    - It filters the batch_epr_docs_annotations DataFrame based on the target_date_range.
    - If filtered annotations exist, it calculates pretty name count features; otherwise, it creates a DataFrame with the client_idcode.
    - The resulting DataFrame is displayed if the verbosity level is 6 or higher.

    Example:
    ```python
    annotations = get_current_pat_annotations('patient123', '(2023,1,1)', batch_epr_docs_annotations, config_obj)
    ```
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration. (get_current_pat_annotations)")

    start_time = config_obj.start_time

    p_bar_entry = 'annotations_epr'

    update_pbar(current_pat_client_id_code, start_time, 0,
                p_bar_entry, t, config_obj, config_obj.skipped_counter)

    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(
        target_date_range)

    if (batch_epr_docs_annotations is not None):

        filtered_batch_epr_docs_annotations = filter_dataframe_by_timestamp(batch_epr_docs_annotations,
                                                                            start_year,
                                                                            start_month,
                                                                            end_year,
                                                                            end_month,
                                                                            start_day, end_day, 'updatetime')

        if (len(filtered_batch_epr_docs_annotations) > 0):

            df_pat_target = calculate_pretty_name_count_features(
                filtered_batch_epr_docs_annotations)

        else:
            if config_obj.verbosity >= 6:
                print("len(filtered_batch_epr_docs_annotations)>0",
                      len(filtered_batch_epr_docs_annotations) > 0)
            df_pat_target = pd.DataFrame(
                data=[current_pat_client_id_code], columns=['client_idcode'])

    else:
        df_pat_target = pd.DataFrame(
            data=[current_pat_client_id_code], columns=['client_idcode'])

    if config_obj.verbosity >= 6:
        display(df_pat_target)

    return df_pat_target
