
import os
import pickle
import time

import numpy as np
import pandas as pd
import paramiko
from IPython.display import display
from IPython.utils import io

from pat2vec.util.methods_annotation import (
    calculate_pretty_name_count_features,
    check_pat_document_annotation_complete, filter_annot_dataframe,
    get_pat_document_annotation_batch)
from pat2vec.util.methods_get import (dump_results, exist_check,
                                      filter_dataframe_by_timestamp,
                                      get_start_end_year_month, update_pbar)


def get_current_pat_report_annotations(client_id, date_range, report_annotations, config=None, progress_bar=None, cohort_searcher=None, medcat=None, search_term=None):
    """
    Retrieves and processes annotations for a specific patient within a given date range.

    Parameters:
    - client_id (str): The unique identifier for the patient.
    - date_range (str): The date range in the format '(YYYY,MM,DD)'.
    - report_annotations (pd.DataFrame): DataFrame containing report annotations.
    - config (ConfigObject): Configuration object with settings and parameters.
    - progress_bar (obj, optional): Placeholder for a progress bar object.
    - cohort_searcher (obj, optional): Placeholder for a cohort searcher object from cogstack search functions.
    - medcat (obj, optional): Placeholder for a medcat object with the model used to annotate.
    - search_term (str, optional): Term used for filtering annotations.

    Returns:
    - pd.DataFrame: DataFrame containing processed annotations for the specified patient.

    Raises:
    - ValueError: If config is None, a valid configuration must be provided.

    Notes:
    - The function uses the provided configuration to set up parameters such as start time and verbosity level.
    - It filters the report_annotations DataFrame based on the date_range.
    - If filtered annotations exist, it calculates pretty name count features; otherwise, it creates a DataFrame with the client_id.
    - The resulting DataFrame is displayed if the verbosity level is 6 or higher.

    Example:
    ```python
    annotations = get_current_pat_report_annotations('patient123', '(2023,1,1)', report_annotations, config_obj)
    ```
    """
    if config is None:
        raise ValueError(
            "config cannot be None. Please provide a valid configuration.")

    start_time = config.start_time

    p_bar_entry = 'annotations_report'

    update_pbar(client_id, start_time, 0,
                p_bar_entry, progress_bar, config, config.skipped_counter)

    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(
        date_range, config=config)

    if report_annotations is not None:

        filtered_report_annotations = filter_dataframe_by_timestamp(report_annotations,
                                                                    start_year,
                                                                    start_month,
                                                                    end_year,
                                                                    end_month,
                                                                    start_day, end_day, 'updatetime', dropna=True)

        if len(filtered_report_annotations) > 0:

            processed_annotations = calculate_pretty_name_count_features(
                filtered_report_annotations,
                suffix=search_term)

        else:
            if config.verbosity >= 6:
                print("len(filtered_report_annotations)>0",
                      len(filtered_report_annotations) > 0)
            processed_annotations = pd.DataFrame(
                data=[client_id], columns=['client_id'])

    else:
        processed_annotations = pd.DataFrame(
            data=[client_id], columns=['client_id'])

    if config.verbosity >= 6:
        display(processed_annotations)

    return processed_annotations
