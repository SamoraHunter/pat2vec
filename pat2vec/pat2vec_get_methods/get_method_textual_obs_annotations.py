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
    check_pat_document_annotation_complete,
    filter_annot_dataframe,
    get_pat_document_annotation_batch,
)
from pat2vec.util.methods_get import (
    dump_results,
    exist_check,
    filter_dataframe_by_timestamp,
    get_start_end_year_month,
    update_pbar,
)


def get_current_pat_textual_obs_annotations(
    current_pat_client_id_code,
    target_date_range,
    textual_obs_annotations,
    config_obj=None,
    t=None,
    cohort_searcher_with_terms_and_search=None,
    cat=None,
):
    """
    Retrieves and processes current patient report annotations based on the provided parameters.

    Parameters:
        current_pat_client_id_code (str): The client ID code of the current patient.
        target_date_range (str): The target date range for the annotations.
        textual_obs_annotations (pandas.DataFrame): The textual observation annotations.
        config_obj (object): The configuration object. Defaults to None.
        t (object): The object for tracking progress. Defaults to None.
        cohort_searcher_with_terms_and_search (object): The cohort searcher object. Defaults to None.
        cat (object): The object for entity recognition. Defaults to None.

    Returns:
        pandas.DataFrame: The processed annotations.
    """

    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration."
        )
    if textual_obs_annotations is None:
        raise ValueError(
            "textual_obs_annotations cannot be None. Please provide a valid DataFrame."
        )
    if current_pat_client_id_code is None:
        raise ValueError(
            "current_pat_client_id_code cannot be None. Please provide a valid client ID code."
        )
    if target_date_range is None:
        raise ValueError(
            "target_date_range cannot be None. Please provide a valid target date range."
        )

    # get the start and end time for the provided target date range
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration."
        )

    start_time = config_obj.start_time

    p_bar_entry = "annotations_textual_obs"
    try:
        update_pbar(
            current_pat_client_id_code,
            start_time,
            0,
            p_bar_entry,
            t,
            config_obj,
            config_obj.skipped_counter,
        )
    except Exception as e:
        print(e)

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    # filter the textual observation annotations based on the provided target date range
    if textual_obs_annotations is not None:

        filtered_textual_obs_annotations = filter_dataframe_by_timestamp(
            textual_obs_annotations,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "basicobs_entered",
            dropna=True,
        )

        # if there are any filtered annotations, calculate the pretty name count features
        if len(filtered_textual_obs_annotations) > 0:

            processed_annotations = calculate_pretty_name_count_features(
                filtered_textual_obs_annotations
            )

        else:
            # if there are no filtered annotations, create a DataFrame with the client ID code
            if config_obj.verbosity >= 6:
                print(
                    "len(filtered_report_annotations)>0",
                    len(filtered_textual_obs_annotations) > 0,
                )
            processed_annotations = pd.DataFrame(
                data=[current_pat_client_id_code], columns=["client_idcode"]
            )

    else:
        # if the textual observation annotations are None, create a DataFrame with the client ID code
        processed_annotations = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        )

    # display the processed annotations if the verbosity level is 6 or higher
    if config_obj.verbosity >= 6:
        display(processed_annotations)

    return processed_annotations
