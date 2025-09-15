import os
import pickle
import time
from typing import Callable, Optional, Tuple

import numpy as np
import pandas as pd
import paramiko
from IPython.display import display
from IPython.utils import io

from pat2vec.util.filter_dataframe_by_timestamp import \
    filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_annotation import (
    calculate_pretty_name_count_features,
    check_pat_document_annotation_complete)
from pat2vec.util.methods_annotation_filter_annot_dataframe import \
    filter_annot_dataframe
from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import \
    get_pat_document_annotation_batch
from pat2vec.util.methods_get import dump_results, exist_check, update_pbar


def get_current_pat_annotations_mrc_cs(
    current_pat_client_id_code: str,
    target_date_range: Tuple,
    batch_mct_docs_annotations: Optional[pd.DataFrame],
    config_obj: Optional[object] = None,
    t: Optional[object] = None,
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
    cat: Optional[object] = None,
) -> pd.DataFrame:
    """Retrieves and processes MRC document annotations for a patient.

    This function filters a batch of pre-existing MRC (Minimum-Risk-of-Bias
    Clinical Synopsis) document annotations for a specific patient within a
    given date range. It then calculates count-based features from the
    'pretty_name' of the annotations.

    Args:
        current_pat_client_id_code (str): The unique identifier for the patient.
        target_date_range (Tuple): The date range to filter annotations by.
        batch_mct_docs_annotations (Optional[pd.DataFrame]): DataFrame containing
            MCT document annotations for a batch of patients.
        config_obj (Optional[object]): Configuration object with settings such as
            `verbosity` and `start_time`. Defaults to None.
        t (Optional[object]): A progress bar object for updating status. Defaults
            to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): Placeholder
            for a cohort searcher function, unused in this implementation.
            Defaults to None.
        cat (Optional[object]): Placeholder for a MedCAT object, unused in this
            implementation. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated annotation features
            for the specified patient. If no annotations are found, a DataFrame
            with only the 'client_idcode' is returned.

    Raises:
        ValueError: If `config_obj` is None.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration. (get_current_pat_annotations_mrc_cs)"
        )

    start_time = config_obj.start_time

    p_bar_entry = "annotations_mrc_cs"

    update_pbar(
        current_pat_client_id_code,
        start_time,
        0,
        p_bar_entry,
        t,
        config_obj,
        config_obj.skipped_counter,
    )

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    # display(batch_epr_docs_annotations)

    if batch_mct_docs_annotations is not None:

        filtered_batch_mct_docs_annotations = filter_dataframe_by_timestamp(
            batch_mct_docs_annotations,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "observationdocument_recordeddtm",
            dropna=True,
        )

        if len(filtered_batch_mct_docs_annotations) > 0:

            df_pat_target = calculate_pretty_name_count_features(
                filtered_batch_mct_docs_annotations, suffix="mct"
            )
        else:
            if config_obj.verbosity >= 6:
                print(
                    "len(filtered_batch_mct_docs_annotations)>0",
                    len(filtered_batch_mct_docs_annotations) > 0,
                )
            df_pat_target = pd.DataFrame(
                data=[current_pat_client_id_code], columns=["client_idcode"]
            )

    else:
        df_pat_target = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        )

    if config_obj.verbosity >= 6:
        display(df_pat_target)

    return df_pat_target
