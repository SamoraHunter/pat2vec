from typing import Callable, Optional, Tuple

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_annotation import calculate_pretty_name_count_features
from pat2vec.util.methods_get import update_pbar


def get_current_pat_epic_orders_annotations(
    current_pat_client_id_code: str,
    target_date_range: Tuple,
    epic_orders_annotations: Optional[pd.DataFrame],
    config_obj: Optional[object] = None,
    t: Optional[object] = None,
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
    cat: Optional[object] = None,
) -> pd.DataFrame:
    """Retrieves and processes Epic order annotations for a patient.

    This function filters a batch of pre-existing Epic Order annotations
    for a specific patient within a given date range. It then calculates
    count-based features from the 'pretty_name' of the annotations.

    Args:
        current_pat_client_id_code (str): The unique identifier for the patient.
        target_date_range (Tuple): The date range to filter annotations by.
        epic_orders_annotations (Optional[pd.DataFrame]): DataFrame containing
            Epic Order annotations for a batch of patients.
        config_obj (Optional[object]): Configuration object with settings.
        t (Optional[object]): A progress bar object for updating status.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated annotation features.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. (get_current_pat_epic_orders_annotations)"
        )

    start_time = config_obj.start_time
    p_bar_entry = "annotations_epic_orders"

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

    if epic_orders_annotations is not None:
        filtered_annots = filter_dataframe_by_timestamp(
            epic_orders_annotations,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "document_CreatedWhen",
            dropna=True,
        )

        if len(filtered_annots) > 0:
            df_pat_target = calculate_pretty_name_count_features(
                filtered_annots, suffix="epic_orders"
            )
        else:
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
