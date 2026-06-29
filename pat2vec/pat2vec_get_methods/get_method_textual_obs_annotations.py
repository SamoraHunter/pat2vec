from typing import Callable, Optional, Tuple

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_annotation import calculate_pretty_name_count_features
from pat2vec.util.methods_get import update_pbar


def get_current_pat_textual_obs_annotations(
    current_pat_client_id_code: str,
    target_date_range: Tuple,
    textual_obs_annotations: Optional[pd.DataFrame],
    config_obj: Optional[object] = None,
    t: Optional[object] = None,
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
    cat: Optional[object] = None,
) -> pd.DataFrame:
    """Retrieves and processes textual observation annotations for a patient.

    This function filters a batch of pre-existing textual observation annotations
    for a specific patient within a given date range. It then calculates
    count-based features from the 'pretty_name' of the annotations.

    Args:
        current_pat_client_id_code: The unique identifier for the patient.
        target_date_range: A tuple containing (start_date, end_date) defining
            the time period to filter annotations by.
        textual_obs_annotations: DataFrame containing textual observation
            annotations for a batch of patients. Must contain 'basicobs_entered'
            column for timestamp filtering and 'pretty_name' for feature
            extraction.
        config_obj: Configuration object with settings such as `verbosity` and
            `start_time`. Cannot be None.
        t: Optional progress bar object for updating status during processing.
            Defaults to None.
        cohort_searcher_with_terms_and_search: Placeholder for a cohort searcher
            function, unused in this implementation. Defaults to None.
        cat: Placeholder for a MedCAT object, unused in this implementation.
            Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated annotation features
            for the specified patient. If no annotations are found, returns a
            DataFrame with only the 'client_idcode' column.

    Raises:
        ValueError: If `config_obj` is None.
        ValueError: If `textual_obs_annotations` is None.
        ValueError: If `current_pat_client_id_code` is None.
        ValueError: If `target_date_range` is None.
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
        # Use textual_obs_time_field from config, default to "basicobs_entered"
        time_column = getattr(config_obj, "textual_obs_time_field", "basicobs_entered")

        # Standardize time column names to match the configured field
        # Check for alternative time columns that might exist in textual obs data
        if time_column not in textual_obs_annotations.columns:
            alternative_columns = [
                "updatetime",
                "observationdocument_recordeddtm",
                "document_CreatedWhen",
                "basicobs_entered",
            ]
            found_col = None
            for alt_col in alternative_columns:
                if alt_col in textual_obs_annotations.columns:
                    found_col = alt_col
                    break

            # Rename to time_column if a source column was found
            if found_col and found_col != time_column:
                textual_obs_annotations = textual_obs_annotations.rename(
                    columns={found_col: time_column}
                )

        filtered_textual_obs_annotations = filter_dataframe_by_timestamp(
            textual_obs_annotations,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            time_column,
            dropna=True,
        )

        # if there are any filtered annotations, calculate the pretty name count features
        if len(filtered_textual_obs_annotations) > 0:

            processed_annotations = calculate_pretty_name_count_features(
                filtered_textual_obs_annotations, suffix="textual_obs"
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

    return processed_annotations  # Return the processed annotation vector
