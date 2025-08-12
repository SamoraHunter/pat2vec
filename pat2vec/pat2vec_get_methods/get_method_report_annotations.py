import pandas as pd
from IPython.display import display
from IPython.utils import io

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_annotation import calculate_pretty_name_count_features
from pat2vec.util.methods_get import (
    update_pbar,
)


def get_current_pat_report_annotations(
    current_pat_client_id_code,
    target_date_range,
    report_annotations,
    config_obj=None,
    t=None,
    cohort_searcher_with_terms_and_search=None,
    cat=None,
):
    """
    Retrieves and processes annotations for a specific patient within a given date range.

    Parameters:
    - client_idcode (str): The unique identifier for the patient.
    - target_date_range (str): The date range in the format '(YYYY,MM,DD)'.
    - report_annotations (pd.DataFrame): DataFrame containing report annotations.
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
    - It filters the report_annotations DataFrame based on the target_date_range.
    - If filtered annotations exist, it calculates pretty name count features; otherwise, it creates a DataFrame with the client_idcode.
    - The resulting DataFrame is displayed if the verbosity level is 6 or higher.

    Example:
    ```python
    annotations = get_current_pat_report_annotations('patient123', '(2023,1,1)', report_annotations, config_obj)
    ```
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration."
        )

    start_time = config_obj.start_time

    p_bar_entry = "annotations_report"
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

    if report_annotations is not None:

        filtered_report_annotations = filter_dataframe_by_timestamp(
            report_annotations,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "updatetime",
            dropna=True,
        )

        if len(filtered_report_annotations) > 0:

            processed_annotations = calculate_pretty_name_count_features(
                filtered_report_annotations
            )

        else:
            if config_obj.verbosity >= 6:
                print(
                    "len(filtered_report_annotations)>0",
                    len(filtered_report_annotations) > 0,
                )
            processed_annotations = pd.DataFrame(
                data=[current_pat_client_id_code], columns=["client_idcode"]
            )

    else:
        processed_annotations = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        )

    if config_obj.verbosity >= 6:
        display(processed_annotations)

    return processed_annotations
