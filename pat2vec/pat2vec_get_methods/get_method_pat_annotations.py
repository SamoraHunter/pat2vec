import pandas as pd
from IPython.display import display
from pat2vec.util.methods_annotation import (
    calculate_pretty_name_count_features,
)
from pat2vec.util.methods_get import (
    filter_dataframe_by_timestamp,
    get_start_end_year_month,
    update_pbar,
)


def get_current_pat_annotations(
    current_pat_client_id_code,
    target_date_range,
    batch_epr_docs_annotations,
    config_obj=None,
    t=None,
    cohort_searcher_with_terms_and_search=None,
    cat=None,
):
    """
    Retrieves and processes annotations for a specific patient within a given date range.

    This function is responsible for retrieving annotations for a single patient
    within a given time range. It takes in a configuration object, a target date
    range, a DataFrame containing EPR document annotations, and an optional cohort
    searcher and medcat object.

    The function uses the provided configuration to set up parameters such as start
    time and verbosity level. It then filters the batch_epr_docs_annotations DataFrame
    based on the target_date_range. If filtered annotations exist, it calculates
    pretty name count features and returns the resulting DataFrame. If not, it creates
    a DataFrame with the client_idcode. The resulting DataFrame is displayed if the
    verbosity level is 6 or higher.

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
    - TypeError: If batch_epr_docs_annotations is not a pd.DataFrame.

    """

    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration. (get_current_pat_annotations)"
        )

    if batch_epr_docs_annotations is not None and not isinstance(
        batch_epr_docs_annotations, pd.DataFrame
    ):
        raise TypeError("batch_epr_docs_annotations must be a pd.DataFrame.")

    start_time = config_obj.start_time

    p_bar_entry = "annotations_epr"

    update_pbar(
        current_pat_client_id_code,
        start_time,
        0,
        p_bar_entry,
        t,
        config_obj,
        config_obj.skipped_counter,
    )

    # Extract start and end dates from the target date range
    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    # Filter the batch_epr_docs_annotations DataFrame based on the target_date_range
    if batch_epr_docs_annotations is not None:

        # Filter the dataframe based on the target date range
        filtered_batch_epr_docs_annotations = filter_dataframe_by_timestamp(
            batch_epr_docs_annotations,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "updatetime",
            dropna=True,
        )

        if len(filtered_batch_epr_docs_annotations) > 0:

            # Calculate pretty name count features for the filtered dataframe
            df_pat_target = calculate_pretty_name_count_features(
                filtered_batch_epr_docs_annotations
            )

        else:
            # If filtered annotations don't exist, create a DataFrame with the client_idcode
            if config_obj.verbosity >= 6:
                print(
                    "len(filtered_batch_epr_docs_annotations)>0",
                    len(filtered_batch_epr_docs_annotations) > 0,
                )
            df_pat_target = pd.DataFrame(
                data=[current_pat_client_id_code], columns=["client_idcode"]
            )

    else:
        # If the batch_epr_docs_annotations DataFrame is None, create a DataFrame with the client_idcode
        df_pat_target = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        )

    if config_obj.verbosity >= 6:
        # Display the processed DataFrame if the verbosity level is 6 or higher
        display(df_pat_target)

    return df_pat_target
