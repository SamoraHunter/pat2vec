from collections.abc import Callable

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_annotation import calculate_pretty_name_count_features
from pat2vec.util.methods_get import update_pbar


def get_current_pat_epic_orders_annotations(
    current_pat_client_id_code: str,
    target_date_range: tuple,
    epic_orders_annotations: pd.DataFrame | None,
    config_obj: object | None = None,
    t: object | None = None,
    cohort_searcher_with_terms_and_search: Callable | None = None,
    cat: object | None = None,
) -> pd.DataFrame:
    """Retrieves and processes Epic order annotations for a patient.

    This function filters a batch of pre-existing Epic Order annotations
    for a specific patient within a given date range. It then calculates
    count-based features from the 'pretty_name' of the annotations.

    Args:
        current_pat_client_id_code (str): The unique identifier for the patient.
        target_date_range (Tuple): A tuple containing the start and end dates for
            filtering annotations as (start_year, start_month, end_year, end_month).
        epic_orders_annotations (Optional[pd.DataFrame]): DataFrame containing
            Epic Order annotations for a batch of patients. Can be None if no
            batch data is available.
        config_obj (Optional[object]): Configuration object with settings such as
            `batch_mode`, `verbosity`, `start_time`. Required for determining
            processing behavior.
        t (Optional[object]): A progress bar object for updating status during
            processing. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): Optional callable
            search function for direct database queries. Currently unused but included
            for API consistency. Defaults to None.
        cat (Optional[object]): Optional MedCAT object for annotation processing.
            Currently unused but included for API consistency. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated annotation features for
            the specified patient. Includes 'client_idcode' column and count-based
            features derived from 'pretty_name' values (e.g., epic_orders_pretty_name_X).

    Raises:
        ValueError: If `config_obj` is None.

    Examples:
        >>> config = ConfigObject(batch_mode=True, verbosity=0)
        >>> result = get_current_pat_epic_orders_annotations(
        ...     current_pat_client_id_code="12345",
        ...     target_date_range=(2020, 1, 2021, 12),
        ...     epic_orders_annotations=batch_df,
        ...     config_obj=config
        ... )
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
        # Handle empty DataFrames or DataFrames without timestamp column
        if (
            epic_orders_annotations.empty
            or "updatetime" not in epic_orders_annotations.columns
            and "document_CreatedWhen" not in epic_orders_annotations.columns
        ):
            # Empty or missing required columns - return just client_idcode
            df_pat_target = pd.DataFrame(
                data=[current_pat_client_id_code], columns=["client_idcode"]
            )
            return df_pat_target

        # Handle column name mismatch: annotations use 'updatetime' but we check for 'document_CreatedWhen'
        time_column = "document_CreatedWhen"
        alternative_columns = [
            "updatetime",
            "basicobs_entered",
            "observationdocument_recordeddtm",
            "document_CreatedWhen",
        ]
        found_col = None
        for alt_col in alternative_columns:
            if alt_col in epic_orders_annotations.columns:
                found_col = alt_col
                break

        # If no timestamp column found, return empty result
        if not found_col:
            df_pat_target = pd.DataFrame(
                data=[current_pat_client_id_code], columns=["client_idcode"]
            )
            return df_pat_target

        if found_col and found_col != time_column:
            epic_orders_annotations = epic_orders_annotations.rename(
                columns={found_col: time_column}
            )

        filtered_annots = filter_dataframe_by_timestamp(
            epic_orders_annotations,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            time_column,
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
