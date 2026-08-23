from collections.abc import Callable

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_annotation import (
    calculate_pretty_name_count_features,
)
from pat2vec.util.methods_get import update_pbar


def get_current_pat_annotations_mrc_cs(
    current_pat_client_id_code: str,
    target_date_range: tuple,
    batch_mct_docs_annotations: pd.DataFrame | None,
    config_obj: object | None = None,
    t: object | None = None,
    cohort_searcher_with_terms_and_search: Callable | None = None,
    cat: object | None = None,
) -> pd.DataFrame:
    """Retrieves and processes MRC document annotations for a patient.

    This function filters a batch of pre-existing MRC (Minimum-Risk-of-Bias
    Clinical Synopsis) document annotations for a specific patient within a
    given date range. It then calculates count-based features from the
    'pretty_name' of the annotations.

    Args:
    ----
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
    -------
        pd.DataFrame: A DataFrame containing the calculated annotation features
            for the specified patient. If no annotations are found, a DataFrame
            with only the 'client_idcode' is returned.

    Raises:
    ------
        ValueError: If `config_obj` is None.

    """
    if config_obj is None:
        msg = "config_obj cannot be None. Please provide a valid configuration. (get_current_pat_annotations_mrc_cs)"
        raise ValueError(
            msg,
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

    # Get all unique pretty names from the full batch for expected_names
    # This ensures even when filtered results are empty, we have consistent feature structure
    unique_pretty_names = None
    if (
        batch_mct_docs_annotations is not None
        and not batch_mct_docs_annotations.empty
        and "pretty_name" in batch_mct_docs_annotations.columns
    ):
        unique_pretty_names = (
            batch_mct_docs_annotations["pretty_name"].dropna().unique()
        )

    # display(batch_epr_docs_annotations)

    filtered_batch_mct_docs_annotations = None
    if batch_mct_docs_annotations is not None and len(batch_mct_docs_annotations) > 0:
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

    if (
        filtered_batch_mct_docs_annotations is not None
        and len(filtered_batch_mct_docs_annotations) > 0
    ):
        df_pat_target = calculate_pretty_name_count_features(
            filtered_batch_mct_docs_annotations,
            suffix="mct",
            patient_id=current_pat_client_id_code,
            expected_names=unique_pretty_names,
        )
    elif unique_pretty_names is not None and len(unique_pretty_names) > 0:
        # When filtered annotations are empty but we have expected names,
        # create zero-valued columns for each unique pretty_name from source
        feature_columns = [
            f"pretty_name_count_mct_{name}" for name in unique_pretty_names
        ]
        df_pat_target = pd.DataFrame(
            {
                "client_idcode": [current_pat_client_id_code],
                **{col: [0.0] for col in feature_columns},
            },
        )
    else:
        # No pretty names available - return just client_idcode
        df_pat_target = pd.DataFrame(
            data=[current_pat_client_id_code],
            columns=["client_idcode"],
        )

    if config_obj.verbosity >= 6:
        display(df_pat_target)

    return df_pat_target
