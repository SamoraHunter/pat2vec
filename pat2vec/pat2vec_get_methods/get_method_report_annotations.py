import logging
from collections.abc import Callable

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_annotation import calculate_pretty_name_count_features
from pat2vec.util.methods_get import update_pbar

logger = logging.getLogger(__name__)


def get_current_pat_report_annotations(
    current_pat_client_id_code: str,
    target_date_range: tuple,
    report_annotations: pd.DataFrame | None,
    config_obj: object | None = None,
    t: object | None = None,
    cohort_searcher_with_terms_and_search: Callable | None = None,
    cat: object | None = None,
) -> pd.DataFrame:
    """Retrieves and processes report annotations for a patient.

    This function filters a batch of pre-existing report annotations for a
    specific patient within a given date range. It then calculates count-based
    features from the 'pretty_name' of the annotations.

    Args:
    ----
        current_pat_client_id_code (str): The unique identifier for the patient.
        target_date_range (Tuple): The date range to filter annotations by.
        report_annotations (Optional[pd.DataFrame]): DataFrame containing
            report annotations for a batch of patients.
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
        msg = "config_obj cannot be None. Please provide a valid configuration."
        raise ValueError(
            msg,
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
        logger.debug(e)

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    # Get all unique pretty names from the full batch for expected_names
    # This ensures even when filtered results are empty, we have consistent feature structure
    unique_pretty_names = None
    if (
        report_annotations is not None
        and not report_annotations.empty
        and "pretty_name" in report_annotations.columns
    ):
        # Get all unique pretty names, then filter out NULL/empty ones
        all_values = report_annotations["pretty_name"].unique()
        # Filter to non-null, non-empty strings
        unique_pretty_names = [
            v for v in all_values if pd.notna(v) and str(v).strip() != ""
        ]

    if report_annotations is not None:
        # Use reports_time_field from config, default to "updatetime"
        time_column = getattr(config_obj, "reports_time_field", "updatetime")

        # Standardize time column names to match the configured field
        # Check for alternative time columns that might exist in reports data
        if time_column not in report_annotations.columns:
            alternative_columns = [
                "basicobs_entered",
                "observationdocument_recordeddtm",
                "document_CreatedWhen",
                "updatetime",
            ]
            found_col = None
            for alt_col in alternative_columns:
                if alt_col in report_annotations.columns:
                    found_col = alt_col
                    break

            # Rename to time_column if a source column was found
            if found_col and found_col != time_column:
                report_annotations = report_annotations.rename(
                    columns={found_col: time_column},
                )

        filtered_report_annotations = filter_dataframe_by_timestamp(
            report_annotations,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            time_column,
            dropna=True,
        )

        if len(filtered_report_annotations) > 0:
            processed_annotations = calculate_pretty_name_count_features(
                filtered_report_annotations,
                suffix="reports",
                patient_id=current_pat_client_id_code,
                expected_names=unique_pretty_names,
            )
        else:
            # When filtered annotations are empty, return just client_idcode (no features)
            processed_annotations = pd.DataFrame(
                data=[current_pat_client_id_code],
                columns=["client_idcode"],
            )

    else:
        processed_annotations = pd.DataFrame(
            data=[current_pat_client_id_code],
            columns=["client_idcode"],
        )

    if config_obj.verbosity >= 6:
        display(processed_annotations)

    return processed_annotations
