import logging
import os

import pandas as pd
from IPython.display import display
from tqdm import tqdm

from pat2vec.util.elasticsearch_index_config import ASCRIBE_TRANSGLOG_FIELDS
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_get import update_pbar
from pat2vec.util.parse_date import validate_input_dates

logger = logging.getLogger(__name__)


def search_ascribe_translog(
    cohort_searcher_with_terms_and_search=None,
    nhs_numbers=None,
    casenumber=None,
    time_field="logdatetime",
    fields_override: list[str] | None = None,
    start_year: int | str = 1995,
    start_month: int | str = 1,
    start_day: int | str = 1,
    end_year: int | str = 2025,
    end_month: int | str = 12,
    end_day: int | str = 12,
    additional_custom_search_string=None,
    index_name: str = "ascribe_translog",
    term_name: str | None = None,
    output_filename: str | None = "ascribe_translog_results.csv",
    overwrite: bool = False,
    config_obj: object | None = None,
    t: tqdm | None = None,
):
    """Searches for ascribe translog data within a date range.

    This function queries an Elasticsearch index for clinical transcription logs
    associated with specific patient identifiers (nhsnumber by default, or casenumber)
    within a specified time range. Results can be saved to CSV or loaded from existing
    files if not overwritten.

    Args:
    ----
        cohort_searcher_with_terms_and_search: A callable search function that takes
            index_name, fields_list, term_name, entered_list, and search_string as
            arguments. Required for fetching data.
        nhs_numbers: NHS number(s) to search for. Can be a single string or a list.
            Will use 'nhsnumber' field in Elasticsearch. This is the default method.
            Defaults to None.
        casenumber: Hospital casenumber(s) (client_idcode/hospital numbers) to search
            for. Can be a single string or a list. Will use 'casenumber' field in
            Elasticsearch. Optional fallback when nhsnumber is not available.
            Defaults to None.
        time_field: Name of the timestamp field to filter on. Defaults to "logdatetime".
        fields_override: Optional list of specific fields to retrieve. If None, uses
            default ASCRIBE_TRANSGLOG_FIELDS.
        start_year: Start year for the date range filter. Defaults to 1995.
        start_month: Start month for the date range filter (1-12). Defaults to 1.
        start_day: Start day for the date range filter (1-31). Defaults to 1.
        end_year: End year for the date range filter. Defaults to 2025.
        end_month: End month for the rate filter (1-12). Defaults to 12.
        end_day: End day for the date range filter (1-31). Defaults to 12.
        additional_custom_search_string: Optional additional search query string to
            append to the main search. Defaults to None.
        index_name: Name of the Elasticsearch index to search. Defaults to
            "ascribe_translog".
        term_name: The field name for filtering. If not provided, defaults to
            "nhsnumber" if nhs_numbers is provided, otherwise "casenumber".
            Defaults to None.
        output_filename: Path where results should be saved as CSV. If None, results
            are not saved to file. Defaults to "ascribe_translog_results.csv".
        overwrite: If True, overwrites existing output files. If False and the file
            exists, loads data from the file instead of searching. Defaults to False.
        config_obj: Optional configuration object with root_path and proj_name attributes
            for constructing file paths. Defaults to None.
        t: Optional tqdm progress bar instance for updating progress during search.
            Defaults to None.

    Returns:
    -------
        pd.DataFrame: A DataFrame containing the search results with columns for
            casenumber/nhsnumber (depending on search field), logdatetime, and other
            translog fields.

    Raises:
    ------
        ValueError: If cohort_searcher_with_terms_and_search is None or if neither
            nhs_numbers nor casenumber is provided.
        t: Optional tqdm progress bar instance for updating progress during search.
            Defaults to None.

    """
    start_time = config_obj.start_time

    update_pbar(
        current_pat_client_id_code="",
        start_time=start_time,
        stage_int=0,
        stage_str="ascribe_translog",
        t=t,
        config_obj=config_obj,
    )

    if (
        output_filename
        and config_obj
        and hasattr(config_obj, "root_path")
        and hasattr(config_obj, "proj_name")
    ):
        output_filename = os.path.join(
            config_obj.root_path,
            config_obj.proj_name,
            output_filename,
        )

    if output_filename and os.path.exists(output_filename) and not overwrite:
        logger.debug(f"Loading existing data from {output_filename}")
        return pd.read_csv(output_filename)

    if cohort_searcher_with_terms_and_search is None:
        msg = "cohort_searcher_with_terms_and_search cannot be None."
        raise ValueError(msg)

    start_year, start_month, start_day, end_year, end_month, end_day = (
        validate_input_dates(
            start_year,
            start_month,
            start_day,
            end_year,
            end_month,
            end_day,
        )
    )

    search_string = f"{time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    fields_to_use = ASCRIBE_TRANSGLOG_FIELDS
    if fields_override:
        fields_to_use = fields_override

    entered_list = None
    actual_term_name = term_name or "nhsnumber"

    if nhs_numbers is not None:
        entered_list = nhs_numbers if isinstance(nhs_numbers, list) else [nhs_numbers]
        actual_term_name = "nhsnumber"
    elif casenumber is not None:
        entered_list = casenumber if isinstance(casenumber, list) else [casenumber]
        actual_term_name = "casenumber"
    else:
        msg = "Either nhs_numbers or casenumber must be provided."
        raise ValueError(msg)

    logger.info(
        f"Ascribe translog search query: index={index_name}, term={actual_term_name}, entered_list={entered_list[:3] if len(entered_list) > 3 else entered_list}, search_string={search_string}",
    )

    results = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_to_use,
        term_name=actual_term_name,
        entered_list=entered_list if isinstance(entered_list, list) else [entered_list],
        search_string=search_string,
    )

    if output_filename:
        if os.path.dirname(output_filename):
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        logger.debug(f"Saving data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def get_ascribe_translog(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
    t=None,
):
    """Retrieves ascribe_translog features for a patient.

    This function processes clinical transcription logs for a specific patient
    within a given date range. It supports batch mode processing (using pre-filtered
    batches) or real-time search mode. By default, uses nhsnumber to search the
    Elasticsearch index.

    Args:
    ----
        current_pat_client_id_code: The unique identifier for the patient whose
            translog data is being retrieved. This should be the NHS number by default.
        target_date_range: A tuple representing the date range to filter logs by.
        pat_batch: A DataFrame containing pre-filtered translog data for a batch
            of patients. Used in batch mode processing.
        config_obj: Configuration object with attributes like batch_mode, verbosity,
            and methods for date handling. Required for determining processing mode.
        cohort_searcher_with_terms_and_search: Optional callable search function used
            when not in batch mode to query Elasticsearch directly using nhsnumber.
        t: Optional progress bar instance for updating status during processing.
            Defaults to None.

    Returns:
    -------
        pd.DataFrame: A DataFrame containing extracted features from translog data.
            Includes a 'client_idcode' column and one-hot encoded columns for each
            unique value found in description, kind, ward, consultant, specialty,
            and transtype fields.

    Raises:
    ------
        ValueError: If config_obj is None.

    """
    if config_obj is None:
        msg = "config_obj cannot be None."
        raise ValueError(msg)

    batch_mode = config_obj.batch_mode
    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    time_field_es = "logdatetime"

    if pat_batch.empty and batch_mode:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    if batch_mode:
        current_pat_raw = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            time_field_es,
        )
    else:
        current_pat_raw = search_ascribe_translog(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            nhs_numbers=current_pat_client_id_code,
            time_field=time_field_es,
            output_filename=None,
            config_obj=config_obj,
            t=t,
        )

    for old_col, new_col in [
        ("nhsnumber", "client_idcode"),
        ("casenumber", "client_idcode"),
    ]:
        if old_col in current_pat_raw.columns:
            current_pat_raw = current_pat_raw.rename(
                columns={old_col: new_col},
            )
            break

    if len(current_pat_raw) == 0:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    feature_columns = [
        "description",
        "kind",
        "ward",
        "consultant",
        "specialty",
        "transtype",
        "storesdescription",
        "directioncode",
        "pack_quantity",
    ]

    features = pd.DataFrame(
        data=[current_pat_client_id_code],
        columns=["client_idcode"],
    )

    for col in feature_columns:
        if col in current_pat_raw.columns:
            unique_values = current_pat_raw[col].dropna().unique()
            for val in unique_values:
                sanitized_val = "".join(c if c.isalnum() else "_" for c in str(val))
                features[f"ascribe_translog_{col}_{sanitized_val}"] = 1

    if config_obj.verbosity >= 6:
        display(features)

    return features
