import logging
import os

import pandas as pd
from IPython.display import display
from tqdm import tqdm

from pat2vec.util.elasticsearch_index_config import EPIC_LAB_RESULTS_FIELDS
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_get import update_pbar
from pat2vec.util.parse_date import validate_input_dates

logger = logging.getLogger(__name__)


def search_epic_lab_results(
    cohort_searcher_with_terms_and_search=None,
    patient_durable_keys=None,
    id_field_name="document_PatientDurableKey",
    time_field="document_CollectedDate",
    fields_override: list[str] | None = None,
    start_year: int | str = 1995,
    start_month: int | str = 1,
    start_day: int | str = 1,
    end_year: int | str = 2025,
    end_month: int | str = 12,
    end_day: int | str = 12,
    additional_custom_search_string=None,
    index_name: str = "epic_lab_results",
    output_filename: str | None = "epic_lab_results_results.csv",
    overwrite: bool = False,
    config_obj: object | None = None,
    t: tqdm | None = None,
):
    """Searches for Epic lab results data for patients within a date range.

    Args:
    ----
        cohort_searcher_with_terms_and_search: A callable function that performs
            the cohort search with terms and search string. Cannot be None.
        patient_durable_keys: Patient durable keys (DurableKey) to search for.
            Can be a single string or list of strings. Cannot be None.
        id_field_name: Name of the field containing the patient identifier.
            Defaults to "document_PatientDurableKey".
        time_field: Name of the field containing the timestamp for date filtering.
            Defaults to "document_CollectedDate".
        fields_override: Optional list of field names to include in the search
            results. If None, uses EPIC_LAB_RESULTS_FIELDS defaults.
        start_year: Starting year for date range filtering. Defaults to 1995.
        start_month: Starting month for date range filtering. Defaults to 1.
        start_day: Starting day for date range filtering. Defaults to 1.
        end_year: Ending year for date range filtering. Defaults to 2025.
        end_month: Ending month for date range filtering. Defaults to 12.
        end_day: Ending day for date range filtering. Defaults to 12.
        additional_custom_search_string: Optional custom search string to append
            to the main search query.
        index_name: Name of the Elasticsearch index to search. Defaults to
            "epic_lab_results".
        output_filename: Optional filename to save results as CSV. If provided
            and file exists, will load existing data unless overwrite=True.
            Defaults to "epic_lab_results_results.csv".
        overwrite: If True, overwrites existing output file. Defaults to False.
        config_obj: Configuration object with `root_path` and `proj_name`
            attributes for constructing output path. Can be None.
        t: Optional tqdm progress bar instance for updating progress during search.
            Defaults to None.

    Returns:
    -------
        pd.DataFrame: DataFrame containing the search results matching the
            specified criteria. Columns depend on the fields searched.

    Raises:
    ------
        ValueError: If `cohort_searcher_with_terms_and_search` is None.
        ValueError: If `patient_durable_keys` is None.

    """
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

    start_time = config_obj.start_time

    update_pbar(
        current_pat_client_id_code="",
        start_time=start_time,
        stage_int=0,
        stage_str="epic_lab_results",
        t=t,
        config_obj=config_obj,
    )

    if output_filename and os.path.exists(output_filename) and not overwrite:
        logger.debug(f"Loading existing epic lab results data from {output_filename}")
        return pd.read_csv(output_filename)

    if cohort_searcher_with_terms_and_search is None:
        msg = "cohort_searcher_with_terms_and_search cannot be None."
        raise ValueError(msg)
    if patient_durable_keys is None:
        msg = "patient_durable_keys cannot be None."
        raise ValueError(msg)

    if isinstance(patient_durable_keys, str):
        patient_durable_keys = [patient_durable_keys]

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

    fields_to_use = EPIC_LAB_RESULTS_FIELDS
    if fields_override:
        fields_to_use = fields_override

    results = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_to_use,
        term_name=id_field_name,
        entered_list=patient_durable_keys,
        search_string=search_string,
    )

    if output_filename:
        if os.path.dirname(output_filename):
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        logger.debug(f"Saving epic lab results data to {output_filename}")
        results.to_csv(output_filename, index=False)

    # Rename ES columns to match database schema expectations
    column_mapping = {
        "document_PatientDurableKey": "client_idcode",
        "document_CollectedDate": "document_CollectedDate",  # Already correct per MAPPINGS
        "id": "document_guid",
        "document_Name": "document_description",
        "document_Content": "body_analysed",
    }

    for old_col, new_col in column_mapping.items():
        if old_col in results.columns:
            results = results.rename(columns={old_col: new_col})

    return results


def get_epic_lab_results(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
    t=None,
):
    """Retrieves epic_lab_results features for a patient within a date range.

    Args:
    ----
        current_pat_client_id_code: The unique identifier code for the patient
            to retrieve lab result data for.
        target_date_range: The date range tuple specifying the time period to
            search for lab results.
        pat_batch: DataFrame containing batched patient records. Used in batch
            mode to filter data by timestamp.
        config_obj: Configuration object with attributes like `batch_mode`,
            `verbosity`, and methods like `get_start_end_year_month`. Cannot
            be None.
        cohort_searcher_with_terms_and_search: Optional callable function for
            searching lab results when not in batch mode. Required when
            batch_mode=False.

    Returns:
    -------
        pd.DataFrame: DataFrame containing extracted features from lab results
            including binary flags for unique lab names and abnormal levels
            observed. Always includes 'client_idcode' column.

    Raises:
    ------
        ValueError: If `config_obj` is None.

    """
    if config_obj is None:
        msg = "config_obj cannot be None."
        raise ValueError(msg)

    batch_mode = config_obj.batch_mode
    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    id_field_name = "document_PatientDurableKey"
    time_field = "document_CollectedDate"

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
            time_field,
        )
    else:
        current_pat_raw = search_epic_lab_results(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            patient_durable_keys=current_pat_client_id_code,
            id_field_name=id_field_name,
            time_field=time_field,
            output_filename=None,
            config_obj=config_obj,
            t=t,
        )

    # Standardize identifier column for pat2vec joining
    if id_field_name in current_pat_raw.columns:
        current_pat_raw = current_pat_raw.rename(
            columns={id_field_name: "client_idcode"},
        )

    features = pd.DataFrame(
        data=[current_pat_client_id_code],
        columns=["client_idcode"],
    )

    if len(current_pat_raw) == 0:
        return features

    # Extract binary features based on document_Name/document_description (renamed column)
    lab_name_col = (
        "document_Name"
        if "document_Name" in current_pat_raw.columns
        else "document_description"
    )
    if lab_name_col in current_pat_raw.columns:
        unique_lab_names = current_pat_raw[lab_name_col].dropna().unique()
        for name_val in unique_lab_names:
            sanitized_name = "".join(c if c.isalnum() else "_" for c in name_val)
            features[f"epic_lab_name_{sanitized_name}"] = 1

    # Extract binary features based on document_AbnormalLevel (may not exist in dummy data)
    abnormal_col = "document_AbnormalLevel"
    if abnormal_col in current_pat_raw.columns:
        unique_abnormal_levels = current_pat_raw[abnormal_col].dropna().unique()
        for level_val in unique_abnormal_levels:
            sanitized_level = "".join(c if c.isalnum() else "_" for c in level_val)
            features[f"epic_lab_abnormal_{sanitized_level}"] = 1

    if config_obj.verbosity >= 6:
        display(features)

    return features
