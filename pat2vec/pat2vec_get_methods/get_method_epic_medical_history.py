import os
from typing import Union, Optional, List

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

EPIC_MEDICAL_HISTORY_FIELDS = [
    "document_PatientDurableKey",
    "document_CreatedWhen",
    "document_Name",
    "document_Diagnosis",
    "document_Comment",  # Changed to match generator
    "document_SourceId",
    "id",
]


def search_epic_medical_history(
    cohort_searcher_with_terms_and_search=None,
    patient_durable_keys=None,
    id_field_name="document_PatientDurableKey",
    time_field="document_CreatedWhen",
    fields_override: Optional[List[str]] = None,
    start_year: Union[int, str] = 1995,
    start_month: Union[int, str] = 1,
    start_day: Union[int, str] = 1,
    end_year: Union[int, str] = 2025,
    end_month: Union[int, str] = 12,
    end_day: Union[int, str] = 12,
    additional_custom_search_string=None,
    index_name: str = "epic_medical_history",
    output_filename: Optional[str] = "epic_medical_history_results.csv",
    overwrite: bool = False,
    config_obj: Optional[object] = None,
):
    """Searches for Epic medical history data for patients within a date range.

    Args:
        cohort_searcher_with_terms_and_search: The cohort searcher function for
            executing the search query. Cannot be None.
        patient_durable_keys: Patient durable keys (string or list of strings) to
            search. Cannot be None.
        id_field_name (str): Field name for patient ID lookup in Elasticsearch.
            Defaults to "document_PatientDurableKey".
        time_field (str): Timestamp field for date range filtering. Defaults to
            "document_CreatedWhen".
        fields_override (Optional[List[str]]): Custom list of fields to return.
            Uses default EPIC_MEDICAL_HISTORY_FIELDS if None.
        start_year (Union[int, str]): Start year for date range. Defaults to 1995.
        start_month (Union[int, str]): Start month for date range. Defaults to 1.
        start_day (Union[int, str]): Start day for date range. Defaults to 1.
        end_year (Union[int, str]): End year for date range. Defaults to 2025.
        end_month (Union[int, str]): End month for date range. Defaults to 12.
        end_day (Union[int, str]): End day for date range. Defaults to 12.
        additional_custom_search_string: Additional custom search string to append
            to the query.
        index_name (str): Elasticsearch index name to search. Defaults to
            "epic_medical_history".
        output_filename (Optional[str]): Path to save results as CSV. Set to None
            to skip saving. Defaults to "epic_medical_history_results.csv".
        overwrite (bool): If True, re-run search even if output file exists.
            Defaults to False.
        config_obj (Optional[object]): Configuration object with root_path and
            proj_name attributes for path construction. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame containing the searched medical history records.

    Raises:
        ValueError: If cohort_searcher_with_terms_and_search or patient_durable_keys
            is None.
    """
    if (
        output_filename
        and config_obj
        and hasattr(config_obj, "root_path")
        and hasattr(config_obj, "proj_name")
    ):
        output_filename = os.path.join(
            config_obj.root_path, config_obj.proj_name, output_filename
        )

    if output_filename and os.path.exists(output_filename) and not overwrite:
        print(f"Loading existing epic medical history data from {output_filename}")
        return pd.read_csv(output_filename)

    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
    if patient_durable_keys is None:
        raise ValueError("patient_durable_keys cannot be None.")

    if isinstance(patient_durable_keys, str):
        patient_durable_keys = [patient_durable_keys]

    start_year, start_month, start_day, end_year, end_month, end_day = (
        validate_input_dates(
            start_year, start_month, start_day, end_year, end_month, end_day
        )
    )

    search_string = f"{time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    fields_to_use = EPIC_MEDICAL_HISTORY_FIELDS
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
        print(f"Saving epic medical history data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def get_epic_medical_history(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieves epic_medical_history features for a patient within a date range.

    Args:
        current_pat_client_id_code: The client ID code of the patient to retrieve
            data for.
        target_date_range (Tuple): Date range tuple specifying the time period to
            search.
        pat_batch (pd.DataFrame): Patient batch DataFrame for batch mode processing.
        config_obj (Optional[object]): Configuration object with settings like
            batch_mode and verbosity. Cannot be None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The cohort
            searcher function used when batch_mode is False. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame containing extracted medical history features for
            the patient, including binary flags for each unique document name,
            diagnosis, and comment found.

    Raises:
        ValueError: If config_obj is None.
    """
    if config_obj is None:
        raise ValueError("config_obj cannot be None.")

    batch_mode = config_obj.batch_mode
    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    id_field_name = "document_PatientDurableKey"
    time_field = "document_CreatedWhen"

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
        current_pat_raw = search_epic_medical_history(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            patient_durable_keys=current_pat_client_id_code,
            id_field_name=id_field_name,
            time_field=time_field,
            output_filename=None,
            config_obj=config_obj,
        )

    # Standardize identifier column for pat2vec joining
    if id_field_name in current_pat_raw.columns:
        current_pat_raw.rename(columns={id_field_name: "client_idcode"}, inplace=True)

    features = pd.DataFrame(
        data=[current_pat_client_id_code], columns=["client_idcode"]
    )

    if len(current_pat_raw) == 0:
        return features

    # Extract binary features based on document name
    if "document_Name" in current_pat_raw.columns:
        unique_names = current_pat_raw["document_Name"].dropna().unique()
        for name_val in unique_names:
            sanitized_name = "".join(c if c.isalnum() else "_" for c in name_val)
            features[f"epic_med_hist_name_{sanitized_name}"] = 1

    # Extract binary features based on diagnosis
    if "document_Diagnosis" in current_pat_raw.columns:
        unique_diagnoses = current_pat_raw["document_Diagnosis"].dropna().unique()
        for diag_val in unique_diagnoses:
            sanitized_diag = "".join(c if c.isalnum() else "_" for c in diag_val)
            features[f"epic_med_hist_diag_{sanitized_diag}"] = 1

    # Extract binary features based on document_Comment
    if "document_Comment" in current_pat_raw.columns:
        unique_comments = current_pat_raw["document_Comment"].dropna().unique()
        for comment_val in unique_comments:
            sanitized_comment = "".join(c if c.isalnum() else "_" for c in comment_val)
            features[f"epic_med_hist_comment_{sanitized_comment}"] = 1

    if config_obj.verbosity >= 6:
        display(features)

    return features
