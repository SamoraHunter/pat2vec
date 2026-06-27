import os
from typing import Union, Optional, List

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

EPIC_CLINICAL_NOTES_FIELDS = [
    "document_PatientDurableKey",
    "document_CreatedWhen",
    "document_ServiceDate",  # Re-added as it might be used in some contexts
    "document_Name",
    "document_Author",
    "document_AuthorSpecialty",
    "document_Content",
    "id",
]


def search_epic_clinical_notes(
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
    index_name: str = "epic_clinical_notes",
    output_filename: Optional[str] = "epic_clinical_notes_results.csv",
    overwrite: bool = False,
    config_obj: Optional[object] = None,
):
    """Searches for Epic clinical notes data for patients within a date range.

    Uses a cohort searcher to query the Elasticsearch index for Epic clinical
    notes. If `output_filename` is provided, the function will attempt to load
    existing data from disk or save the search results to disk.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Required for executing queries.
        patient_durable_keys (Optional[Union[str, List[str]]]): The patient durable key(s)
            for filtering clinical notes. Required for specifying patients.
        id_field_name (str): The field name to filter on for patient keys.
            Defaults to "document_PatientDurableKey".
        time_field (str): The timestamp field for filtering notes.
            Defaults to "document_CreatedWhen".
        fields_override (Optional[List[str]]): A list of fields to override the
            default `EPIC_CLINICAL_NOTES_FIELDS`. Defaults to None.
        start_year (Union[int, str]): Start year for the search. Defaults to 1995.
        start_month (Union[int, str]): Start month for the search. Defaults to 1.
        start_day (Union[int, str]): Start day for the search. Defaults to 1.
        end_year (Union[int, str]): End year for the search. Defaults to 2025.
        end_month (Union[int, str]): End month for the search. Defaults to 12.
        end_day (Union[int, str]): End day for the search. Defaults to 12.
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.
        index_name (str): The name of the Elasticsearch index to search.
            Defaults to "epic_clinical_notes".
        output_filename (Optional[str]): The filename or path to a CSV file to
            load from or save to. Defaults to "epic_clinical_notes_results.csv".
        overwrite (bool): If True, perform the search even if `output_filename`
            exists. Defaults to False.
        config_obj (Optional[object]): Configuration object containing root_path
            and proj_name. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the raw Epic clinical notes data.

    Raises:
        ValueError: When `cohort_searcher_with_terms_and_search` or `patient_durable_keys`
            is None, or when date components are invalid.
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
        print(f"Loading existing epic clinical notes data from {output_filename}")
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

    fields_to_use = EPIC_CLINICAL_NOTES_FIELDS
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
        print(f"Saving epic clinical notes data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def get_epic_clinical_notes(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieves Epic clinical notes features for a patient within a date range.

    This function retrieves Epic clinical notes data, either from a pre-loaded
    batch DataFrame or by querying the database, and then processes it to create
    one-hot encoded binary features based on document names (e.g., "Progress Note").

    Args:
        current_pat_client_id_code: The client ID code of the patient.
        target_date_range (Tuple): A tuple representing the target date range as
            (start_year, start_month, end_year, end_month).
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object. Required for processing settings.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Used when not in batch mode.

    Returns:
        pd.DataFrame: A DataFrame containing Epic clinical notes features for the
            specified patient. Binary columns are created for each unique document name.
            If no data is found, a DataFrame with only the 'client_idcode' is returned.

    Raises:
        ValueError: If `config_obj` is None.
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
        current_pat_raw = search_epic_clinical_notes(
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

    # Extract binary features based on document name (e.g., "Progress Note", "Discharge Summary")
    if "document_Name" in current_pat_raw.columns:
        unique_doc_names = current_pat_raw["document_Name"].dropna().unique()
        for doc_name in unique_doc_names:
            # Sanitize doc_name for column names
            sanitized_doc_name = "".join(c if c.isalnum() else "_" for c in doc_name)
            features[f"epic_note_type_{sanitized_doc_name}"] = 1

    if config_obj.verbosity >= 6:
        display(features)

    return features
