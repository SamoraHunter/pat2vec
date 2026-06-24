import os
from typing import Union, Optional, List

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

EPIC_CLINICAL_NOTES_APPOINTMENTS_FIELDS = [
    "document_PatientDurableKey",
    "document_CreatedWhen",
    "document_UpdatedWhen",
    "document_Name",
    "document_Content",
    "document_EncounterEpicCsn",
    "document_EncounterKey",
    "id",
]


def search_epic_clinical_notes_appointments(
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
    index_name: str = "epic_clinical_notes_appointments",
    output_filename: Optional[str] = "epic_clinical_notes_appointments_results.csv",
    overwrite: bool = False,
    config_obj: Optional[object] = None,
):
    """Searches for Epic clinical notes related to appointments within a date range.

    This function queries an Elasticsearch index for clinical notes and appointments
    associated with specific patients within a specified time range. Results can be
    saved to CSV or loaded from existing files if not overwritten.

    Args:
        cohort_searcher_with_terms_and_search: A callable search function that takes
            index_name, fields_list, term_name, entered_list, and search_string as
            arguments. Required for fetching data.
        patient_durable_keys: Patient identifier(s) to search for. Can be a single
            string or a list of strings.
        id_field_name: Name of the field containing patient identifiers in the index.
            Defaults to "document_PatientDurableKey".
        time_field: Name of the timestamp field to filter on. Defaults to
            "document_CreatedWhen".
        fields_override: Optional list of specific fields to retrieve. If None, uses
            default EPIC_CLINICAL_NOTES_APPOINTMENTS_FIELDS.
        start_year: Start year for the date range filter. Defaults to 1995.
        start_month: Start month for the date range filter (1-12). Defaults to 1.
        start_day: Start day for the date range filter (1-31). Defaults to 1.
        end_year: End year for the date range filter. Defaults to 2025.
        end_month: End month for the date range filter (1-12). Defaults to 12.
        end_day: End day for the date range filter (1-31). Defaults to 12.
        additional_custom_search_string: Optional additional search query string to
            append to the main search. Defaults to None.
        index_name: Name of the Elasticsearch index to search. Defaults to
            "epic_clinical_notes_appointments".
        output_filename: Path where results should be saved as CSV. If None, results
            are not saved to file. Defaults to
            "epic_clinical_notes_appointments_results.csv".
        overwrite: If True, overwrites existing output files. If False and the file
            exists, loads data from the file instead of searching. Defaults to False.
        config_obj: Optional configuration object with root_path and proj_name attributes
            for constructing file paths. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the search results with columns for
            document_PatientDurableKey, document_CreatedWhen, document_UpdatedWhen,
            document_Name, document_Content, and other specified fields.

    Raises:
        ValueError: If cohort_searcher_with_terms_and_search is None or if
            patient_durable_keys is None.
        ValueError: If validate_input_dates fails to validate the date parameters.
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
        print(f"Loading existing data from {output_filename}")
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

    fields_to_use = EPIC_CLINICAL_NOTES_APPOINTMENTS_FIELDS
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
        print(f"Saving data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def get_epic_clinical_notes_appointments(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieves epic_clinical_notes_appointments features for a patient.

    This function processes clinical notes and appointments for a specific patient
    within a given date range. It supports batch mode processing (using pre-filtered
    batches) or real-time search mode.

    Args:
        current_pat_client_id_code: The unique identifier for the patient whose
            annotations are being retrieved.
        target_date_range: A tuple representing the date range to filter notes by.
            Used in conjunction with config_obj to determine start/end dates.
        pat_batch: A DataFrame containing pre-filtered annotation data for a batch
            of patients. Used in batch mode processing.
        config_obj: Configuration object with attributes like batch_mode, verbosity,
            and methods for date handling. Required for determining processing mode.
        cohort_searcher_with_terms_and_search: Optional callable search function used
            when not in batch mode to query the database directly.

    Returns:
        pd.DataFrame: A DataFrame containing extracted features from clinical notes
            and appointments. Includes a 'client_idcode' column and one-hot encoded
            columns for each unique document_Name found (with sanitized names).

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
        current_pat_raw = search_epic_clinical_notes_appointments(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            patient_durable_keys=current_pat_client_id_code,
            id_field_name=id_field_name,
            time_field=time_field,
            output_filename=None,
            config_obj=config_obj,
        )

    if id_field_name in current_pat_raw.columns:
        current_pat_raw.rename(columns={id_field_name: "client_idcode"}, inplace=True)

    features = pd.DataFrame(
        data=[current_pat_client_id_code], columns=["client_idcode"]
    )

    if len(current_pat_raw) == 0:
        return features

    if "document_Name" in current_pat_raw.columns:
        unique_names = current_pat_raw["document_Name"].dropna().unique()
        for name_val in unique_names:
            sanitized_name = "".join(c if c.isalnum() else "_" for c in str(name_val))
            features[f"epic_note_appt_name_{sanitized_name}"] = 1

    if config_obj.verbosity >= 6:
        display(features)

    return features
