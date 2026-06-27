import os
from typing import Union, Optional, List

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

EPIC_ENCOUNTER_FIELDS = [
    "activity_PatientDurableKey",
    "activity_AdmissionDate",
    "activity_DischargeDate",
    "activity_Department",
    "activity_Type",
    "activity_VisitClass",
    "activity_HospitalService",
    "id",
]


def search_epic_encounters(
    cohort_searcher_with_terms_and_search=None,
    patient_durable_keys=None,
    id_field_name="activity_PatientDurableKey",
    time_field="activity_AdmissionDate",
    fields_override: Optional[List[str]] = None,
    start_year: Union[int, str] = 1995,
    start_month: Union[int, str] = 1,
    start_day: Union[int, str] = 1,
    end_year: Union[int, str] = 2025,
    end_month: Union[int, str] = 12,
    end_day: Union[int, str] = 12,
    additional_custom_search_string=None,
    index_name: str = "epic_encounters",
    output_filename: Optional[str] = "epic_encounters_results.csv",
    overwrite: bool = False,
    config_obj: Optional[object] = None,
):
    """Searches for Epic encounter data for patients within a date range.

    Args:
        cohort_searcher_with_terms_and_search: A callable function that performs
            the cohort search with terms and search string. Cannot be None.
        patient_durable_keys: Patient durable keys (DurableKey) to search for.
            Can be a single string or list of strings. Cannot be None.
        id_field_name: Name of the field containing the patient identifier.
            Defaults to "activity_PatientDurableKey".
        time_field: Name of the field containing the timestamp for date filtering.
            Defaults to "activity_AdmissionDate".
        fields_override: Optional list of field names to include in the search
            results. If None, uses EPIC_ENCOUNTER_FIELDS defaults.
        start_year: Starting year for date range filtering. Defaults to 1995.
        start_month: Starting month for date range filtering. Defaults to 1.
        start_day: Starting day for date range filtering. Defaults to 1.
        end_year: Ending year for date range filtering. Defaults to 2025.
        end_month: Ending month for date range filtering. Defaults to 12.
        end_day: Ending day for date range filtering. Defaults to 12.
        additional_custom_search_string: Optional custom search string to append
            to the main search query.
        index_name: Name of the Elasticsearch index to search. Defaults to
            "epic_encounters".
        output_filename: Optional filename to save results as CSV. If provided
            and file exists, will load existing data unless overwrite=True.
            Defaults to "epic_encounters_results.csv".
        overwrite: If True, overwrites existing output file. Defaults to False.
        config_obj: Configuration object with `root_path` and `proj_name`
            attributes for constructing output path. Can be None.

    Returns:
        pd.DataFrame: DataFrame containing the search results matching the
            specified criteria. Columns depend on the fields searched.

    Raises:
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
            config_obj.root_path, config_obj.proj_name, output_filename
        )

    if output_filename and os.path.exists(output_filename) and not overwrite:
        print(f"Loading existing epic encounters data from {output_filename}")
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

    fields_to_use = EPIC_ENCOUNTER_FIELDS
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
        print(f"Saving epic encounters data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def get_epic_encounters(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieves epic_encounters features for a patient within a date range.

    Args:
        current_pat_client_id_code: The unique identifier code for the patient
            to retrieve encounter data for.
        target_date_range: The date range tuple specifying the time period to
            search for encounter data.
        pat_batch: DataFrame containing batched patient records. Used in batch
            mode to filter data by timestamp.
        config_obj: Configuration object with attributes like `batch_mode`,
            `verbosity`, and methods like `get_start_end_year_month`. Cannot
            be None.
        cohort_searcher_with_terms_and_search: Optional callable function for
            searching encounter data when not in batch mode. Required when
            batch_mode=False.

    Returns:
        pd.DataFrame: DataFrame containing extracted features from encounters
            including binary flags for unique encounter types and visit classes
            observed. Always includes 'client_idcode' column.

    Raises:
        ValueError: If `config_obj` is None.
    """
    if config_obj is None:
        raise ValueError("config_obj cannot be None.")

    batch_mode = config_obj.batch_mode
    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    id_field_name = "activity_PatientDurableKey"
    time_field = "activity_AdmissionDate"

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
        current_pat_raw = search_epic_encounters(
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

    # Extract binary features based on activity type (e.g., Inpatient, Outpatient)
    if "activity_Type" in current_pat_raw.columns:
        unique_types = current_pat_raw["activity_Type"].dropna().unique()
        for t_val in unique_types:
            features[f"epic_enc_type_{t_val}"] = 1

    # Extract binary features based on visit class (e.g., Office Visit)
    if "activity_VisitClass" in current_pat_raw.columns:
        unique_classes = current_pat_raw["activity_VisitClass"].dropna().unique()
        for c_val in unique_classes:
            features[f"epic_enc_class_{c_val}"] = 1

    if config_obj.verbosity >= 6:
        display(features)

    return features
