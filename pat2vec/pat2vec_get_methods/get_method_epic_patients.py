import os
from typing import Union, Optional, List

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

EPIC_PATIENTS_FIELDS = [
    "patient_DurableKey",
    "patient_CreatedWhen",
    "patient_UpdatedWhen",
    "patient_Age",
    "patient_Gender",
    "patient_Ethnicity",
    "patient_SmokingStatus",
    "patient_MaritalStatus",
    "patient_IsCancer",
    "patient_IsFetus",
    "patient_DateOfDeath",
    "id",
]


def search_epic_patients(
    cohort_searcher_with_terms_and_search=None,
    patient_durable_keys=None,
    id_field_name="patient_DurableKey",
    time_field="patient_CreatedWhen",
    fields_override: Optional[List[str]] = None,
    start_year: Union[int, str] = 1995,
    start_month: Union[int, str] = 1,
    start_day: Union[int, str] = 1,
    end_year: Union[int, str] = 2025,
    end_month: Union[int, str] = 12,
    end_day: Union[int, str] = 12,
    additional_custom_search_string=None,
    index_name: str = "epic_patients",
    output_filename: Optional[str] = "epic_patients_results.csv",
    overwrite: bool = False,
    config_obj: Optional[object] = None,
):
    """Searches for Epic patient data for patients within a date range."""
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
        print(f"Loading existing epic patient data from {output_filename}")
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

    fields_to_use = fields_override if fields_override else EPIC_PATIENTS_FIELDS

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
        print(f"Saving epic patient data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def get_epic_patients(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieves epic_patients features for a patient within a date range."""
    if config_obj is None:
        raise ValueError("config_obj cannot be None.")

    batch_mode = config_obj.batch_mode
    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    id_field_name = "patient_DurableKey"
    time_field = "patient_CreatedWhen"

    if pat_batch.empty:
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
        current_pat_raw = search_epic_patients(
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

    # Take the latest record if multiple exist for a patient within the time window
    current_pat_raw = current_pat_raw.sort_values(
        by=time_field, ascending=False
    ).drop_duplicates(subset=["client_idcode"])

    # Extract numerical feature: patient_Age
    if "patient_Age" in current_pat_raw.columns:
        features["epic_pat_age"] = current_pat_raw["patient_Age"].iloc[0]

    # Extract binary features from categorical fields
    categorical_fields = [
        "patient_Gender",
        "patient_Ethnicity",
        "patient_SmokingStatus",
        "patient_MaritalStatus",
    ]
    for field in categorical_fields:
        if field in current_pat_raw.columns:
            value = current_pat_raw[field].iloc[0]
            if pd.notna(value):
                sanitized_value = "".join(c if c.isalnum() else "_" for c in str(value))
                features[
                    f"epic_pat_{field.lower().replace('patient_', '')}_{sanitized_value}"
                ] = 1

    # Extract binary features for boolean/status flags
    boolean_fields = ["patient_IsCancer", "patient_IsFetus"]
    for field in boolean_fields:
        if field in current_pat_raw.columns:
            value = current_pat_raw[field].iloc[0]
            if pd.notna(value):
                features[f"epic_pat_{field.lower().replace('patient_', '')}"] = int(
                    value
                )

    # Extract deceased status
    if "patient_DateOfDeath" in current_pat_raw.columns:
        if pd.notna(current_pat_raw["patient_DateOfDeath"].iloc[0]):
            features["epic_pat_deceased"] = 1
        else:
            features["epic_pat_deceased"] = 0

    if config_obj.verbosity >= 6:
        display(features)

    return features
