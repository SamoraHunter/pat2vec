import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.methods_get import (
    filter_dataframe_by_timestamp,
    get_start_end_year_month,
)


def get_appointments(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieves pims_apps features for a given patient within a specified date range.

    Parameters:
    - current_pat_client_id_code (str): The client ID code of the patient.
    - target_date_range (tuple): A tuple representing the target date range.
    - pat_batch (pd.DataFrame): The DataFrame containing patient data.
    - batch_mode (bool, optional): Indicates whether batch mode is enabled. Defaults to False.
    - cohort_searcher_with_terms_and_search (callable, optional): The function for cohort searching. Defaults to None.

    Returns:
    - pd.DataFrame: A DataFrame containing pims_apps features for the specified patient.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration.",
            get_appointments,
        )

    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )
    search_term = ""

    appointments_time_field = config_obj.appointments_time_field

    if batch_mode:
        current_pat_raw = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            timestamp_string={appointments_time_field},
        )
    else:
        current_pat_raw = cohort_searcher_with_terms_and_search(
            index_name="pims_apps*",
            fields_list=[
                "Popular",
                "AppointmentType",
                "AttendanceReference",
                "ClinicCode",
                "ClinicDesc",
                "Consultant",
                "DateModified",
                "DNA",
                "HospitalID",
                "PatNHSNo",
                "Specialty",
                "AppointmentDateTime",
                "Attended",
                "CancDesc",
                "CancRefNo",
                "ConsultantCode",
                "DateCreated",
                "Ethnicity",
                "Gender",
                "NHSNoStatusCode",
                "NotSpec",
                "PatDateOfBirth",
                "PatForename",
                "PatPostCode",
                "PatSurname",
                "PiMsPatRefNo",
                "Primarykeyfieldname",
                "Primarykeyfieldvalue",
                "SessionCode",
                "SpecialtyCode",
            ],
            term_name="HospitalID.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f"{appointments_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
        )

    current_pat_raw.rename(columns={"HospitalID": "client_idcode"}, inplace=True)

    if len(current_pat_raw) == 0:

        features = pd.DataFrame(
            data=[current_pat_client_id_code], columns=["client_idcode"]
        )

    else:

        current_pat_raw = current_pat_raw[current_pat_raw["Attended"].astype(int) == 1]

        # Calculate value counts for consultant_code, ClinicCode, and AppointmentType
        counts_consultant = current_pat_raw["ConsultantCode"].value_counts()
        counts_clinic = current_pat_raw["ClinicCode"].value_counts()
        counts_appointment_type = current_pat_raw["AppointmentType"].value_counts()

        # Create a dictionary to hold the new feature vector
        feature_vector = {}

        # Iterate over counts_consultant and populate the feature vector
        for consultant, count in counts_consultant.items():
            feature_vector[f"ConsultantCode_{consultant}"] = count

        # Iterate over counts_clinic and populate the feature vector
        for clinic, count in counts_clinic.items():
            feature_vector[f"ClinicCode_{clinic}"] = count

        # Iterate over counts_appointment_type and populate the feature vector
        for appointment_type, count in counts_appointment_type.items():
            feature_vector[f"AppointmentType_{appointment_type}"] = count

        # Convert the feature vector into a DataFrame with a single row
        features = pd.DataFrame([feature_vector])

        features["client_idcode"] = current_pat_client_id_code

    if config_obj.verbosity >= 6:
        display(features)

    return features
