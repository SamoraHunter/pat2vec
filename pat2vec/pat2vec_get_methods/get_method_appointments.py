import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

APPOINTMENT_FIELDS = [
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
]


def search_appointments(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    appointments_time_field="AppointmentDateTime",
    start_year="1995",
    start_month="01",
    start_day="01",
    end_year="2025",
    end_month="12",
    end_day="12",
    additional_custom_search_string=None,
):
    """Searches for appointment data for a specific patient within a date range.

    Uses a cohort searcher to find appointment data.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        appointments_time_field (str): The timestamp field for filtering
            appointments. Defaults to 'AppointmentDateTime'.
        start_year (str): Start year for the search. Defaults to '1995'.
        start_month (str): Start month for the search. Defaults to '01'.
        start_day (str): Start day for the search. Defaults to '01'.
        end_year (str): End year for the search. Defaults to '2025'.
        end_month (str): End month for the search. Defaults to '12'.
        end_day (str): End day for the search. Defaults to '12'.
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the raw appointment data.
    """
    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")
    if appointments_time_field is None:
        raise ValueError("appointments_time_field cannot be None.")
    if any(
        x is None
        for x in [start_year, start_month, start_day, end_year, end_month, end_day]
    ):
        raise ValueError("Date components cannot be None.")
    # Ensure client_id_codes is a list for the search function
    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = (
        validate_input_dates(
            start_year, start_month, start_day, end_year, end_month, end_day
        )
    )

    search_string = f"{appointments_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    return cohort_searcher_with_terms_and_search(
        index_name="pims_apps*",
        fields_list=APPOINTMENT_FIELDS,
        term_name="HospitalID.keyword",
        entered_list=client_id_codes,
        search_string=search_string,
    )


def get_appointments(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieves pims_apps features for a given patient within a date range.

    This function retrieves appointment data, either from a pre-loaded batch
    DataFrame or by searching, and then processes it to create one-hot encoded
    features for consultant, clinic, and appointment type.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing pims_apps features for the
            specified patient. If no data is found, a DataFrame with only the
            'client_idcode' is returned.
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
            timestamp_string=appointments_time_field,
        )
    else:
        current_pat_raw = search_appointments(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            appointments_time_field=appointments_time_field,
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
        )

    current_pat_raw.rename(columns={"HospitalID": "client_idcode"}, inplace=True)

    if len(current_pat_raw) == 0:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    else:
        current_pat_raw = current_pat_raw[current_pat_raw["Attended"].astype(int) == 1]

        if not current_pat_raw.empty:
            # One-hot encode and sum up attendances per patient
            consultant_features = (
                pd.get_dummies(
                    current_pat_raw, columns=["ConsultantCode"], prefix="ConsultantCode"
                )
                .groupby("client_idcode")
                .sum(numeric_only=True)
                .reset_index()
            )
            clinic_features = (
                pd.get_dummies(
                    current_pat_raw, columns=["ClinicCode"], prefix="ClinicCode"
                )
                .groupby("client_idcode")
                .sum(numeric_only=True)
                .reset_index()
            )
            appointment_type_features = (
                pd.get_dummies(
                    current_pat_raw,
                    columns=["AppointmentType"],
                    prefix="AppointmentType",
                )
                .groupby("client_idcode")
                .sum(numeric_only=True)
                .reset_index()
            )

            # Merge all features
            features = consultant_features.merge(
                clinic_features, on="client_idcode", how="outer"
            ).merge(appointment_type_features, on="client_idcode", how="outer")
        else:
            features = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

        # Ensure all requested patients are in the output, filling missing ones with NaN/0
        all_clients_df = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})
        all_clients_df["client_idcode"] = all_clients_df["client_idcode"].astype(str)
        features["client_idcode"] = features["client_idcode"].astype(str)

        features = pd.merge(all_clients_df, features, on="client_idcode", how="left")

    if config_obj.verbosity >= 6:
        display(features)

    return features
