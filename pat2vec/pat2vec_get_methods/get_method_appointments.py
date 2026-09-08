import logging

_logger = logging.getLogger(__name__)

import os

import pandas as pd
from IPython.display import display

from pat2vec.util.elasticsearch_index_config import APPOINTMENT_FIELDS
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates


def search_appointments(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    appointments_time_field="AppointmentDateTime",
    fields_override: list[str] | None = None,
    start_year="1995",
    start_month="01",
    start_day="01",
    end_year="2025",
    end_month="12",
    end_day="12",
    additional_custom_search_string=None,
    index_name: str = "pims_apps*",
    term_name: str = "HospitalID",
    output_filename: str | None = "appointments_search_results.csv",
    overwrite: bool = False,
    config_obj: object | None = None,
):
    """Searches for appointment data for a specific patient within a date range.

    Uses a cohort searcher to find appointment data. If `output_filename` is
    provided, the function will attempt to load existing data from disk or
    save the search results to disk.

    Args:
    ----
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        appointments_time_field (str): The timestamp field for filtering
            appointments. Defaults to 'AppointmentDateTime'.
        fields_override (Optional[List[str]]): A list of fields to override the
            default `APPOINTMENT_FIELDS`. Defaults to None.
        start_year (str): Start year for the search. Defaults to '1995'.
        start_month (str): Start month for the search. Defaults to '01'.
        start_day (str): Start day for the search. Defaults to '01'.
        end_year (str): End year for the search. Defaults to '2025'.
        end_month (str): End month for the search. Defaults to '12'.
        end_day (str): End day for the search. Defaults to '12'.
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.
        index_name (str): The name of the Elasticsearch index to search.
            Defaults to "pims_apps*".
        term_name (str): The field to filter on for the `client_id_codes`.
            Defaults to "HospitalID.keyword".
        output_filename (Optional[str]): The filename or path to a CSV file to
            load from or save to. Defaults to "appointments_search_results.csv".
        overwrite (bool): If True, perform the search even if `output_filename`
            exists. Defaults to False.
        config_obj (Optional[object]): Configuration object containing root_path.
            Defaults to None.

    Returns:
    -------
        pd.DataFrame: A DataFrame containing the raw appointment data.

    Raises:
    ------
        ValueError: When `cohort_searcher_with_terms_and_search`, `client_id_codes`,
            `appointments_time_field`, or date components are None.

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

    if output_filename and os.path.exists(output_filename) and not overwrite:
        print(f"Loading existing appointment data from {output_filename}")
        return pd.read_csv(output_filename)

    if cohort_searcher_with_terms_and_search is None:
        msg = "cohort_searcher_with_terms_and_search cannot be None."
        raise ValueError(msg)
    if client_id_codes is None:
        msg = "client_id_codes cannot be None."
        raise ValueError(msg)
    if appointments_time_field is None:
        msg = "appointments_time_field cannot be None."
        raise ValueError(msg)
    if any(
        x is None
        for x in [start_year, start_month, start_day, end_year, end_month, end_day]
    ):
        msg = "Date components cannot be None."
        raise ValueError(msg)
    # Ensure client_id_codes is a list for the search function
    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

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

    search_string = f"{appointments_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    fields_to_use = APPOINTMENT_FIELDS
    if fields_override:
        fields_to_use = fields_override

    results = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_to_use,
        term_name=term_name,
        entered_list=client_id_codes,
        search_string=search_string,
    )

    if output_filename:
        if os.path.dirname(output_filename):
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        print(f"Saving appointment data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


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
    ----
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.

    Returns:
    -------
        pd.DataFrame: A DataFrame containing pims_apps features for the
            specified patient. If no data is found, a DataFrame with only the

    """
    print(
        f"DEBUG get_appointments CALLED: client={current_pat_client_id_code}, date_range={target_date_range}",
    )

    if config_obj is None:
        msg = "config_obj cannot be None. Please provide a valid configuration."
        raise ValueError(
            msg,
            get_appointments,
        )

    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    appointments_time_field = config_obj.appointments_time_field

    print(
        f"DEBUG get_appointments: batch_mode={batch_mode}, pat_batch.empty={pat_batch.empty}",
    )
    if not pat_batch.empty:
        print(
            f"DEBUG get_appointments: pat_batch.shape={pat_batch.shape}, columns={list(pat_batch.columns)}",
        )
        print(
            f"DEBUG appointments search: {start_year}-{start_month}-{start_day} to {end_year}-{end_month}-{end_day}, time_field={appointments_time_field}",
        )

    if pat_batch.empty:
        if batch_mode and cohort_searcher_with_terms_and_search is not None:
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
                output_filename=None,
                config_obj=config_obj,
            )
        else:
            return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})
    elif batch_mode:
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
            output_filename=None,
            config_obj=config_obj,
        )

    if "HospitalID" in current_pat_raw.columns:
        _logger.debug(
            f"Renaming HospitalID to client_idcode, shape before: {current_pat_raw.shape}",
        )
        current_pat_raw = current_pat_raw.rename(
            columns={"HospitalID": "client_idcode"},
        )

    # Ensure client_idcode is present for grouping
    # Drop any existing client_idcode first to avoid duplicates
    client_code_cols = [c for c in current_pat_raw.columns if c == "client_idcode"]
    if len(client_code_cols) > 1:
        # Keep only the first occurrence, remove duplicates
        current_pat_raw = current_pat_raw.loc[:, ~current_pat_raw.columns.duplicated()]

    if "client_idcode" not in current_pat_raw.columns:
        current_pat_raw["client_idcode"] = current_pat_client_id_code

    if len(current_pat_raw) == 0:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    if "Attended" in current_pat_raw.columns:
        current_pat_raw = current_pat_raw[current_pat_raw["Attended"].astype(int) == 1]

    if not current_pat_raw.empty:
        # One-hot encode and sum up attendances per patient
        consultant_features = pd.DataFrame(
            {"client_idcode": [current_pat_client_id_code]},
        )
        clinic_features = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})
        appointment_type_features = pd.DataFrame(
            {"client_idcode": [current_pat_client_id_code]},
        )

        if "ConsultantCode" in current_pat_raw.columns:
            consultant_features = (
                pd.get_dummies(
                    current_pat_raw,
                    columns=["ConsultantCode"],
                    prefix="ConsultantCode",
                )
                .groupby("client_idcode")
                .sum(numeric_only=True)
                .reset_index()
            )
            # Convert counts to binary (0 or 1) for presence/absence indicators
            consultant_cols = [
                c
                for c in consultant_features.columns
                if c.startswith("ConsultantCode_")
            ]
            for col in consultant_cols:
                consultant_features[col] = (consultant_features[col] > 0).astype(int)

        if "ClinicCode" in current_pat_raw.columns:
            clinic_features = (
                pd.get_dummies(
                    current_pat_raw,
                    columns=["ClinicCode"],
                    prefix="ClinicCode",
                )
                .groupby("client_idcode")
                .sum(numeric_only=True)
                .reset_index()
            )
            # Convert counts to binary (0 or 1) for presence/absence indicators
            clinic_cols = [
                c for c in clinic_features.columns if c.startswith("ClinicCode_")
            ]
            for col in clinic_cols:
                clinic_features[col] = (clinic_features[col] > 0).astype(int)

        if "AppointmentType" in current_pat_raw.columns:
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
            # Convert counts to binary (0 or 1) for presence/absence indicators
            type_cols = [
                c
                for c in appointment_type_features.columns
                if c.startswith("AppointmentType_")
            ]
            for col in type_cols:
                appointment_type_features[col] = (
                    appointment_type_features[col] > 0
                ).astype(int)

        # Merge all features
        features = consultant_features.merge(
            clinic_features,
            on="client_idcode",
            how="outer",
        ).merge(appointment_type_features, on="client_idcode", how="outer")
        # Fill any missing columns with 0 for binary feature columns
        expected_cols = [
            c
            for c in features.columns
            if (c.startswith(("ConsultantCode_", "ClinicCode_", "AppointmentType_")))
        ]
        for col in expected_cols:
            if col in features.columns:
                features[col] = features[col].fillna(0).astype(int)
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


def get_appointments_features(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Wrapper function for get_appointments following BMI pattern.

    Args:
    ----
        current_pat_client_id_code: The client ID code of the patient.
        target_date_range: A tuple representing the target date range.
        pat_batch: The DataFrame containing patient data for batch mode.
        config_obj: Configuration object. Defaults to None.
        cohort_searcher_with_terms_and_search: The function for cohort searching.

    Returns:
    -------
        pd.DataFrame or list: Appointments features for the specified patient(s).

    Raises:
    ------
        ValueError: If config_obj is None.

    """
    return get_appointments(
        current_pat_client_id_code,
        target_date_range,
        pat_batch,
        config_obj=config_obj,
        cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
    )
