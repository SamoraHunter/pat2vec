from pat2vec.util.helper_functions import get_df_from_db
from pat2vec.util.methods_get import exist_check


import pandas as pd
from sqlalchemy import text


import logging
import os
from typing import Any


def get_pat_batch_appointments(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of appointments for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of appointments.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    appointments_time_field = config_obj.appointments_time_field

    batch_target = pd.DataFrame()

    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_appointments"
            schema_name = "raw_data"

            if not config_obj.overwrite_stored_pat_observations:
                df = get_df_from_db(
                    config_obj,
                    schema_name,
                    table_name,
                    patient_ids=[current_pat_client_id_code],
                    patient_id_column="HospitalID",
                )
                if not df.empty:
                    return df
        except Exception as e:
            logging.error(
                f"Error with database backend for appointments for patient {current_pat_client_id_code}: {e}"
            )
            return pd.DataFrame()

    appointments_target_path = os.path.join(
        config_obj.pre_appointments_batch_path, str(current_pat_client_id_code) + ".csv"
    )
    existence_check = exist_check(appointments_target_path, config_obj)

    should_fetch = False
    if config_obj.storage_backend == "database":
        should_fetch = True
    elif (
        config_obj.store_pat_batch_observations
        and not existence_check
        or existence_check is False
    ):
        should_fetch = True

    try:
        if should_fetch:
            batch_target = cohort_searcher_with_terms_and_search(
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
                term_name="HospitalID.keyword",  # alt HospitalID.keyword #warn non case
                entered_list=[current_pat_client_id_code],
                search_string=f"{appointments_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_docs
                or config_obj.overwrite_stored_pat_observations
            ):
                if config_obj.storage_backend == "database":
                    try:
                        engine = config_obj.db_engine
                        if engine:
                            with engine.begin() as connection:
                                table_name = "raw_appointments"
                                schema_name = "raw_data"
                                db_table = (
                                    f"{schema_name}_{table_name}"
                                    if engine.name == "sqlite"
                                    else table_name
                                )
                                db_schema = (
                                    None if engine.name == "sqlite" else schema_name
                                )
                                if config_obj.overwrite_stored_pat_observations:
                                    del_query = text(
                                        f"DELETE FROM {db_table if engine.name == 'sqlite' else f'{schema_name}.{table_name}'} WHERE \"HospitalID\" = :pat_id"
                                    )
                                    connection.execute(
                                        del_query,
                                        {"pat_id": current_pat_client_id_code},
                                    )
                                batch_target.to_sql(
                                    name=db_table,
                                    con=connection,
                                    schema=db_schema,
                                    if_exists="append",
                                    index=False,
                                )
                    except Exception as e:
                        logging.error(f"Failed to save appointments batch to DB: {e}")
                else:
                    batch_target.to_csv(appointments_target_path)
        else:
            batch_target = pd.read_csv(appointments_target_path)
        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch appointments orders: {e}")
        return pd.DataFrame()
