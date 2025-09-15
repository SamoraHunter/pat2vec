from datetime import datetime, timedelta
import logging
import os
import re
from datetime import datetime, timedelta, timezone
import string
from typing import Optional, cast
import uuid
from typing import Any, Dict, List, Tuple, Union
import pandas as pd
from faker import Faker
import pytz
from pat2vec.pat2vec_get_methods.get_method_bmi import BMI_FIELDS
from pat2vec.pat2vec_get_methods.get_method_core02 import CORE_O2_FIELDS
from pat2vec.pat2vec_get_methods.get_method_core_resus import CORE_RESUS_FIELDS
from transformers import pipeline
import random
import string
import numpy as np
from IPython import display
import numpy as np
import calendar
from datetime import datetime, timedelta
import random

random_state = 42
Faker.seed(random_state)
# Set random seed
np.random.seed(random_state)
random.seed(random_state)

faker = Faker()

from pat2vec.util.dummy_data_files.dummy_lists import (
    blood_test_names,
    diagnostic_names,
    drug_names,
    ethnicity_list,
)


def maybe_nan(value: Any, probability: float = 0.2) -> Union[Any, float]:
    """Returns a value or NaN based on a probability.

    Args:
        value: The value to potentially return.
        probability: The probability of returning `np.nan` instead of the value.
            Defaults to 0.2.

    Returns:
        The original value or `np.nan`.
    """
    return value if random.random() > probability else np.nan


def create_random_date_from_globals(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> datetime:
    """Generates a random datetime within a given month-level range.

    Args:
        start_year: The starting year.
        start_month: The starting month.
        end_year: The ending year.
        end_month: The ending month.

    Returns:
        A random datetime object within the specified range.
    """
    # Define the start date as the beginning of the first day
    start_dt = datetime(start_year, start_month, 1)

    # Find the last day of the end month (e.g., 29 for Feb 2024, 31 for Mar 2024)
    _, num_days_in_end_month = calendar.monthrange(end_year, end_month)

    # Define the end date as the last second of the last day
    end_dt = datetime(end_year, end_month, num_days_in_end_month, 23, 59, 59)

    # Calculate the total number of seconds between the two dates
    time_difference = end_dt - start_dt
    total_seconds = int(time_difference.total_seconds())

    if total_seconds <= 0:
        return start_dt

    random_second = random.randrange(total_seconds)
    return start_dt + timedelta(seconds=random_second)


def generate_epr_documents_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    use_GPT: bool = True,
    fields_list: List[str] = [
        "client_idcode",
        "document_guid",
        "document_description",
        "body_analysed",
        "updatetime",
        "clientvisit_visitidcode",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epr_documents' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        use_GPT: If True, uses a text generation model for the document body.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy EPR document data.
    """

    print(f"entered_list: {entered_list}")
    print(f"num_rows: {num_rows}")

    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "document_guid": [str(uuid.uuid4()).split("-")[0] for _ in range(num_rows)],
            "document_description": [f"clinical_note_summary" for i in range(num_rows)],
            # "body_analysed": [faker.paragraph() for _ in range(num_rows)],
            "body_analysed": [
                (
                    generate_patient_timeline(current_pat_client_id_code)
                    if use_GPT
                    else
                    # generate_patient_timeline_faker(current_pat_client_id_code)
                    get_patient_timeline_dummy(current_pat_client_id_code)
                )
                for _ in range(num_rows)
            ],
            "updatetime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                str(uuid.uuid4()).split("-")[0] for _ in range(num_rows)
            ],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    try:
        # print(f"Number of DataFrames in df_holder_list: {len(df_holder_list)}")
        df = pd.concat(df_holder_list, axis=0, ignore_index=True)

        return df
    except Exception as e:
        print(e)
        raise e


def generate_epr_documents_personal_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = [
        "client_idcode",
        "client_firstname",
        "client_lastname",
        "client_dob",
        "client_gendercode",
        "client_racecode",
        "client_deceaseddtm",
        "updatetime",
    ],
) -> pd.DataFrame:
    """Generates dummy personal data for the 'epr_documents' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy personal data.
    """
    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        ethnicity = faker.random_element(ethnicity_list)

        first_name = faker.first_name()
        last_name = faker.last_name()
        dob = faker.date_of_birth(minimum_age=18, maximum_age=90).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        gender = random.choice(["male", "female"])

        # TODO: implement low change of death event, if so use date of death else None
        death_probability = 0.1
        client_deceaseddtm_val = (
            faker.date_time_this_decade()
            if random.random() < death_probability
            else None
        )

        data = {
            "client_idcode": [current_pat_client_id_code] * num_rows,
            "client_firstname": [maybe_nan(first_name) for _ in range(num_rows)],
            "client_lastname": [maybe_nan(last_name) for _ in range(num_rows)],
            "client_dob": [maybe_nan(dob) for _ in range(num_rows)],
            "client_gendercode": [maybe_nan(gender) for _ in range(num_rows)],
            "client_racecode": [maybe_nan(ethnicity) for _ in range(num_rows)],
            "client_deceaseddtm": [
                maybe_nan(client_deceaseddtm_val) for _ in range(num_rows)
            ],
            "updatetime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%d")
                for _ in range(num_rows)
            ],
        }
        if num_rows == 0:
            data = {
                "client_idcode": [current_pat_client_id_code],
                "client_firstname": [np.nan],
                "client_lastname": [np.nan],
                "client_dob": [np.nan],
                "client_gendercode": [np.nan],
                "client_racecode": [np.nan],
                "client_deceaseddtm": [np.nan],
                "updatetime": [np.nan],
            }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    # fields_list = fields_list + ["_id", "_index", "_score"]

    # df = df[fields_list]
    # df.reset_index(drop=True, inplace=True)

    return df


def generate_diagnostic_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = [
        "order_guid",
        "client_idcode",
        "order_name",
        "order_summaryline",
        "order_holdreasontext",
        "order_entered",
        "order_createdwhen",
        "clientvisit_visitidcode",
        "_id",
        "_index",
        "_score",
        "order_performeddtm",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'diagnostic_orders' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy diagnostic order data.
    """

    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "order_guid": [f"order_{i}" for i in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "order_name": [
                faker.random_element(diagnostic_names) for _ in range(num_rows)
            ],
            "order_summaryline": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
            "order_holdreasontext": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
            "order_entered": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "order_createdwhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": ["{i}" for i in range(num_rows)],
            "_index": ["{np.nan}" for _ in range(num_rows)],
            "_score": ["{np.nan}" for _ in range(num_rows)],
            "order_performeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
        }
        # print("generate_diagnostic_orders_data")
        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    fields_list = fields_list + ["_id", "_index", "_score"]

    return df


def generate_drug_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = [
        "order_guid",
        "client_idcode",
        "order_name",
        "order_summaryline",
        "order_holdreasontext",
        "order_entered",
        "order_createdwhen",
        "clientvisit_visitidcode",
        "_id",
        "_index",
        "_score",
        "order_performeddtm",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'drug_orders' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy drug order data.
    """
    # print("generate_drug_orders_data")
    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]
        data = {
            "order_guid": [f"order_{i}" for i in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            # New value for drug_name
            "order_name": [faker.random_element(drug_names) for _ in range(num_rows)],
            # New value for drug_description
            "order_summaryline": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
            # New value for dosage
            "order_holdreasontext": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
            "order_entered": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "order_createdwhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": ["{i}" for i in range(num_rows)],
            "_index": ["{np.nan}" for i in range(num_rows)],
            "_score": ["{np.nan}" for i in range(num_rows)],
            "order_performeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    fields_list = fields_list + ["_id", "_index", "_score"]

    # Ensure only target columns are present. Useful if source data isn't directly from ES.
    df = df[fields_list]
    df.reset_index(drop=True, inplace=True)
    return df


def generate_observations_MRC_text_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    use_GPT: bool = False,
    fields_list: List[str] = [
        "observation_guid",
        "client_idcode",
        "obscatalogmasteritem_displayname",
        "observation_valuetext_analysed",
        "observationdocument_recordeddtm",
        "clientvisit_visitidcode",
        "_id",
        "_index",
        "_score",
        "textualObs",
    ],
) -> pd.DataFrame:
    """Generates dummy MRC text data for the 'observations' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        use_GPT: If True, uses a text generation model for the document body.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy observation data.
    """

    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "observation_guid": [f"obs_{i}" for i in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": "AoMRC_ClinicalSummary_FT",
            "observation_valuetext_analysed": [
                (
                    generate_patient_timeline(current_pat_client_id_code)
                    if use_GPT
                    else generate_patient_timeline_faker(current_pat_client_id_code)
                )
                for _ in range(num_rows)
            ],
            # 'observation_valuetext_analysed': [faker.paragraph() for _ in range(num_rows)],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": ["{i}" for i in range(num_rows)],
            "_index": ["{np.nan}" for i in range(num_rows)],
            "_score": ["{np.nan}" for i in range(num_rows)],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    # filter df by fields list except ['_id', '_index', '_score']

    fields_list = fields_list + ["_id", "_index", "_score"]

    df = df[fields_list]

    df.reset_index(drop=True, inplace=True)
    return df


def generate_observations_Reports_text_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    use_GPT: bool = False,
    fields_list: List[str] = [
        "basicobs_guid",
        "client_idcode",
        "basicobs_itemname_analysed",
        "basicobs_value_analysed",
        "textualObs",
        "updatetime",
        "clientvisit_visitidcode",
        "_id",
        "_index",
        "_score",
    ],
) -> pd.DataFrame:
    """Generates dummy report text data for the 'basic_observations' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        use_GPT: If True, uses a text generation model for the document body.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy report data.
    """
    # print("generate_observations_Reports_text_data")
    random.seed(random_state)
    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "basicobs_guid": [f"obs_{i}" for i in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "basicobs_itemname_analysed": "Report",
            "basicobs_value_analysed": "",
            "textualObs": [
                (
                    generate_patient_timeline(current_pat_client_id_code)
                    if use_GPT
                    else generate_patient_timeline_faker(current_pat_client_id_code)
                )
                for _ in range(num_rows)
            ],
            # 'observation_valuetext_analysed': [faker.paragraph() for _ in range(num_rows)],
            "updatetime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": ["{i}" for i in range(num_rows)],
            "_index": ["{np.nan}" for i in range(num_rows)],
            "_score": ["{np.nan}" for i in range(num_rows)],
        }

        df = pd.DataFrame(data)
        # display(df)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    fields_list = fields_list + ["_id", "_index", "_score"]

    df = df[fields_list]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_appointments_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = [
        "client_idcode",
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
        "_id",
        "_index",
        "_score",
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
) -> pd.DataFrame:
    """Generates dummy data for the 'pims_apps' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy appointment data.
    """
    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "Popular": [faker.random_number(digits=3) for _ in range(num_rows)],
            "AppointmentType": [
                faker.random_element(["Type A", "Type B", "Type C"])
                for _ in range(num_rows)
            ],
            "AttendanceReference": [
                faker.random_number(digits=6) for _ in range(num_rows)
            ],
            "ClinicCode": [faker.random_number(digits=4) for _ in range(num_rows)],
            "ClinicDesc": [faker.word() for _ in range(num_rows)],
            "Consultant": [faker.name() for _ in range(num_rows)],
            "DateModified": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "DNA": [faker.random_element(["Y", "N"]) for _ in range(num_rows)],
            "HospitalID": [current_pat_client_id_code for _ in range(num_rows)],
            "PatNHSNo": [faker.random_number(digits=10) for _ in range(num_rows)],
            "Specialty": [
                faker.random_element(["Specialty A", "Specialty B", "Specialty C"])
                for _ in range(num_rows)
            ],
            "_id": [str(i) for i in range(num_rows)],
            "_index": [str(None) for _ in range(num_rows)],
            "_score": [str(None) for _ in range(num_rows)],
            "AppointmentDateTime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "Attended": [faker.random_element([0, 1]) for _ in range(num_rows)],
            "CancDesc": [faker.sentence() for _ in range(num_rows)],
            "CancRefNo": [faker.random_number(digits=8) for _ in range(num_rows)],
            "ConsultantCode": [faker.random_number(digits=4) for _ in range(num_rows)],
            "DateCreated": [faker.date_time_this_year() for _ in range(num_rows)],
            "Ethnicity": [
                faker.random_element(["Ethnicity A", "Ethnicity B", "Ethnicity C"])
                for _ in range(num_rows)
            ],
            "Gender": [
                faker.random_element(["Male", "Female"]) for _ in range(num_rows)
            ],
            "NHSNoStatusCode": [faker.random_number(digits=2) for _ in range(num_rows)],
            "NotSpec": [faker.random_element(["Y", "N"]) for _ in range(num_rows)],
            "PatDateOfBirth": [faker.date_of_birth() for _ in range(num_rows)],
            "PatForename": [faker.first_name() for _ in range(num_rows)],
            "PatPostCode": [faker.postcode() for _ in range(num_rows)],
            "PatSurname": [faker.last_name() for _ in range(num_rows)],
            "PiMsPatRefNo": [faker.random_number(digits=6) for _ in range(num_rows)],
            "Primarykeyfieldname": [faker.word() for _ in range(num_rows)],
            "Primarykeyfieldvalue": [
                faker.random_number(digits=4) for _ in range(num_rows)
            ],
            "SessionCode": [faker.random_number(digits=3) for _ in range(num_rows)],
            "SpecialtyCode": [faker.random_number(digits=4) for _ in range(num_rows)],
        }

        df = pd.DataFrame(data)

        df_holder_list.append(df)

    df = pd.concat(df_holder_list, ignore_index=True)
    fields_list = fields_list + ["_id", "_index", "_score"]

    # Ensure only target columns are present. Useful if source data isn't directly from ES.
    df = df[fields_list]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_observations_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    search_term: str,
    use_GPT: bool = False,
    fields_list: List[str] = [
        "observation_guid",
        "client_idcode",
        "obscatalogmasteritem_displayname",
        "observation_valuetext_analysed",
        "observationdocument_recordeddtm",
        "clientvisit_visitidcode",
        "_id",
        "_index",
        "_score",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'observations' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        search_term: The search term to use for the display name.
        use_GPT: If True, uses a text generation model for the document body.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy observation data.
    """

    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "observation_guid": [f"obs_{i}" for i in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [search_term],
            "observation_valuetext_analysed": [
                random.uniform(0, 100) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": ["{i}" for i in range(num_rows)],
            "_index": ["{np.nan}" for i in range(num_rows)],
            "_score": ["{np.nan}" for i in range(num_rows)],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list, ignore_index=True)
    fields_list = fields_list + ["_id", "_index", "_score"]

    df = df[fields_list]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_basic_observations_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = [
        "client_idcode",
        "basicobs_itemname_analysed",
        "basicobs_value_numeric",
        "basicobs_entered",
        "clientvisit_serviceguid",
        "_id",
        "_index",
        "_score",
        "order_guid",
        "order_name",
        "order_summaryline",
        "order_holdreasontext",
        "order_entered",
        "clientvisit_visitidcode",
        "updatetime",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'basic_observations' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy basic observation data.
    """
    # print("generate_basic_observations_data")
    random.seed(random_state)
    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "client_idcode": [current_pat_client_id_code] * num_rows,
            "basicobs_itemname_analysed": [
                faker.random_element(blood_test_names) for _ in range(num_rows)
            ],
            "basicobs_value_numeric": [random.uniform(1, 100) for _ in range(num_rows)],
            "basicobs_entered": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_serviceguid": [f"service_{i}" for i in range(num_rows)],
            "_id": [None for i in range(num_rows)],
            "_index": [None for i in range(num_rows)],
            "_score": [None for i in range(num_rows)],
            "order_guid": [f"order_{i}" for i in range(num_rows)],
            "order_name": [None for i in range(num_rows)],
            "order_summaryline": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
            "order_holdreasontext": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
            "order_entered": ["{np.nan}" for i in range(num_rows)],
            "clientvisit_visitidcode": ["{np.nan}" for i in range(num_rows)],
            "updatetime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list, ignore_index=True)
    # fields_list = fields_list + ["_id", "_index", "_score"]
    fields_list = fields_list + ["_id", "_index", "_score"]
    df = df[fields_list]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_basic_observations_textual_obs_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = [
        "client_idcode",
        "basicobs_itemname_analysed",
        "basicobs_value_numeric",
        "basicobs_entered",
        "clientvisit_serviceguid",
        "_id",
        "_index",
        "_score",
        "basicobs_guid",
        "clientvisit_serviceguid",
        "updatetime",
        "textualObs",
    ],
) -> pd.DataFrame:

    # print("generate_basic_observations_textual_obs_data")
    """
    Generates dummy textual data for the 'basic_observations' index.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy textual observation data.
    """
    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "client_idcode": [current_pat_client_id_code] * num_rows,
            "basicobs_itemname_analysed": [
                faker.random_element(blood_test_names) for _ in range(num_rows)
            ],
            "basicobs_value_numeric": [random.uniform(1, 100) for _ in range(num_rows)],
            "basicobs_value_analysed": [faker.sentence() for _ in range(num_rows)],
            "basicobs_entered": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_serviceguid": [f"service_{i}" for i in range(num_rows)],
            "_id": [None for i in range(num_rows)],
            "_index": [None for i in range(num_rows)],
            "_score": [None for i in range(num_rows)],
            "basicobs_guid": [f"obs_{i}" for i in range(num_rows)],
            "clientvisit_serviceguid": ["{np.nan}" for i in range(num_rows)],
            "updatetime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "textualObs": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list, ignore_index=True)
    fields_list = fields_list + ["_id", "_index", "_score"]

    df = df[fields_list]
    df.reset_index(drop=True, inplace=True)

    return df


def extract_date_range(date_string: str) -> Optional[Tuple[int, int, int, int, int, int]]:
    """Extracts a date range from a string.

    The expected format is "YYYY-MM-DD TO YYYY-MM-DD".

    Args:
        date_string: The string containing the date range.

    Returns:
        A tuple of six integers (start_year, start_month, start_day,
        end_year, end_month, end_day), or None if the pattern is not found.
    """
    pattern = r"(\d{4})-(\d{2})-(\d{2}) TO (\d{4})-(\d{2})-(\d{2})"
    match = re.search(pattern, date_string)
    if match:
        global_start_year = int(match.group(1))
        global_start_month = int(match.group(2))
        global_start_day = int(match.group(3))
        global_end_year = int(match.group(4))
        global_end_month = int(match.group(5))
        global_end_day = int(match.group(6))
        return (
            global_start_year,
            global_start_month,
            global_start_day,
            global_end_year,
            global_end_month,
            global_end_day,
        )
    else:
        return None


def cohort_searcher_with_terms_and_search_dummy(
    index_name: str,
    fields_list: List[str],
    term_name: str,
    entered_list: List[str],
    search_string: str,
) -> pd.DataFrame:
    """Generates dummy data based on simulated Elasticsearch query parameters.

    This function acts as a stand-in for a real CogStack/Elasticsearch query,
    routing requests to different dummy data generator functions based on the
    `index_name` and `search_string`.

    Args:
        index_name: The name of the target index (e.g., 'epr_documents').
        fields_list: A list of fields to be returned in the DataFrame.
        term_name: The field name for the term-level query (e.g., 'client_idcode').
        entered_list: The list of values for the term-level query.
        search_string: A string simulating a query string search, used for
            routing to the correct data generator.

    Returns:
        A pandas DataFrame containing the generated dummy data.
    """

    # set here for drop in replacement of function
    use_GPT = False

    verbose = False

    (
        global_start_year,
        global_start_month,
        global_start_day,
        global_end_year,
        global_end_month,
        global_end_day,
    ) = extract_date_range(search_string)  # type: ignore

    if verbose:
        print("cohort_searcher_with_terms_and_search_dummy:", search_string)

    if "client_firstname" in fields_list:
        if verbose:
            print("Generating data for 'client_firstname'")
        num_rows = random.randint(0, 10)
        df = generate_epr_documents_personal_data(
            num_rows, entered_list, global_start_year, global_start_month,
            global_end_year, global_end_month, fields_list=fields_list,
        )
        return df

    elif index_name == "epr_documents":
        if verbose:
            print("Generating data for 'epr_documents'")
        probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
        num_rows = random.choices(range(1, 6), probabilities)[0]
        df = generate_epr_documents_data(
            num_rows, entered_list, global_start_year, global_start_month,
            global_end_year, global_end_month, use_GPT=use_GPT, fields_list=fields_list,
        )
        return df

    elif index_name == "basic_observations":
        # Nested checks for 'basic_observations' index
        if "basicobs_itemname_analysed:report" in search_string:
            if verbose:
                print("Generating text data for 'basic_observations, reports'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_observations_Reports_text_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, use_GPT=use_GPT, fields_list=fields_list,
            )
            return df

        elif fields_list == [ "client_idcode", "basicobs_itemname_analysed", "basicobs_value_numeric", "basicobs_value_analysed", "basicobs_entered", "clientvisit_serviceguid", "basicobs_guid", "updatetime", "textualObs"]:
            if verbose:
                print("Generating data for 'basic_observations textualObs'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_basic_observations_textual_obs_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, fields_list=fields_list,
            )
            return df

        else: # Fallback for other basic_observations
            if verbose:
                print("Generating data for 'basicobs_value_numeric'")
            num_rows = random.randint(0, 10)
            df = generate_basic_observations_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, fields_list=fields_list,
            )
            return df

    elif index_name == "observations":
        # Single entry point for the 'observations' index with nested triage
        if any(term in search_string for term in ["OBS BMI", "OBS Weight", "OBS Height"]):
            if verbose:
                print("Generating data for 'bmi'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_bmi_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, fields_list=fields_list,
            )
            return df

        elif '"CORE_SpO2"' in search_string:
            if verbose:
                print("Generating data for 'core_o2'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_core_o2_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, fields_list=fields_list,
            )
            return df

        elif '"CORE_RESUS_STATUS"' in search_string:
            if verbose:
                print("Generating data for 'core_resus_status'")
            probabilities = [0.7, 0.25, 0.05]
            num_rows = random.choices(range(1, 4), probabilities)[0]
            df = generate_core_resus_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, fields_list=fields_list,
            )
            return df

        elif "CORE_HospitalSite" in search_string:
            if verbose:
                print("Generating data for 'hospital_site'")
            probabilities = [0.8, 0.1, 0.05, 0.03, 0.02]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_hospital_site_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, fields_list=fields_list,
            )
            return df

        elif "AoMRC_ClinicalSummary_FT" in search_string:
            if verbose:
                print("Generating mrc text data for 'observations'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_observations_MRC_text_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, use_GPT=use_GPT, fields_list=fields_list,
            )
            return df

        else: # Generic fallback for any other 'observations' request
            if verbose:
                print("Generating data for generic 'observations'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            search_term = str(extract_search_term_obscatalogmasteritem_displayname(search_string))
            df = generate_observations_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, search_term, fields_list=fields_list,
            )
            return df

    elif index_name == "order":
        if "medication" in search_string:
            if verbose:
                print("Generating data for 'orders' with medication")
            num_rows = random.randint(0, 10)
            df = generate_drug_orders_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, fields_list=fields_list,
            )
            return df

        elif "diagnostic" in search_string:
            if verbose:
                print("Generating data for 'orders' with diagnostic")
            num_rows = random.randint(0, 10)
            df = generate_diagnostic_orders_data(
                num_rows, entered_list, global_start_year, global_start_month,
                global_end_year, global_end_month, fields_list=fields_list,
            )
            return df

    elif index_name == "pims_apps*":
        if verbose:
            print("Generating data for 'pims_apps'")
        num_rows = random.randint(0, 10)
        df = generate_appointments_data(
            num_rows, entered_list, global_start_year, global_start_month,
            global_end_year, global_end_month, fields_list=fields_list,
        )
        return df

    else:
        print(
            "No matching triage rule found. Returning an empty DataFrame.",
            search_string,
        )
        return pd.DataFrame(columns=["updatetime", "_index", "_id", "_score"] + fields_list)


# # Example usage for epr_documents with personal information:
# epr_documents_personal_df = cohort_searcher_with_terms_and_search_dummy(
#     index_name="epr_documents",
#     fields_list=["client_idcode", "client_firstname", "client_lastname", "client_dob", "client_gendercode", "client_racecode", "client_deceaseddtm", "updatetime"],
#     term_name="client_idcode.keyword",
#     entered_list=['D3232DUM23'],  # Add more client IDs as needed
#     global_start_year=2022,
#     global_start_month=1,
#     global_end_year=2023,
#     global_end_month=12,
#     search_string=f'updatetime:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
# )

# display(epr_documents_personal_df)


def generate_patient_timeline(client_idcode: str) -> str:

    # Set the logging level to suppress INFO messages
    """Generates a random patient timeline using a GPT-2 model.

    Creates a short, semi-realistic clinical note timeline for a patient,
    including demographic information and a series of timestamped entries.

    Args:
        client_idcode: The client ID for the patient.

    Returns:
        A string containing the patient's dummy timeline.
    """
    logging.getLogger("transformers").setLevel(logging.WARNING)
    generator = pipeline("text-generation", model="gpt2")

    probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]  # Adjust as needed

    # Perform a weighted random selection based on the defined probabilities
    num_entries = random.choices(range(1, 6), probabilities)[0]

    starting_age = random.randint(18, 99)
    # Initialize patient demographic information
    patient_info = {
        "client_idcode": client_idcode,
        "Age": starting_age,
        "Gender": random.choice(["Male", "Female"]),
        "DOB": datetime.utcnow() - timedelta(days=365 * starting_age),
    }

    # Generate clinical note summaries
    timeline = []

    # Generate a random timestamp between 1995 and the current time
    current_time = datetime.utcfromtimestamp(
        random.randint(789331200, int(datetime.now().timestamp()))
    )

    for i in range(num_entries):
        entry_timestamp = current_time + timedelta(days=random.randint(1, 30))
        entry_text = generator(
            "Patient presented with:", max_length=50, do_sample=True
        )[0]["generated_text"]

        # Update patient information
        patient_info["Age"] += (entry_timestamp - current_time).days / 365

        # Format entry
        entry_summary = f"Entered on - {entry_timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC:\n{entry_text}\n"
        timeline.append(entry_summary)

        current_time = entry_timestamp

    # Construct the final timeline
    patient_demographics = f"Patient Demographics:\client_idcode: {patient_info['client_idcode']}\client_idcode: {patient_info['Age']:.1f}\nGender: {patient_info['Gender']}\nDOB: {patient_info['DOB'].strftime('%Y-%m-%d')}"
    timeline.insert(0, f"{patient_demographics}\n\nClinical Note Timeline:\n")
    patient_timeline = "\n".join(timeline)

    return patient_timeline


def generate_patient_timeline_faker(client_idcode: str) -> str:
    """Generates a fake patient timeline using the Faker library.

    Creates a short, semi-realistic clinical note timeline for a patient,
    including demographic information and a series of timestamped entries
    with fake sentences.

    Args:
        client_idcode: The client ID for the patient.

    Returns:
        A string containing the patient's dummy timeline.
    """
    probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]  # Adjust as needed

    # Perform a weighted random selection based on the defined probabilities
    num_entries = random.choices(range(1, 6), probabilities)[0]

    starting_age = random.randint(18, 99)
    # Initialize patient demographic information
    patient_info = {
        "client_idcode": client_idcode,
        "Age": starting_age,
        "Gender": random.choice(["Male", "Female"]),
        "DOB": datetime.utcnow() - timedelta(days=365 * starting_age),
    }

    # Generate clinical note summaries
    timeline = []

    # Generate a random timestamp between 1995 and the current time
    current_time = datetime.utcfromtimestamp(
        random.randint(789331200, int(datetime.now().timestamp()))
    )

    for i in range(num_entries):
        entry_timestamp = current_time + timedelta(days=random.randint(1, 30))
        entry_text = faker.sentence(nb_words=15)

        # Update patient information
        patient_info["Age"] += (entry_timestamp - current_time).days / 365

        # Format entry
        entry_summary = f"Entered on - {entry_timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC:\n{entry_text}\n"
        timeline.append(entry_summary)

        current_time = entry_timestamp

    # Construct the final timeline
    patient_demographics = f"Patient Demographics:\nclient_idcode: {patient_info['client_idcode']}\nAge: {patient_info['Age']:.1f}\nGender: {patient_info['Gender']}\nDOB: {patient_info['DOB'].strftime('%Y-%m-%d')}"
    timeline.insert(0, f"{patient_demographics}\n\nClinical Note Timeline:\n")
    patient_timeline = "\n".join(timeline)

    return patient_timeline


def extract_search_term_obscatalogmasteritem_displayname(search_string: str) -> str:
    # Using regular expression to find the part after 'obscatalogmasteritem_displayname:'
    """Extracts a search term from an 'obscatalogmasteritem_displayname' query.

    This function uses a regular expression to find a term enclosed in
    parentheses following 'obscatalogmasteritem_displayname:'. It cleans the
    term by removing quotes and stripping any trailing 'AND' or 'OR' clauses.

    Args:
        search_string: The input query string.

    Returns:
        The extracted search term, or the original string if no match is found.
    """
    match = re.search(r"obscatalogmasteritem_displayname:\((.*?)\)", search_string)
    if match:
        # Get the matched group and remove punctuation
        search_term = match.group(1).replace('"', "").replace("'", "").strip()
        # Ignore anything after 'AND' or 'OR'
        search_term = search_term.split("AND", 1)[0].split("OR", 1)[0].strip()
        return search_term
    else:
        return search_string


def run_generate_patient_timeline_and_append(
    n: int = 10, output_path: str = os.path.join("test_files", "dummy_timeline.csv")
) -> None:
    # This function is used to generate a dummy patient timeline text for each client_idcode and
    # append it to an existing CSV file or create a new one if it doesn't exist
    # Check for null pointer references and unhandled exceptions

    """Generates and appends dummy patient timelines to a CSV file.

    This function creates `n` dummy patient timelines and appends them to a
    specified CSV file. If the file doesn't exist, it will be created.

    Args:
        n: The number of patient timelines to generate. Defaults to 10.
        output_path: The path to the output CSV file. Defaults to
            "test_files/dummy_timeline.csv".

    Raises:
        FileNotFoundError: If the output_path does not exist and cannot be created.
        Exception: For any other unexpected errors during timeline generation or file operations.
    """

    try:
        # Check if the CSV file exists, if not, create a new DataFrame
        if os.path.exists(output_path):  # If the CSV file exists
            df = pd.read_csv(output_path)  # Read existing CSV file
        else:  # If the CSV file doesn't exist
            df = pd.DataFrame(
                columns=["client_idcode", "body_analysed"]
            )  # Create a new DataFrame with two columns
    except FileNotFoundError:
        print(f"FileNotFoundError: {output_path} doesn't exist!")
        return

    for _ in range(n):  # Loop n times
        # Generate a random client_idcode using regex
        client_idcode = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=9)
        )

        # Generate patient timeline text
        try:
            patient_timeline_text = generate_patient_timeline(client_idcode)
        except Exception as e:
            print(f"Exception: {e}")
            return

        # Append to DataFrame
        try:
            df = df.append(
                {
                    "client_idcode": client_idcode,
                    "body_analysed": patient_timeline_text,
                },
                ignore_index=True,
            )  # Append a new row to the DataFrame
        except Exception as e:
            print(f"Exception: {e}")
            return

    # Write DataFrame to CSV with append mode
    try:
        df.to_csv(
            output_path, mode="a", header=not os.path.exists(output_path), index=False
        )  # Write to CSV file
    except Exception as e:
        print(f"Exception: {e}")
        return


def get_patient_timeline_dummy(
    client_idcode: str,
    output_path: str = os.path.join("test_files", "dummy_timeline.csv"),
) -> Optional[str]:
    """Retrieves a random patient timeline from a pre-generated CSV file.

    Args:
        client_idcode: The client ID to search for (currently unused, as a
            random row is always selected).
        output_path: The path to the CSV file containing dummy timelines.

    Returns:
        The text of a random patient timeline, or None if the file is not found
        or is invalid.
    """
    try:
        df: pd.DataFrame = pd.read_csv(output_path)
    except FileNotFoundError:
        print(f"FileNotFoundError: {output_path} doesn't exist!")
        return None

    # Check if the DataFrame is empty
    if df.empty:
        print("DataFrame is empty!")
        return None

    # Check if the 'client_idcode' column exists in the DataFrame
    if "client_idcode" not in df.columns:
        print("'client_idcode' column doesn't exist in the DataFrame!")
        return None

    # Check if the 'body_analysed' column exists in the DataFrame
    if "body_analysed" not in df.columns:
        print("'body_analysed' column doesn't exist in the DataFrame!")
        return None

    # Get a random row from the DataFrame, we don't care which one we get
    sample: pd.DataFrame = df.sample(1, random_state=random_state)

    # Check if we got a valid row
    if len(sample) == 0:
        print("Sample is empty!")
        return None

    # Get the value of the 'body_analysed' column from the random row
    try:
        return cast(str, sample.iloc[0]["body_analysed"])
    except KeyError:
        print("KeyError: 'body_analysed' column doesn't exist in the DataFrame!")
        return None


def generate_uuid(prefix: str, length: int = 7) -> str:
    """Generates a UUID-like string with a given prefix.

    Args:
        prefix: The prefix for the UUID, must be 'P' or 'V'.
        length: The length of the random part of the string. Defaults to 7.

    Returns:
        The generated UUID-like string.
    """
    if prefix not in ("P", "V"):
        raise ValueError("Prefix must be 'P' or 'V'")

    # Generate random characters for the rest of the string
    chars = string.ascii_uppercase + string.digits
    random_chars = "".join(random.choices(chars, k=length))

    return f"{prefix}{random_chars}"


def generate_uuid_list(n: int, prefix: str, length: int = 7) -> List[str]:
    """Generates a list of n UUID-like strings.

    Args:
        n: The number of UUIDs to generate.
        prefix: The prefix for each UUID.
        length: The length of the random part of each UUID.

    Returns:
        A list of generated UUID-like strings.
    """
    uuid_list = [generate_uuid(prefix, length) for _ in range(n)]
    return uuid_list


def generate_hospital_site_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = [
        "observation_guid",
        "client_idcode",
        "obscatalogmasteritem_displayname",
        "observation_valuetext_analysed",
        "observationdocument_recordeddtm",
        "clientvisit_visitidcode",
    ],
) -> pd.DataFrame:
    """Generates dummy data for hospital site observations.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy hospital site data.
    """
    df_holder_list = []

    # Define possible values for hospital sites, including key terms 'DH' and 'PRUH'
    # for downstream feature calculation.
    hospital_site_values = [
        "King's College Hospital (DH)",
        "Princess Royal University Hospital (PRUH)",
        "Orpington Hospital",
        "Queen Mary's Hospital, Sidcup",
        "St Thomas' Hospital",
    ]

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],

            # This field is constant based on the SEARCH_TERM in your code.
            "obscatalogmasteritem_displayname": ["CORE_HospitalSite" for _ in range(num_rows)],

            # This field contains the actual site name.
            "observation_valuetext_analysed": [
                maybe_nan(faker.random_element(elements=hospital_site_values))
                for _ in range(num_rows)
            ],

            # Generate a random date for when the observation was recorded.
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                )
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{faker.random_number(digits=8, fix_len=True)}" for _ in range(num_rows)],
        }
        df = pd.DataFrame(data)
        df_holder_list.append(df)

    # Concatenate all generated dataframes into a single one.
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    # Ensure only the specified columns are present in the final dataframe.
    final_df = final_df[fields_list]

    return final_df

def generate_bmi_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = BMI_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for BMI, Weight, and Height observations.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy BMI-related data.
    """
    df_holder_list = []
    observation_types = ["OBS BMI", "OBS Weight", "OBS Height"]

    for client_id_code in entered_list:
        # Generate data for this specific client
        display_names = [random.choice(observation_types) for _ in range(num_rows)]
        values = []
        for name in display_names:
            if name == "OBS BMI":
                # Generate a realistic BMI value (15.0 to 45.0)
                value = f"{random.uniform(15.0, 45.0):.2f}"
            elif name == "OBS Weight":
                # Generate a realistic weight in kg (40.0 to 150.0)
                value = f"{random.uniform(40.0, 150.0):.2f}"
            else:  # OBS Height
                # Generate a realistic height in cm (140.0 to 200.0)
                value = f"{random.uniform(140.0, 200.0):.2f}"
            values.append(value)

        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": display_names,
            "observation_valuetext_analysed": values,
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year, global_start_month,
                    global_end_year, global_end_month
                ) for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    return final_df[fields_list]


def generate_core_o2_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = CORE_O2_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for CORE_SpO2 (oxygen saturation) observations.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy SpO2 data.
    """
    df_holder_list = []

    # Realistic categorical values for SpO2 and oxygen delivery
    spo2_values = ['98%', '97%', '96%', '95%', '94%', '93%', 'On Air', '2L O2 NP', '4L O2 NP', 'NRB Mask']

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": ["CORE_SpO2" for _ in range(num_rows)],
            "observation_valuetext_analysed": [random.choice(spo2_values) for _ in range(num_rows)],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year, global_start_month,
                    global_end_year, global_end_month
                ) for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    return final_df[fields_list]

def generate_core_resus_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = CORE_RESUS_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for CORE_RESUS_STATUS observations.

    Args:
        num_rows: Number of rows to generate for each client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy resuscitation status data.
    """
    df_holder_list = []

    # These are the exact values the feature calculation function looks for.
    resuscitation_statuses = [
        "For cardiopulmonary resuscitation",
        "Not for cardiopulmonary resuscitation"
    ]

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": ["CORE_RESUS_STATUS" for _ in range(num_rows)],
            "observation_valuetext_analysed": [random.choice(resuscitation_statuses) for _ in range(num_rows)],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year, global_start_month,
                    global_end_year, global_end_month
                ) for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    return final_df[fields_list]
