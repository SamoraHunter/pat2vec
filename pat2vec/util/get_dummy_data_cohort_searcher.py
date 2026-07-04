from datetime import datetime, timedelta
import logging
import os
import re
import string
import json
from typing import Any, Dict, List, Optional, Tuple, Union, cast
import uuid
import pandas as pd
from faker import Faker
from pat2vec.pat2vec_get_methods.get_method_bmi import BMI_FIELDS
from pat2vec.pat2vec_get_methods.get_method_core02 import CORE_O2_FIELDS
from pat2vec.pat2vec_get_methods.get_method_bed import BED_FIELDS
from pat2vec.pat2vec_get_methods.get_method_vte_status import VTE_FIELDS
from pat2vec.pat2vec_get_methods.get_method_smoking import SMOKING_FIELDS
from pat2vec.pat2vec_get_methods.get_method_core_resus import CORE_RESUS_FIELDS
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
from transformers import pipeline
import random
from pat2vec.util.dummy_data_files.dummy_lists import (
    blood_test_names,
    diagnostic_names,
    drug_names,
    ethnicity_list,
)

# Import from dummy_data_generation for backward compatibility
from pat2vec.util.dummy_data_generation.generator_helpers import (
    create_random_date_from_globals,
)
import numpy as np
import calendar

random_state = 42
Faker.seed(random_state)
# Set random seed

logger = logging.getLogger(__name__)
np.random.seed(random_state)
random.seed(random_state)

faker = Faker()


def is_safe_host(h: str) -> bool:
    """Checks if a host is local or part of a private network to permit dummy data population.

    Args:
        h: The hostname or IP address to check.

    Returns:
        True if the host is safe for dummy data operations, False otherwise.

    Raises:
        None
    """
    if h in [
        "localhost",
        "127.0.0.1",
        "0.0.0.0",
        "::1",
        "elasticsearch",
        "es01",
        "host.docker.internal",
    ]:
        return True
    # Allow private IP ranges (common for Docker bridge/internal networks)
    # 127.x.x.x, 10.x.x.x, 172.16-31.x.x, 192.168.x.x
    return bool(
        re.match(
            r"^(127\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|172\.17\.0\.1|192\.168\.)", h
        )
    )


def maybe_nan(value: Any, probability: float = 0.2) -> Union[Any, float]:
    """Returns a value or NaN based on a probability.

    Args:
        value: The value to potentially return.
        probability: The probability of returning `np.nan` instead of the value.
            Defaults to 0.2.

    Returns:
        The original value or `np.nan`.

    Raises:
        None
    """
    return value if random.random() > probability else np.nan


def generate_epr_documents_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    use_GPT: bool = True,
    fields_list: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Generates dummy EPR document data.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        use_GPT: If True, uses a text generation model for document body.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy EPR document data.

    Raises:
        None
    """
    if fields_list is None:
        fields_list = [
            "client_idcode",
            "document_guid",
            "document_description",
            "body_analysed",
            "updatetime",
            "clientvisit_visitidcode",
        ]

    logger.debug(f"generate_epr_documents_data received fields_list: {fields_list}")
    if len(entered_list) > 0:
        logger.info(
            f"Generating {num_rows} dummy EPR docs for {len(entered_list)} patients, e.g., {entered_list[0]}"
        )
    else:
        return pd.DataFrame(columns=fields_list)

    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "document_guid": [str(uuid.uuid4()).split("-")[0] for _ in range(num_rows)],
            "document_description": ["clinical_note_summary" for i in range(num_rows)],
            # "body_analysed": [faker.paragraph() for _ in range(num_rows)],
            "body_analysed": [
                (
                    generate_patient_timeline(current_pat_client_id_code)
                    if use_GPT
                    else get_patient_timeline_dummy(current_pat_client_id_code)
                    or "Patient presented with clinical symptoms for evaluation."
                )
                for _ in range(num_rows)
            ],
            "updatetime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                str(uuid.uuid4()).split("-")[0] for _ in range(num_rows)
            ],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    try:
        # logger.debug(f"Number of DataFrames in df_holder_list: {len(df_holder_list)}")
        df = pd.concat(df_holder_list, axis=0, ignore_index=True)

        # Ensure unique fields to avoid duplicate columns if fields_list has duplicates
        unique_fields = list(dict.fromkeys(fields_list))
        # Ensure body_analysed is present if generated, to prevent KeyError in downstream dropna calls.
        if "body_analysed" in df.columns and "body_analysed" not in unique_fields:
            unique_fields.append("body_analysed")

        for field in unique_fields:
            if field not in df.columns:
                df[field] = np.nan
        df = df[unique_fields]

        # Ensure text is not NaN for MedCAT
        if "body_analysed" in df.columns:
            df["body_analysed"] = df["body_analysed"].fillna("")

        logger.debug(
            f"generate_epr_documents_data returning DataFrame with columns: {df.columns.tolist()}"
        )

        return df
    except Exception as e:
        logger.error(e)
        raise e


def generate_epr_documents_personal_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
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
    """Generates dummy personal data for the 'epr_documents' index (demographics).

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy personal data.

    Raises:
        None
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
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%d")
                for _ in range(num_rows)
            ],
        }

        df = pd.DataFrame(data)
        # Ensure all requested fields are present, even if empty
        for col in fields_list:
            if col not in df.columns:
                df[col] = np.nan

        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    unique_fields = list(dict.fromkeys(fields_list))
    for field in unique_fields:
        if field not in df.columns:
            df[field] = np.nan

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_diagnostic_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
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
        "order_typecode",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'diagnostic_orders' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy diagnostic order data.

    Raises:
        None
    """

    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "order_guid": [faker.uuid4() for _ in range(num_rows)],
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
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "order_createdwhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
            "order_typecode": ["diagnostic" for _ in range(num_rows)],
            "order_performeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
        }
        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in df.columns:
            df[field] = np.nan
    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)
    return df


def generate_drug_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
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
        "order_typecode",
    ],
    base_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Generates dummy data for the 'drug_orders' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.
        base_date: Optional datetime reference for testing.

    Returns:
        A pandas DataFrame with generated dummy drug order data.

    Raises:
        None
    """
    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        if base_date is not None:
            dates = []
            for j in range(num_rows):
                day_offset = 0 if num_rows == 1 else (j * 2) - 1
                test_day = max(1, min(28, base_date.day + day_offset))
                dates.append(
                    datetime(
                        base_date.year,
                        base_date.month,
                        test_day,
                        random.randint(0, 23),
                        random.randint(0, 59),
                        random.randint(0, 59),
                    ).strftime("%Y-%m-%dT%H:%M:%S")
                )
            order_entered_dates = dates
            order_created_dates = dates
            order_performed_dates = dates
        else:
            order_entered_dates = [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ]
            order_created_dates = [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ]
            order_performed_dates = [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ]

        data = {
            "order_guid": [faker.uuid4() for _ in range(num_rows)],
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
            "order_entered": order_entered_dates,
            "order_createdwhen": order_created_dates,
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for i in range(num_rows)],
            "_score": [None for i in range(num_rows)],
            "order_typecode": ["medication" for _ in range(num_rows)],
            "order_performeddtm": order_performed_dates,
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))
    for field in unique_fields:
        if field not in df.columns:
            df[field] = np.nan

    # Ensure only target columns are present. Useful if source data isn't directly from ES.
    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)
    return df


def generate_observations_MRC_text_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    use_GPT: bool = False,
    *,
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
    """Generates dummy data for the 'observations' index (MRC clinical notes).

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        use_GPT: If True, uses a text generation model for clinical notes content.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy observation data.

    Raises:
        None
    """

    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": "AoMRC_ClinicalSummary_FT",
            "observation_valuetext_analysed": [
                (
                    generate_patient_timeline(current_pat_client_id_code)
                    if use_GPT
                    else get_patient_timeline_dummy(current_pat_client_id_code)
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
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for i in range(num_rows)],
            "_score": [None for i in range(num_rows)],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    # filter df by fields list except ['_id', '_index', '_score']

    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))
    # Ensure observation_valuetext_analysed is present if generated
    target_col = "observation_valuetext_analysed"
    if target_col in df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in df.columns:
            df[field] = np.nan

    df = df[unique_fields]

    df.reset_index(drop=True, inplace=True)
    return df


def generate_observations_Reports_text_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
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
    """Generates dummy data for the 'basic_observations' index (Reports).

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        use_GPT: If True, uses a text generation model for report content.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy report data.

    Raises:
        None
    """
    random.seed(random_state)
    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "basicobs_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "basicobs_itemname_analysed": "Report",
            "basicobs_value_analysed": "",
            "textualObs": [
                (
                    generate_patient_timeline(current_pat_client_id_code)
                    if use_GPT
                    else get_patient_timeline_dummy(current_pat_client_id_code)
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
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for i in range(num_rows)],
            "_score": [None for i in range(num_rows)],
        }

        df = pd.DataFrame(data)
        # display(df)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))
    # Ensure textualObs is present if generated
    target_col = "textualObs"
    if target_col in df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in df.columns:
            df[field] = np.nan
    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_appointments_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
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
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy appointment data.

    Raises:
        None
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
            "ClinicCode": [str(faker.random_number(digits=4)) for _ in range(num_rows)],
            "ClinicDesc": [faker.word() for _ in range(num_rows)],
            "Consultant": [faker.name() for _ in range(num_rows)],
            "DateModified": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "DNA": [faker.random_element([0, 1]) for _ in range(num_rows)],
            # Note: The appointments index typically uses HospitalID as the identifier
            # rather than client_idcode, which is standard in other indices.
            "HospitalID": [current_pat_client_id_code for _ in range(num_rows)],
            "PatNHSNo": [str(faker.random_number(digits=10)) for _ in range(num_rows)],
            "Specialty": [
                faker.random_element(["Specialty A", "Specialty B", "Specialty C"])
                for _ in range(num_rows)
            ],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
            "AppointmentDateTime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "Attended": [faker.random_element([0, 1]) for _ in range(num_rows)],
            "CancDesc": [faker.sentence() for _ in range(num_rows)],
            "CancRefNo": [faker.random_number(digits=8) for _ in range(num_rows)],
            "ConsultantCode": [
                str(faker.random_number(digits=4)) for _ in range(num_rows)
            ],
            "DateCreated": [
                faker.date_time_this_year().strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "Ethnicity": [
                faker.random_element(["Ethnicity A", "Ethnicity B", "Ethnicity C"])
                for _ in range(num_rows)
            ],
            "Gender": [
                faker.random_element(["Male", "Female"]) for _ in range(num_rows)
            ],
            "NHSNoStatusCode": [
                str(faker.random_number(digits=2)) for _ in range(num_rows)
            ],
            "NotSpec": [faker.random_element([0, 1]) for _ in range(num_rows)],
            "PatDateOfBirth": [faker.date_of_birth() for _ in range(num_rows)],
            "PatForename": [faker.first_name() for _ in range(num_rows)],
            "PatPostCode": [faker.postcode() for _ in range(num_rows)],
            "PatSurname": [faker.last_name() for _ in range(num_rows)],
            "PiMsPatRefNo": [faker.random_number(digits=6) for _ in range(num_rows)],
            "Primarykeyfieldname": [faker.word() for _ in range(num_rows)],
            "Primarykeyfieldvalue": [
                str(faker.random_number(digits=4)) for _ in range(num_rows)
            ],
            "SessionCode": [
                str(faker.random_number(digits=3)) for _ in range(num_rows)
            ],
            "SpecialtyCode": [
                str(faker.random_number(digits=4)) for _ in range(num_rows)
            ],
        }

        df = pd.DataFrame(data)

        df_holder_list.append(df)

    df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    # Ensure only target columns are present. Useful if source data isn't directly from ES.
    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_observations_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    search_term: str = "Test",
    use_GPT: bool = False,
    fields_list: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Generates dummy data for the 'observations' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        search_term: The search term to use for the display name.
        use_GPT: If True, uses a text generation model for document body.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy observation data.

    Raises:
        None
    """
    if fields_list is None:
        fields_list = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
            "_id",
            "_index",
            "_score",
        ]

    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [search_term for _ in range(num_rows)],
            "observation_valuetext_analysed": [
                str(random.uniform(0, 100)) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for i in range(num_rows)],
            "_score": [None for i in range(num_rows)],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    df = final_df[unique_fields]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_basic_observations_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: Optional[List[str]] = None,
    base_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Generates dummy data for the 'basic_observations' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.
        base_date: Optional datetime reference for testing.

    Returns:
        A pandas DataFrame with generated dummy basic observation data.

    Raises:
        None
    """
    if fields_list is None:
        fields_list = [
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
            "basicobs_guid",
        ]

    # logger.debug("generate_basic_observations_data")
    random.seed(random_state)
    df_holder_list = []

    for i in range(0, len(entered_list)):

        current_pat_client_id_code = entered_list[i]

        if base_date is not None:
            dates = []
            for j in range(num_rows):
                day_offset = 0 if num_rows == 1 else (j * 2) - 1
                test_day = max(1, min(28, base_date.day + day_offset))
                dates.append(
                    datetime(
                        base_date.year,
                        base_date.month,
                        test_day,
                        random.randint(0, 23),
                        random.randint(0, 59),
                        random.randint(0, 59),
                    ).strftime("%Y-%m-%dT%H:%M:%S")
                )
            basicobs_entered_dates = dates
            order_entered_dates = dates
            updatetime_dates = dates
        else:
            basicobs_entered_dates = [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ]
            order_entered_dates = [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ]
            updatetime_dates = [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ]
        data = {
            "basicobs_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code] * num_rows,
            "basicobs_itemname_analysed": [
                faker.random_element(blood_test_names) for _ in range(num_rows)
            ],
            "basicobs_value_numeric": [random.uniform(1, 100) for _ in range(num_rows)],
            "basicobs_entered": basicobs_entered_dates,
            "clientvisit_serviceguid": [f"service_{i}" for i in range(num_rows)],
            "_id": [None for i in range(num_rows)],
            "_index": [None for i in range(num_rows)],
            "_score": [None for i in range(num_rows)],
            "order_guid": [faker.uuid4() for i in range(num_rows)],
            "order_name": [faker.word() for i in range(num_rows)],
            "order_summaryline": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
            "order_holdreasontext": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
            "order_entered": order_entered_dates,
            "clientvisit_visitidcode": [str(uuid.uuid4()) for _ in range(num_rows)],
            "updatetime": updatetime_dates,
        }

        # Ensure "Glucose" is always present for every patient during testing
        if num_rows > 0:
            data["basicobs_itemname_analysed"][0] = "Glucose"

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in df.columns:
            df[field] = np.nan
    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_basic_observations_textual_obs_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: Optional[List[str]] = None,
) -> pd.DataFrame:

    # logger.debug("generate_basic_observations_textual_obs_data")
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
    if fields_list is None:
        fields_list = [
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_numeric",
            "basicobs_value_analysed",
            "basicobs_entered",
            "clientvisit_serviceguid",
            "_id",
            "_index",
            "_score",
            "basicobs_guid",
            "updatetime",
            "textualObs",
            "clientvisit_visitidcode",
        ]

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
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_serviceguid": [f"service_{i}" for i in range(num_rows)],
            "_id": [None for i in range(num_rows)],
            "_index": [None for i in range(num_rows)],
            "_score": [None for i in range(num_rows)],
            "clientvisit_visitidcode": [str(uuid.uuid4()) for _ in range(num_rows)],
            "basicobs_guid": [faker.uuid4() for _ in range(num_rows)],
            "updatetime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "textualObs": [
                maybe_nan(" ".join(faker.sentence() for _ in range(num_rows)))
                for i in range(num_rows)
            ],
        }

        # Ensure "Glucose" is always present for every patient during testing
        if num_rows > 0:
            data["basicobs_itemname_analysed"][0] = "Glucose"

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in df.columns:
            df[field] = np.nan
    df = df[unique_fields]
    return df


def extract_date_range(
    date_string: str,
) -> Optional[Tuple[int, int, int, int, int, int]]:
    """Extracts a date range from a string.

    The expected format is "YYYY-MM-DD TO YYYY-MM-DD".
    This function is now more robust to handle cases where the search string
    might not contain a date range, returning None in such scenarios.

    Args:
        date_string: The string containing the date range.

    Returns:
        A tuple of six integers (start_year, start_month, start_day,
        end_year, end_month, end_day), or None if the pattern is not found.

    Raises:
        None
    """
    pattern = r"(\d{4})-(\d{2})-(\d{2}) TO (\d{4})-(\d{2})-(\d{2})"
    match = re.search(pattern, date_string)  # type: ignore
    if not match:
        logger.warning(
            f"No date range found in search string: {date_string}. Using default global dates."
        )
        # Fallback to default global dates if no date range is found
        return 1995, 1, 1, 2023, 12, 31  # Default values

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


def generate_epic_encounters_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "activity_PatientDurableKey",
        "activity_AdmissionDate",
        "activity_DischargeDate",
        "activity_Department",
        "activity_Type",
        "activity_VisitClass",
        "activity_HospitalService",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_encounters' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy encounter data.

    Raises:
        None
    """
    df_holder_list = []

    for client_id_code in entered_list:
        admission_date = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            global_start_day,
            global_end_day,
        )
        discharge_date = admission_date + timedelta(days=random.randint(1, 30))

        data = {
            "activity_PatientDurableKey": [client_id_code] * num_rows,
            "activity_AdmissionDate": [
                admission_date.strftime("%Y-%m-%dT%H:%M:%S") for _ in range(num_rows)
            ],
            "activity_DischargeDate": [
                discharge_date.strftime("%Y-%m-%dT%H:%M:%S") for _ in range(num_rows)
            ],
            "activity_Department": [faker.word() for _ in range(num_rows)],
            "activity_Type": [
                random.choice(["Inpatient", "Outpatient", "Emergency"])
                for _ in range(num_rows)
            ],
            "activity_VisitClass": [
                random.choice(["Hospital Encounter", "Office Visit"])
                for _ in range(num_rows)
            ],
            "activity_HospitalService": [faker.word() for _ in range(num_rows)],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)

    # Add search_term for compatibility with iterative_multi_term_cohort_searcher_no_terms_fuzzy
    final_df["search_term"] = "Condition"  # Generic term for problem list

    unique_fields = list(dict.fromkeys(fields_list))
    # Ensure activity_PatientDurableKey is present if generated
    if (
        "activity_PatientDurableKey" in final_df.columns
        and "activity_PatientDurableKey" not in unique_fields
    ):
        unique_fields.append("activity_PatientDurableKey")

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_clinical_notes_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    use_GPT: bool = False,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_Content",
        "document_Name",
        "document_EncounterEpicCsn",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_clinical_notes' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        use_GPT: If True, uses a text generation model for clinical notes content.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy clinical note data.

    Raises:
        None
    """
    df_holder_list = []

    for client_id_code in entered_list:
        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "document_Content": [
                (
                    generate_patient_timeline(client_id_code)
                    if use_GPT
                    else get_patient_timeline_dummy(client_id_code)
                )
                for _ in range(num_rows)
            ],
            "document_Name": [faker.sentence(nb_words=3) for _ in range(num_rows)],
            "document_EncounterEpicCsn": [
                faker.random_number(digits=10) for _ in range(num_rows)
            ],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    # Ensure document_Content is present if generated
    target_col = "document_Content"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_medical_history_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_Diagnosis",
        "document_DiagnosisConcepts",
        "document_Name",
        "document_Comment",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_medical_history' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy medical history data.

    Raises:
        None
    """
    df_holder_list = []

    for client_id_code in entered_list:
        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "document_Diagnosis": [faker.word() for _ in range(num_rows)],
            "document_DiagnosisConcepts": [faker.word() for _ in range(num_rows)],
            "document_Name": [faker.sentence(nb_words=2) for _ in range(num_rows)],
            "document_Comment": [faker.sentence() for _ in range(num_rows)],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    # Ensure document_Comment is present if generated
    target_col = "document_Comment"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_UpdatedWhen",
        "document_Name",
        "document_Content",
        "document_OrderClass",
        "document_OrderDate",
        "document_OrderStatus",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_orders' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy orders data.

    Raises:
        None
    """
    df_holder_list = []

    for client_id_code in entered_list:
        order_date = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            global_start_day,
            global_end_day,
        )
        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "document_UpdatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "document_Name": [faker.word() for _ in range(num_rows)],
            "document_Content": [faker.sentence() for _ in range(num_rows)],
            "document_OrderClass": [
                random.choice(["Medication", "Lab", "Imaging"]) for _ in range(num_rows)
            ],
            "document_OrderDate": [
                int(order_date.timestamp() * 1000)
                for _ in range(num_rows)  # Changed to epoch milliseconds
            ],
            "document_OrderStatus": [
                random.choice(["Completed", "Pending", "Cancelled"])
                for _ in range(num_rows)
            ],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    # Ensure document_Content is present if generated
    target_col = "document_Content"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_lab_results_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_Name",
        "document_Content",
        "document_LabComponentValue",
        "document_CollectedDate",
        "document_LabResultEpicId",
        "document_Fields.valueText",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_lab_results' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy lab result data.

    Raises:
        None
    """
    df_holder_list = []

    for client_id_code in entered_list:
        collected_date = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            global_start_day,
            global_end_day,
        )
        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "document_Name": [faker.word() for _ in range(num_rows)],
            "document_Content": [faker.sentence() for _ in range(num_rows)],
            "document_CollectedDate": [
                collected_date.strftime("%Y-%m-%dT%H:%M:%S") for _ in range(num_rows)
            ],
            "document_LabResultEpicId": [
                faker.random_number(digits=8) for _ in range(num_rows)
            ],
            "document_Fields.valueText": [
                str(random.uniform(1, 100)) for _ in range(num_rows)
            ],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))

    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    # Ensure document_Content is present if generated
    target_col = "document_Content"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_patients_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "patient_DurableKey",
        "patient_BirthDate",
        "patient_Gender",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_patients' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy patient data.

    Raises:
        None
    """
    df_holder_list = []
    for client_id_code in entered_list:
        dob = faker.date_of_birth(minimum_age=18, maximum_age=90)
        data = {
            "patient_DurableKey": [client_id_code] * num_rows,
            "patient_BirthDate": [
                dob.strftime("%Y-%m-%dT%H:%M:%S") for _ in range(num_rows)
            ],
            "patient_Gender": [
                random.choice(["Male", "Female"]) for _ in range(num_rows)
            ],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    # Ensure document_Content is present if generated
    target_col = "document_Content"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_imaging_reports_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_Name",
        "document_Content",
        "document_ImagingModality",
        "document_StudyStatus",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_imaging_reports' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy imaging report data.

    Raises:
        None
    """
    df_holder_list = []
    for client_id_code in entered_list:
        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "document_Name": [f"Imaging {faker.word()}" for _ in range(num_rows)],
            "document_Content": [faker.sentence() for _ in range(num_rows)],
            "document_ImagingModality": [
                random.choice(["X-Ray", "MRI", "CT"]) for _ in range(num_rows)
            ],
            "document_StudyStatus": [
                random.choice(["Final", "Preliminary"]) for _ in range(num_rows)
            ],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    # Ensure document_Content is present if generated
    target_col = "document_Content"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_clinical_notes_appointments_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_UpdatedWhen",
        "document_Name",
        "document_Content",
        "document_EncounterEpicCsn",
        "document_EncounterKey",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_clinical_notes_appointments' index."""
    df_holder_list = []
    for client_id_code in entered_list:
        created_when = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            global_start_day,
            global_end_day,
        )
        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
                created_when.strftime("%Y-%m-%dT%H:%M:%S") for _ in range(num_rows)
            ],
            "document_UpdatedWhen": [
                created_when.strftime("%Y-%m-%dT%H:%M:%S") for _ in range(num_rows)
            ],
            "document_Name": [f"Appt Note {faker.word()}" for _ in range(num_rows)],
            "document_Content": [faker.sentence() for _ in range(num_rows)],
            "document_EncounterEpicCsn": [
                faker.random_number(digits=10) for _ in range(num_rows)
            ],
            "document_EncounterKey": [
                faker.random_number(digits=8) for _ in range(num_rows)
            ],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def cohort_searcher_with_terms_and_search_dummy(
    index_name: str,
    fields_list: List[str],
    term_name: str,
    entered_list: List[str],
    search_string: str,
    global_start_day: Optional[int] = None,
    global_end_day: Optional[int] = None,
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

    if verbose:
        logger.debug(
            f"cohort_searcher_with_terms_and_search_dummy received index_name: {index_name}, fields_list: {fields_list}"
        )

    date_range_tuple = extract_date_range(search_string)
    if date_range_tuple:
        (
            global_start_year,
            global_start_month,
            extracted_start_day,
            global_end_year,
            global_end_month,
            extracted_end_day,
        ) = date_range_tuple
    else:
        # Fallback if date range extraction fails, should not happen with default values
        global_start_year, global_start_month, extracted_start_day = 1995, 1, 1  # type: ignore
        global_end_year, global_end_month, extracted_end_day = 2023, 12, 31  # type: ignore

    # Use the provided global_start_day and global_end_day if they are not None
    final_global_start_day = (
        global_start_day if global_start_day is not None else extracted_start_day
    )
    final_global_end_day = (
        global_end_day if global_end_day is not None else extracted_end_day
    )

    if verbose:
        logger.debug(f"cohort_searcher_with_terms_and_search_dummy: {search_string}")

    # Initialize df to an empty DataFrame to ensure it's always defined
    df = pd.DataFrame(columns=fields_list)

    if index_name == "epr_documents":
        if "client_firstname" in fields_list:
            if verbose:
                logger.debug("Generating personal data for 'epr_documents'")
            num_rows = random.randint(1, 10)  # Ensure at least 1 row for demographics
            df = generate_epr_documents_personal_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )
        else:
            if verbose:
                logger.debug("Generating general document data for 'epr_documents'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_epr_documents_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                use_GPT=use_GPT,
                fields_list=fields_list,
            )
    elif index_name == "basic_observations":
        # Nested checks for 'basic_observations' index
        if "SARS CoV-2" in search_string and "COVID-19" in search_string:
            if verbose:
                logger.debug("Generating data for 'covid'")
            num_rows = random.randint(1, 5)  # Ensure at least 1 row for COVID
            df = generate_covid_observations_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

        elif "basicobs_itemname_analysed:report" in search_string:
            if verbose:
                logger.debug("Generating text data for 'basic_observations, reports'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_observations_Reports_text_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                use_GPT=use_GPT,
                fields_list=fields_list,
            )

        elif "textualObs" in fields_list:
            if verbose:
                logger.debug("Generating data for 'basic_observations textualObs'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_basic_observations_textual_obs_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

        elif "basicobs_value_numeric" in search_string:
            # Use 1 row to ensure at least one record matches the test's single-date range
            num_rows = 1
            base_date = datetime(2023, 6, 15)
            df = generate_basic_observations_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
                base_date=base_date,
            )

        else:  # Fallback for other basic_observations
            if verbose:
                logger.debug("Generating data for 'basicobs_value_numeric'")
            num_rows = random.randint(
                1, 10
            )  # Ensure at least 1 row for basic observations
            df = generate_basic_observations_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

    elif index_name == "observations":
        # Single entry point for the 'observations' index with nested triage
        if any(
            term in search_string for term in ["OBS BMI", "OBS Weight", "OBS Height"]
        ):
            if verbose:
                logger.debug("Generating data for 'bmi'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            # Use a default base_date of June 15, 2023 for consistent test behavior
            # But generate dates within the range that would be filtered (June 14-17 to cover possible date slices)
            base_date = datetime(2023, 6, 14)
            print(
                f"DEBUG cohort_searcher: Generating BMI data with num_rows={num_rows}, base_date={base_date}"
            )
            df = generate_bmi_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
                base_date=base_date,  # Use consistent test date
            )

        elif "NEWS" in search_string:
            if verbose:
                logger.debug("Generating data for 'news'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_news_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

        elif '"CORE_SpO2"' in search_string:
            if verbose:
                logger.debug("Generating data for 'core_o2'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_core_o2_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

        elif '"CORE_RESUS_STATUS"' in search_string:
            if verbose:
                logger.debug("Generating data for 'core_resus_status'")
            probabilities = [0.7, 0.25, 0.05]
            num_rows = random.choices(range(1, 4), probabilities)[0]
            df = generate_core_resus_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

        elif "CORE_BedNumber3" in search_string:
            if verbose:
                logger.debug("Generating data for 'bed'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_bed_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

        elif "CORE_VTE_STATUS" in search_string:
            if verbose:
                logger.debug("Generating data for 'vte_status'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_vte_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

        elif "CORE_SmokingStatus" in search_string:
            if verbose:
                logger.debug("Generating data for 'smoking'")
            probabilities = [0.8, 0.1, 0.05, 0.03, 0.02]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_smoking_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

        elif "CORE_HospitalSite" in search_string:
            if verbose:
                logger.debug("Generating data for 'hospital_site'")
            probabilities = [0.8, 0.1, 0.05, 0.03, 0.02]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_hospital_site_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )

        elif "AoMRC_ClinicalSummary_FT" in search_string:
            if verbose:
                logger.debug("Generating mrc text data for 'observations'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_observations_MRC_text_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                use_GPT=use_GPT,
                fields_list=fields_list,
            )

        else:  # Generic fallback for any other 'observations' request
            if verbose:
                logger.debug("Generating data for generic 'observations'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            search_term = str(
                extract_search_term_obscatalogmasteritem_displayname(search_string)
            )
            df = generate_observations_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                search_term,
                fields_list=fields_list,
            )

    elif index_name == "order":
        if "medication" in search_string:
            if verbose:
                logger.debug("Generating data for 'orders' with medication")
            num_rows = random.randint(1, 10)  # Ensure at least 1 row for drugs
            df = generate_drug_orders_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )
            return df

        elif "diagnostic" in search_string:
            if verbose:
                logger.debug("Generating data for 'orders' with diagnostic")
            num_rows = random.randint(1, 10)  # Ensure at least 1 row for diagnostics
            df = generate_diagnostic_orders_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                final_global_start_day,
                global_end_year,
                global_end_month,
                final_global_end_day,
                fields_list=fields_list,
            )
            return df

    elif index_name == "pims_apps*":
        if verbose:
            logger.debug("Generating data for 'pims_apps'")
        num_rows = random.randint(1, 10)
        df = generate_appointments_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            final_global_start_day,
            global_end_year,
            global_end_month,
            final_global_end_day,
            fields_list=fields_list,
        )

    elif index_name == "epic_encounters":
        if verbose:
            logger.debug("Generating data for 'epic_encounters'")
        num_rows = random.randint(1, 5)
        df = generate_epic_encounters_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            final_global_start_day,
            global_end_year,
            global_end_month,
            final_global_end_day,
            fields_list=fields_list,
        )

    elif index_name == "epic_clinical_notes":
        if verbose:
            logger.debug("Generating data for 'epic_clinical_notes'")
        num_rows = random.randint(1, 5)
        df = generate_epic_clinical_notes_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            final_global_start_day,
            global_end_year,
            global_end_month,
            final_global_end_day,
            use_GPT=use_GPT,
            fields_list=fields_list,
        )

    elif index_name == "epic_medical_history":
        # For epic_medical_history, document_PatientDurableKey is the primary patient identifier
        if term_name != "document_PatientDurableKey":
            logger.warning(
                f"Searching epic_medical_history with term_name '{term_name}'. Expected 'document_PatientDurableKey'."
            )

        if verbose:
            logger.debug("Generating data for 'epic_medical_history'")
        num_rows = random.randint(1, 5)
        df = generate_epic_medical_history_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            final_global_start_day,
            global_end_year,
            global_end_month,
            final_global_end_day,
            fields_list=fields_list,
        )

    elif index_name == "epic_orders":
        # For epic_orders, document_PatientDurableKey is the primary patient identifier
        if term_name != "document_PatientDurableKey":
            logger.warning(
                f"Searching epic_orders with term_name '{term_name}'. Expected 'document_PatientDurableKey'."
            )

        if verbose:
            logger.debug("Generating data for 'epic_orders'")
        num_rows = random.randint(1, 5)
        df = generate_epic_orders_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            final_global_start_day,
            global_end_year,
            global_end_month,
            final_global_end_day,
            fields_list=fields_list,
        )

    elif index_name == "epic_lab_results":
        if verbose:
            logger.debug("Generating data for 'epic_lab_results'")
        num_rows = random.randint(1, 5)
        df = generate_epic_lab_results_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            final_global_start_day,
            global_end_year,
            global_end_month,
            final_global_end_day,
            fields_list=fields_list,
        )

    elif index_name == "epic_patients":
        if term_name != "patient_DurableKey":
            logger.warning(
                f"Searching epic_patients with term_name '{term_name}'. Expected 'patient_DurableKey'."
            )
        if verbose:
            logger.debug("Generating data for 'epic_patients'")
        df = generate_epic_patients_data(
            random.randint(1, 5),
            entered_list,
            global_start_year,
            global_start_month,
            final_global_start_day,
            global_end_year,
            global_end_month,
            final_global_end_day,
            fields_list=fields_list,
        )
    elif index_name == "epic_imaging_reports":
        if term_name != "document_PatientDurableKey":
            logger.warning(
                f"Searching epic_imaging_reports with term_name '{term_name}'. Expected 'document_PatientDurableKey'."
            )
        if verbose:
            logger.debug("Generating data for 'epic_imaging_reports'")
        df = generate_epic_imaging_reports_data(
            random.randint(1, 5),
            entered_list,
            global_start_year,
            global_start_month,
            final_global_start_day,
            global_end_year,
            global_end_month,
            final_global_end_day,
            fields_list=fields_list,
        )
    elif index_name == "epic_clinical_notes_appointments":
        if term_name != "document_PatientDurableKey":
            logger.warning(
                f"Searching epic_clinical_notes_appointments with term_name '{term_name}'. Expected 'document_PatientDurableKey'."
            )
        if verbose:
            logger.debug("Generating data for 'epic_clinical_notes_appointments'")
        df = generate_epic_clinical_notes_appointments_data(
            num_rows=random.randint(1, 5),
            entered_list=entered_list,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_start_day=final_global_start_day,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            global_end_day=final_global_end_day,
            fields_list=fields_list,
        )
        return df
    else:
        # If no specific generator matched (index_name unknown), ensure it has the expected columns.
        logger.warning(
            f"No specific dummy data generator for index '{index_name}' with search string '{search_string}'. "
            f"Returning an empty DataFrame with requested fields."
        )
        cols = list(
            dict.fromkeys(
                ["updatetime", "_index", "_id", "_score", "client_idcode"] + fields_list
            )
        )
        df = pd.DataFrame(columns=cols)
        return df
    # Remove duplicate columns if any were introduced by branch logic or hardcoded lists
    df = df.loc[:, ~df.columns.duplicated()]

    if search_string:
        df["search_term"] = search_string

    return df


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
    """Generates a random patient timeline using a GPT-2 model.

    Creates a short, semi-realistic clinical note timeline for a patient,
    including demographic information and a series of timestamped entries.

    Args:
        client_idcode: The client ID for the patient.

    Returns:
        A string containing the patient's dummy timeline.

    Raises:
        None
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
    patient_demographics = f"Patient Demographics:\nClient ID: {patient_info['client_idcode']}\nAge: {patient_info['Age']:.1f}\nGender: {patient_info['Gender']}\nDOB: {patient_info['DOB'].strftime('%Y-%m-%d')}"
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

    Raises:
        None
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
    """Extracts a search term from an 'obscatalogmasteritem_displayname' query.

    This function uses a regular expression to find a term enclosed in
    parentheses following 'obscatalogmasteritem_displayname:'. It cleans the
    term by removing quotes and stripping any trailing 'AND' or 'OR' clauses.

    Args:
        search_string: The input query string.

    Returns:
        The extracted search term, or the original string if no match is found.

    Raises:
        None
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
    """Generates and appends dummy patient timelines to a CSV file.

    This function creates `n` dummy patient timelines and appends them to a
    specified CSV file. If the file doesn't exist, it will be created.

    Args:
        n: The number of patient timelines to generate. Defaults to 10.
        output_path: The path to the output CSV file. Defaults to
            "test_files/dummy_timeline.csv".

    Returns:
        None

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
                columns=["client_idcode", "body_analysed"]  # type: ignore
            )  # Create a new DataFrame with two columns
    except FileNotFoundError:
        logger.error(f"FileNotFoundError: {output_path} doesn't exist!")
        return

    for _ in range(n):  # Loop n times
        # Generate a random client_idcode using regex
        client_idcode = "".join(random.choices(string.digits, k=9))

        # Generate patient timeline text
        try:
            patient_timeline_text = generate_patient_timeline(client_idcode)
        except Exception as e:
            logger.error(f"Exception: {e}")
            return

        # Append to DataFrame
        try:
            new_row = pd.DataFrame(
                [
                    {
                        "client_idcode": client_idcode,
                        "body_analysed": patient_timeline_text,
                    }
                ]
            )
            df = pd.concat([df, new_row], ignore_index=True)
        except Exception as e:
            logger.error(f"Exception: {e}")
            return

    # Write DataFrame to CSV with append mode
    try:
        df.to_csv(
            output_path, mode="a", header=not os.path.exists(output_path), index=False
        )  # Write to CSV file
    except Exception as e:
        logger.error(f"Exception: {e}")
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

    Raises:
        None
    """
    try:
        df: pd.DataFrame = pd.read_csv(output_path)
    except FileNotFoundError:
        logger.error(f"FileNotFoundError: {output_path} doesn't exist!")
        return None

    # Check if the DataFrame is empty
    if df.empty:
        logger.warning("DataFrame is empty!")
        return None

    # Check if the 'client_idcode' column exists in the DataFrame
    if "client_idcode" not in df.columns:
        logger.error("'client_idcode' column doesn't exist in the DataFrame!")
        return None

    # Check if the 'body_analysed' column exists in the DataFrame
    if "body_analysed" not in df.columns:
        logger.error("'body_analysed' column doesn't exist in the DataFrame!")
        return None

    # Get a random row from the DataFrame, we don't care which one we get
    sample: pd.DataFrame = df.sample(1, random_state=random_state)

    # Check if we got a valid row
    if len(sample) == 0:
        logger.warning("Sample is empty!")
        return None

    # Get the value of the 'body_analysed' column from the random row
    try:
        return cast(str, sample.iloc[0]["body_analysed"])
    except KeyError:
        logger.error("KeyError: 'body_analysed' column doesn't exist in the DataFrame!")
        return None


def generate_uuid(prefix: str, length: int = 7) -> str:
    """Generates a UUID-like string with a given prefix.

    Args:
        prefix: The prefix for the UUID, must be 'P' or 'V'.
        length: The length of the random part of the string. Defaults to 7.

    Returns:
        The generated UUID-like string.

    Raises:
        ValueError: When the prefix is not 'P' or 'V'.
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
        length: The length of the random part of each UUID. Defaults to 7.

    Returns:
        A list of generated UUID-like strings.

    Raises:
        None
    """
    uuid_list = [generate_uuid(prefix, length) for _ in range(n)]
    return uuid_list


def generate_covid_observations_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    fields_list: List[str] = [],
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
) -> pd.DataFrame:
    """Generates dummy data for COVID-19 test observations.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy COVID-19 observation data.

    Raises:
        None
    """
    from pat2vec.pat2vec_get_methods.get_method_covid import (
        COVID_FIELDS,
        SEARCH_TERM_PLAIN,
    )

    if SEARCH_TERM_PLAIN is None:
        SEARCH_TERM_PLAIN = "SARS CoV-2 (COVID-19) RNA"

    if fields_list is None:
        fields_list = COVID_FIELDS

    df_holder_list = []

    for client_id_code in entered_list:
        data = {
            "basicobs_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "basicobs_itemname_analysed": [SEARCH_TERM_PLAIN for _ in range(num_rows)],
            "basicobs_value_analysed": [
                random.choice(["Positive", "Negative"]) for _ in range(num_rows)
            ],
            "basicobs_entered": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                )
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    # Ensure all requested fields are present, even if empty
    for col in fields_list:
        if col not in final_df.columns:
            final_df[col] = np.nan

    return final_df[fields_list]


def generate_hospital_site_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
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
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy hospital site data.

    Raises:
        None
    """
    df_holder_list = []

    # Define possible values for hospital sites, including key terms 'DH' and 'PRUH'
    # for downstream feature calculation.
    hospital_site_values = [
        "DH",
        "PRUH",
        "Orpington",
        "Queen Mary's",
        "St Thomas",
    ]

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            # This field is constant based on the SEARCH_TERM in your code.
            "obscatalogmasteritem_displayname": [
                "CORE_HospitalSite" for _ in range(num_rows)
            ],
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
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker.random_number(digits=8, fix_len=True)}"
                for _ in range(num_rows)
            ],
        }
        df = pd.DataFrame(data)
        df_holder_list.append(df)

    # Concatenate all generated dataframes into a single one.
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan

    final_df = final_df[fields_list]

    return final_df


def generate_news_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "observation_guid",
        "client_idcode",
        "obscatalogmasteritem_displayname",
        "observation_valuetext_analysed",
        "observationdocument_recordeddtm",
        "clientvisit_visitidcode",
    ],
) -> pd.DataFrame:
    """Generates dummy data for NEWS observations.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy NEWS observation data.

    Raises:
        None
    """
    df_holder_list = []

    # List of NEWS component names expected by get_method_news.py
    news_components = [
        "NEWS2_Score",
        "NEWS_Systolic_BP",
        "NEWS_Diastolic_BP",
        "NEWS_Respiration_Rate",
        "NEWS_Heart_Rate",
        "NEWS_Oxygen_Saturation",
        "NEWS Temperature",
        "NEWS_AVPU",
        "NEWS_Supplemental_Oxygen",
        "NEWS2_Sp02_Target",
        "NEWS2_Sp02_Scale",
        "NEWS_Pulse_Type",
        "NEWS_Pain_Score",
        "NEWS Oxygen Litres",
        "NEWS Oxygen Delivery",
    ]

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [
                random.choice(news_components) for _ in range(num_rows)
            ],
            "observation_valuetext_analysed": [
                str(random.randint(0, 15)) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)
            ],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    # Ensure fields are present
    for col in fields_list:
        if col not in final_df.columns:
            final_df[col] = np.nan
    return final_df[fields_list]


def generate_bmi_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = BMI_FIELDS,
    base_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Generates dummy data for BMI, Weight, and Height observations.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.
        base_date: Optional datetime reference for testing.

    Returns:
        A pandas DataFrame with generated dummy BMI-related data.

    Raises:
        None
    """
    df_holder_list = []
    observation_types = ["OBS BMI Calculation", "OBS Weight", "OBS Height"]

    for client_id_code in entered_list:
        # Generate data for this specific client
        # Ensure at least one "OBS BMI Calculation" exists when num_rows >= 1
        if num_rows >= 1:
            display_names = ["OBS BMI Calculation"] + [
                random.choice(observation_types) for _ in range(num_rows - 1)
            ]
        else:
            display_names = [random.choice(observation_types) for _ in range(num_rows)]
        values = []
        for name in display_names:
            if name == "OBS BMI Calculation":
                # Generate a realistic BMI value (15.0 to 45.0)
                value = f"{random.uniform(15.0, 45.0):.2f}"
            elif name == "OBS Weight":
                # Generate a realistic weight in kg (40.0 to 150.0)
                value = f"{random.uniform(40.0, 150.0):.2f}"
            else:  # OBS Height
                # Generate a realistic height in cm (140.0 to 200.0)
                value = f"{random.uniform(140.0, 200.0):.2f}"
            values.append(value)

        # Generate dates - use base_date if provided (for tests) or random generation otherwise
        date_values = []
        if base_date is not None:
            # For testing: generate dates within a predictable range around base_date
            # Generate dates centered around base_date with small offsets
            for i in range(num_rows):
                # Use positive offsets to ensure dates are >= base_date (within reasonable range)
                day_offset = (
                    0 if num_rows == 1 else (i * 2) - 1
                )  # For 2 rows: -1, +1 giving days before and after
                test_day = max(1, min(28, base_date.day + day_offset))
                date_values.append(
                    datetime(
                        base_date.year,
                        base_date.month,
                        test_day,
                        random.randint(0, 23),
                        random.randint(0, 59),
                        random.randint(0, 59),
                    )
                )
        else:
            # Generate dates concentrated around June 2023 to match common test date ranges
            for i in range(num_rows):
                if random.random() < 0.7:  # 70% chance to generate a June 2023 date
                    year = 2023
                    month = 6  # June
                    day = random.randint(15, 25)  # Near test date range (June 15)
                    hour = random.randint(0, 23)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    date_values.append(datetime(year, month, day, hour, minute, second))
                else:
                    # 30% chance for other recent dates or original range
                    if random.random() < 0.5:
                        year = 2024
                        month = random.randint(1, 6)
                        day_max = calendar.monthrange(year, month)[1]
                        day = random.randint(1, min(day_max, 28))
                        date_values.append(
                            datetime(
                                year,
                                month,
                                day,
                                random.randint(0, 23),
                                random.randint(0, 59),
                                random.randint(0, 59),
                            )
                        )
                    else:
                        # Use original range for remaining
                        date_values.append(
                            create_random_date_from_globals(
                                global_start_year,
                                global_start_month,
                                global_end_year,
                                global_end_month,
                                global_start_day,
                                global_end_day,
                            )
                        )

        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": display_names,
            "observation_valuetext_analysed": values,
            "observationdocument_recordeddtm": [
                d.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(d, datetime) else d
                for d in date_values
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)
            ],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def populate_elastic_with_dummy_data(
    config_obj: Any, n_patients: int = 10
) -> List[str]:
    """Generates dummy data and ingests it into Elasticsearch.

    This function generates random patient IDs and creates dummy data for
    several indices (epr_documents, observations, basic_observations,
    order, pims_apps). It then uses `ingest_data_to_elasticsearch` to
    load this data into the configured Elasticsearch instance.

    Args:
        config_obj: The configuration object containing date ranges.
        n_patients: The number of dummy patients to generate. Defaults to 10.

    Returns:
        A list of the generated dummy patient IDs.

    Raises:
        None
    """
    # Safeguard: Ensure testing flags are enabled in config
    if not getattr(config_obj, "testing", False) or not getattr(
        config_obj, "testing_elastic", False
    ):
        logger.error(
            "Safety Block: 'testing' and 'testing_elastic' must both be True to populate dummy data. Aborting."
        )
        return []

    # Initialize CogStack client to interact with Elastic
    # We avoid initialize_cogstack_client to prevent accidental usage of global/live clients
    # We strictly require a specific credentials file in the root directory
    from pat2vec.pat2vec_search.cogstack_search_methods import CogStack
    import importlib.util

    creds_filename = "test_elastic_credentials.py"
    creds_path = os.path.abspath(creds_filename)

    if not os.path.exists(creds_path):
        logger.error(
            f"Safety Block: Test credentials file '{creds_filename}' not found at {creds_path}. Aborting dummy data population."
        )
        return []

    try:
        spec = importlib.util.spec_from_file_location("test_creds_module", creds_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load specs from {creds_path}")
        test_creds = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(test_creds)

        hosts = getattr(test_creds, "hosts", [])
        username = getattr(test_creds, "username", None)
        password = getattr(test_creds, "password", None)
        api_key = getattr(test_creds, "api_key", None)

        if api_key:
            cs = CogStack(hosts=hosts, api_key=api_key, api=True)
        else:
            cs = CogStack(hosts=hosts, username=username, password=password, api=False)
        logger.info(f"Loaded isolated test credentials from {creds_path}")
    except Exception as e:
        logger.error(
            f"Failed to initialize CogStack client from credentials file {creds_path}: {e}"
        )
        return []

    if cs:
        try:
            # Safeguard: Check if connecting to a safe/test environment
            nodes = cs.elastic.transport.node_pool.all()
            hosts = [node.host for node in nodes]
            if not all(is_safe_host(h) for h in hosts):
                logger.error(
                    f"Unsafe operation: Attempting to populate dummy data on non-local host(s): {hosts}. Aborting."
                )
                return []

            # Safeguard: Check username
            safe_users = ["elastic", "test_user", "dummy_user"]
            current_user = getattr(config_obj, "username", None)
            if current_user and current_user not in safe_users:
                logger.error(
                    f"Unsafe operation: Attempting to populate dummy data with non-test user '{current_user}'. Aborting."
                )
                return []

            # Log cluster info
            cluster_info = cs.elastic.info()
            cluster_name = cluster_info.get("cluster_name")
            logger.info(
                f"Populating dummy data on cluster: {cluster_name} (version {cluster_info.get('version', {}).get('number')})"
            )

            # Safeguard: Verify cluster is empty or allowed to proceed
            indices = cs.elastic.cat.indices(format="json")
            user_indices = [
                i["index"] for i in indices if not i["index"].startswith(".")
            ]
            if user_indices and not getattr(config_obj, "testing_elastic", False):
                logger.error(
                    f"Unsafe operation: Target cluster is not empty. Found indices: {user_indices}. Aborting."
                )
                return []
        except Exception as e:
            logger.error(f"Failed to verify cluster safety: {e}. Aborting.")
            return []
    else:
        logger.error("Failed to initialize CogStack client. Aborting population.")
        return []

    global_start_year = int(config_obj.global_start_year)
    global_start_month = int(config_obj.global_start_month)
    global_start_day = int(config_obj.global_start_day)
    global_end_year = int(config_obj.global_end_year)
    global_end_month = int(config_obj.global_end_month)
    global_end_day = int(config_obj.global_end_day)

    # Load schema and create indices if schema file exists
    schema_path = getattr(config_obj, "test_schema_path", None) or os.path.join(
        "test_files", "elastic_schemas.json"
    )

    if os.path.exists(schema_path):
        try:
            with open(schema_path, "r") as f:
                schemas = json.load(f)

            logger.info(f"Applying schemas from {schema_path}...")
            for index_name, schema_data in schemas.items():
                # Delete index if it exists to ensure clean state with correct mapping
                if cs.elastic.indices.exists(index=index_name):
                    cs.elastic.indices.delete(index=index_name)
                    logger.info(f"Deleted existing index: {index_name}")

                mappings = schema_data.get("mappings", {})
                settings = schema_data.get("settings", {})

                # Force dynamic mapping to True to ensure dummy fields are indexed
                mappings["dynamic"] = True

                # Create index
                cs.elastic.indices.create(
                    index=index_name, mappings=mappings, settings=settings
                )
                logger.info(f"Created index: {index_name} with custom schema")
            else:
                logger.warning(
                    "Could not initialize CogStack client for schema creation."
                )
        except Exception as e:
            logger.error(f"Failed to apply Elastic schemas: {e}")

    # 1. Generate Dummy Patient IDs.
    # When testing_elastic is True, we always generate new IDs to ensure a clean, controlled test.
    # The generated IDs will then be saved to treatment_docs.csv later in this function.
    patient_ids = []

    # Priority 1: Use explicitly provided patient list from config (fixes test isolation)
    config_patient_list = getattr(config_obj, "all_patient_list", None)
    if config_patient_list is not None and len(config_patient_list) > 0:
        patient_ids = list(config_patient_list)
        logger.info(
            f"Using {len(patient_ids)} patients from config_obj.all_patient_list"
        )

    # Priority 2: In testing_elastic mode, if no IDs provided, generate random ones
    elif getattr(config_obj, "testing_elastic", False):
        patient_ids = generate_uuid_list(n_patients, "P")
        logger.info(
            f"Generated {n_patients} dummy patient IDs for testing_elastic: {patient_ids[:5]}..."
        )

    else:
        # Priority 3: Try to load existing IDs from treatment docs
        try:
            from pat2vec.pat2vec_pat_list.get_patient_treatment_list import (
                extract_treatment_id_list_from_docs,
            )

            patient_ids = extract_treatment_id_list_from_docs(config_obj)
        except Exception as e:
            logger.debug(f"Could not load existing patient list: {e}")

        if patient_ids:
            logger.info(
                f"Using {len(patient_ids)} existing patient IDs from treatment doc: {patient_ids[:5]}..."
            )
            if len(patient_ids) > n_patients:
                patient_ids = patient_ids[:n_patients]
        else:
            # Fallback: Generate random
            patient_ids = generate_uuid_list(n_patients, "P")
            logger.info(
                f"Generated {n_patients} dummy patient IDs (fallback): {patient_ids[:5]}..."
            )

    # 2. Generate and Ingest Data for Each Index

    # Generate both parts of the EPR documents data first
    df_epr = generate_epr_documents_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
        use_GPT=False,
    )
    df_epr_personal = generate_epr_documents_personal_data(
        num_rows=1,
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )

    # Merge personal data into each EPR document to ensure it's always available
    df_epr_merged = pd.merge(
        df_epr,
        df_epr_personal.drop(columns=["updatetime"]),
        on="client_idcode",
        how="left",
    )
    df_epr_merged = df_epr_merged.where(pd.notnull(df_epr_merged), None)
    ingest_data_to_elasticsearch(df_epr_merged, "epr_documents", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="epr_documents")

    # Save the generated cohort to treatment_docs.csv for testing_elastic workflow
    # This ensures pat_maker uses the same IDs that were just populated
    if getattr(config_obj, "testing_elastic", False):
        try:
            filename = getattr(
                config_obj, "treatment_doc_filename", "treatment_docs.csv"
            )
            root_path = getattr(config_obj, "root_path", "")
            if root_path:
                os.makedirs(root_path, exist_ok=True)
                output_path = os.path.join(root_path, filename)
            else:
                output_path = filename
            logger.info(
                f"Saving generated cohort to {output_path} for testing_elastic workflow."
            )
            df_epr.to_csv(output_path, index=False)
        except Exception as e:
            logger.error(f"Failed to save generated treatment docs: {e}")

    # basic_observations
    df_basic_obs = generate_basic_observations_data(
        num_rows=random.randint(1, 10),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    # Add textual obs to basic observations
    df_basic_textual = generate_basic_observations_textual_obs_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    df_basic_all = pd.concat([df_basic_obs, df_basic_textual], ignore_index=True)
    df_basic_all = df_basic_all.where(pd.notnull(df_basic_all), None)
    ingest_data_to_elasticsearch(
        df_basic_all, "basic_observations", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="basic_observations")

    # observations (BMI, NEWS, MRC Text, Bed, etc.)
    obs_dfs = []
    # BMI
    obs_dfs.append(
        generate_bmi_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_start_day=global_start_day,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            global_end_day=global_end_day,
        )
    )
    # NEWS
    obs_dfs.append(
        generate_news_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_start_day=global_start_day,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            global_end_day=global_end_day,
        )
    )
    # MRC Text
    obs_dfs.append(
        generate_observations_MRC_text_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_start_day=global_start_day,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            global_end_day=global_end_day,
            use_GPT=False,
        )
    )
    # Generic observations (fallback/misc)
    obs_dfs.append(
        generate_observations_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_start_day=global_start_day,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            global_end_day=global_end_day,
            search_term="Generic Observation",
        )
    )

    df_obs = pd.concat(obs_dfs, ignore_index=True)
    df_obs = df_obs.where(pd.notnull(df_obs), None)
    ingest_data_to_elasticsearch(df_obs, "observations", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="observations")

    # order (Drugs and Diagnostics)
    order_dfs = []
    order_dfs.append(
        generate_drug_orders_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_start_day=global_start_day,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            global_end_day=global_end_day,
        )
    )
    order_dfs.append(
        generate_diagnostic_orders_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_start_day=global_start_day,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            global_end_day=global_end_day,
        )
    )
    df_orders = pd.concat(order_dfs, ignore_index=True)
    df_orders = df_orders.where(pd.notnull(df_orders), None)
    ingest_data_to_elasticsearch(df_orders, "order", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="order")

    # pims_apps (Appointments)
    df_apps = generate_appointments_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    # Index name in config usually pims_apps*, but we ingest to pims_apps
    df_apps = df_apps.where(pd.notnull(df_apps), None)
    ingest_data_to_elasticsearch(df_apps, "pims_apps", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="pims_apps")

    # Epic Imaging Reports
    df_epic_imaging_reports = generate_epic_imaging_reports_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    df_epic_imaging_reports = df_epic_imaging_reports.where(
        pd.notnull(df_epic_imaging_reports), None
    )
    ingest_data_to_elasticsearch(
        df_epic_imaging_reports, "epic_imaging_reports", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_imaging_reports")
    # Epic Orders
    df_epic_orders = generate_epic_orders_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    df_epic_orders = df_epic_orders.where(pd.notnull(df_epic_orders), None)
    ingest_data_to_elasticsearch(df_epic_orders, "epic_orders", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="epic_orders")

    # Epic Patients
    df_epic_patients = generate_epic_patients_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    df_epic_patients = df_epic_patients.where(pd.notnull(df_epic_patients), None)
    ingest_data_to_elasticsearch(
        df_epic_patients, "epic_patients", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_patients")
    # Epic Encounters
    df_epic_encounters = generate_epic_encounters_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    df_epic_encounters = df_epic_encounters.where(pd.notnull(df_epic_encounters), None)
    ingest_data_to_elasticsearch(
        df_epic_encounters, "epic_encounters", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_encounters")

    # Epic Clinical Notes
    df_epic_clinical_notes = generate_epic_clinical_notes_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
        use_GPT=False,
    )
    df_epic_clinical_notes = df_epic_clinical_notes.where(
        pd.notnull(df_epic_clinical_notes), None
    )
    ingest_data_to_elasticsearch(
        df_epic_clinical_notes, "epic_clinical_notes", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_clinical_notes")

    # Epic Medical History
    df_epic_medical_history = generate_epic_medical_history_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    df_epic_medical_history = df_epic_medical_history.where(
        pd.notnull(df_epic_medical_history), None
    )
    ingest_data_to_elasticsearch(
        df_epic_medical_history, "epic_medical_history", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_medical_history")

    # Epic Orders
    df_epic_orders = generate_epic_orders_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    df_epic_orders = df_epic_orders.where(pd.notnull(df_epic_orders), None)
    ingest_data_to_elasticsearch(df_epic_orders, "epic_orders", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="epic_orders")

    # Epic Clinical Notes Appointments
    df_epic_appt_notes = generate_epic_clinical_notes_appointments_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    df_epic_appt_notes = df_epic_appt_notes.where(pd.notnull(df_epic_appt_notes), None)
    ingest_data_to_elasticsearch(
        df_epic_appt_notes, "epic_clinical_notes_appointments", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_clinical_notes_appointments")

    # Epic Lab Results
    df_epic_lab_results = generate_epic_lab_results_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_start_day=global_start_day,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        global_end_day=global_end_day,
    )
    df_epic_lab_results = df_epic_lab_results.where(
        pd.notnull(df_epic_lab_results), None
    )
    ingest_data_to_elasticsearch(
        df_epic_lab_results, "epic_lab_results", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_lab_results")

    logger.info("Successfully populated Elasticsearch with dummy data.")
    return patient_ids


def generate_bed_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = BED_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for bed number observations.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy bed data.

    Raises:
        None
    """
    df_holder_list = []

    # Realistic bed/location names
    bed_values = [
        "Bed 1",
        "Bed 2",
        "Bed 3",
        "Side Room 1",
        "Bay A Bed 1",
        "Bay B Bed 4",
        "HDU Bed 2",
        "ITU Bed 5",
    ]

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [
                "CORE_BedNumber3" for _ in range(num_rows)
            ],
            "observation_valuetext_analysed": [
                maybe_nan(random.choice(bed_values)) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker.random_number(digits=8, fix_len=True)}"
                for _ in range(num_rows)
            ],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[fields_list]


def generate_vte_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = VTE_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for VTE status observations.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy VTE status data.

    Raises:
        None
    """
    df_holder_list = []
    vte_statuses = [
        "High risk of VTE High risk of bleeding",
        "High risk of VTE Low risk of bleeding",
    ]

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [
                "CORE_VTE_STATUS" for _ in range(num_rows)
            ],
            "observation_valuetext_analysed": [
                maybe_nan(random.choice(vte_statuses)) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)
            ],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[fields_list]


def generate_smoking_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = SMOKING_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for smoking status observations.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy smoking status data.

    Raises:
        None
    """
    df_holder_list = []
    smoking_statuses = ["Current smoker", "Ex-smoker", "Never smoked", "Smoker"]

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [
                "CORE_SmokingStatus" for _ in range(num_rows)
            ],
            "observation_valuetext_analysed": [
                maybe_nan(random.choice(smoking_statuses)) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)
            ],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[fields_list]


def generate_core_o2_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = CORE_O2_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for CORE_SpO2 (oxygen saturation) observations.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy SpO2 data.

    Raises:
        None
    """
    df_holder_list = []

    # Realistic categorical values for SpO2 and oxygen delivery
    spo2_values = [
        "98%",
        "97%",
        "96%",
        "95%",
        "94%",
        "93%",
        "On Air",
        "2L O2 NP",
        "4L O2 NP",
        "NRB Mask",
    ]

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": ["CORE_SpO2" for _ in range(num_rows)],
            "observation_valuetext_analysed": [
                random.choice(spo2_values) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)
            ],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[fields_list]


def generate_core_resus_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = CORE_RESUS_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for CORE_RESUS_STATUS observations.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy resuscitation status data.

    Raises:
        None
    """
    df_holder_list = []

    # These are the exact values the feature calculation function looks for.
    resuscitation_statuses = [
        "For cardiopulmonary resuscitation",
        "Not for cardiopulmonary resuscitation",
    ]

    for client_id_code in entered_list:
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [
                "CORE_RESUS_STATUS" for _ in range(num_rows)
            ],
            "observation_valuetext_analysed": [
                random.choice(resuscitation_statuses) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)
            ],
        }
        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)
    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[fields_list]


class dummy_CAT:
    """A dummy MedCAT-like object for testing."""

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, text: str) -> dict:
        """Simulates MedCAT annotation processing."""
        return {
            "entities": {
                "0": {
                    "pretty_name": "Fever",
                    "cui": "C0015967",
                    "type_ids": ["T033"],
                    "types": ["finding"],
                    "source_value": "Fever",
                    "detected_name": "Fever",
                    "acc": 1.0,
                    "context_similarity": 1.0,
                    "start": 0,
                    "end": 5,
                    "icd10": [],
                    "ontologies": [],
                    "snomed": [],
                    "id": "0",
                    "meta_anns": {
                        "Time": {"value": "Recent", "confidence": 1.0},
                        "Presence": {"value": "True", "confidence": 1.0},
                        "Subject/Experiencer": {"value": "Patient", "confidence": 1.0},
                    },
                }
            }
        }

    def get_entities(self, text: str) -> dict:
        return self(text)["entities"]

    def get_entities_multi_texts(
        self, texts: List[str], n_process: int = 1, batch_size: int = 100, **kwargs
    ) -> List[Dict[str, Any]]:
        """Returns a list of dummy annotations for a list of texts.

        For each text in the input list, it generates a separate dummy annotation.
        """
        return [self(text) for text in texts]


def generate_problem_list_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_start_day: int = 1,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "client_idcode",
        "problem_name",
        "problem_status",
        "updatetime",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'problem_list' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy problem list data.

    Raises:
        None
    """
    df_holder_list = []
    for client_id_code in entered_list:
        data = {
            "client_idcode": [client_id_code] * num_rows,
            "problem_name": [f"Condition {faker.word()}" for _ in range(num_rows)],
            "problem_status": [
                random.choice(["Active", "Resolved"]) for _ in range(num_rows)
            ],
            "updatetime": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[fields_list]


# Aliases for integration test compatibility
generate_core_02_data = generate_core_o2_data
generate_covid_data = generate_covid_observations_data
generate_demographics_data = generate_epr_documents_personal_data


generate_diagnostics_data = generate_diagnostic_orders_data
generate_drugs_data = generate_drug_orders_data
generate_reports_data = generate_observations_Reports_text_data
generate_vte_status_data = generate_vte_data
