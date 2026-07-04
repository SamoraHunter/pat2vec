"""EPR documents dummy data generation functions.

This module provides functions for generating synthetic EPR (Electronic Patient
Records) document data and related personal information for testing and development.
"""

import logging
import random
import uuid
from typing import List, Optional

from faker import Faker

import pandas as pd

from pat2vec.util.dummy_data_files import dummy_lists
from pat2vec.util.dummy_data_generation.generator_helpers import (
    create_random_date_from_globals,
    maybe_nan,
)
from pat2vec.util.dummy_data_generation.sequence_generators import (
    generate_patient_timeline,
    get_patient_timeline_dummy,
)

ethnicity_list = getattr(dummy_lists, "ethnicity_list", [])
logger = logging.getLogger(__name__)

random_state = 42
Faker.seed(random_state)
faker = Faker()
random.seed(random_state)


def generate_epr_documents_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    use_GPT: bool = True,
    global_start_day: int = 1,
    global_end_day: int = 31,
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
        use_GPT: If True, uses a text generation model for document body.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy EPR document data.
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
        df = pd.concat(df_holder_list, axis=0, ignore_index=True)
        unique_fields = list(dict.fromkeys(fields_list))

        if "body_analysed" in df.columns and "body_analysed" not in unique_fields:
            unique_fields.append("body_analysed")

        for field in unique_fields:
            if field not in df.columns:
                df[field] = pd.np.nan

        df = df[unique_fields]

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
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
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
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
        }

        df = pd.DataFrame(data)

        for col in fields_list:
            if col not in df.columns:
                df[col] = pd.np.nan

        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    unique_fields = list(dict.fromkeys(fields_list))

    for field in unique_fields:
        if field not in df.columns:
            df[field] = pd.np.nan

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)

    return df
