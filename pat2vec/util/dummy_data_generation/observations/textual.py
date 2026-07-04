"""Textual observation generators for MRC clinical notes and Reports."""

import random
from typing import List

import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals
from ..sequence_generators import generate_patient_timeline, get_patient_timeline_dummy

random_state = 42
faker = Faker()
faker.seed_instance(random_state)
random.seed(random_state)


def generate_observations_MRC_text_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
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
    """Generates dummy data for the 'observations' index (MRC clinical notes)."""
    df_holder_list = []

    for i in range(0, len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        timeline_generator = (
            generate_patient_timeline if use_GPT else get_patient_timeline_dummy
        )

        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": ["AoMRC_ClinicalSummary_FT"],
            "observation_valuetext_analysed": [
                (
                    timeline_generator(current_pat_client_id_code)
                    if use_GPT
                    else get_patient_timeline_dummy(current_pat_client_id_code) or ""
                )
                for _ in range(num_rows)
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
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)

    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    target_col = "observation_valuetext_analysed"
    if target_col in df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in df.columns:
            df[field] = None

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)
    return df


def generate_observations_Reports_text_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
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
    """Generates dummy data for the 'basic_observations' index (Reports)."""
    random.seed(random_state)
    df_holder_list = []

    timeline_generator = (
        generate_patient_timeline if use_GPT else get_patient_timeline_dummy
    )

    for i in range(0, len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        data = {
            "basicobs_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "basicobs_itemname_analysed": ["Report"],
            "basicobs_value_analysed": [""],
            "textualObs": [
                (
                    timeline_generator(current_pat_client_id_code)
                    if use_GPT
                    else get_patient_timeline_dummy(current_pat_client_id_code) or ""
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
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)

    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    target_col = "textualObs"
    if target_col in df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in df.columns:
            df[field] = None

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)
    return df
