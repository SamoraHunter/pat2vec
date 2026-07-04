"""Basic observation generators."""

import random
import uuid
from datetime import datetime
from typing import List, Optional

import pandas as pd
from faker import Faker

from pat2vec.util.dummy_data_generation.generator_helpers import (
    create_random_date_from_globals,
    maybe_nan,
)
from pat2vec.util.dummy_data_files import dummy_lists

random_state = 42
faker = Faker()
faker.seed_instance(random_state)
random.seed(random_state)


def generate_basic_observations_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: Optional[List[str]] = None,
    base_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Generates dummy data for the 'basic_observations' index."""
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

    random.seed(random_state)
    df_holder_list = []

    blood_test_names = getattr(dummy_lists, "blood_test_names", [])

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
                (
                    faker.random_element(blood_test_names)
                    if blood_test_names
                    else f"Test_{i}"
                )
                for _ in range(num_rows)
            ],
            "basicobs_value_numeric": [random.uniform(1, 100) for _ in range(num_rows)],
            "basicobs_entered": basicobs_entered_dates,
            "clientvisit_serviceguid": [f"service_{i}" for i in range(num_rows)],
            "_id": [None for _ in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
            "order_guid": [faker.uuid4() for _ in range(num_rows)],
            "order_name": [faker.word() for _ in range(num_rows)],
            "order_summaryline": [
                maybe_nan(" ".join(faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
            "order_holdreasontext": [
                maybe_nan(" ".join(faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
            "order_entered": order_entered_dates,
            "clientvisit_visitidcode": [str(uuid.uuid4()) for _ in range(num_rows)],
            "updatetime": updatetime_dates,
        }

        if num_rows > 0 and blood_test_names:
            data["basicobs_itemname_analysed"][0] = "Glucose"

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    df = pd.concat(df_holder_list, ignore_index=True)

    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in df.columns:
            df[field] = None

    return df


def generate_observations_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
    global_end_day: int = 31,
    search_term: str = "Test",
    fields_list: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Generates generic observation data for the 'observations' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range. Defaults to 2023.
        global_end_month: End month for the random date range. Defaults to 12.
        search_term: The search term to use for the display name. Defaults to "Test".
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy observation data.
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

    random.seed(random_state)
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
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = None

    return final_df[unique_fields]


def generate_basic_observations_textual_obs_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Generates dummy textual data for the 'basic_observations' index."""
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

    blood_test_names = getattr(dummy_lists, "blood_test_names", [])

    for i in range(0, len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        data = {
            "client_idcode": [current_pat_client_id_code] * num_rows,
            "basicobs_itemname_analysed": [
                (
                    faker.random_element(blood_test_names)
                    if blood_test_names
                    else f"Test_{i}"
                )
                for _ in range(num_rows)
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
            "_id": [None for _ in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
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
                maybe_nan(" ".join(faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
        }

        if num_rows > 0 and blood_test_names:
            data["basicobs_itemname_analysed"][0] = "Glucose"

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    df = pd.concat(df_holder_list, ignore_index=True)

    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in df.columns:
            df[field] = None

    df = df[unique_fields]
    return df
