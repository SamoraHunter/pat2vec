"""Diagnostic and Drug order generators.

This module contains functions for generating synthetic diagnostic and drug order data.
"""

import random
from datetime import datetime
from typing import List, Optional

import pandas as pd
from faker import Faker

from .generator_helpers import create_random_date_from_globals, maybe_nan

# Global instances
random_state = 42
faker = Faker()
faker.seed_instance(random_state)
random.seed(random_state)


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
        "order_typecode",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'diagnostic_orders' index."""
    df_holder_list = []

    from pat2vec.util.dummy_data_files import dummy_lists

    diagnostic_names = getattr(dummy_lists, "diagnostic_names", [])

    for i in range(0, len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        data = {
            "order_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "order_name": [
                (
                    faker.random_element(diagnostic_names)
                    if diagnostic_names
                    else f"Diagnostic_{i}"
                )
                for _ in range(num_rows)
            ],
            "order_summaryline": [
                maybe_nan(" ".join(faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
            "order_holdreasontext": [
                maybe_nan(" ".join(faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
            "order_entered": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "order_createdwhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
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
            df[field] = None

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)
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
        "order_typecode",
    ],
    base_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Generates dummy data for the 'drug_orders' index."""
    df_holder_list = []

    from pat2vec.util.dummy_data_files import dummy_lists

    drug_names = getattr(dummy_lists, "drug_names", [])

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
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ]
            order_created_dates = [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ]
            order_performed_dates = [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ]

        data = {
            "order_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "order_name": [
                faker.random_element(drug_names) if drug_names else f"Drug_{i}"
                for _ in range(num_rows)
            ],
            "order_summaryline": [
                maybe_nan(" ".join(faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
            "order_holdreasontext": [
                maybe_nan(" ".join(faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
            "order_entered": order_entered_dates,
            "order_createdwhen": order_created_dates,
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
            "order_typecode": ["medication" for _ in range(num_rows)],
            "order_performeddtm": order_performed_dates,
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in df.columns:
            df[field] = None

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)
    return df
