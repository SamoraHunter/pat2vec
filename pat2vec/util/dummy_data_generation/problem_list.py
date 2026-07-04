"""Problem list generator."""

import random
from typing import List

import pandas as pd
from faker import Faker

from .generator_helpers import create_random_date_from_globals

random_state = 42
faker = Faker()
faker.seed_instance(random_state)
random.seed(random_state)


def generate_problem_list_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "client_idcode",
        "problem_name",
        "problem_status",
        "updatetime",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'problem_list' index."""
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

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = None

    return final_df[fields_list]
