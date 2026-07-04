"""NEWS observation generator."""

import random
from typing import List

import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals

random_state = 42
faker = Faker()
faker.seed_instance(random_state)
random.seed(random_state)


def generate_news_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
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
    """Generates dummy data for NEWS observations."""
    df_holder_list = []

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

    for col in fields_list:
        if col not in final_df.columns:
            final_df[col] = None

    return final_df[fields_list]
