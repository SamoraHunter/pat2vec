"""NEWS observation generator."""

import random
from typing import List

import numpy as np
import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals

random_state = 42
faker = Faker()
faker.seed_instance(random_state)
np.random.seed(random_state)
random.seed(random_state)

MAX_NEWS_COMPONENTS = 15


CLINICAL_RANGES = {
    "NEWS_Systolic_BP": (70, 250),
    "NEWS_Diastolic_BP": (40, 150),
    "NEWS_Respiration_Rate": (8, 50),
    "NEWS_Heart_Rate": (40, 200),
    "NEWS_Oxygen_Saturation": (85, 100),
    "NEWS Temperature": (35.0, 42.0),
    "NEWS_Pain_Score": (0, 10),
}


AVPU_VALUES = ["A", "V", "P", "U"]
AVPU_WEIGHTS = [0.70, 0.15, 0.10, 0.05]

SUPPLEMENTAL_OXYGEN_VALUES = ["No", "Yes"]
SUPPLEMENTAL_OXYGEN_WEIGHTS = [0.60, 0.40]

PULSE_TYPE_VALUES = ["Regular", "Irregular", "Strong", "Weak"]
PULSE_TYPE_WEIGHTS = [0.70, 0.15, 0.10, 0.05]

OXYGEN_DELIVERY_VALUES = [
    "None",
    "Nasal Prongs",
    "Face Mask",
    "Venturi Mask",
    "High Flow Nasal Cannula",
    "Non-Rebreather Mask",
]
OXYGEN_DELIVERY_WEIGHTS = [0.60, 0.20, 0.10, 0.05, 0.03, 0.02]

SP02_TARGET_VALUES = ["94-98%", "92-94%"]
SP02_SCALE_VALUES = ["Standard", "High"]


def generate_clinically_coherent_value(
    component: str,
    respiration_rate: float = None,
    heart_rate: float = None,
    temp_celsius: float = None,
    pain_score: int = None,
    avpu: str = None,
) -> str:
    """Generate a clinically coherent value for a NEWS component based on other vital signs."""

    if component == "NEWS_Systolic_BP":
        if respiration_rate is not None and respiration_rate > 20:
            return str(int(random.uniform(130, 250)))
        elif heart_rate is not None and heart_rate > 100:
            return str(int(random.uniform(140, 250)))
        else:
            return str(int(random.uniform(70, 250)))

    elif component == "NEWS_Diastolic_BP":
        if respiration_rate is not None and respiration_rate > 20:
            return str(int(random.uniform(85, 150)))
        elif heart_rate is not None and heart_rate > 100:
            return str(int(random.uniform(90, 150)))
        else:
            return str(int(random.uniform(40, 150)))

    elif component == "NEWS_Respiration_Rate":
        if temp_celsius is not None and temp_celsius > 38.0:
            return str(int(random.uniform(20, 35)))
        else:
            return str(int(random.uniform(8, 50)))

    elif component == "NEWS_Heart_Rate":
        if respiration_rate is not None and respiration_rate > 24:
            return str(int(random.uniform(100, 160)))
        elif temp_celsius is not None and temp_celsius > 38.5:
            return str(int(random.uniform(90, 150)))
        else:
            return str(int(random.uniform(40, 200)))

    elif component == "NEWS_Oxygen_Saturation":
        if respiration_rate is not None and respiration_rate > 20:
            return str(int(random.uniform(89, 96)))
        elif heart_rate is not None and heart_rate > 110:
            return str(int(random.uniform(88, 97)))
        else:
            return str(int(random.uniform(85, 100)))

    elif component == "NEWS Temperature":
        if pain_score is not None and pain_score >= 7:
            return f"{random.uniform(36.5, 39.5):.1f}"
        else:
            return f"{random.uniform(35.0, 42.0):.1f}"

    elif component == "NEWS_AVPU":
        if temp_celsius is not None and temp_celsius > 39.0:
            return random.choices(AVPU_VALUES, weights=AVPU_WEIGHTS)[0]
        else:
            return random.choices(AVPU_VALUES, weights=AVPU_WEIGHTS)[0]

    elif component == "NEWS_Supplemental_Oxygen":
        if pain_score is not None and pain_score >= 7:
            return random.choices(
                SUPPLEMENTAL_OXYGEN_VALUES, weights=SUPPLEMENTAL_OXYGEN_WEIGHTS
            )[0]
        else:
            return random.choices(
                SUPPLEMENTAL_OXYGEN_VALUES, weights=SUPPLEMENTAL_OXYGEN_WEIGHTS
            )[0]

    elif component == "NEWS2_Sp02_Target":
        return random.choice(SP02_TARGET_VALUES)

    elif component == "NEWS2_Sp02_Scale":
        return random.choice(SP02_SCALE_VALUES)

    elif component == "NEWS_Pulse_Type":
        return random.choices(PULSE_TYPE_VALUES, weights=PULSE_TYPE_WEIGHTS)[0]

    elif component == "NEWS_Pain_Score":
        if temp_celsius is not None and temp_celsius > 38.5:
            return str(int(random.uniform(4, 10)))
        else:
            return str(int(random.uniform(0, 10)))

    elif component == "NEWS Oxygen Litres":
        if pain_score is not None and pain_score >= 7:
            return f"{random.uniform(5, 15):.1f}"
        else:
            return f"{random.uniform(0, 15):.1f}"

    elif component == "NEWS Oxygen Delivery":
        return random.choices(OXYGEN_DELIVERY_VALUES, weights=OXYGEN_DELIVERY_WEIGHTS)[
            0
        ]

    else:
        return str(random.randint(0, 15))


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
        "observationdocument_recordddtm",
        "clientvisit_visitidcode",
    ],
) -> pd.DataFrame:
    """Generates dummy data for NEWS observations."""
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

    df_holder_list = []

    for client_id_code in entered_list:
        observation_data = []

        respiration_rate_val = int(random.uniform(8, 50))
        heart_rate_val = int(random.uniform(40, 200))
        temp_celsius_val = float(f"{random.uniform(35.0, 42.0):.1f}")
        pain_score_val = int(random.uniform(0, 10))

        for _ in range(num_rows):
            random_observation_type = random.choice(news_components)

            if random_observation_type == "NEWS_Respiration_Rate":
                respiration_rate_for_correlation = respiration_rate_val
            else:
                respiration_rate_for_correlation = None

            value_text = generate_clinically_coherent_value(
                random_observation_type,
                respiration_rate=respiration_rate_for_correlation,
                heart_rate=heart_rate_val,
                temp_celsius=temp_celsius_val,
                pain_score=pain_score_val,
            )

            obs_datetime_str = create_random_date_from_globals(
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                global_start_day,
                global_end_day,
            ).strftime("%Y-%m-%dT%H:%M:%S")

            observation_data.append(
                {
                    "observation_guid": faker.uuid4(),
                    "client_idcode": client_id_code,
                    "obscatalogmasteritem_displayname": random_observation_type,
                    "observation_valuetext_analysed": value_text,
                    "observationdocument_recordddtm": obs_datetime_str,
                    "clientvisit_visitidcode": f"visit_{faker.random_number(digits=8)}",
                }
            )

        df_holder_list.append(pd.DataFrame(observation_data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    for col in fields_list:
        if col not in final_df.columns:
            final_df[col] = None

    return final_df[fields_list]
