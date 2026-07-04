"""Epic encounters generator."""

import random
from datetime import timedelta
from typing import List

import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals

random_state = 42
Faker.seed(random_state)
faker = Faker()
random.seed(random_state)


def generate_epic_encounters_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
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
    """Generates dummy data for the 'epic_encounters' index."""
    df_holder_list = []

    for client_id_code in entered_list:
        admission_date = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
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

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    final_df["search_term"] = "Condition"

    unique_fields = list(dict.fromkeys(fields_list))

    if (
        "activity_PatientDurableKey" in final_df.columns
        and "activity_PatientDurableKey" not in unique_fields
    ):
        unique_fields.append("activity_PatientDurableKey")

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = None

    return final_df[unique_fields]
