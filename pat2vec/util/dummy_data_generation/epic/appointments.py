from ..generator_helpers import create_random_date_from_globals

import random

from faker import Faker
import numpy as np
import pandas as pd

random_state = 42
Faker.seed(random_state)
faker = Faker()
np.random.seed(random_state)
random.seed(random_state)


def generate_epic_clinical_notes_appointments_data(
    num_rows: int,
    entered_list: list[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    fields_list: list[str] = [
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
