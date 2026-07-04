"""Epic lab results generator."""

import random
from typing import List

import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals

random_state = 42
Faker.seed(random_state)
faker = Faker()
random.seed(random_state)


def generate_epic_lab_results_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
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
    """Generates dummy data for the 'epic_lab_results' index."""
    df_holder_list = []

    for client_id_code in entered_list:
        collected_date = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
        )

        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
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

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    unique_fields = list(dict.fromkeys(fields_list))

    target_col = "document_Content"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = None

    return final_df[unique_fields]
