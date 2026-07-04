import random

from faker import Faker
import numpy as np
import pandas as pd

random_state = 42
Faker.seed(random_state)
faker = Faker()
np.random.seed(random_state)
random.seed(random_state)


def generate_epic_patients_data(
    num_rows: int,
    entered_list: list[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    fields_list: list[str] = [
        "patient_DurableKey",
        "patient_BirthDate",
        "patient_Gender",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_patients' index.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy patient data.
    Raises:
        None
    """
    df_holder_list = []
    for client_id_code in entered_list:
        dob = faker.date_of_birth(minimum_age=18, maximum_age=90)
        data = {
            "patient_DurableKey": [client_id_code] * num_rows,
            "patient_BirthDate": [
                dob.strftime("%Y-%m-%dT%H:%M:%S") for _ in range(num_rows)
            ],
            "patient_Gender": [
                random.choice(["Male", "Female"]) for _ in range(num_rows)
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
            final_df[field] = np.nan
    return final_df[unique_fields]
