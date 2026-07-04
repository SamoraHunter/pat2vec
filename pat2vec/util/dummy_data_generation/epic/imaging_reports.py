import random

from faker import Faker
import numpy as np
import pandas as pd

from ..generator_helpers import create_random_date_from_globals

random_state = 42
Faker.seed(random_state)
faker = Faker()
np.random.seed(random_state)
random.seed(random_state)


def generate_epic_imaging_reports_data(
    num_rows: int,
    entered_list: list[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    fields_list: list[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_Name",
        "document_Content",
        "document_ImagingModality",
        "document_StudyStatus",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_imaging_reports' index.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy imaging report data.
    Raises:
        None
    """
    df_holder_list = []
    for client_id_code in entered_list:
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
            "document_Name": [f"Imaging {faker.word()}" for _ in range(num_rows)],
            "document_Content": [faker.sentence() for _ in range(num_rows)],
            "document_ImagingModality": [
                random.choice(["X-Ray", "MRI", "CT"]) for _ in range(num_rows)
            ],
            "document_StudyStatus": [
                random.choice(["Final", "Preliminary"]) for _ in range(num_rows)
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
