import random
from typing import List, Optional

import numpy as np
import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals

random_state = 42
Faker.seed(random_state)
np.random.seed(random_state)
random.seed(random_state)
faker = Faker()


def generate_epic_clinical_notes_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    use_GPT: bool = False,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_Content",
        "document_Name",
        "document_EncounterEpicCsn",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_clinical_notes' index.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    use_GPT: If True, uses a text generation model for clinical notes content.
        fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy clinical note data.
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
            "document_Content": [
                (
                    generate_patient_timeline(client_id_code)
                    if use_GPT
                    else get_patient_timeline_dummy(client_id_code)
                )
                for _ in range(num_rows)
            ],
            "document_Name": [faker.sentence(nb_words=3) for _ in range(num_rows)],
            "document_EncounterEpicCsn": [
                faker.random_number(digits=10) for _ in range(num_rows)
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


def generate_epic_medical_history_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_Diagnosis",
        "document_DiagnosisConcepts",
        "document_Name",
        "document_Comment",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_medical_history' index.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy medical history data.
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
            "document_Diagnosis": [faker.word() for _ in range(num_rows)],
            "document_DiagnosisConcepts": [faker.word() for _ in range(num_rows)],
            "document_Name": [faker.sentence(nb_words=2) for _ in range(num_rows)],
            "document_Comment": [faker.sentence() for _ in range(num_rows)],
            "id": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    target_col = "document_Comment"
    if target_col in final_df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)
    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]


def generate_epic_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    fields_list: List[str] = [
        "document_PatientDurableKey",
        "document_CreatedWhen",
        "document_UpdatedWhen",
        "document_Name",
        "document_Content",
        "document_OrderClass",
        "document_OrderDate",
        "document_OrderStatus",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_orders' index.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy orders data.
    Raises:
        None
    """
    df_holder_list = []
    for client_id_code in entered_list:
        order_date = create_random_date_from_globals(
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
            "document_UpdatedWhen": [
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
            "document_OrderClass": [
                random.choice(["Medication", "Lab", "Imaging"]) for _ in range(num_rows)
            ],
            "document_OrderDate": [
                int(order_date.timestamp() * 1000) for _ in range(num_rows)
            ],
            "document_OrderStatus": [
                random.choice(["Completed", "Pending", "Cancelled"])
                for _ in range(num_rows)
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


def generate_patient_timeline(client_id_code: str) -> Optional[str]:
    """Generates a patient timeline using GPT."""
    return None


def get_patient_timeline_dummy(client_id_code: str) -> Optional[str]:
    """Gets a dummy patient timeline."""
    return None
