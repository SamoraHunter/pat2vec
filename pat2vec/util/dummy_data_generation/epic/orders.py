import random
from datetime import timedelta
from typing import List

import numpy as np
import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals

random_state = 42
Faker.seed(random_state)
np.random.seed(random_state)
random.seed(random_state)
faker = Faker()

REALISTIC_ORDER_DISTRIBUTION = {"Medication": 0.45, "Lab": 0.30, "Imaging": 0.25}

CLINICAL_ORDER_NAMES = {
    "Medication": [
        "AMOXILLIN",
        "ACE INHIBITORS",
        "STATINS",
        "NSAIDS",
        "ANTIHISTAMINES",
        "PROTON PUMP INHIBITORS",
        "BETA BLOCKERS",
        "DIURETICS",
        "CORTICOSTEROIDS",
        "ANTIBIOTICS",
        "ANTIVIRALS",
        "ANTIFUNGALS",
        "CHEMOTHERAPY AGENTS",
        "IMMUNOSUPPRESSANTS",
        "ANTICOAGULANTS",
        "ANTIPLATELET AGENTS",
        "BISPHONATES",
        "IRON SUPPLEMENTS",
        "VITAMIN D",
        "CALCIUM CHANNEL BLOCKERS",
    ],
    "Lab": [
        "CBC (COMPLETE BLOOD COUNT)",
        "CHEMISTRY PANEL",
        "LIPID PANEL",
        " thyroid function tests",
        "Liver Function Tests",
        "URINALYSIS",
        "BLOOD CULTURE",
        "SERUM ELECTROLYTES",
        "HEMOGLOBIN A1C",
        "CARDIAC BIOMARKERS",
        "COAGULATION PANEL",
        "BILIRUBIN PANEL",
        "TUMOR MARKERS",
        "DRUG SCREENING",
        "VITAMIN LEVELS",
        "HORMONE PANEL",
    ],
    "Imaging": [
        "CHEST X-RAY",
        "CT HEAD",
        "CT ABDOMEN",
        "MRI BRAIN",
        "MUSKELSKETAL MRI",
        "ECHO CARDIOGRAM",
        "SKELETAL SURVEY",
        "DEXA BORDERSCAN",
        "CAROTID Doppler",
        "CORONARY ARTERY CT",
        "PET SCAN",
        "ULTRASOUND ABDOMEN",
        "FLUOROSCOPY",
    ],
}

ORDER_CONTENT_TEMPLATES = {
    "Medication": [
        "Prescription for {} as directed by physician",
        " {} to be administered orally daily",
        "Intravenous administration of {} per protocol",
    ],
    "Lab": [
        "Laboratory test request for {}",
        "Routine monitoring with {}",
        "Diagnostic testing requiring {}",
        "Periodic assessment using {}",
    ],
    "Imaging": [
        "Radiology study requesting {}",
        "Diagnostic imaging with {}",
        "Functional assessment using {}",
    ],
}

ORDER_STATUS_DISTRIBUTION = {"Completed": 0.70, "Pending": 0.15, "Cancelled": 0.15}


def _select_order_class_with_distribution() -> str:
    """Selects order class based on realistic临床分布 patterns."""
    classes = list(REALISTIC_ORDER_DISTRIBUTION.keys())
    weights = list(REALISTIC_ORDER_DISTRIBUTION.values())
    return np.random.choice(classes, p=weights)


def _select_order_status_with_distribution() -> str:
    """Selects order status based on clinical workflow patterns."""
    statuses = list(ORDER_STATUS_DISTRIBUTION.keys())
    weights = list(ORDER_STATUS_DISTRIBUTION.values())
    return np.random.choice(statuses, p=weights)


def _generate_realistic_timestamps(
    base_date,
    created_time_offset_minutes: int = 0,
    updated_time_offset_minutes: int = 30,
) -> tuple:
    """Generates realistic workflow timestamps with proper timing relationships.

    Args:
        base_date: The base datetime for order creation.
        created_time_offset_minutes: Minutes offset for CreatedWhen (default 0).
        updated_time_offset_minutes: Max minutes offset for UpdatedWhen (default 30).

    Returns:
        tuple: (CreatedWhen, UpdatedWhen) as ISO-formatted strings.
    """
    from ..generator_helpers import create_random_date_from_globals

    created_when = create_random_date_from_globals(
        base_date.year,
        base_date.month,
        base_date.year,
        base_date.month,
        base_date.day,
        base_date.day,
    )

    if random.random() < 0.85:
        time_diff_minutes = random.randint(0, updated_time_offset_minutes)
        updated_when = created_when + timedelta(minutes=time_diff_minutes)
    else:
        updated_when = created_when

    return created_when.strftime("%Y-%m-%dT%H:%M:%S"), updated_when.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def _generate_order_name(order_class: str) -> str:
    """Generates realistic order name based on class type.

    Args:
        order_class: The order class (Medication, Lab, or Imaging).

    Returns:
        Realistic order name string.
    """
    if order_class in CLINICAL_ORDER_NAMES:
        return np.random.choice(CLINICAL_ORDER_NAMES[order_class])
    return faker.word()


def _generate_order_content(order_class: str) -> str:
    """Generates realistic order content based on class type.

    Args:
        order_class: The order class (Medication, Lab, or Imaging).

    Returns:
        Realistic clinical content string.
    """
    if order_class in ORDER_CONTENT_TEMPLATES:
        template = np.random.choice(ORDER_CONTENT_TEMPLATES[order_class])
        return template.format(np.random.choice(CLINICAL_ORDER_NAMES[order_class]))
    return faker.sentence()


def generate_epic_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
    global_end_day: int = 31,
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
    """Generates realistic dummy data for the 'epic_orders' index with clinical patterns.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range (default 1).
        global_end_day: End day for the random date range (default 31).
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated realistic orders data following Epic patterns.
    """
    df_holder_list = []

    for client_id_code in entered_list:
        base_date = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            global_start_day,
            global_end_day,
        )

        created_data = [
            _select_order_class_with_distribution() for _ in range(num_rows)
        ]

        data = {
            "document_PatientDurableKey": [client_id_code] * num_rows,
            "document_CreatedWhen": [
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
            "document_UpdatedWhen": [
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
            "document_Name": [_generate_order_name(cls) for cls in created_data],
            "document_Content": [_generate_order_content(cls) for cls in created_data],
            "document_OrderClass": created_data,
            "document_OrderDate": [
                int(base_date.timestamp() * 1000) for _ in range(num_rows)
            ],
            "document_OrderStatus": [
                _select_order_status_with_distribution() for _ in range(num_rows)
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
