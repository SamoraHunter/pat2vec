"""Epic encounters generator."""

import random
from datetime import timedelta
from typing import Dict, List

import numpy as np
import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals

random_state = 42
Faker.seed(random_state)
faker = Faker()
np.random.seed(random_state)
random.seed(random_state)

REALISTIC_ADMISSION_TYPES: List[str] = ["Inpatient", "Outpatient", "Emergency"]
ADMISSION_TYPE_WEIGHTS: List[float] = [0.30, 0.50, 0.20]

DEPARTMENT_SERVICE_MAP: Dict[str, str] = {
    "Internal Medicine": "Medicine",
    "Surgery": "Surgical Services",
    "Cardiology": "Cardiology",
    "Respiratory": "Respiratory Services",
    "Orthopedics": "Orthopedic Surgery",
    "Critical Care": "Intensive Care Unit",
    "Neurology": "Neurology",
    "Oncology": "Oncology Services",
    "Pediatrics": "Pediatric Services",
    "Obstetrics & Gynecology": "Women's Health",
    "Emergency Department": "Emergency Services",
    "Psychiatry": "Behavioral Health",
    "Radiology": "Diagnostic Services",
    "Laboratory": "Diagnostic Services",
    "Pharmacy": "Pharmacy Services",
}

DEPARTMENT_MAJOR_CATEGORIES: List[str] = [
    "Internal Medicine",
    "Surgery",
    "Cardiology",
    "Respiratory",
    "Orthopedics",
    "Other Departments",
]

MAJOR_CATEGORY_WEIGHTS: List[float] = [0.20, 0.18, 0.12, 0.10, 0.08, 0.32]

EMERGENCY_LENGTH_ADJUSTMENT = 2
ELDERLY_LENGTH_ADJUSTMENT = 1.5


def generate_realistic_department() -> str:
    """Generates a realistic hospital department based on distribution patterns."""
    category = np.random.choice(DEPARTMENT_MAJOR_CATEGORIES, p=MAJOR_CATEGORY_WEIGHTS)

    if category == "Other Departments":
        other_depts = [
            "Critical Care",
            "Neurology",
            "Oncology",
            "Pediatrics",
            "Obstetrics & Gynecology",
            "Emergency Department",
            "Psychiatry",
            "Radiology",
            "Laboratory",
            "Pharmacy",
        ]
        return random.choice(other_depts)

    return category


def calculate_length_of_stay(
    admission_type: str,
    patient_age: int,
    department: str,
) -> int:
    """Calculates realistic length of stay based on clinical factors.

    Args:
        admission_type: Type of admission (Inpatient, Outpatient, Emergency).
        patient_age: Patient age in years.
        department: Department/service where patient is treated.

    Returns:
        Length of stay in days.
    """
    base_los = 0

    if admission_type == "Outpatient":
        return 0
    elif admission_type == "Inpatient":
        department_base = {
            "Internal Medicine": 4,
            "Surgery": 5,
            "Cardiology": 6,
            "Respiratory": 7,
            "Orthopedics": 8,
            "Critical Care": 10,
            "Neurology": 7,
            "Oncology": 9,
            "Pediatrics": 4,
            "Obstetrics & Gynecology": 3,
            "Psychiatry": 12,
        }
        base_los = department_base.get(department, 5)
    elif admission_type == "Emergency":
        base_los = 2
    else:
        return 0

    los_multiplier = (
        EMERGENCY_LENGTH_ADJUSTMENT if admission_type == "Emergency" else 1.0
    )
    age_multiplier = ELDERLY_LENGTH_ADJUSTMENT if patient_age >= 65 else 1.0

    adjusted_los = int(base_los * los_multiplier * age_multiplier)

    return max(adjusted_los, 1)


def generate_epic_encounters_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
    global_end_day: int = 31,
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
    """Generates dummy data for the 'epic_encounters' index with realistic patterns.

    Realistic encounter patterns:
    - Inpatient: 30% (admission with overnight stay)
    - Outpatient: 50% (same-day visit)
    - Emergency: 20% (urgent care, 24h availability)

    Department distribution follows real hospital utilization patterns.
    Length of stay correlates with admission type, patient age, and department.
    """
    df_holder_list = []

    for client_id_code in entered_list:
        patient_seed = random_state + hash(client_id_code) % (2**32)
        random.seed(patient_seed)
        np.random.seed(patient_seed)

        num_admissions = max(1, num_rows // 5)
        admission_dates = [
            create_random_date_from_globals(
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                global_start_day,
                global_end_day,
            )
            for _ in range(num_admissions)
        ]

        data = {
            "activity_PatientDurableKey": [],
            "activity_AdmissionDate": [],
            "activity_DischargeDate": [],
            "activity_Department": [],
            "activity_Type": [],
            "activity_VisitClass": [],
            "activity_HospitalService": [],
            "id": [],
        }

        for admission_date in admission_dates:
            admission_type = np.random.choice(
                REALISTIC_ADMISSION_TYPES, p=ADMISSION_TYPE_WEIGHTS
            )

            department = generate_realistic_department()
            hospital_service = DEPARTMENT_SERVICE_MAP[department]

            patient_age = np.random.randint(18, 91)
            los = calculate_length_of_stay(admission_type, patient_age, department)

            if admission_type == "Outpatient":
                discharge_date = admission_date + timedelta(
                    hours=np.random.randint(2, 8)
                )
            else:
                discharge_date = admission_date + timedelta(days=los)

            visit_class = (
                "Hospital Encounter"
                if admission_type in ["Inpatient", "Emergency"]
                else "Office Visit"
            )

            admission_str = admission_date.strftime("%Y-%m-%dT%H:%M:%S")
            discharge_str = discharge_date.strftime("%Y-%m-%dT%H:%M:%S")

            data["activity_PatientDurableKey"].append(client_id_code)
            data["activity_AdmissionDate"].append(admission_str)
            data["activity_DischargeDate"].append(discharge_str)
            data["activity_Department"].append(department)
            data["activity_Type"].append(admission_type)
            data["activity_VisitClass"].append(visit_class)
            data["activity_HospitalService"].append(hospital_service)
            data["id"].append(faker.uuid4())

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
