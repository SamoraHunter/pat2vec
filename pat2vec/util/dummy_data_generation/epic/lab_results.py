"""Epic lab results generator."""

import random
from datetime import timedelta
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from faker import Faker

from ..generator_helpers import create_random_date_from_globals

random_state = 42
Faker.seed(random_state)
faker = Faker()
np.random.seed(random_state)
random.seed(random_state)

LAB_TEST_CATEGORIES: Dict[str, List[Dict]] = {
    "CBC": [
        {"name": "WBC", "unit": "x10^9/L", "min": 3.0, "max": 12.0, "category": "CBC"},
        {"name": "RBC", "unit": "x10^12/L", "min": 3.8, "max": 5.8, "category": "CBC"},
        {
            "name": "Hemoglobin",
            "unit": "g/L",
            "min": 110,
            "max": 170,
            "category": "CBC",
        },
        {
            "name": "Platelets",
            "unit": "x10^9/L",
            "min": 150,
            "max": 450,
            "category": "CBC",
        },
        {"name": "Neutrophils", "unit": "%", "min": 25, "max": 75, "category": "CBC"},
        {"name": "Lymphocytes", "unit": "%", "min": 15, "max": 45, "category": "CBC"},
        {"name": "Monocytes", "unit": "%", "min": 2, "max": 12, "category": "CBC"},
        {"name": "Eosinophils", "unit": "%", "min": 0, "max": 8, "category": "CBC"},
        {"name": "Basophils", "unit": "%", "min": 0, "max": 3, "category": "CBC"},
        {
            "name": "Hematocrit",
            "unit": "L/L",
            "min": 0.35,
            "max": 0.52,
            "category": "CBC",
        },
    ],
    "Chemistry": [
        {
            "name": "Glucose",
            "unit": "mmol/L",
            "min": 3.0,
            "max": 18.0,
            "category": "Chemistry",
        },
        {
            "name": "Urea",
            "unit": "mmol/L",
            "min": 2.5,
            "max": 8.5,
            "category": "Chemistry",
        },
        {
            "name": "Creatinine",
            "unit": "µmol/L",
            "min": 60,
            "max": 120,
            "category": "Chemistry",
        },
        {
            "name": "Sodium",
            "unit": "mmol/L",
            "min": 135,
            "max": 148,
            "category": "Electrolytes",
        },
        {
            "name": "Potassium",
            "unit": "mmol/L",
            "min": 3.2,
            "max": 5.2,
            "category": "Electrolytes",
        },
        {
            "name": "Chloride",
            "unit": "mmol/L",
            "min": 98,
            "max": 108,
            "category": "Electrolytes",
        },
        {
            "name": "Calcium",
            "unit": "mmol/L",
            "min": 2.1,
            "max": 2.6,
            "category": "Chemistry",
        },
        {
            "name": "Magnesium",
            "unit": "mmol/L",
            "min": 0.7,
            "max": 1.0,
            "category": "Chemistry",
        },
        {
            "name": "Phosphate",
            "unit": "mmol/L",
            "min": 0.8,
            "max": 1.5,
            "category": "Chemistry",
        },
        {
            "name": "ALT",
            "unit": "U/L",
            "min": 5,
            "max": 60,
            "category": "Liver enzymes",
        },
        {
            "name": "AST",
            "unit": "U/L",
            "min": 10,
            "max": 45,
            "category": "Liver enzymes",
        },
        {
            "name": "ALP",
            "unit": "U/L",
            "min": 40,
            "max": 130,
            "category": "Liver enzymes",
        },
        {
            "name": "Bilirubin",
            "unit": "µmol/L",
            "min": 2,
            "max": 20,
            "category": "Liver enzymes",
        },
    ],
    "Microbiology": [
        {"name": "Culture Result", "unit": "", "category": "Microbiology"},
        {"name": "Sensitivity Panel", "unit": "", "category": "Microbiology"},
        {"name": "Gram Stain", "unit": "", "category": "Microbiology"},
    ],
    "Coagulation": [
        {
            "name": "PT",
            "unit": "seconds",
            "min": 8,
            "max": 14,
            "category": "Coagulation",
        },
        {"name": "INR", "unit": "", "min": 0.8, "max": 1.2, "category": "Coagulation"},
        {
            "name": "APTT",
            "unit": "seconds",
            "min": 25,
            "max": 35,
            "category": "Coagulation",
        },
    ],
}

LAB_CATEGORY_TEST_WEIGHTS: Dict[str, List[float]] = {
    "CBC": [0.35, 0.20, 0.15, 0.10, 0.08, 0.05, 0.03, 0.02, 0.01, 0.01],
    "Chemistry": [
        0.25,
        0.20,
        0.15,
        0.10,
        0.10,
        0.08,
        0.07,
        0.04,
        0.03,
        0.03,
        0.02,
        0.02,
        0.01,
    ],
    "Microbiology": [0.50, 0.30, 0.20],
    "Coagulation": [0.40, 0.30, 0.30],
}


def generate_lab_test_name(category: str) -> Dict:
    """Generates a realistic lab test with clinical correlation patterns.

    Args:
        category: The lab category to sample from (CBC, Chemistry, etc.)

    Returns:
        A dict containing name, unit, and value range for the test.
    """
    tests = LAB_TEST_CATEGORIES[category]
    weights = LAB_CATEGORY_TEST_WEIGHTS[category]
    return np.random.choice(tests, p=weights)


def generate_lab_value(test_info: Dict) -> Tuple[float, str]:
    """Generates a realistic lab value within clinically appropriate range.

    Args:
        test_info: Dict containing min/max ranges for the test.

    Returns:
        Tuple of (value, unit).
    """
    if "min" not in test_info or "max" not in test_info:
        return None, test_info.get("unit", "")

    base_value = np.random.uniform(test_info["min"], test_info["max"])

    if random.random() < 0.15:
        abnormal_sign = np.random.choice(["abnormal high", "abnormal low"])
        if abnormal_sign == "abnormal high":
            base_value *= np.random.uniform(1.2, 2.0)
        else:
            base_value *= np.random.uniform(0.6, 0.9)

    return round(base_value, 2), test_info.get("unit", "")


def generate_lab_panel(category: str, num_tests: int) -> List[Dict]:
    """Generates a clinically coherent panel of related lab tests.

    Args:
        category: The main category for the panel.
        num_tests: Number of tests in the panel.

    Returns:
        List of test records with correlated values.
    """
    samples = LAB_TEST_CATEGORIES[category][:]
    np.random.shuffle(samples)

    panel = []
    taken = set()

    for _ in range(min(num_tests, len(samples))):
        test_info = samples.pop()
        test_name = test_info["name"]

        if test_name in taken:
            continue
        taken.add(test_name)

        value, unit = generate_lab_value(test_info)

        description_templates = {
            "CBC": [
                "Complete Blood Count",
                "Full Blood Examination",
                "CBC with Differential",
            ],
            "Chemistry": [
                "Comprehensive Metabolic Panel",
                "Basic Metabolic Panel",
                "Biochemistry Profile",
            ],
            "Microbiology": ["Blood Culture", "Urine Culture", "Wound Culture"],
            "Coagulation": [
                "Coagulation Profile",
                "Clotting Factors",
                "Thrombosis Screen",
            ],
        }

        description = np.random.choice(
            description_templates.get(category, ["Lab Test"])
        )

        panel.append(
            {
                "name": test_name,
                "description": description,
                "value": value,
                "unit": unit,
                "category": (
                    test_info["category"] if "category" in test_info else category
                ),
            }
        )

    return panel


def generate_epic_lab_results_data(
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
        "document_Name",
        "document_Content",
        "document_LabComponentValue",
        "document_CollectedDate",
        "document_LabResultEpicId",
        "document_Fields.valueText",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_lab_results' index with realistic clinical patterns.

    Realistic lab result patterns:
    - CBC panel: WBC, RBC, Hemoglobin, Platelets, Neutrophils, Lymphocytes
    - Chemistry: Glucose, Urea, Creatinine, Electrolytes (Na, K, Cl), Liver enzymes
    - Microbiology: Culture results with sensitivity patterns
    - Coagulation: PT, INR, APTT

    Clinical correlations:
    - Related tests are generated together (panels)
    - Values stay within realistic reference ranges per test type
    - 15% of values are clinically abnormal
    - Created dates precede collected dates by realistic lab processing time
    """
    df_holder_list = []
    category_order = ["CBC", "Chemistry", "Coagulation", "Microbiology"]

    for client_id_code in entered_list:
        patient_seed = random_state + hash(client_id_code) % (2**32)
        random.seed(patient_seed)
        np.random.seed(patient_seed)

        lab_test_count = 0
        data_rows = []

        while lab_test_count < num_rows:
            remaining = num_rows - lab_test_count

            category = np.random.choice(category_order, p=[0.35, 0.45, 0.1, 0.1])

            panel_size = min(np.random.randint(2, 6), remaining)

            panel = generate_lab_panel(category, panel_size)

            admission_date = create_random_date_from_globals(
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                global_start_day,
                global_end_day,
            )

            collected_date_offset = np.random.randint(0, 72)
            collected_date = admission_date + timedelta(hours=collected_date_offset)

            created_date_offset = collected_date_offset + np.random.randint(1, 48)
            created_date = admission_date + timedelta(hours=created_date_offset)

            epic_id_prefix = f"LAB{faker.random_number(digits=6)}"

            for test in panel:
                collected_str = collected_date.strftime("%Y-%m-%dT%H:%M:%S")
                created_str = created_date.strftime("%Y-%m-%dT%H:%M:%S")

                value_text = str(test["value"]) if test["value"] is not None else ""
                value_text += f" {test['unit']}" if test["unit"] else ""

                data_rows.append(
                    {
                        "document_PatientDurableKey": client_id_code,
                        "document_CreatedWhen": created_str,
                        "document_Name": test["name"],
                        "document_Content": test["description"],
                        "document_LabComponentValue": (
                            test["value"] if test["value"] is not None else ""
                        ),
                        "document_CollectedDate": collected_str,
                        "document_LabResultEpicId": epic_id_prefix
                        + str(faker.random_number(digits=2)),
                        "document_Fields.valueText": value_text,
                        "id": faker.uuid4(),
                    }
                )

            lab_test_count += len(panel)

        df = pd.DataFrame(data_rows[:num_rows])
        df_holder_list.append(df)

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
