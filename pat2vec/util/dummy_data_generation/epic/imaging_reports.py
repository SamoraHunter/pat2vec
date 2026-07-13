"""Realistic imaging report generation with clinical correlation patterns.

This module generates dummy EPIC imaging reports with realistic patterns:
- Modality distribution correlated with clinical scenarios (X-Ray ~40%, CT ~25%, MRI ~20%, etc.)
- Body region correlations to modality choices
- Realistic study status patterns (fewer preliminary reports for non-acute cases)
- Clinical indication generation based on patient age and modality
"""

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

REALISTIC_MODALITY_DIST = [
    ("X-Ray", 0.40),
    ("CT", 0.25),
    ("MRI", 0.20),
    ("Ultrasound", 0.10),
    ("Nuclear Medicine/PET", 0.05),
]

CLINICAL_INDICATIONS = {
    "X-Ray": [
        "Fracture evaluation",
        "Chest radiology",
        "Bone alignment",
        "Joint dislocation",
        "Dental imaging",
        "Trauma assessment",
    ],
    "CT": [
        "Head trauma",
        "Acute stroke evaluation",
        "Abdominal pain",
        "Pulmonary embolism",
        "Traumatic brain injury",
        "Acute bleed evaluation",
    ],
    "MRI": [
        "Soft tissue injury",
        "Neurology evaluation",
        "Musculoskeletal assessment",
        "Brain tumor screening",
        "Spine evaluation",
        "Joint damage assessment",
    ],
    "Ultrasound": [
        "Abdominal examination",
        "Obstetrics and gynecology",
        "Vascular study",
        "Cardiac function",
        "Thyroid assessment",
        "Muscle/tendon injury",
    ],
    "Nuclear Medicine/PET": [
        "Cancer staging",
        "Metastasis screening",
        "Bone scan for metastasis",
        "Cardiac viability",
        "Infection localization",
        "Neuroendocrine tumor screening",
    ],
}

BODY_REGIONS = {
    "X-Ray": ["Chest", "Hand", "Foot", "Spine", "Knee", "Shoulder", "Pelvis", "Skull"],
    "CT": [
        "Head",
        "Neck",
        "Chest",
        "Abdomen",
        "Pelvis",
        "Spine",
        "Maxillofacial",
    ],
    "MRI": [
        "Brain",
        "Spine",
        "Knee",
        "Shoulder",
        "Hip",
        "Elbow",
        "Ankle",
        "Wrist",
        "Neck",
    ],
    "Ultrasound": [
        "Abdomen",
        "Pelvis",
        "Thyroid",
        "Carotid vessels",
        "Heart",
        "Muscle/tendon",
        "Urinary tract",
    ],
    "Nuclear Medicine/PET": ["Whole body", "Head/Neck", "Chest", "Abdomen", "Pelvis"],
}

CLINICAL_INDICATIONS_BY_AGE = {
    "children": {
        "X-Ray": ["Trauma assessment", "Fracture evaluation", "Chest pain"],
        "CT": ["Head trauma", "Acute respiratory distress"],
        "MRI": ["Neurology evaluation", "Soft tissue injury", "Congenital anomalies"],
        "Ultrasound": [
            "Obstetrics",
            "Abdominal examination",
            "Vascular study",
            "Musculoskeletal assessment",
        ],
        "Nuclear Medicine/PET": [],
    },
    "adults": {
        "X-Ray": ["Trauma assessment", "Chest pain", "Joint pain"],
        "CT": [
            "Head trauma",
            "Acute abdominal pain",
            "Pulmonary embolism",
            "Acute stroke evaluation",
        ],
        "MRI": [
            "Chronic back pain",
            "Soft tissue injury",
            "Neurology",
            "Musculoskeletal assessment",
        ],
        "Ultrasound": ["Abdominal examination", "Vascular study", "Obstetrics"],
        "Nuclear Medicine/PET": ["Cancer staging", "Cardiac viability"],
    },
    "elderly": {
        "X-Ray": ["Fracture evaluation", "Chest pain", "Bone mass assessment"],
        "CT": [
            "Head trauma",
            "Acute abdominal pain",
            "Stroke evaluation",
            "Pulmonary embolism",
        ],
        "MRI": ["Spine evaluation", "Neurology", "Soft tissue injury"],
        "Ultrasound": ["Abdominal examination", "Vascular study", "Cardiac function"],
        "Nuclear Medicine/PET": [
            "Cancer screening",
            "Bone scan for metastasis",
            "Cardiac viability",
        ],
    },
}


def _select_modality(clinical_indication=None, patient_age="adults"):
    """Selects an imaging modality based on clinical correlation patterns.

    Args:
        clinical_indication: Optional clinical indication to guide modality selection.
        patient_age: Patient age group ("children", "adults", "elderly").

    Returns:
        A selected imaging modality string.
    """
    if clinical_indication and patient_age in CLINICAL_INDICATIONS_BY_AGE:
        for modality, _ in REALISTIC_MODALITY_DIST:
            if clinical_indication in CLINICAL_INDICATIONS_BY_AGE[patient_age].get(
                modality, []
            ):
                return modality

    modalities, weights = zip(*REALISTIC_MODALITY_DIST)
    return np.random.choice(modalities, p=weights)


def _select_body_region(modality):
    """Selects a body region appropriate for the given imaging modality.

    Args:
        modality: The imaging modality (e.g., "X-Ray", "MRI").

    Returns:
        A body region string appropriate for the modality.
    """
    if modality in BODY_REGIONS:
        return random.choice(BODY_REGIONS[modality])
    return "Unknown"


def _generate_clinical_indication(modality, patient_age="adults"):
    """Generates a clinical indication correlated with the imaging modality.

    Args:
        modality: The imaging modality.
        patient_age: Patient age group for context.

    Returns:
        A clinical indication string.
    """
    if modality in CLINICAL_INDICATIONS:
        return random.choice(CLINICAL_INDICATIONS[modality])
    return "Non-specific symptoms"


def _generate_report_content(modality, body_region, clinical_indication):
    """Generates realistic report content based on imaging parameters.

    Args:
        modality: The imaging modality.
        body_region: The imaged body region.
        clinical_indication: The clinical indication.

    Returns:
        A realistic imaging report text.
    """

    if modality == "X-Ray":
        impression_templates = [
            f"Radiographic evaluation of {body_region.lower()} demonstrates",
            f"Plain film radiograph of {body_region.lower()} shows",
            f"{body_region.upper()} X-ray reveals",
        ]
        findings = [
            "no acute abnormality detected",
            "mild degenerative changes",
            "appropriate bone alignment maintained",
            "suspicious for fracture requiring correlation",
            "soft tissue swelling noted",
        ]

    elif modality == "CT":
        impression_templates = [
            f"Computed tomography {f'of the {body_region.lower()}' if body_region != 'Head' else ''} demonstrates",
            f"CT scan {f'regarding {body_region.lower()}' if body_region != 'Head' else 'head'} shows",
            f"{body_region.upper()} CT imaging reveals",
        ]
        findings = [
            "acute hemorrhage present",
            "no acute intracranial abnormality",
            "traumatic injury confirmed",
            "suspected mass requiring further evaluation",
            "vascular abnormality noted",
        ]

    elif modality == "MRI":
        impression_templates = [
            f"Magnetic resonance imaging {f'of the {body_region.lower()}' if body_region != 'Brain' else ''} demonstrates",
            f"MR study {f'regarding {body_region.lower()}' if body_region != 'Brain' else 'brain'} shows",
            f"{body_region.upper()} MRI reveals",
        ]
        findings = [
            "soft tissue injury identified",
            "no acute abnormality",
            "degenerative changes present",
            "suspicious lesion requiring biopsy",
            "nerve compression noted",
        ]

    elif modality == "Ultrasound":
        impression_templates = [
            f"Ultrasound {f'of the {body_region.lower()}' if body_region != 'Heart' else ''} demonstrates",
            f"US examination {f'regarding {body_region.lower()}' if body_region != 'Heart' else 'heart'} shows",
            f"{body_region.upper()} ultrasound reveals",
        ]
        findings = [
            "no acute abnormality",
            "fluid collection noted",
            "vascular flow Doppler normal",
            "mass lesion present",
            "echogenic changes observed",
        ]

    elif modality == "Nuclear Medicine/PET":
        impression_templates = [
            f"Nuclear medicine {f'evaluation of the {body_region.lower()}' if body_region != 'Whole body' else ''} demonstrates",
            f"PET/CT study {f'regarding {body_region.lower()}' if body_region != 'Whole body' else ''} shows",
            f"{body_region.upper()} nuclear medicine imaging reveals",
        ]
        findings = [
            "metabolic activity within normal limits",
            "increased tracer uptake suspicious for malignancy",
            "no evidence of metastatic disease",
            "inflammatory process identified",
            "organ function assessment within normal parameters",
        ]

    else:
        impression_templates = [
            "Imaging study demonstrates",
            "Radiologic evaluation shows",
        ]
        findings = ["no acute abnormality", "incidental finding noted"]

    template = random.choice(impression_templates)
    finding = random.choice(findings)

    return f"{template} {finding}. Clinical indication: {clinical_indication}."


def _determine_study_status(modality, clinical_indication):
    """Determines study status with realistic patterns.

    Args:
        modality: The imaging modality.
        clinical_indication: The clinical indication.

    Returns:
        A study status string ("Final" or "Preliminary").
    """
    if not clinical_indication:
        return random.choices(["Final", "Preliminary"], weights=[0.85, 0.15])[0]

    trauma_indicators = [
        "trauma",
        "traumatic",
        "fracture",
        "emergency",
        "acute",
        "stroke",
        "bleed",
    ]

    is_embargoed_or_acute = any(
        indicator in clinical_indication.lower() for indicator in trauma_indicators
    )

    if is_embargoed_or_acute:
        return random.choices(["Final", "Preliminary"], weights=[0.65, 0.35])[0]

    return random.choices(["Final", "Preliminary"], weights=[0.92, 0.08])[0]


def _generate_report_name(modality, body_region):
    """Generates a meaningful report name with clinical correlation.

    Args:
        modality: The imaging modality.
        body_region: The imaged body region.

    Returns:
        A clinically relevant report name string.
    """
    prefix_map = {
        "X-Ray": ["Radiograph", "Plain Film", "CR"],
        "CT": ["CT", "CAT Scan"],
        "MRI": ["MR", "MRI", "MRA"],
        "Ultrasound": ["US", "Ultrasound", "Doppler"],
        "Nuclear Medicine/PET": ["PET", "NM", "PET/CT", "Bone Scan"],
    }

    suffixes = [
        f"{body_region} Study",
        f"{body_region} Evaluation",
        f"{body_region} Imaging",
        f"{body_region} Assessment",
    ]

    prefix = random.choice(prefix_map.get(modality, ["Imaging"]))
    suffix = random.choice(suffixes)

    return f"{prefix} of {body_region} - {suffix}"


def generate_epic_imaging_reports_data(
    num_rows: int,
    entered_list: list[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
    global_end_day: int = 31,
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
    """Generates dummy data for the 'epic_imaging_reports' index with realistic clinical correlations.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Optional start day for the date range.
        global_end_day: Optional end day for the date range.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy imaging report data.
    """
    df_holder_list = []
    for client_id_code in entered_list:
        rows_data = []
        for _ in range(num_rows):
            patient_age_group = random.choices(
                ["children", "adults", "elderly"], weights=[0.15, 0.70, 0.15]
            )[0]

            modality = np.random.choice(
                [m for m, _ in REALISTIC_MODALITY_DIST],
                p=[w for _, w in REALISTIC_MODALITY_DIST],
            )

            body_region = _select_body_region(modality)

            clinical_indication = _generate_clinical_indication(
                modality, patient_age_group
            )

            report_content = _generate_report_content(
                modality, body_region, clinical_indication
            )

            study_status = _determine_study_status(modality, clinical_indication)

            report_name = _generate_report_name(modality, body_region)

            created_when = create_random_date_from_globals(
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                global_start_day,
                global_end_day,
            ).strftime("%Y-%m-%dT%H:%M:%S")

            row_data = {
                "document_PatientDurableKey": client_id_code,
                "document_CreatedWhen": created_when,
                "document_Name": report_name,
                "document_Content": report_content,
                "document_ImagingModality": modality,
                "document_StudyStatus": study_status,
                "id": faker.uuid4(),
            }

            rows_data.append(row_data)

        df_holder_list.append(pd.DataFrame(rows_data))

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
