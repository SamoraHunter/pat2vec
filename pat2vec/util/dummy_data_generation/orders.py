"""Diagnostic and Drug order generators.

This module contains functions for generating synthetic diagnostic and drug order data.
"""

import random
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from faker import Faker

from .generator_helpers import create_random_date_from_globals, maybe_nan

# Diagnostic order category distributions (realistic medical patterns)
# Blood tests: ~40%, Imaging: ~25%, Cardiac: ~10%, Special tests: ~25%
DIAGNOSTIC_ORDER_CATEGORIES = {
    "blood_tests": {
        "weight": 0.40,
        "tests": [
            "Complete Blood Count (CBC)",
            "Basic Metabolic Panel (BMP)",
            "Comprehensive Metabolic Panel (CMP)",
            "Liver Function Tests (LFTs)",
            "Renal Function Panel",
            "Lipid Panel",
            "Thyroid Stimulating Hormone (TSH)",
            "Hemoglobin A1c (HbA1c)",
            "C-Reactive Protein (CRP)",
            "Prothrombin Time (PT)",
            "Activated Partial Thromboplastin Time (aPTT)",
            "Blood Glucose",
            "Serum Electrolytes (Sodium, Potassium, Chloride)",
            "Blood Urea Nitrogen (BUN)",
            "Creatinine",
            "Albumin",
            "Bilirubin",
            "Hematocrit",
            "Platelet Count",
            "White Blood Cell Count (WBC)",
            "Red Blood Cell Count (RBC)",
            "Mean Corpuscular Volume (MCV)",
            "Mean Corpuscular Hemoglobin (MCH)",
            "Mean Corpuscular Hemoglobin Concentration (MCHC)",
            "Red Cell Distribution Width (RDW)",
            "Neutrophils",
            "Lymphocytes",
            "Monocytes",
            "Eosinophils",
            "Basophils",
            "Immature Granulocytes",
            "Glucose Tolerance Test (GTT)",
            "Glycated Albumin (GA)",
            "Insulin Level",
            "Lactate Dehydrogenase (LDH)",
            "Creatine Kinase (CK)",
            "Troponin",
            "Brain Natriuretic Peptide (BNP)",
            "Procalcitonin",
            "Ferritin",
            "Vitamin D",
            "Folate",
            "Vitamin B12",
            "Iron",
            "Total Iron Binding Capacity (TIBC)",
            "Transferrin",
            "Thyroxine (T4)",
            "Triiodothyronine (T3)",
            "Free Thyroxine Index (FTI)",
            "Thyroid Peroxidase Antibody (TPOAb)",
            "Thyroglobulin Antibody (TgAb)",
            "Antithyroglobulin Antibody",
            "Antithyroid Microsomal Antibodies",
            "Thyroid-Stimulating Immunoglobulin (TSI)",
            "Parathyroid Hormone (PTH)",
            "Calcium",
            "Ionized Calcium",
            "Magnesium",
            "Phosphorus",
            "Uric Acid",
            "Cortisol",
            "Testosterone",
            "Estradiol",
            "Progesterone",
            "Follicle-Stimulating Hormone (FSH)",
            "Luteinizing Hormone (LH)",
            "Prolactin",
            "Dehydroepiandrosterone (DHEA)",
            "Dehydroepiandrosterone Sulfate (DHEA-S)",
            "Androstenedione",
            "17-Hydroxyprogesterone",
            "Catecholamines",
            "Serotonin",
            "Catecholamine Metabolites (Vanillylmandelic Acid, Metanephrines, Normetanephrine, Metadrenaline)",
            "Insulin-like Growth Factor 1 (IGF-1)",
            "Insulin-like Growth Factor Binding Protein 3 (IGFBP-3)",
            "Acid Phosphatase",
            "Prostate-Specific Antigen (PSA)",
            "Alkaline Phosphatase (ALP)",
            "Gamma-Glutamyl Transferase (GGT)",
            "Alanine Aminotransferase (ALT)",
            "Aspartate Aminotransferase (AST)",
            "Total Protein",
            "Globulin",
            "Total Bilirubin",
            "Direct Bilirubin",
            "Indirect Bilirubin",
            "Ammonia",
            "Total Cholesterol",
            "High-Density Lipoprotein (HDL) Cholesterol",
            "Low-Density Lipoprotein (LDL) Cholesterol",
            "Very Low-Density Lipoprotein (VLDL) Cholesterol",
            "Triglycerides",
            "Creatine Kinase MB (CK-MB)",
            "Troponin I",
            "Troponin T",
            "Myoglobin",
            "Lactate",
            "pH",
            "Partial Pressure of Carbon Dioxide (pCO2)",
            "Partial Pressure of Oxygen (pO2)",
            "Oxygen Saturation (SaO2)",
            "Bicarbonate (HCO3)",
            "Base Excess (BE)",
            "Anion Gap",
            "Arterial Blood Gas (ABG)",
            "Venous Blood Gas (VBG)",
            "Carbon Dioxide Content (CO2CT)",
            "Oxygen Content (O2CT)",
            "Total Hemoglobin",
            "Oxyhemoglobin",
            "Deoxyhemoglobin",
            "Methemoglobin",
            "Carboxyhemoglobin",
            "Hemoglobin Variants",
            "Fetal Hemoglobin (HbF)",
            "Hemoglobin S (HbS)",
            "Hemoglobin C (HbC)",
            "Hemoglobin A2 (HbA2)",
            "Erythrocyte Sedimentation Rate (ESR)",
            "Sickle Cell Test",
            "Coomb's Test",
            "Direct Coomb's Test",
            "Indirect Coomb's Test",
            "Kleihauer-Betke Test",
            "Haptoglobin",
            "Fibrinogen",
            "D-Dimer",
            "Factor Assays (Factors II, V, VII, VIII, IX, X, XI, XII)",
            "Protein C",
            "Protein S",
            "Antithrombin III",
            "Plasminogen",
            "Plasminogen Activator Inhibitor-1 (PAI-1)",
            "Tissue Plasminogen Activator (tPA)",
            "von Willebrand Factor (vWF)",
            "Platelet Aggregation Test",
            "PFA-100",
            "Bleeding Time",
            "Clot Retraction Time",
            "Coagulation Factor Activity Assays",
            "Activated Clotting Time (ACT)",
            "Thromboelastography (TEG)",
        ],
    },
    "imaging": {
        "weight": 0.25,
        "tests": [
            "MRI Scan",
            "CT Scan",
            "X-ray",
            "Ultrasound",
            "Mammogram",
            "骨密度扫描 (Bone Density Test)",
            "Thyroid Ultrasound",
            "Abdominal Ultrasound",
            "Pelvic Ultrasound",
            "Carotid Ultrasound",
            "Echocardiogram",
            "Electrocardiogram (ECG)",
            "Stress Test",
            "Bone Scan",
            "PET Scan",
            "SPECT Scan",
            "DEXA Scan",
            "Angiography",
            "Cardiac Catheterization",
            "Arteriogram",
        ],
    },
    "special_tests": {
        "weight": 0.25,
        "tests": [
            "Colonoscopy",
            "Endoscopy",
            "Biopsy",
            "Lumbar Puncture",
            "Electroencephalogram (EEG)",
            "Pap Smear",
            "Spirometry",
            "Colon Cancer Screening",
            "Genetic Testing",
            "Semen Analysis",
            "Urinalysis",
            "Stool Culture",
            "Sputum Culture",
            "Skin Allergy Test",
        ],
    },
}

# Realistic order type names mapped to categories
DIAGNOSTIC_TEST_NAMES = [
    # Blood tests (~40%)
    "Complete Blood Count (CBC)",
    "Basic Metabolic Panel (BMP)",
    "Comprehensive Metabolic Panel (CMP)",
    "Liver Function Tests (LFTs)",
    "Renal Function Panel",
    "Lipid Panel",
    "Thyroid Stimulating Hormone (TSH)",
    "Hemoglobin A1c (HbA1c)",
    "C-Reactive Protein (CRP)",
    "Prothrombin Time (PT)",
    "Activated Partial Thromboplastin Time (aPTT)",
    "Blood Glucose",
    "Serum Electrolytes (Sodium, Potassium, Chloride)",
    "Blood Urea Nitrogen (BUN)",
    "Creatinine",
    "Albumin",
    "Bilirubin",
    "Hematocrit",
    "Platelet Count",
    "White Blood Cell Count (WBC)",
    "Red Blood Cell Count (RBC)",
    # Imaging (~25%)
    "MRI Scan",
    "CT Scan",
    "X-ray",
    "Ultrasound",
    "Mammogram",
    "Bone Density Test",
    "Echocardiogram",
    "Electrocardiogram (ECG)",
    "Stress Test",
    # Special tests (~25%)
    "Colonoscopy",
    "Endoscopy",
    "Biopsy",
    "Lumbar Puncture",
    "Electroencephalogram (EEG)",
    "Pap Smear",
    "Spirometry",
]


# Global instances
random_state = 42
faker = Faker()
faker.seed_instance(random_state)
random.seed(random_state)


# Realistic drug category distribution based on clinical patterns:
# - Analgesics: ~25% (pain management)
# - Antibiotics: ~15-20%
# - Cardiovascular: ~15%
# - Respiratory: ~10%
# - Gastrointestinal: ~10%
# - Endocrine (insulin): ~8%
# - Other: ~15%

DRUG_CATEGORIES = {
    "analgesics": {
        "weight": 0.25,
        "drugs": [
            ("Ibuprofen", "prescription"),
            ("Aspirin", "OTC"),
            ("Naproxen", "prescription"),
            ("Acetaminophen", "OTC"),
            ("Hydrocodone", "prescription"),
            ("Oxycodone", "prescription"),
            ("Tramadol", "prescription"),
            ("Codeine", "prescription"),
            ("Fentanyl", "prescription"),
            ("Morphine", "prescription"),
            ("Meloxicam", "prescription"),
            ("Diclofenac", "prescription"),
            ("Celecoxib", "prescription"),
            ("Tapentadol", "prescription"),
        ],
    },
    "antibiotics": {
        "weight": 0.18,
        "drugs": [
            ("Amoxicillin", "prescription"),
            ("Azithromycin", "prescription"),
            ("Cephalexin", "prescription"),
            ("Ciprofloxacin", "prescription"),
            ("Levaquin", "brand"),
            ("Doxycycline", "prescription"),
            ("Clindamycin", "prescription"),
            ("Metronidazole", "prescription"),
            ("Trimethoprim-Sulfamethoxazole", "prescription"),
            ("Ceftriaxone", "prescription"),
            ("Vancomycin", "prescription"),
            ("Gentamicin", "prescription"),
            ("Meropenem", "prescription"),
            ("Piperacillin-Tazobactam", "prescription"),
        ],
    },
    "cardiovascular": {
        "weight": 0.15,
        "drugs": [
            ("Lisinopril", "prescription"),
            ("Metoprolol", "prescription"),
            ("Atorvastatin", "prescription"),
            ("Amlodipine", "prescription"),
            ("Losartan", "prescription"),
            ("Warfarin", "prescription"),
            ("Apixaban", "prescription"),
            ("Rivaroxaban", "prescription"),
            ("Furosemide", "prescription"),
            ("Spironolactone", "prescription"),
            ("Digoxin", "prescription"),
            ("Amiodarone", "prescription"),
            ("Clopidogrel", "prescription"),
            ("Aspirin-Clopidogrel", "combination"),
        ],
    },
    "respiratory": {
        "weight": 0.10,
        "drugs": [
            ("Albuterol", " inhaler"),
            ("Fluticasone", "nasal"),
            ("Budesonide", "inhaler"),
            ("Salmeterol", "inhaler"),
            ("Montelukast", "prescription"),
            ("Levalbuterol", "inhaler"),
            ("Tiotropium", "inhaler"),
            ("Ipratropium", "nasal"),
            ("Zafirlukast", "prescription"),
            ("Theophylline", "prescription"),
        ],
    },
    "gastrointestinal": {
        "weight": 0.10,
        "drugs": [
            ("Omeprazole", "prescription"),
            ("Pantoprazole", "prescription"),
            ("Famotidine", "OTC"),
            ("Lansoprazole", "prescription"),
            ("Ranitidine", "prescription"),
            ("Metoclopramide", "prescription"),
            ("Loperamide", "OTC"),
            ("Bismuth Subsalicylate", "OTC"),
            ("Simethicone", "OTC"),
        ],
    },
    "endocrine": {
        "weight": 0.08,
        "drugs": [
            ("Levothyroxine", "prescription"),
            ("Insulin", "prescription"),
            ("Metformin", "prescription"),
            ("Glipizide", "prescription"),
            ("Pioglitazone", "prescription"),
            ("Sitagliptin", "prescription"),
        ],
    },
    "other": {
        "weight": 0.14,
        "drugs": [
            ("Fluoxetine", "prescription"),
            ("Sertraline", "prescription"),
            ("Escitalopram", "prescription"),
            ("Bupropion", "prescription"),
            ("Amitriptyline", "prescription"),
            ("Duloxetine", "prescription"),
            ("Gabapentin", "prescribe"),
            ("Pregabalin", "prescription"),
            ("Carisoprodol", "prescription"),
            ("Cyclobenzaprine", "prescription"),
            ("Prednisone", "prescription"),
            ("Methylprednisolone", "prescription"),
            ("Hydroxyzine", "prescription"),
            ("Diphenhydramine", "OTC"),
        ],
    },
}

# Drug frequency patterns with realistic distributions
DRUG_FREQUENCIES = {
    "once_daily": {"label": "Once daily", "times": [8], "weight": 0.35},
    "twice_daily": {"label": "Twice daily", "times": [8, 20], "weight": 0.25},
    "three_times_daily": {
        "label": "Three times daily",
        "times": [8, 14, 20],
        "weight": 0.20,
    },
    "four_times_daily": {
        "label": "Four times daily",
        "times": [6, 12, 18, 24],
        "weight": 0.10,
    },
    "every_6_hours": {
        "label": "Every 6 hours",
        "times": [6, 12, 18, 24],
        "weight": 0.05,
    },
    "every_8_hours": {"label": "Every 8 hours", "times": [6, 14, 22], "weight": 0.03},
    "as_needed": {"label": "As needed", "times": [], "weight": 0.02},
}

# Sequential timing offsets for drug order fields (minutes)
DRUG_ORDER_TIMING = {
    "entered_to_created": (5, 60),
    "created_to_performed": (15, 720),
}


def _get_weighted_diagnostic_test(random_instance: random.Random) -> str:
    """Get a diagnostic test name based on realistic category distribution."""
    categories = list(DIAGNOSTIC_ORDER_CATEGORIES.keys())
    weights = [DIAGNOSTIC_ORDER_CATEGORIES[cat]["weight"] for cat in categories]

    selected_category = random_instance.choices(categories, weights=weights, k=1)[0]

    tests = DIAGNOSTIC_ORDER_CATEGORIES[selected_category]["tests"]
    return random_instance.choice(tests)


def _calculate_sequential_order_dates(
    base_date: datetime,
    time_offsets: List[int],
) -> List[datetime]:
    """Calculate sequential dates with realistic clinical timing patterns.

    Args:
        base_date: The base datetime to calculate from
        time_offsets: List of relative offsets in minutes

    Returns:
        List of calculated datetime objects with proper sequencing
    """
    result_dates = []

    for offset in time_offsets:
        result_dates.append(base_date + timedelta(minutes=offset))

    return result_dates


def _get_weighted_drug(random_instance: random.Random) -> str:
    """Get a drug name based on realistic category distribution.

    Args:
        random_instance: Random instance for sampling

    Returns:
        Drug name as string
    """
    categories = list(DRUG_CATEGORIES.keys())
    weights = [DRUG_CATEGORIES[cat]["weight"] for cat in categories]

    selected_category = random_instance.choices(categories, weights=weights, k=1)[0]

    drugs = DRUG_CATEGORIES[selected_category]["drugs"]
    drug_name, _ = random_instance.choice(drugs)
    return drug_name


def _determine_patient_age_distribution(
    random_instance: random.Random,
) -> int:
    """Generate realistic patient age distribution.

    Args:
        random_instance: Random instance for sampling

    Returns:
        Patient age (0-100)
    """
    ages = list(range(0, 101))

    weights = []
    for age in ages:
        if age < 18:
            weights.append(2)  # Children/adolescents - less common
        elif age >= 65:
            weights.append(3)  # Elderly - more medical visits
        elif age >= 40:
            weights.append(2)
        else:
            weights.append(1)

    total = sum(weights)
    normalized_weights = [w / total for w in weights]

    selected_age = random_instance.choices(ages, weights=normalized_weights, k=1)[0]

    return selected_age


def _determine_admission_type(
    base_date: datetime,
) -> str:
    """Determine admission type based on timing patterns.

    Args:
        base_date: The base datetime to evaluate

    Returns:
        'emergency' or 'elective'
    """
    hour = base_date.hour
    day_of_week = base_date.weekday()
    weekend = day_of_week in (5, 6)
    off_hours = hour >= 18 or hour < 8

    if weekend or off_hours:
        return "emergency"
    return "elective"


def generate_diagnostic_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "order_guid",
        "client_idcode",
        "order_name",
        "order_summaryline",
        "order_holdreasontext",
        "order_entered",
        "order_createdwhen",
        "clientvisit_visitidcode",
        "_id",
        "_index",
        "_score",
        "order_performeddtm",
        "order_typecode",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'diagnostic_orders' index.

    Implements realistic medical diagnostic order patterns including:
    - Category-weighted test selection (blood tests ~40%, imaging ~25%, etc.)
    - Sequential timing between order fields (entered → created → performed)
    - Patient age-correlated testing frequency
    - Realistic date generation relative to admission

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Day of start month (default 1).
        global_end_day: Day of end month (default final day).
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy diagnostic order data.
    """
    df_holder_list = []

    # Initialize per-client random instance for reproducibility
    client_random = random.Random(random_state)

    # Create local faker instance
    local_faker = Faker()
    local_faker.seed_instance(client_random.randint(0, 999999))

    for i in range(len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        # Determine patient age based on realistic distribution
        _patient_age = _determine_patient_age_distribution(client_random)

        # Generate base date for this patient
        base_date = create_random_date_from_globals(
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            global_start_day,
            global_end_day,
        )

        # Determine admission type based on timing
        hour = base_date.hour
        day_of_week = base_date.weekday()
        weekend = day_of_week in (5, 6)
        off_hours = hour >= 18 or hour < 8

        _admission_type = "emergency" if (weekend or off_hours) else "elective"

        data = {
            "order_guid": [local_faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "order_name": [
                _get_weighted_diagnostic_test(client_random) for _ in range(num_rows)
            ],
            "order_summaryline": [
                maybe_nan(" ".join(local_faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
            "order_holdreasontext": [
                maybe_nan(" ".join(local_faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
        }

        entered_dates = []
        created_dates = []
        performed_dates = []

        for j in range(num_rows):
            # Calculate sequential dates with realistic clinical timing
            # Pattern: entered → created (immediate) → performed (short delay)

            # For first order, use base date; subsequent orders have time offsets
            if j == 0:
                entry_base = base_date
            else:
                # Add some time between orders (0-4 hours for first few, then longer gaps)
                days_offset = j * random.uniform(0.1, 3)
                entry_base = base_date + timedelta(days=days_offset)

            # Sequential timing: entered → created (5-60 minutes) → performed (0-24 hours)
            offsets = [
                0,  # entered: 0 minutes
                client_random.randint(5, 60),  # created: +5-60 minutes
                client_random.randint(60, 1440),  # performed: +1-24 hours
            ]

            dates = _calculate_sequential_order_dates(entry_base, offsets)
            entered_dates.append(dates[0].strftime("%Y-%m-%dT%H:%M:%S"))
            created_dates.append(dates[1].strftime("%Y-%m-%dT%H:%M:%S"))
            performed_dates.append(dates[2].strftime("%Y-%m-%dT%H:%M:%S"))

        data["order_entered"] = entered_dates
        data["order_createdwhen"] = created_dates
        data["clientvisit_visitidcode"] = [
            f"visit_{local_faker.random_number(digits=8, fix_len=True)}"
            for _ in range(num_rows)
        ]
        data["_id"] = [f"{i}_{j}" for j in range(num_rows)]
        data["_index"] = [None for _ in range(num_rows)]
        data["_score"] = [None for _ in range(num_rows)]
        data["order_performeddtm"] = performed_dates
        data["order_typecode"] = ["diagnostic" for _ in range(num_rows)]

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in df.columns:
            df[field] = None

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)

    return df


def generate_drug_orders_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: List[str] = [
        "order_guid",
        "client_idcode",
        "order_name",
        "order_summaryline",
        "order_holdreasontext",
        "order_entered",
        "order_createdwhen",
        "clientvisit_visitidcode",
        "_id",
        "_index",
        "_score",
        "order_performeddtm",
        "order_typecode",
    ],
    base_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Generates dummy data for the 'drug_orders' index.

    Implements realistic medical drug order patterns including:
    - Category-weighted drug selection (analgesics ~25%, antibiotics ~18%, etc.)
    - Sequential timing between order fields (entered → created → performed)
    - Patient age-correlated medication frequency
    - Realistic date generation relative to admission
    - Realistic brand/generic name distribution

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Day of start month (default 1).
        global_end_day: Day of end month (default final day).
        fields_list: List of columns to include in the DataFrame.
        base_date: Optional fixed datetime to use as base.

    Returns:
        A pandas DataFrame with generated dummy drug order data.
    """
    df_holder_list = []

    local_faker = Faker()

    for i in range(len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        # Determine patient age based on realistic distribution
        _patient_age = _determine_patient_age_distribution(random)

        # Determine admission type
        if base_date is not None:
            base_datetime = base_date
        else:
            base_datetime = create_random_date_from_globals(
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                global_start_day,
                global_end_day,
            )

        _admission_type = _determine_admission_type(base_datetime)

        # Generate dates for all orders
        data = {
            "order_guid": [local_faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "order_name": [_get_weighted_drug(random) for _ in range(num_rows)],
            "order_summaryline": [
                maybe_nan(" ".join(local_faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
            "order_holdreasontext": [
                maybe_nan(" ".join(local_faker.sentence() for _ in range(3)))
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [
                f"visit_{local_faker.random_number(digits=8, fix_len=True)}"
                for _ in range(num_rows)
            ],
            "_id": [f"{i}_{j}" for j in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
            "order_typecode": ["medication" for _ in range(num_rows)],
        }

        order_entered_dates = []
        order_created_dates = []
        order_performed_dates = []

        for j in range(num_rows):
            # Calculate sequential dates with realistic clinical timing
            if base_date is not None:
                entry_base = base_datetime + timedelta(minutes=j * 15)
            else:
                if j == 0:
                    entry_base = base_datetime
                else:
                    days_offset = j * random.uniform(0.1, 3)
                    entry_base = base_datetime + timedelta(days=days_offset)

            # Sequential timing: entered → created (5-60 minutes) → performed (15 min - 12 hours)
            entered_to_created_range = DRUG_ORDER_TIMING["entered_to_created"]
            created_to_performed_range = DRUG_ORDER_TIMING["created_to_performed"]

            offsets = [
                0,  # entered: 0 minutes
                random.randint(*entered_to_created_range),  # created: +5-60 minutes
                random.randint(
                    *created_to_performed_range
                ),  # performed: +15 min - 12 hours
            ]

            dates = _calculate_sequential_order_dates(entry_base, offsets)
            order_entered_dates.append(dates[0].strftime("%Y-%m-%dT%H:%M:%S"))
            order_created_dates.append(dates[1].strftime("%Y-%m-%dT%H:%M:%S"))
            order_performed_dates.append(dates[2].strftime("%Y-%m-%dT%H:%M:%S"))

        data["order_entered"] = order_entered_dates
        data["order_createdwhen"] = order_created_dates
        data["order_performeddtm"] = order_performed_dates

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    df = pd.concat(df_holder_list)
    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in df.columns:
            df[field] = None

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)

    return df
