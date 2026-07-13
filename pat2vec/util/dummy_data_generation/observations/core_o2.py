from datetime import datetime
import calendar
from pat2vec.pat2vec_get_methods.get_method_core02 import CORE_O2_FIELDS
import random
from typing import List, Optional
import pandas as pd
import numpy as np

random_state = 42
np.random.seed(random_state)
random.seed(random_state)


CLINICAL_SPO2_RANGES = {
    "normal": (95, 100),
    "mild_hypoxia": (90, 94),
    "moderate_hypoxia": (86, 89),
    "severe_hypoxia": (70, 85),
}

OXYGEN_DELIVERY_METHODS = {
    "OnAir": {"spO2_range": (95, 100), "weight": 0.4},
    "2L O2 NP": {"spO2_range": (88, 96), "weight": 0.2},
    "4L O2 NP": {"spO2_range": (85, 94), "weight": 0.15},
    "NRB Mask": {"spO2_range": (80, 92), "weight": 0.1},
}

DEFAULT_SPO2_VALUES = ["98%", "97%", "96%", "95%", "94%", "93%"]


def generate_clinically_coherent_spO2(
    baseline_severity: str = "normal",
) -> str:
    """Generate a clinically coherent SpO2 value based on expected severity level.

    Clinical ranges:
    - Normal: 95-100%
    - Mild hypoxia: 90-94%
    - Moderate hypoxia: 86-89%
    - Severe hypoxia: <86%

    Oxygen delivery method correlations:
    - OnAir: typically >=95% (normal perfusion)
    - 2L O2 NP: mild-to-moderate hypoxia support
    - 4L O2 NP: moderate hypoxia support
    - NRB Mask: severe hypoxia support

    Args:
        baseline_severity: The expected clinical severity level.

    Returns:
        A str representing the SpO2 value or oxygen delivery method.
    """
    np.random.seed(random_state + hash(baseline_severity) % 10000)
    random.seed(random_state + hash(baseline_severity) % 10000)

    spO2_range = CLINICAL_SPO2_RANGES.get(baseline_severity, (95, 100))

    if baseline_severity == "normal":
        severity_weights = [0.35, 0.30, 0.20, 0.10, 0.04, 0.01]
    elif baseline_severity == "mild_hypoxia":
        severity_weights = [0.05, 0.10, 0.25, 0.35, 0.18, 0.07]
    elif baseline_severity == "moderate_hypoxia":
        severity_weights = [0.02, 0.05, 0.15, 0.30, 0.32, 0.16]
    else:
        severity_weights = [0.01, 0.03, 0.10, 0.25, 0.35, 0.26]

    _value_idx = random.choices(
        range(len(DEFAULT_SPO2_VALUES)), weights=severity_weights, k=1
    )[0]

    method_prob = random.random()

    if baseline_severity == "normal":
        if method_prob < 0.40:
            return "OnAir"
        value = random.randint(95, min(spO2_range[1], 100))
    elif baseline_severity == "mild_hypoxia":
        if method_prob < 0.35:
            return random.choice(["2L O2 NP", "4L O2 NP"])
        value = random.randint(90, min(spO2_range[1], 94))
    elif baseline_severity == "moderate_hypoxia":
        if method_prob < 0.60:
            return random.choice(["2L O2 NP", "4L O2 NP", "NRB Mask"])
        value = random.randint(86, min(spO2_range[1], 89))
    else:
        if method_prob < 0.55:
            return "NRB Mask"
        value = random.randint(max(spO2_range[0], 70), min(spO2_range[1], 85))

    return f"{value}%"


def generate_core_o2_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: Optional[List[str]] = CORE_O2_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for CORE_SpO2 (oxygen saturation) observations with clinically coherent patterns.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame. Defaults to CORE_O2_FIELDS.

    Returns:
        A pandas DataFrame with generated dummy SpO2 data.
    """
    df_holder_list = []

    current_year = 2026
    _, max_day = calendar.monthrange(global_end_year, global_end_month)
    safe_end_day = min(global_end_day, max_day)

    for client_id_code in entered_list:
        np.random.seed(random_state + hash(client_id_code) % 10000)
        random.seed(random_state + hash(client_id_code) % 10000)

        num_observations = max(1, num_rows)

        patient_dates = []
        spo2_values = []

        for i in range(num_observations):
            seed_val = random_state + i + hash(client_id_code) % 10000
            np.random.seed(seed_val)
            random.seed(seed_val)

            days_range = (global_end_year - global_start_year) * 365 + (
                global_end_month - global_start_month
            ) * 30

            recency_bias = min(max(days_range / 365, 1.0), 10.0)
            decay_rate = max(0.1, 1.0 / recency_bias)

            days_from_max = (
                datetime(global_end_year, global_end_month, safe_end_day)
                - datetime(current_year, 7, 1)
            ).total_seconds() / (24 * 3600)
            exponential_weight = np.exp(-decay_rate * abs(days_from_max) / 365)

            uniform_rand = random.random()

            if uniform_rand < min(0.85, 0.7 + 0.15 * exponential_weight):
                target_year = current_year
                target_month = random.randint(1, 12)
            elif uniform_rand < min(0.95, 0.85 + 0.1):
                target_year = current_year - 1
                target_month = random.randint(1, 12)
            else:
                year_range = global_end_year - global_start_year + 1
                target_year = int(
                    global_start_year + (random.random() ** 2) * year_range
                )
                target_year = min(max(target_year, global_start_year), global_end_year)
                target_month = random.randint(1, 12)

            _, day_max = calendar.monthrange(target_year, target_month)
            target_day = random.randint(1, min(day_max, 28))

            obs_datetime = datetime(
                target_year,
                target_month,
                target_day,
                random.randint(0, 23),
                random.randint(0, 59),
                random.randint(0, 59),
            )
            patient_dates.append(obs_datetime)

            np.random.seed(seed_val + 1000)
            random.seed(seed_val + 1000)

            severity_prob = random.random()
            if severity_prob < 0.4:
                baseline_severity = "normal"
            elif severity_prob < 0.75:
                baseline_severity = "mild_hypoxia"
            elif severity_prob < 0.92:
                baseline_severity = "moderate_hypoxia"
            else:
                baseline_severity = "severe_hypoxia"

            spO2_value = generate_clinically_coherent_spO2(
                baseline_severity=baseline_severity
            )

            spo2_values.append(spO2_value)

        for i in range(num_observations):
            seed_val = random_state + i + hash(client_id_code) % 10000
            np.random.seed(seed_val)
            random.seed(seed_val)

            obs_guid = f"{client_id_code[:4].upper()}-{np.random.randint(1000, 9999)}-{np.random.randint(1000, 9999)}-{np.random.randint(1000, 9999)}"

            visit_num = np.random.randint(10**7, 10**8 - 1)
            visit_id = f"visit_{visit_num}"

            obs_datetime_str = (
                patient_dates[i].strftime("%Y-%m-%dT%H:%M:%S")
                if i < len(patient_dates)
                else ""
            )

            spo2_val = spo2_values[i] if i < len(spo2_values) else ""

            data_row = {
                "observation_guid": [obs_guid],
                "client_idcode": [client_id_code],
                "obscatalogmasteritem_displayname": ["CORE_SpO2"],
                "observation_valuetext_analysed": [spo2_val],
                "observationdocument_recordeddtm": [obs_datetime_str],
                "clientvisit_visitidcode": [visit_id],
            }

            df_holder_list.append(pd.DataFrame(data_row))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan

    return final_df[fields_list]
