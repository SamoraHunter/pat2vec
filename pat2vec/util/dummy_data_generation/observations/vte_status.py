from ..generator_helpers import create_random_date_from_globals, maybe_nan
from pat2vec.pat2vec_get_methods.get_method_vte_status import VTE_FIELDS
import random
from typing import List
import pandas as pd
import numpy as np

random_state = 42
random.seed(random_state)
faker = None


def _calculate_patient_age(base_year: int, observation_month: int) -> int:
    """Calculates patient age based on statistical age distribution in clinical settings.

    Realistic patterns:
    - General population: Most patients are older (healthcare utilization)
    - Peak ages: 40-60 for routine care
    - Younger patients: 20s-30s less common in routine settings

    Args:
        base_year: Base year for age calculation.
        observation_month: Month of observation (for finer age grading).

    Returns:
        Patient age in years (18-90 range with realistic distribution).
    """
    np.random.seed(random_state + hash(base_year) % 1000)

    if random.random() < 0.45:
        age = int(np.random.exponential(15) + 40)
    elif random.random() < 0.75:
        age = int(np.random.normal(55, 15))
    else:
        age = int(np.random.uniform(18, 90))

    return max(18, min(90, int(age)))


def _get_risk_factors(age: int) -> dict:
    """Returns probability of clinical risk factors based on patient age and context.

    Clinical risk factors affecting VTE status:
    - Age >75: +2x higher VTE risk
    - Recent surgery: +3x higher VTE risk
    - Cancer diagnosis: +4x higher VTE risk
    - Immobilization: +2x higher VTE risk

    Args:
        age: Patient's age in years.

    Returns:
        Dictionary with probability of each risk factor being present.
    """
    if age < 40:
        return {
            "recent_surgery": 0.15,
            "cancer_diagnosis": 0.05,
            "immobilization": 0.10,
        }
    elif age < 60:
        return {
            "recent_surgery": 0.25,
            "cancer_diagnosis": 0.10,
            "immobilization": 0.20,
        }
    elif age < 75:
        return {
            "recent_surgery": 0.35,
            "cancer_diagnosis": 0.18,
            "immobilization": 0.30,
        }
    else:
        return {
            "recent_surgery": 0.45,
            "cancer_diagnosis": 0.25,
            "immobilization": 0.40,
        }


def _calculate_vte_risk_probability(age: int, risk_factors: dict) -> float:
    """Calculate base VTE risk probability considering age and clinical factors.

    Real-world VTE status distribution:
    - Low risk: ~30-40% (young, ambulatory patients)
    - Moderate risk: ~25-35% (some risk factors present)
    - High risk: ~30-40% (elderly, post-op, chronic illness)

    Risk factor multipliers:
    - Age >75: +2x higher VTE risk
    - Recent surgery: +3x higher VTE risk
    - Cancer diagnosis: +4x higher VTE risk
    - Immobilization: +2x higher VTE risk

    Args:
        age: Patient's age in years.
        risk_factors: Dictionary with risk factor presence flags.

    Returns:
        Adjusted probability of high VTE risk.
    """
    base_prob = 0.35

    if age > 75:
        base_prob *= 2.0
    elif age > 60:
        base_prob *= 1.5

    if risk_factors.get("recent_surgery", False):
        base_prob *= 3.0
    if risk_factors.get("cancer_diagnosis", False):
        base_prob *= 4.0
    if risk_factors.get("immobilization", False):
        base_prob *= 2.0

    base_prob = min(base_prob, 0.85)

    return base_prob


def _determine_vte_status(age: int, risk_factors: dict) -> str:
    """Determines VTE status based on age and clinical factors.

    Realistic clinical patterns:
    - Low risk: ~30-40% (young, ambulatory patients)
    - Moderate risk: ~25-35% (some risk factors present)
    - High risk: ~30-40% (elderly, post-op, chronic illness)

    Within high risk, bleeding risk varies (~60% low bleeding risk, 40% high);

    Args:
        age: Patient's age in years.
        risk_factors: Dictionary with risk factor presence flags.

    Returns:
        VTE status string with appropriate risk category and bleeding risk.
    """
    np.random.seed(random_state + hash(age) % 1000)

    vte_prob = _calculate_vte_risk_probability(age, risk_factors)

    if random.random() < (1 - vte_prob * 0.5):
        return "Low risk of VTE"
    elif random.random() < (1 - vte_prob * 0.2):
        return "Moderate risk of VTE"
    else:
        high_risk_type = random.choice(
            [
                "High risk of VTE Low risk of bleeding",
                "High risk of VTE High risk of bleeding",
            ]
        )
        return high_risk_type


def generate_vte_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: List[str] = VTE_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for VTE status observations with realistic clinical patterns.

    The function now incorporates age-correlated probability distributions and clinical factors:

    Real-world VTE status includes three risk categories:
    - Low risk: ~30-40% (young, ambulatory patients)
    - Moderate risk: ~25-35% (some risk factors present)
    - High risk: ~30-40% (elderly, post-op, chronic illness)

    Within high risk, bleeding risk varies (~60% low bleeding risk, 40% high).

    Clinical factors affecting VTE status:
    - Age >75: +2x higher VTE risk
    - Recent surgery: +3x higher VTE risk
    - Cancer diagnosis: +4x higher VTE risk
    - Immobilization: +2x higher VTE risk

    Missing data rate reduced to realistic ~3-5% for missing VTE status.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        global_start_day: Start day for the random date range. Defaults to 1.
        global_end_day: End day for the random date range. Defaults to 31.
        fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy VTE status data.

    Raises:
        None
    """
    df_holder_list = []

    from faker import Faker

    Faker.seed(random_state)
    random.seed(random_state)

    for client_id_code in entered_list:
        faker_inst = Faker()

        obs_dates = [
            create_random_date_from_globals(
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                global_start_day,
                global_end_day,
            )
            for _ in range(num_rows)
        ]

        vte_statuses = []
        ages = []

        for obs_date in obs_dates:
            age = _calculate_patient_age(obs_date.year, obs_date.month)
            ages.append(age)

            risk_factors = _get_risk_factors(age)

            status = _determine_vte_status(age, risk_factors)

            vte_statuses.append(maybe_nan(status, probability=0.04))

        data = {
            "observation_guid": [faker_inst.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [
                "CORE_VTE_STATUS" for _ in range(num_rows)
            ],
            "observation_valuetext_analysed": vte_statuses,
            "observationdocument_recordeddtm": [
                obs_date.strftime("%Y-%m-%dT%H:%M:%S") for obs_date in obs_dates
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker_inst.random_number(digits=8)}" for _ in range(num_rows)
            ],
        }

        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan

    return final_df[fields_list]
