from ..generator_helpers import create_random_date_from_globals, maybe_nan
from pat2vec.pat2vec_get_methods.get_method_smoking import SMOKING_FIELDS
import random
from typing import List
import pandas as pd
import numpy as np

try:
    from faker import Faker
except ImportError:
    Faker = None

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

    _base_date = base_year + (observation_month - 1) / 12

    if random.random() < 0.45:
        age = int(np.random.exponential(15) + 40)
    elif random.random() < 0.75:
        age = int(np.random.normal(55, 15))
    else:
        age = int(np.random.uniform(18, 90))

    return max(18, min(90, int(age)))


def _get_smoking_probabilities(age: int) -> dict:
    """Returns probability distribution for smoking status based on patient age.

    Realistic clinical patterns:
    - Never smoked: ~60% in general population, higher in elderly
    - Ex-smoker: ~20-25%, increases with age (esp. 40+)
    - Current smoker: ~15-20%, peaks in 20s-40s, declines sharply with age
    - "Smoker" (generic): often used interchangeably with current smoker

    Age-related patterns:
    - Under 30: Higher current smoking rates (15-25%)
    - 30-60: Moderate current smoking (10-20%)
    - Over 60: Lower current smoking (5-15%), higher ex-smoker rates
    - Over 80: Mostly never/ex-smokers, very low current smokers (<5%)

    Args:
        age: Patient's age in years.

    Returns:
        Dictionary with probabilities for each smoking status category.
    """
    if age < 20:
        return {
            "Never smoked": 0.85,
            "Ex-smoker": 0.05,
            "Current smoker": 0.10,
            "Smoker": 0.00,
        }

    elif age < 30:
        return {
            "Never smoked": 0.72,
            "Ex-smoker": 0.08,
            "Current smoker": 0.20,
            "Smoker": 0.00,
        }

    elif age < 45:
        return {
            "Never smoked": 0.65,
            "Ex-smoker": 0.12,
            "Current smoker": 0.18,
            "Smoker": 0.05,
        }

    elif age < 60:
        return {
            "Never smoked": 0.60,
            "Ex-smoker": 0.18,
            "Current smoker": 0.15,
            "Smoker": 0.02,
        }

    elif age < 80:
        return {
            "Never smoked": 0.55,
            "Ex-smoker": 0.30,
            "Current smoker": 0.08,
            "Smoker": 0.01,
        }

    else:
        return {
            "Never smoked": 0.62,
            "Ex-smoker": 0.35,
            "Current smoker": 0.03,
            "Smoker": 0.00,
        }


def _select_smoking_status(age: int) -> str:
    """Selects smoking status based on age-correlated probability distribution.

    Args:
        age: Patient's age in years.

    Returns:
        Smoking status string selected according to realistic clinical patterns.
    """
    probabilities = _get_smoking_probabilities(age)

    categories = list(probabilities.keys())
    weights = list(probabilities.values())

    choices = random.choices(categories, weights=weights, k=1)

    return choices[0]


def generate_smoking_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: List[str] = SMOKING_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for smoking status observations with realistic clinical patterns.

    The function now incorporates age-correlated probability distributions for smoking status:
    - Never smoked: ~60% in general population, higher in elderly
    - Ex-smoker: ~20-25%, increases with age (esp. 40+)
    - Current smoker: ~15-20%, peaks in 20s-40s, declines sharply with age
    - "Smoker" (generic): often used interchangeably with current smoker

    Realistic clinical patterns by age:
    - Under 30: Higher current smoking rates (15-25%)
    - 30-60: Moderate current smoking (10-20%)
    - Over 60: Lower current smoking (5-15%), higher ex-smoker rates
    - Over 80: Mostly never/ex-smokers, very low current smokers (<5%)

    Missing data rate reduced to realistic ~3-5% for missing smoking status.

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
        A pandas DataFrame with generated dummy smoking status data.

    Raises:
        None
    """
    df_holder_list = []

    Faker.seed(random_state)
    random.seed(random_state)
    faker_inst = faker if faker is not None else Faker()

    for client_id_code in entered_list:
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

        smoking_statuses = []
        ages = []

        for obs_date in obs_dates:
            age = _calculate_patient_age(obs_date.year, obs_date.month)
            ages.append(age)

            status = _select_smoking_status(age)

            smoking_statuses.append(maybe_nan(status, probability=0.04))

        data = {
            "observation_guid": [faker_inst.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [
                "CORE_SmokingStatus" for _ in range(num_rows)
            ],
            "observation_valuetext_analysed": smoking_statuses,
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
