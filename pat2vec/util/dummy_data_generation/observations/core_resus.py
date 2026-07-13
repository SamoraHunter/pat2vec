from ..generator_helpers import create_random_date_from_globals
from pat2vec.pat2vec_get_methods.get_method_core_resus import CORE_RESUS_FIELDS
import random
from typing import List, Optional
import pandas as pd
import numpy as np
from datetime import datetime

try:
    from faker import Faker
except ImportError:
    Faker = None

random_state = 42
random.seed(random_state)
faker = None if Faker is None else Faker()


def _calculate_age_at_observation(dob: datetime, observation_date: datetime) -> int:
    """Calculates age in years at the time of observation.

    Args:
        dob: Date of birth as a datetime object.
        observation_date: The date of the observation.

    Returns:
        Age in years as an integer.
    """
    age = observation_date.year - dob.year
    if (observation_date.month, observation_date.day) < (dob.month, dob.day):
        age -= 1
    return age


def _get_dnr_probability(age: int, is_icu_hdu: bool = False) -> float:
    """Returns probability of "Not for CPR" status based on age and context.

    Realistic clinical patterns:
    - Young patients (<60): >95% "Not for CPR"
    - Middle age (60-80): 80-90% "Not for CPR" with gradual increase in DNR
    - Elderly (>80): ~70% "Not for CPR", 20-25% "For CPR"

    ICU/HDU patients have higher rates of DNR/DNH decisions.

    Args:
        age: Patient's age in years.
        is_icu_hdu: Whether the observation is from ICU/HDU context.

    Returns:
        Probability of "Not for cardiopulmonary resuscitation".
    """
    if age < 60:
        base_prob = random.uniform(0.95, 0.98)
    elif age < 80:
        age_factor = (age - 60) / 20
        base_prob = 0.80 + age_factor * 0.10
    else:
        base_prob = random.uniform(0.70, 0.75)

    if is_icu_hdu:
        base_prob += random.uniform(0.10, 0.20)
        base_prob = min(base_prob, 0.99)

    return base_prob


def _determine_resuscitation_status(
    age: int, observation_date: datetime, is_icu_hdu: bool = False
) -> str:
    """Determines resuscitation status based on age and context.

    Args:
        age: Patient's age in years.
        observation_date: Date of the observation.
        is_icu_hdu: Whether this is an ICU/HDU context.

    Returns:
        Either "For cardiopulmonary resuscitation" or "Not for cardiopulmonary resuscitation".
    """
    not_for_cpr_prob = _get_dnr_probability(age, is_icu_hdu)

    if random.random() < not_for_cpr_prob:
        return "Not for cardiopulmonary resuscitation"
    else:
        return "For cardiopulmonary resuscitation"


def generate_core_resus_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: Optional[List[str]] = CORE_RESUS_FIELDS,
) -> pd.DataFrame:
    """Generates dummy data for CORE_RESUS_STATUS observations with realistic clinical patterns.

    The function now incorporates age-correlated probability distributions for resuscitation status:
    - Young patients (<60): >95% "Not for CPR"
    - Middle age (60-80): 80-90% "Not for CPR" with gradual increase in DNR
    - Elderly (>80): ~70% "Not for CPR", 20-25% "For CPR"

    ICU/HDU contexts have higher rates of DNR/DNH decisions.

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
        A pandas DataFrame with generated dummy resuscitation status data.

    Raises:
        None
    """
    df_holder_list = []

    Faker.seed(random_state)
    random.seed(random_state)
    faker_inst = faker if faker is not None else Faker()

    for client_id_code in entered_list:
        dob = faker_inst.date_of_birth(minimum_age=18, maximum_age=90)

        data = {
            "observation_guid": [faker_inst.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [
                "CORE_RESUS_STATUS" for _ in range(num_rows)
            ],
            "observation_valuetext_analysed": [],
            "observationdocument_recordeddtm": [],
            "clientvisit_visitidcode": [],
        }

        observation_dates = []
        resus_statuses = []
        visit_ids = []

        for _ in range(num_rows):
            obs_date = create_random_date_from_globals(
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                global_start_day,
                global_end_day,
            )

            age_at_observation = _calculate_age_at_observation(dob, obs_date)

            is_icu_hdu = random.random() < 0.15

            status = _determine_resuscitation_status(
                age_at_observation, obs_date, is_icu_hdu
            )

            resus_statuses.append(status)
            observation_dates.append(obs_date.strftime("%Y-%m-%dT%H:%M:%S"))
            visit_ids.append(f"visit_{faker_inst.random_number(digits=8)}")

        data["observation_valuetext_analysed"] = resus_statuses
        data["observationdocument_recordeddtm"] = observation_dates
        data["clientvisit_visitidcode"] = visit_ids

        df_holder_list.append(pd.DataFrame(data))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan

    return final_df[fields_list]
