from datetime import datetime
import calendar
import random
from typing import List, Optional

from faker import Faker
import numpy as np
import pandas as pd

from pat2vec.pat2vec_get_methods.get_method_bmi import BMI_FIELDS

random_state = 42
Faker.seed(random_state)
np.random.seed(random_state)
random.seed(random_state)
faker = Faker()


def generate_uuid_string(seed_val):
    """Generate a deterministic UUID-like string using seeded RNG."""
    np.random.seed(seed_val)

    hex_chars = "0123456789abcdef"
    sections = [
        "".join(np.random.choice(list(hex_chars), 8)),
        "".join(np.random.choice(list(hex_chars), 4)),
        "".join(np.random.choice(list(hex_chars), 4)),
        "".join(np.random.choice(list(hex_chars), 4)),
        "".join(np.random.choice(list(hex_chars), 12)),
    ]

    return "-".join(sections)


def generate_bmi_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: List[str] = BMI_FIELDS,
    base_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Generates dummy data for BMI, Weight, and Height observations with realistic statistical distributions.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
        fields_list: List of columns to include in the DataFrame.
        base_date: Optional datetime reference for testing.

    Returns:
        A pandas DataFrame with generated dummy BMI-related data.
    Raises:
        None
    """
    df_holder_list = []
    observation_types = ["OBS BMI Calculation", "OBS Weight", "OBS Height"]

    current_year = 2026

    _, max_day = calendar.monthrange(global_end_year, global_end_month)
    safe_end_day = min(global_end_day, max_day)
    max_date = datetime(global_end_year, global_end_month, safe_end_day, 23, 59, 59)

    _, min_day = calendar.monthrange(global_start_year, global_start_month)
    safe_start_day = min(global_start_day, min_day)
    min_date = datetime(global_start_year, global_start_month, safe_start_day, 0, 0, 0)

    for client_id_code in entered_list:
        num_observations = max(1, num_rows)

        observation_type_counts = {
            "OBS BMI Calculation": 0,
            "OBS Weight": 0,
            "OBS Height": 0,
        }

        patient_dates = []
        bmi_values = []
        weight_values = []
        height_values = []

        num_months = (global_end_year - global_start_year) * 12 + (
            global_end_month - global_start_month
        )
        if num_months <= 0:
            num_months = 1

        year_weight = (current_year - global_start_year) / max(num_months / 12, 1)

        for i in range(num_observations):
            random.seed(random_state + i + hash(client_id_code) % 10000)

            date_range_days = (max_date - min_date).total_seconds()
            time_spread_factor = min(
                max(date_range_days / (365 * 24 * 3600), 1.0), 10.0
            )
            decay_rate = max(0.1, 1.0 / time_spread_factor)

            days_from_max = (
                max_date - datetime(current_year, 7, 1)
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

        if num_observations >= 1:
            bmi_count = max(1, int(num_observations * 0.6))
            weight_count = max(1, int(num_observations * 0.7))
            height_count = max(1, int(num_observations * 0.7))

            total_computed = 0
            for i in range(num_observations):
                if total_computed < bmi_count:
                    obs_type = "OBS BMI Calculation"
                    total_computed += 1
                elif len([o for o in observation_type_counts.values()]) < 3:
                    remaining_types = [
                        t
                        for t in observation_types
                        if observation_type_counts.get(t, 0) == 0
                    ]
                    if remaining_types:
                        obs_type = random.choice(remaining_types)
                    else:
                        obs_type = random.choice(observation_types)
                else:
                    obs_type = random.choices(
                        observation_types, weights=[0.4, 0.3, 0.3], k=1
                    )[0]

                observation_type_counts[obs_type] = (
                    observation_type_counts.get(obs_type, 0) + 1
                )

                sample_seed = random_state + i + hash(client_id_code) % 10000
                np.random.seed(sample_seed)

                age_from_start_year = (
                    current_year - target_year if "target_year" in locals() else 45
                )
                age = max(18, min(90, int(age_from_start_year + random.gauss(0, 10))))

                male_prob = 0.5
                is_male = random.random() < male_prob

                mean_bmi = 25.0
                variance_factor = 1.0 + (age / 100) * 0.3
                bmi_seed = sample_seed
                np.random.seed(bmi_seed)

                actual_bmi = max(
                    12.0, min(50.0, np.random.normal(mean_bmi, 4.5 * variance_factor))
                )

                mean_weight_kg = 70.0 if not is_male else 80.0
                weight_variance = 15.0 + (age / 100) * 5
                actual_weight = max(
                    35.0, min(200.0, np.random.normal(mean_weight_kg, weight_variance))
                )

                mean_height_cm = 165.0 if is_male else 160.0
                height_variance = 8.0 + (age / 100) * 3
                actual_height = max(
                    120.0, min(230.0, np.random.normal(mean_height_cm, height_variance))
                )

                bmi_values.append(actual_bmi)
                weight_values.append(actual_weight)
                height_values.append(actual_height)

        patient_seed = hash(client_id_code) % 10000
        for i in range(num_observations):
            random.seed(patient_seed + i)

            obs_guid = generate_uuid_string(patient_seed + i)

            np.random.seed(patient_seed + i + 1000)
            visit_num = np.random.randint(10**7, 10**8 - 1)
            visit_id = f"visit_{visit_num}"

            patient_date = patient_dates[i]

            if isinstance(patient_date, datetime):
                obs_datetime_str = patient_date.strftime("%Y-%m-%dT%H:%M:%S")
            else:
                obs_datetime_str = ""

            row_bmi = bmi_values[i] if len(bmi_values) > i else np.nan
            row_weight = weight_values[i] if len(weight_values) > i else np.nan
            row_height = height_values[i] if len(height_values) > i else np.nan

            current_obs_type = (
                observation_types[i % len(observation_types)]
                if i < len(observation_types)
                else random.choice(observation_types)
            )

            if current_obs_type == "OBS BMI Calculation":
                value_str = f"{row_bmi:.2f}"
            elif current_obs_type == "OBS Weight":
                value_str = f"{row_weight:.2f}"
            else:
                value_str = f"{row_height:.2f}"

            data_row = {
                "observation_guid": [obs_guid],
                "client_idcode": [client_id_code],
                "obscatalogmasteritem_displayname": [current_obs_type],
                "observation_valuetext_analysed": [value_str],
                "observationdocument_recordeddtm": [obs_datetime_str],
                "clientvisit_visitidcode": [visit_id],
            }

            df_holder_list.append(pd.DataFrame(data_row))

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    unique_fields = list(dict.fromkeys(fields_list))

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan

    return final_df[unique_fields]
