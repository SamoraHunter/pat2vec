import random

from faker import Faker
import numpy as np
import pandas as pd

random_state = 42
Faker.seed(random_state)
faker = Faker()
np.random.seed(random_state)
random.seed(random_state)


def generate_age_with_demographic_weighting(
    minimum_age: int = 18, maximum_age: int = 90
) -> int:
    """Generates age with realistic demographic distribution.

    Realistic population pyramid: more elderly than young due to
    demographic shift and higher birth rates in past decades.
    Uses weighted probability based on inverted U-shape distribution.

    Args:
        minimum_age: Minimum age in years.
        maximum_age: Maximum age in years.

    Returns:
        Randomly selected age with realistic distribution.
    """
    ages = np.arange(minimum_age, maximum_age + 1)

    weights = []
    for age in ages:
        if age < 30:
            weights.append(0.5)
        elif age < 50:
            weights.append(1.0)
        elif age < 70:
            weights.append(1.2)
        else:
            weights.append(1.3)

    normalized_weights = np.array(weights) / sum(weights)

    return int(np.random.choice(ages, p=normalized_weights))


def generate_gender_population_balanced(female_fraction: float = 0.51) -> str:
    """Generates gender with population-balanced distribution.

    Uses realistic ~51% female, ~49% male ratio based on
    general human population statistics.

    Args:
        female_fraction: Fraction of females (default 0.51).

    Returns:
        "Female" or "Male".
    """
    return "Female" if random.random() < female_fraction else "Male"


def generate_epic_patients_data(
    num_rows: int,
    entered_list: list[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    fields_list: list[str] = [
        "patient_DurableKey",
        "patient_BirthDate",
        "patient_Gender",
        "id",
    ],
) -> pd.DataFrame:
    """Generates dummy data for the 'epic_patients' index.

    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.

    Returns:
        A pandas DataFrame with generated dummy patient data.

    Raises:
        None
    """
    df_holder_list = []

    random_state_base = random_state
    for client_id_code in entered_list:
        patient_seed = random_state_base + hash(client_id_code) % (2**32)
        random.seed(patient_seed)
        np.random.seed(patient_seed)

        age = generate_age_with_demographic_weighting()
        gender = generate_gender_population_balanced()

        dob_year = 2022 - age
        dob_month = np.random.randint(1, 13)
        dob_day = np.random.randint(1, 29)

        birthdate = pd.Timestamp(year=dob_year, month=dob_month, day=dob_day)
        dob_str = birthdate.strftime("%Y-%m-%dT%H:%M:%S")

        data = {
            "patient_DurableKey": [client_id_code] * num_rows,
            "patient_BirthDate": [dob_str for _ in range(num_rows)],
            "patient_Gender": [gender for _ in range(num_rows)],
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
