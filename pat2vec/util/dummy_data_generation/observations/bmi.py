from datetime import datetime
import calendar
import random
from typing import List, Optional

from faker import Faker
import numpy as np
import pandas as pd

from ..generator_helpers import create_random_date_from_globals
from pat2vec.pat2vec_get_methods.get_method_bmi import BMI_FIELDS

random_state = 42
Faker.seed(random_state)
np.random.seed(random_state)
random.seed(random_state)
faker = Faker()


def generate_bmi_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    fields_list: List[str] = BMI_FIELDS,
    base_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """Generates dummy data for BMI, Weight, and Height observations.
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
    for client_id_code in entered_list:
        # Generate data for this specific client
        # Ensure at least one "OBS BMI Calculation" exists when num_rows >= 1
        if num_rows >= 1:
            display_names = ["OBS BMI Calculation"] + [
                random.choice(observation_types) for _ in range(num_rows - 1)
            ]
        else:
            display_names = [random.choice(observation_types) for _ in range(num_rows)]
        values = []
        for name in display_names:
            if name == "OBS BMI Calculation":
                # Generate a realistic BMI value (15.0 to 45.0)
                value = f"{random.uniform(15.0, 45.0):.2f}"
            elif name == "OBS Weight":
                # Generate a realistic weight in kg (40.0 to 150.0)
                value = f"{random.uniform(40.0, 150.0):.2f}"
            else:  # OBS Height
                # Generate a realistic height in cm (140.0 to 200.0)
                value = f"{random.uniform(140.0, 200.0):.2f}"
            values.append(value)
        # Generate dates - use base_date if provided (for tests) or random generation otherwise
        date_values = []
        if base_date is not None:
            # For testing: generate dates within a predictable range around base_date
            # Generate dates centered around base_date with small offsets
            for i in range(num_rows):
                # Use positive offsets to ensure dates are >= base_date (within reasonable range)
                day_offset = (
                    0 if num_rows == 1 else (i * 2) - 1
                )  # For 2 rows: -1, +1 giving days before and after
                test_day = max(1, min(28, base_date.day + day_offset))
                date_values.append(
                    datetime(
                        base_date.year,
                        base_date.month,
                        test_day,
                        random.randint(0, 23),
                        random.randint(0, 59),
                        random.randint(0, 59),
                    )
                )
        else:
            # Generate dates concentrated around June 2023 to match common test date ranges
            for i in range(num_rows):
                if random.random() < 0.7:  # 70% chance to generate a June 2023 date
                    year = 2023
                    month = 6  # June
                    day = random.randint(15, 25)  # Near test date range (June 15)
                    hour = random.randint(0, 23)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    date_values.append(datetime(year, month, day, hour, minute, second))
                else:
                    # 30% chance for other recent dates or original range
                    if random.random() < 0.5:
                        year = 2024
                        month = random.randint(1, 6)
                        day_max = calendar.monthrange(year, month)[1]
                        day = random.randint(1, min(day_max, 28))
                        date_values.append(
                            datetime(
                                year,
                                month,
                                day,
                                random.randint(0, 23),
                                random.randint(0, 59),
                                random.randint(0, 59),
                            )
                        )
                    else:
                        # Use original range for remaining
                        date_values.append(
                            create_random_date_from_globals(
                                global_start_year,
                                global_start_month,
                                global_end_year,
                                global_end_month,
                            )
                        )
        data = {
            "observation_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": display_names,
            "observation_valuetext_analysed": values,
            "observationdocument_recordeddtm": [
                d.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(d, datetime) else d
                for d in date_values
            ],
            "clientvisit_visitidcode": [
                f"visit_{faker.random_number(digits=8)}" for _ in range(num_rows)
            ],
        }
        df_holder_list.append(pd.DataFrame(data))
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    unique_fields = list(dict.fromkeys(fields_list))
    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = np.nan
    return final_df[unique_fields]
