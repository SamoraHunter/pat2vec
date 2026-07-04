from ..generator_helpers import create_random_date_from_globals
from pat2vec.pat2vec_get_methods.get_method_core02 import CORE_O2_FIELDS
import random
from typing import List, Optional
import pandas as pd
import numpy as np

random_state = 42
random.seed(random_state)
faker = None


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
    """Generates dummy data for CORE_SpO2 (oxygen saturation) observations.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy SpO2 data.
    Raises:
        None
    """
    df_holder_list = []
    from faker import Faker

    Faker.seed(random_state)
    random.seed(random_state)

    spo2_values = [
        "98%",
        "97%",
        "96%",
        "95%",
        "94%",
        "93%",
        "On Air",
        "2L O2 NP",
        "4L O2 NP",
        "NRB Mask",
    ]
    for client_id_code in entered_list:
        faker_inst = Faker()
        data = {
            "observation_guid": [faker_inst.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": ["CORE_SpO2" for _ in range(num_rows)],
            "observation_valuetext_analysed": [
                random.choice(spo2_values) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
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
