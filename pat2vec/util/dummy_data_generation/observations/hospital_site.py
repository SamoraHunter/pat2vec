from ..generator_helpers import create_random_date_from_globals, maybe_nan
import random
from typing import List, Optional
import pandas as pd
import numpy as np

random_state = 42
random.seed(random_state)
faker = None


def generate_hospital_site_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int = 2023,
    global_end_month: int = 12,
    global_start_day: int = 1,
    global_end_day: int = 31,
    fields_list: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Generates dummy data for hospital site observations.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy hospital site data.
    Raises:
        None
    """
    if fields_list is None:
        fields_list = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
        ]
    df_holder_list = []
    from faker import Faker

    Faker.seed(random_state)
    random.seed(random_state)

    hospital_site_values = [
        "DH",
        "PRUH",
        "Orpington",
        "Queen Mary's",
        "St Thomas",
    ]
    for client_id_code in entered_list:
        faker_inst = Faker()
        data = {
            "observation_guid": [faker_inst.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [
                "CORE_HospitalSite" for _ in range(num_rows)
            ],
            "observation_valuetext_analysed": [
                maybe_nan(faker_inst.random_element(elements=hospital_site_values))
                for _ in range(num_rows)
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
                f"visit_{faker_inst.random_number(digits=8, fix_len=True)}"
                for _ in range(num_rows)
            ],
        }
        df = pd.DataFrame(data)
        df_holder_list.append(df)
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    for field in fields_list:
        if field not in final_df.columns:
            final_df[field] = np.nan
    final_df = final_df[fields_list]
    return final_df
