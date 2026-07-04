import random
from typing import List
from faker import Faker
import pandas as pd
import numpy as np
from .generator_helpers import create_random_date_from_globals

random_state = 42
random.seed(random_state)
faker = Faker()


def generate_covid_observations_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    fields_list: List[str] = [],
    global_end_year: int = 2023,
    global_end_month: int = 12,
) -> pd.DataFrame:
    """Generates dummy data for COVID-19 test observations.
    Args:
        num_rows: Number of rows to generate per client.
        entered_list: List of client IDs to generate data for.
        global_start_year: Start year for the random date range.
        global_start_month: Start month for the random date range.
        global_end_year: End year for the random date range.
        global_end_month: End month for the random date range.
    fields_list: List of columns to include in the DataFrame.
    Returns:
        A pandas DataFrame with generated dummy COVID-19 observation data.
    Raises:
        None
    """
    from pat2vec.pat2vec_get_methods.get_method_covid import (
        COVID_FIELDS,
        SEARCH_TERM_PLAIN,
    )

    if SEARCH_TERM_PLAIN is None:
        SEARCH_TERM_PLAIN = "SARS CoV-2 (COVID-19) RNA"
    if fields_list is None:
        fields_list = COVID_FIELDS
    df_holder_list = []
    for client_id_code in entered_list:
        data = {
            "basicobs_guid": [faker.uuid4() for _ in range(num_rows)],
            "client_idcode": [client_id_code for _ in range(num_rows)],
            "basicobs_itemname_analysed": [SEARCH_TERM_PLAIN for _ in range(num_rows)],
            "basicobs_value_analysed": [
                random.choice(["Positive", "Negative"]) for _ in range(num_rows)
            ],
            "basicobs_entered": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [faker.uuid4() for _ in range(num_rows)],
        }
        df_holder_list.append(pd.DataFrame(data))
    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)
    final_df = pd.concat(df_holder_list, ignore_index=True)
    for col in fields_list:
        if col not in final_df.columns:
            final_df[col] = np.nan
    return final_df[fields_list]
