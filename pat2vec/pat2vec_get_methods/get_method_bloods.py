from datetime import datetime, timezone

import numpy as np
import pandas as pd
from IPython.display import display
from scipy import stats

from pat2vec.util.methods_get import (
    filter_dataframe_by_timestamp,
    get_start_end_year_month,
)


def get_current_pat_bloods(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    batch_mode=False,
    cohort_searcher_with_terms_and_search=None,
    config_obj=None,
):
    """
    Retrieves blood test data for a given patient within a specified date range.

    Parameters:
    - current_pat_client_id_code (str): The client ID code of the patient.
    - target_date_range (tuple): A tuple representing the target date range.
    - pat_batch (pd.DataFrame): The DataFrame containing patient data.
    - batch_mode (bool, optional): Indicates whether batch mode is enabled. Defaults to False.
    - cohort_searcher_with_terms_and_search (callable, optional): The function for cohort searching. Defaults to None.

    Returns:
    - pd.DataFrame: A DataFrame containing blood test data for the specified patient.
    """

    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    bloods_time_field = config_obj.bloods_time_field

    if batch_mode:
        current_pat_bloods = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            bloods_time_field,
        )
    else:
        current_pat_bloods = cohort_searcher_with_terms_and_search(
            index_name="basic_observations",
            fields_list=[
                "client_idcode",
                "basicobs_itemname_analysed",
                "basicobs_value_numeric",
                "basicobs_entered",
                "clientvisit_serviceguid",
                "updatetime",
            ],
            term_name=config_obj.client_idcode_term_name,
            entered_list=[current_pat_client_id_code],
            search_string="basicobs_value_numeric:* AND "
            + f"{bloods_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
        )

    if batch_mode:
        current_pat_bloods["datetime"] = current_pat_bloods[bloods_time_field].copy()
    else:
        current_pat_bloods["datetime"] = pd.to_datetime(
            current_pat_bloods[bloods_time_field], errors="coerce"
        )

    basicobs_itemname_analysed_list = list(
        current_pat_bloods["basicobs_itemname_analysed"].unique()
    )

    basicobs_itemname_analysed_df_dict = {
        elem: current_pat_bloods[current_pat_bloods.basicobs_itemname_analysed == elem]
        for elem in basicobs_itemname_analysed_list
    }

    df_unique = current_pat_bloods.copy()

    df_unique.drop_duplicates(subset="client_idcode", inplace=True)

    df_unique.reset_index(inplace=True)

    filtered_list = []

    obs_columns_list = basicobs_itemname_analysed_list

    obs_columns_set = list(set(obs_columns_list))

    filtered_column_list = filtered_list

    obs_columns_set_columns_for_df = []
    for i in range(0, len(obs_columns_set)):
        obs_columns_set_columns_for_df.append(obs_columns_set[i] + "_mean")
        obs_columns_set_columns_for_df.append(obs_columns_set[i] + "_median")
        obs_columns_set_columns_for_df.append(obs_columns_set[i] + "_mode")
        obs_columns_set_columns_for_df.append(obs_columns_set[i] + "_std")
        obs_columns_set_columns_for_df.append(obs_columns_set[i] + "_num-tests")
        obs_columns_set_columns_for_df.append(
            obs_columns_set[i] + "_days-since-last-test"
        )
        obs_columns_set_columns_for_df.append(obs_columns_set[i] + "_max")
        obs_columns_set_columns_for_df.append(obs_columns_set[i] + "_min")
        obs_columns_set_columns_for_df.append(obs_columns_set[i] + "_most-recent")
        obs_columns_set_columns_for_df.append(obs_columns_set[i] + "_earliest-test")
        obs_columns_set_columns_for_df.append(
            obs_columns_set[i] + "_days-between-first-last"
        )
        obs_columns_set_columns_for_df.append(
            obs_columns_set[i] + "_contains-extreme-low"
        )
        obs_columns_set_columns_for_df.append(
            obs_columns_set[i] + "_contains-extreme-high"
        )

    orig_columns = list(df_unique.columns)

    comb_cols = orig_columns + obs_columns_set_columns_for_df

    df_unique = df_unique.reindex(comb_cols, axis=1)

    df_unique = df_unique.copy()
    df_unique.drop(
        [
            "index",
            "_index",
            "_id",
            "_score",
            "basicobs_itemname_analysed",
            "basicobs_value_numeric",
            "basicobs_entered",
            "clientvisit_serviceguid",
            "datetime",
            "updatetime",
        ],
        inplace=True,
        axis=1,
    )

    if batch_mode:

        today = datetime.now(timezone.utc)

    else:
        today = datetime.today()

    clients_id = current_pat_client_id_code

    df_unique_filtered = df_unique.copy()

    i = 0

    for j in range(0, len(basicobs_itemname_analysed_list)):
        col_name = basicobs_itemname_analysed_list[j]

        filtered_df = basicobs_itemname_analysed_df_dict.get(col_name)

        filtered_column_values = filtered_df.basicobs_value_numeric.astype(
            float
        )._get_numeric_data()

        df_len = len(filtered_df)

        if df_len >= 1:
            # Mean assurance*
            agg_val = float(filtered_column_values.values[0])

            df_unique_filtered.at[i, col_name + "_mean"] = agg_val

        if df_len >= 2:
            # try:
            # Mean
            agg_val = filtered_column_values.mean()

            df_unique_filtered.at[i, col_name + "_mean"] = agg_val

            # recent
            agg_val = filtered_df.sort_values(by="datetime").iloc[-1][
                "basicobs_value_numeric"
            ]

            df_unique_filtered.at[i, col_name + "_most-recent"] = agg_val

            # earliest-test
            agg_val = filtered_df.sort_values(by="datetime").iloc[0][
                "basicobs_value_numeric"
            ]
            df_unique_filtered.at[i, col_name + "_earliest-test"] = agg_val

            # days-since-last-test
            date_object = filtered_df.sort_values(by="datetime").iloc[-1]["datetime"]

            delta = today - date_object

            agg_val = delta.days

            df_unique_filtered.at[i, col_name + "_days-since-last-test"] = agg_val

            # n tests

            agg_val = len(filtered_column_values)

            df_unique_filtered.at[i, col_name + "_num-tests"] = agg_val

        if df_len >= 3:
            # try:
            # median
            # agg_val = np.median(filtered_df['basicobs_value_numeric'].to_numpy())
            agg_val = filtered_column_values.median()
            df_unique_filtered.at[i, col_name + "_median"] = agg_val

            # mode
            # agg_val = stats.mode(filtered_df['basicobs_value_numeric'].to_numpy(), keepdims=True)[0][0]

            agg_val = stats.mode(filtered_column_values)[0][0]
            df_unique_filtered.at[i, col_name + "_mode"] = agg_val

            # std
            agg_val = filtered_column_values.std()
            df_unique_filtered.at[i, col_name + "_std"] = agg_val

            # min
            agg_val = min(filtered_column_values)
            df_unique_filtered.at[i, col_name + "_min"] = agg_val

            # max
            agg_val = max(filtered_column_values)
            df_unique_filtered.at[i, col_name + "_max"] = agg_val

            # contains extreme low
            col_name_mean = (
                basicobs_itemname_analysed_df_dict.get(col_name)
                .basicobs_value_numeric._get_numeric_data()
                .mean()
            )
            col_name_std = (
                basicobs_itemname_analysed_df_dict.get(col_name)
                .basicobs_value_numeric._get_numeric_data()
                .std()
            )

            col_name_low = col_name_mean - (col_name_std * 3)

            agg_val = int(float(min(filtered_column_values)) < col_name_low)
            df_unique_filtered.at[i, col_name + "_contains-extreme-low"] = agg_val

            # contains extreme high
            col_name_high = col_name_mean + (col_name_std * 3)

            agg_val = int(float(max(filtered_column_values)) > col_name_high)

            df_unique_filtered.at[i, col_name + "_contains-extreme-high"] = agg_val

            # days_between earliest and last

            earliest = filtered_df.sort_values(by="datetime").iloc[-1]["datetime"]

            oldest = filtered_df.sort_values(by="datetime").iloc[-1]["datetime"]

            delta = earliest - oldest

            agg_val = delta.days

            df_unique_filtered.at[i, col_name + "_days-between-first-last"] = agg_val

            # current_pat_bloods = df_unique_filtered

    if config_obj.verbosity >= 6:
        display(df_unique_filtered)

    return df_unique_filtered
