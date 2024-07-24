from datetime import datetime, timezone

import numpy as np
import pandas as pd
from IPython.display import display
from scipy import stats

from pat2vec.util.methods_get import (
    convert_date,
    filter_dataframe_by_timestamp,
    get_start_end_year_month,
)

from . import convert_date


def get_current_pat_diagnostics(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieves diagnostic test data for a given patient within a specified date range.

    Parameters:
    - current_pat_client_id_code (str): The client ID code of the patient.
    - target_date_range (tuple): A tuple representing the target date range.
    - pat_batch (pd.DataFrame): The DataFrame containing patient data.
    - batch_mode (bool, optional): Indicates whether batch mode is enabled. Defaults to False.
    - cohort_searcher_with_terms_and_search (callable, optional): The function for cohort searching. Defaults to None.

    Returns:
    - pd.DataFrame: A DataFrame containing diagnostic test data for the specified patient.
    """

    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    diagnostic_time_field = config_obj.diagnostic_time_field

    # Diagnostic tests
    if batch_mode:
        diagnostics = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            diagnostic_time_field,
        )
    else:
        diagnostics = cohort_searcher_with_terms_and_search(
            index_name="order",
            fields_list=[
                "client_idcode",
                "order_guid",
                "order_name",
                "order_summaryline",
                "order_holdreasontext",
                "order_entered",
                "clientvisit_visitidcode",
                "order_performeddtm",
            ],
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string='order_typecode:"diagnostic" AND '
            + f"{diagnostic_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
        )

    current_pat_diagnostics = diagnostics.copy()

    if batch_mode:
        current_pat_diagnostics["datetime"] = current_pat_diagnostics[
            diagnostic_time_field
        ].copy()

    else:
        current_pat_diagnostics["datetime"] = (
            pd.Series(current_pat_diagnostics[diagnostic_time_field])
            .dropna()
            .apply(convert_date)
        )

    order_name_list = list(current_pat_diagnostics["order_name"].unique())

    order_name_df_dict = {
        elem: current_pat_diagnostics[current_pat_diagnostics.order_name == elem]
        for elem in order_name_list
    }

    df_unique = current_pat_diagnostics.copy()

    df_unique.drop_duplicates(subset="client_idcode", inplace=True)

    df_unique.reset_index(inplace=True)

    filtered_list = []

    obs_columns_list = order_name_list

    obs_columns_set = list(set(obs_columns_list))

    filtered_column_list = filtered_list

    # print("Building obs_columns_set_columns_for_df")

    obs_columns_set_columns_for_df = []
    for i in range(0, len(obs_columns_set)):
        obs_columns_set_columns_for_df.append(
            obs_columns_set[i] + "_num-diagnostic-order"
        )
        obs_columns_set_columns_for_df.append(
            obs_columns_set[i] + "_days-since-last-diagnostic-order"
        )
        obs_columns_set_columns_for_df.append(
            obs_columns_set[i] + "_days-between-first-last-diagnostic"
        )

    orig_columns = list(df_unique.columns)

    comb_cols = orig_columns + obs_columns_set_columns_for_df

    df_unique = df_unique.reindex(comb_cols, axis=1)

    df_unique = df_unique.copy()
    df_unique.drop(
        [
            "_index",
            "_id",
            "_score",
            "order_guid",
            "order_name",
            "order_summaryline",
            "order_holdreasontext",
            "order_entered",
            "clientvisit_visitidcode",
            "order_performeddtm",
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

    for j in range(0, len(obs_columns_list)):
        col_name = obs_columns_list[j]

        filtered_df = order_name_df_dict.get(col_name)

        df_len = len(filtered_df)
        if df_len >= 1:
            # n tests

            agg_val = len(filtered_df)

            df_unique_filtered.at[i, col_name + "_num-diagnostic-order"] = agg_val

            # days-since-last-test
            date_object = filtered_df.sort_values(by="datetime").iloc[-1]["datetime"]

            delta = today - date_object

            agg_val = delta.days

            df_unique_filtered.at[i, col_name + "_days-since-last-diagnostic-order"] = (
                agg_val
            )

        if df_len >= 2:

            # days_between earliest and last

            earliest = filtered_df.sort_values(by="datetime").iloc[-1]["datetime"]

            oldest = filtered_df.sort_values(by="datetime").iloc[-1]["datetime"]

            delta = earliest - oldest

            agg_val = delta.days

            df_unique_filtered.at[
                i, col_name + "_days-between-first-last-diagnostic"
            ] = agg_val

    try:
        df_unique_filtered.drop("datetime", axis=1, inplace=True)

    except Exception as e:
        print(e)
        pass

    try:
        df_unique_filtered.drop("index", axis=1, inplace=True)

    except Exception as e:
        print(e)
        pass

    if config_obj.verbosity >= 6:
        display(df_unique_filtered)

    return df_unique_filtered
