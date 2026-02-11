from datetime import datetime, timezone
from typing import Union

import numpy as np
import pandas as pd
from IPython.display import display
from scipy import stats

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

BLOODS_FIELDS = [
    "client_idcode",
    "basicobs_itemname_analysed",
    "basicobs_value_numeric",
    "basicobs_entered",
    "clientvisit_serviceguid",
    "updatetime",
]


def search_bloods_data(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    client_idcode_name="client_idcode.keyword",
    bloods_time_field="basicobs_entered",
    start_year: Union[int, str] = 1995,
    start_month: Union[int, str] = 1,
    start_day: Union[int, str] = 1,
    end_year: Union[int, str] = 2025,
    end_month: Union[int, str] = 12,
    end_day: Union[int, str] = 12,
    additional_custom_search_string=None,
):
    """Searches for bloods data for patients within a date range.

    Uses a cohort searcher to find basic observation data that has a numeric value,
    within a specified time window.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        client_idcode_name (str): The name of the client ID code field in the
            index. Defaults to "client_idcode.keyword".
        bloods_time_field (str): The timestamp field for filtering bloods data.
            Defaults to 'basicobs_entered'.
        start_year (Union[int, str]): Start year for the search. Defaults to 1995.
        start_month (Union[int, str]): Start month for the search. Defaults to 1.
        start_day (Union[int, str]): Start day for the search. Defaults to 1.
        end_year (Union[int, str]): End year for the search. Defaults to 2025.
        end_month (Union[int, str]): End month for the search. Defaults to 12.
        end_day (Union[int, str]): End day for the search. Defaults to 12.
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the raw bloods data.

    Raises:
        ValueError: If `cohort_searcher_with_terms_and_search` or `client_id_codes`
            is None.
    """
    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")

    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = (
        validate_input_dates(
            start_year, start_month, start_day, end_year, end_month, end_day
        )
    )

    search_string = f"basicobs_value_numeric:* AND {bloods_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    return cohort_searcher_with_terms_and_search(
        index_name="basic_observations",
        fields_list=BLOODS_FIELDS,
        term_name=client_idcode_name,
        entered_list=client_id_codes,
        search_string=search_string,
    )


def get_current_pat_bloods(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    batch_mode=False,
    cohort_searcher_with_terms_and_search=None,
    config_obj=None,
):
    """Retrieves and engineers features from blood test data for a patient.

    This function fetches blood test data for a patient within a specified date
    range, either from a pre-loaded batch or by searching. It then calculates
    a wide range of statistical features for each type of blood test found,
    such as mean, median, standard deviation, counts, and time-based features.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (Tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        batch_mode (bool): Indicates if batch mode is enabled. This is controlled
            by `config_obj.batch_mode`. Defaults to False.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        config_obj (Optional[object]): Configuration object with settings like
            `batch_mode` and `bloods_time_field`. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated blood test features
            for the specified patient.
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
        current_pat_bloods = search_bloods_data(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            client_idcode_name=config_obj.client_idcode_term_name,
            bloods_time_field=bloods_time_field,
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
        )

    # Ensure only target columns are present. Useful if source data isn't directly from ES.
    current_pat_bloods = current_pat_bloods[
        [
            "_index",
            "_id",
            "_score",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_numeric",
            "basicobs_entered",
            "clientvisit_serviceguid",
            "updatetime",
        ]
    ]

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

    obs_columns_list = basicobs_itemname_analysed_list

    obs_columns_set = list(set(obs_columns_list))

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

            # median
            agg_val = filtered_column_values.median()
            df_unique_filtered.at[i, col_name + "_median"] = agg_val

            # mode
            agg_val = np.atleast_1d(stats.mode(filtered_column_values)[0])[0]
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
