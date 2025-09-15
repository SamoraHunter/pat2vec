from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import \
    filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.methods_get import convert_date
from pat2vec.util.parse_date import validate_input_dates

DIAGNOSTICS_FIELDS = [
    "client_idcode",
    "order_guid",
    "order_name",
    "order_summaryline",
    "order_holdreasontext",
    "order_entered",
    "clientvisit_visitidcode",
    "order_performeddtm",
    "order_createdwhen",
]

COLUMNS_TO_DROP = [
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
    "order_createdwhen",
    "datetime",
    "index",
]


def search_diagnostic_orders(
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
    client_id_codes: Optional[Union[str, List[str]]] = None,
    diagnostic_time_field: str = 'order_createdwhen',
    start_year: str = '1995',
    start_month: str = '01',
    start_day: str = '01',
    end_year: str = '2025',
    end_month: str = '12',
    end_day: str = '12',
    additional_custom_search_string: Optional[str] = None,
) -> pd.DataFrame:
    """Searches for diagnostic order data for patients within a date range.

    Uses a cohort searcher to find diagnostic orders.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        diagnostic_time_field (str): The timestamp field for filtering diagnostic
            orders. Defaults to 'order_createdwhen'.
        start_year (str): Start year for the search. Defaults to '1995'.
        start_month (str): Start month for the search. Defaults to '01'.
        start_day (str): Start day for the search. Defaults to '01'.
        end_year (str): End year for the search. Defaults to '2025'.
        end_month (str): End month for the search. Defaults to '12'.
        end_day (str): End day for the search. Defaults to '12'.
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the raw diagnostic order data.

    Raises:
        ValueError: If essential arguments are None.
    """
    if cohort_searcher_with_terms_and_search is None:
        raise ValueError(
            "cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")
    if diagnostic_time_field is None:
        raise ValueError("diagnostic_time_field cannot be None.")
    if any(
        x is None
        for x in [start_year, start_month, start_day, end_year, end_month, end_day]
    ):
        raise ValueError("Date components cannot be None.")

    # Ensure client_id_codes is a list for the search function
    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = validate_input_dates(
        start_year, start_month, start_day, end_year, end_month, end_day
    )

    # Base search string for diagnostic orders
    search_string = (
        'order_typecode:"diagnostic" AND '
        + f"{diagnostic_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
    )

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    return cohort_searcher_with_terms_and_search(
        index_name="order",
        fields_list=DIAGNOSTICS_FIELDS,
        # Note: using default, can be made configurable
        term_name="client_idcode.keyword",
        entered_list=client_id_codes,
        search_string=search_string,
    )


def prepare_diagnostic_datetime(
    diagnostics_data: pd.DataFrame,
    diagnostic_time_field: str,
    batch_mode: bool = False
) -> pd.DataFrame:
    """Prepares the datetime column for diagnostic data processing.

    Creates a 'datetime' column by either copying the specified time field
    (in batch mode) or converting it to datetime objects.

    Args:
        diagnostics_data (pd.DataFrame): Raw diagnostic data.
        diagnostic_time_field (str): The name of the time field to process.
        batch_mode (bool): Whether the function is running in batch mode.
            Defaults to False.

    Returns:
        pd.DataFrame: The input DataFrame with an added 'datetime' column.
    """
    data = diagnostics_data.copy()

    if batch_mode:
        data["datetime"] = data[diagnostic_time_field].copy()
    else:
        data["datetime"] = (
            pd.Series(data[diagnostic_time_field])
            .dropna()
            .apply(convert_date)
        )

    return data


def calculate_diagnostic_features(
    order_name_df_dict: Dict[str, pd.DataFrame],
    order_name_list: List[str],
    batch_mode: bool = False
) -> Dict:
    """Calculates diagnostic features for each order type.

    Computes features like the number of orders, days since the last order,
    and the time span between the first and last order for each diagnostic
    test type.

    Args:
        order_name_df_dict (Dict[str, pd.DataFrame]): A dictionary mapping order
            names to their corresponding DataFrames.
        order_name_list (List[str]): A list of unique order names to process.
        batch_mode (bool): Whether the function is running in batch mode.
            Defaults to False.

    Returns:
        Dict: A dictionary of calculated features.
    """
    if batch_mode:
        today = datetime.now(timezone.utc)
    else:
        today = datetime.today()

    features = {}

    for col_name in order_name_list:
        filtered_df = order_name_df_dict.get(col_name)

        if filtered_df is None or len(filtered_df) == 0:
            continue

        df_len = len(filtered_df)

        if df_len >= 1:
            # Number of tests
            features[f"{col_name}_num-diagnostic-order"] = df_len

            # Days since last test
            try:
                sorted_df = filtered_df.sort_values(by="datetime")
                date_object = sorted_df.iloc[-1]["datetime"]
                delta = today - date_object
                features[f"{col_name}_days-since-last-diagnostic-order"] = delta.days
            except Exception as e:
                print(
                    f"Error calculating days since last diagnostic for {col_name}: {e}")
                features[f"{col_name}_days-since-last-diagnostic-order"] = None

        if df_len >= 2:
            # Days between earliest and latest (fixed logic from original)
            try:
                sorted_df = filtered_df.sort_values(by="datetime")
                # First record (earliest)
                earliest = sorted_df.iloc[0]["datetime"]
                # Last record (latest)
                latest = sorted_df.iloc[-1]["datetime"]
                delta = latest - earliest
                features[f"{col_name}_days-between-first-last-diagnostic"] = delta.days
            except Exception as e:
                print(
                    f"Error calculating days between first-last diagnostic for {col_name}: {e}")
                features[f"{col_name}_days-between-first-last-diagnostic"] = None

    return features


def create_diagnostic_features_dataframe(
    current_pat_client_id_code: str,
    diagnostic_features: Dict,
    original_data: pd.DataFrame
) -> pd.DataFrame:
    """Creates the final diagnostic features DataFrame.

    Combines the patient's ID with the calculated diagnostic features into a
    single-row DataFrame.

    Args:
        current_pat_client_id_code (str): The patient's client ID.
        diagnostic_features (Dict): The dictionary of calculated features.
        original_data (pd.DataFrame): The original diagnostic data, used for reference.

    Returns:
        pd.DataFrame: A single-row DataFrame containing the final features.
    """
    # Start with basic patient info
    base_df = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    # Add any additional base columns from original data (excluding ones we'll drop)
    if len(original_data) > 0:
        sample_row = original_data.iloc[0:1].copy()

        # Keep only columns that aren't in our drop list
        columns_to_keep = [col for col in sample_row.columns
                           if col not in COLUMNS_TO_DROP and col not in diagnostic_features.keys()]

        for col in columns_to_keep:
            if col != "client_idcode":  # Don't duplicate client_idcode
                base_df[col] = sample_row[col].iloc[0]

    # Add calculated features
    for feature_name, feature_value in diagnostic_features.items():
        base_df[feature_name] = feature_value

    return base_df


def get_current_pat_diagnostics(
    current_pat_client_id_code: str,
    target_date_range: Tuple,
    pat_batch: pd.DataFrame,
    config_obj: Optional[object] = None,
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
) -> pd.DataFrame:
    """Retrieves diagnostic test features for a patient within a date range.

    This function fetches diagnostic order data, either from a pre-loaded batch
    or by searching. It then calculates time-based features for each type of
    diagnostic order found.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (Tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object containing settings like
            `batch_mode` and `diagnostic_time_field`. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing diagnostic test features for the
            specified patient.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration."
        )

    batch_mode = config_obj.batch_mode
    diagnostic_time_field = config_obj.diagnostic_time_field

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    # Get diagnostic data
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
        diagnostics = search_diagnostic_orders(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            diagnostic_time_field=diagnostic_time_field,
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
        )

    # Return basic DataFrame if no diagnostic data found
    if len(diagnostics) == 0:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    # Prepare datetime column
    current_pat_diagnostics = prepare_diagnostic_datetime(
        diagnostics, diagnostic_time_field, batch_mode
    )

    # Group data by order name
    order_name_list = list(current_pat_diagnostics["order_name"].unique())
    order_name_df_dict = {
        elem: current_pat_diagnostics[current_pat_diagnostics.order_name == elem]
        for elem in order_name_list
    }

    # Calculate diagnostic features
    diagnostic_features = calculate_diagnostic_features(
        order_name_df_dict, order_name_list, batch_mode
    )

    # Create final features DataFrame
    result_df = create_diagnostic_features_dataframe(
        current_pat_client_id_code, diagnostic_features, current_pat_diagnostics
    )

    if config_obj.verbosity >= 6:
        display(result_df)

    return result_df
