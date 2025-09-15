import pandas as pd


from datetime import datetime
from typing import Union


def filter_dataframe_by_timestamp(
    df: pd.DataFrame,
    start_year: Union[int, str],
    start_month: Union[int, str],
    end_year: Union[int, str],
    end_month: Union[int, str],
    start_day: Union[int, str],
    end_day: Union[int, str],
    timestamp_string: str,
    dropna: bool = False,
) -> pd.DataFrame:
    """Filters a DataFrame to include only rows within a specified date range.

    This function takes a DataFrame and filters it based on a timestamp column,
    retaining only the rows where the timestamp falls between a given start and
    end date. It handles conversion of the timestamp column to datetime objects
    and ensures the start date is chronologically before the end date.

    Args:
        df: The DataFrame to filter.
        start_year: The year of the start date.
        start_month: The month of the start date.
        start_day: The day of the start date.
        end_year: The year of the end date.
        end_month: The month of the end date.
        end_day: The day of the end date.
        timestamp_string: The name of the column in `df` that contains
            the timestamps to filter on.
        dropna: If True, drops rows with NaN values in the
            timestamp column before filtering. Defaults to False.

    Returns:
        A new DataFrame containing only the rows that fall
        within the specified date range.
    """
    # Work on a copy to avoid modifying the original DataFrame
    df_copy = df.copy()

    # Convert timestamp column to datetime format
    df_copy[timestamp_string] = pd.to_datetime(
        df_copy[timestamp_string], utc=True, errors="coerce"
    )

    # Drop NaN timestamps only if dropna is True
    if dropna:
        df_copy = df_copy.dropna(subset=[timestamp_string])

    # Create start and end datetime objects
    start_datetime = pd.Timestamp(
        datetime(int(start_year), int(start_month), int(start_day), 0, 0, 0), tz="UTC"
    )
    end_datetime = pd.Timestamp(
        datetime(int(end_year), int(end_month), int(end_day), 23, 59, 59, 999999),
        tz="UTC",
    )

    # Ensure start date is earlier than end date
    if start_datetime.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) > end_datetime.replace(hour=0, minute=0, second=0, microsecond=0):
        # Swap the entire dates, keeping the time components
        start_temp = pd.Timestamp(
            datetime(int(end_year), int(end_month), int(end_day), 0, 0, 0), tz="UTC"
        )
        end_temp = pd.Timestamp(
            datetime(
                int(start_year), int(start_month), int(start_day), 23, 59, 59, 999999
            ),
            tz="UTC",
        )
        start_datetime, end_datetime = start_temp, end_temp

    # Filter based on datetime range (this will automatically exclude NaN values)
    filtered_df = df_copy[
        (df_copy[timestamp_string] >= start_datetime)
        & (df_copy[timestamp_string] <= end_datetime)
    ]

    return filtered_df
