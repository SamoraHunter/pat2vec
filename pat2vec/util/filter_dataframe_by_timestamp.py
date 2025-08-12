import pandas as pd


from datetime import datetime


def filter_dataframe_by_timestamp(
    df,
    start_year,
    start_month,
    end_year,
    end_month,
    start_day,
    end_day,
    timestamp_string,
    dropna=False,
):
    """Filters a DataFrame to include only rows within a specified date range.

    This function takes a DataFrame and filters it based on a timestamp column,
    retaining only the rows where the timestamp falls between a given start and
    end date. It handles conversion of the timestamp column to datetime objects
    and ensures the start date is chronologically before the end date.

    Args:
        df (pd.DataFrame): The DataFrame to filter.
        start_year (int): The year of the start date.
        start_month (int): The month of the start date.
        start_day (int): The day of the start date.
        end_year (int): The year of the end date.
        end_month (int): The month of the end date.
        end_day (int): The day of the end date.
        timestamp_string (str): The name of the column in `df` that contains
            the timestamps to filter on.
        dropna (bool, optional): If True, drops rows with NaN values in the
            timestamp column before filtering. Defaults to False.

    Returns:
        pd.DataFrame: A new DataFrame containing only the rows that fall
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
