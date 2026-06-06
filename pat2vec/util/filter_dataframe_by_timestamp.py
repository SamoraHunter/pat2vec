import pandas as pd


from datetime import datetime
from typing import Union, Optional


def filter_dataframe_by_timestamp(
    df: pd.DataFrame,
    start_year: Optional[Union[int, str]],
    start_month: Optional[Union[int, str]],
    end_year: Optional[Union[int, str]],
    end_month: Optional[Union[int, str]],
    start_day: Optional[Union[int, str]],
    end_day: Optional[Union[int, str]],
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

    def _parse_to_utc(value):
        if pd.isna(value) or value is None:
            return pd.NaT
        try:
            ts = pd.Timestamp(value)
            if ts.tzinfo is None:
                return ts.tz_localize("UTC")
            return ts.tz_convert("UTC")
        except Exception:
            return pd.NaT

    # Parse each value individually to handle mixed timezones and formats safely,
    # then force the Series to a tz-aware datetime64[ns, UTC] dtype for safe comparison.
    df_copy[timestamp_string] = pd.to_datetime(
        df_copy[timestamp_string].apply(_parse_to_utc), utc=True, errors="coerce"
    )

    # Drop NaN timestamps only if dropna is True
    if dropna:
        df_copy = df_copy.dropna(subset=[timestamp_string])

    # Create start and end datetime objects
    if all(v is not None for v in [start_year, start_month, start_day]):
        start_datetime = pd.Timestamp(
            datetime(int(start_year), int(start_month), int(start_day), 0, 0, 0),
            tz="UTC",
        )
    else:
        # Use a safe minimum date that allows for time component replacement
        start_datetime = pd.Timestamp("1678-01-01", tz="UTC")

    if all(v is not None for v in [end_year, end_month, end_day]):
        end_datetime = pd.Timestamp(
            datetime(int(end_year), int(end_month), int(end_day), 23, 59, 59, 999999),
            tz="UTC",
        )
    else:
        # Use a safe maximum date that allows for time component replacement
        end_datetime = pd.Timestamp("2261-12-31", tz="UTC")

    # Ensure start date is earlier than end date
    if start_datetime.date() > end_datetime.date():
        # Swap the entire dates, ensuring correct time components
        start_temp = end_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        end_temp = start_datetime.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )
        start_datetime, end_datetime = start_temp, end_temp

    # Filter based on datetime range (this will automatically exclude NaN values)
    filtered_df = df_copy[
        (df_copy[timestamp_string] >= start_datetime)
        & (df_copy[timestamp_string] <= end_datetime)
    ]

    return filtered_df
