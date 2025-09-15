import pandas as pd


from datetime import datetime, timedelta
from typing import Any, Optional


def nearest(
    date: datetime,
    lookup_dates_and_values: pd.DataFrame,
    date_col: str,
    value_col: str,
    max_time_before: timedelta = timedelta(weeks=6),
    max_time_after: timedelta = timedelta(weeks=50),
) -> Optional[Any]:
    """Finds the nearest date and its corresponding value within a time range.

    This function searches a DataFrame for the row with a date closest to a
    given reference date, within a specified time window.

    Args:
        date: The reference date to find the nearest date around.
        lookup_dates_and_values: DataFrame containing dates and values.
        date_col: The name of the column containing dates in the DataFrame.
        value_col: The name of the column containing values to be returned.
        max_time_before: Maximum time before the reference date to consider.
            Defaults to 6 weeks.
        max_time_after: Maximum time after the reference date to consider.
            Defaults to 50 weeks.

    Returns:
        The value from `value_col` corresponding to the nearest date, or None
        if no date is found within the specified range.
    """

    timebefore = date - max_time_before
    timeafter = date + max_time_after
    filtered_lookup = lookup_dates_and_values[
        (lookup_dates_and_values[date_col] > timebefore)
        & (lookup_dates_and_values[date_col] < timeafter)
    ].copy()

    if filtered_lookup.empty:
        return None

    # Find the index of the row with the minimum absolute time difference
    nearest_index = (filtered_lookup[date_col] - date).abs().idxmin()

    # Return the corresponding value
    return filtered_lookup.loc[nearest_index, value_col]
