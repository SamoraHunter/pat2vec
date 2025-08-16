import pandas as pd


from datetime import datetime, timedelta


def nearest(
    date: datetime,
    lookup_dates_and_values: pd.DataFrame,
    date_col: str,
    value_col: str,
    max_time_before: timedelta = timedelta(weeks=6),
    max_time_after: timedelta = timedelta(weeks=50),
):
    """
    Finds the nearest date and its corresponding value within a specified time range.

    Parameters:
    - date (datetime): The reference date to find the nearest date around.
    - lookup_dates_and_values (pd.DataFrame): DataFrame containing dates and their corresponding values.
    - date_col (str): The column name for dates in the DataFrame.
    - value_col (str): The column name for values in the DataFrame.
    - max_time_before (timedelta, optional): Maximum time before the reference date to consider. Defaults to 6 weeks.
    - max_time_after (timedelta, optional): Maximum time after the reference date to consider. Defaults to 50 weeks.

    Returns:
    - The value corresponding to the nearest date within the specified range, or None if no date is found.
    """

    timebefore = date - max_time_before
    timeafter = date + max_time_after
    lookup_dates_and_values = lookup_dates_and_values[
        (lookup_dates_and_values[date_col] > timebefore)
        & (lookup_dates_and_values[date_col] < timeafter)
    ]
    if lookup_dates_and_values.shape[0] == 0:
        return None
    min_date = min(
        lookup_dates_and_values.iterrows(), key=lambda x: abs(x[1][date_col] - date)
    )
    return min_date[1][value_col]
