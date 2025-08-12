import datetime
from typing import Tuple


def get_start_end_year_month(
    target_date_range: Tuple[int, int, int], config_obj=None
) -> Tuple[int, int, int, int, int, int]:
    """Calculates start and end date components based on a time interval.

    This function takes a starting date and adds a time interval defined in a
    configuration object to determine the end date. It then returns the year,
    month, and day for both the start and end dates.

    Args:
        target_date_range (tuple): A tuple of (year, month, day) representing
            the start date.
        config_obj (object, optional): A configuration object that must contain
            the `time_window_interval_delta` attribute. This delta is added
            to the start date to calculate the end date. Defaults to None.

    Returns:
        tuple: A tuple of six integers: (start_year, start_month, end_year,
            end_month, start_day, end_day).

    Raises:
        ValueError: If `config_obj` is not provided.
        AttributeError: If `config_obj` does not have `time_window_interval_delta`.
    """

    if config_obj is None:
        raise ValueError("config_obj cannot be None")

    time_window_interval_delta = config_obj.time_window_interval_delta

    start_year, start_month, start_day = (
        target_date_range[0],
        target_date_range[1],
        target_date_range[2],
    )

    start_date = datetime.date(start_year, start_month, start_day)
    end_date = start_date + time_window_interval_delta

    return (
        start_date.year,
        start_date.month,
        end_date.year,
        end_date.month,
        start_date.day,
        end_date.day,
    )
