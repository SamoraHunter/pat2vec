from dateutil.relativedelta import relativedelta


import logging
from datetime import datetime
from typing import Any, List, Tuple
from zoneinfo import ZoneInfo


def generate_date_list(
    start_date: datetime,
    years: int,
    months: int,
    days: int,
    time_window_interval_delta: relativedelta = relativedelta(days=1),
    config_obj: Any = None,
) -> List[Tuple[int, int, int]]:
    """Generates a list of dates within a range, constrained by global boundaries.

    This function calculates a date range based on a start date and a duration
    (defined by years, months, days). It then generates a list of dates within
    this range at a specified interval (`time_window_interval_delta`). The final
    list is clamped to global start and end dates defined in the `config_obj`.

    Args:
        start_date: The anchor date for the calculation.
        years: The number of years to add or subtract.
        months: The number of months to add or subtract.
        days: The number of days to add or subtract.
        time_window_interval_delta: The step interval between dates.
        config_obj: A configuration object with 'lookback' and global date
            attributes (e.g., `global_start_year`).

    Returns:
        A chronologically sorted list of (year, month, day) tuples.
    """
    # Handle missing config_obj gracefully
    if not config_obj:
        raise ValueError("A valid config_obj must be provided.")

    lookback = config_obj.lookback
    time_delta = relativedelta(years=years, months=months, days=days)

    # Determine the chronological start and end of the desired range
    if lookback:
        # For lookback, start_date is the end of the period
        chronological_start = start_date - time_delta
        chronological_end = start_date
    else:
        # For look forward, start_date is the beginning of the period
        chronological_start = start_date
        chronological_end = start_date + time_delta

    # Make all datetimes timezone-aware using the modern 'zoneinfo'
    # This assumes all naive datetimes are in UTC.
    utc_tz = ZoneInfo("UTC")

    def _ensure_utc(dt):
        if dt is None:
            return None
        return dt.replace(tzinfo=utc_tz) if dt.tzinfo is None else dt.astimezone(utc_tz)

    chronological_start = _ensure_utc(chronological_start)
    chronological_end = _ensure_utc(chronological_end)

    global_start_limit = _ensure_utc(getattr(config_obj, "global_start_date", None))
    global_end_limit = _ensure_utc(getattr(config_obj, "global_end_date", None))

    # If end limit is exactly at midnight, it likely represents an inclusive date.
    # Adjust to end of day to ensure boundary overlaps are captured correctly.
    if (
        global_end_limit
        and global_end_limit.hour == 0
        and global_end_limit.minute == 0
        and global_end_limit.second == 0
    ):
        global_end_limit = global_end_limit.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

    # Clamp the calculated range to the global boundaries
    final_start_date = (
        max(chronological_start, global_start_limit)
        if global_start_limit
        else chronological_start
    )
    final_end_date = (
        min(chronological_end, global_end_limit)
        if global_end_limit
        else chronological_end
    )

    # Use logging instead of print
    if getattr(config_obj, "verbosity", 0) >= 1:
        if final_start_date > chronological_start:
            logging.info(
                f"Adjusted start date from {chronological_start.date()} to {final_start_date.date()} due to global limit."
            )
        if final_end_date < chronological_end:
            logging.info(
                f"Adjusted end date from {chronological_end.date()} to {final_end_date.date()} due to global limit."
            )

    # Validate that we have a valid date range
    if final_start_date > final_end_date:
        if getattr(config_obj, "verbosity", 0) >= 1:
            logging.warning(
                f"Invalid date range after clamping: start_date ({final_start_date.date()}) is after end_date ({final_end_date.date()}). Returning empty list."
            )
        return []

    # Validate that the time_window_interval_delta is a positive duration
    if final_start_date + time_window_interval_delta <= final_start_date:
        raise ValueError("The time interval delta must be a positive duration.")

    # Generate dates with proper bounds checking
    date_list = []
    current_date = final_start_date
    max_iterations = 10000  # Safety check to prevent infinite loops
    iteration_count = 0

    while current_date <= final_end_date and iteration_count < max_iterations:
        # Check if the current date would be valid
        if current_date.year > 0:  # Ensure year is positive
            date_list.append((current_date.year, current_date.month, current_date.day))
        else:
            logging.warning(f"Skipping invalid date with year {current_date.year}")

        try:
            # Safely add the interval
            next_date = current_date + time_window_interval_delta
            # Additional safety check for year bounds
            if next_date.year < 1:
                logging.warning(
                    f"Next date would have invalid year {next_date.year}, stopping iteration"
                )
                break
            current_date = next_date
        except (ValueError, OverflowError) as e:
            logging.error(f"Date calculation error: {e}")
            break

        iteration_count += 1

    if iteration_count >= max_iterations:
        logging.warning(
            f"Maximum iterations ({max_iterations}) reached, stopping date generation"
        )

    # Log the results for debugging
    if getattr(config_obj, "verbosity", 0) >= 1:
        logging.info(
            f"Generated {len(date_list)} dates from {final_start_date.date()} to {final_end_date.date()}"
        )
        if date_list:
            logging.info(f"First date: {date_list[0]}, Last date: {date_list[-1]}")

    return date_list
