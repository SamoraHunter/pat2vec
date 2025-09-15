from datetime import datetime
from dateutil.relativedelta import relativedelta


def calculate_interval(
    start_date: datetime, total_delta: relativedelta, interval_delta: relativedelta
) -> int:
    """Calculates how many 'interval_delta' chunks fit inside 'total_delta'.

    This function iteratively adds the `interval_delta` to the `start_date`
    until it exceeds the total duration specified by `total_delta`. It counts
    how many full intervals fit within this period.

    Args:
        start_date: The starting date of the total period.
        total_delta: The total duration from the start date.
        interval_delta: The duration of a single interval chunk.

    Returns:
        The number of complete intervals that fit within the total duration.

    Raises:
        ValueError: If `interval_delta` is not a positive duration.
    """
    # Check for a zero-length interval to prevent an infinite loop.
    if start_date + interval_delta <= start_date:
        raise ValueError("The time interval delta must be a positive duration.")

    end_date = start_date + total_delta
    current_date = start_date
    n_intervals = 0

    # Keep adding intervals as long as the NEXT interval's end point
    # is less than or equal to the total window's end_date.
    while (current_date + interval_delta) <= end_date:
        current_date += interval_delta
        n_intervals += 1

    return n_intervals


# --- Example Usage ---
# start = datetime(2020, 1, 1)
# total_duration = relativedelta(years=2) # A 2-year total window
# interval = relativedelta(months=6)     # We want to see how many 6-month periods fit

# num = calculate_interval(start_date=start, total_delta=total_duration, interval_delta=interval)

# print(f"Number of {interval} intervals in {total_duration}: {num}")
# # Expected output: 4
