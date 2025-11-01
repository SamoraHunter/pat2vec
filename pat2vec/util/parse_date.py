import datetime
from typing import Union


def validate_input_dates(
    start_year: Union[int, str],
    start_month: Union[int, str],
    start_day: Union[int, str],
    end_year: Union[int, str],
    end_month: Union[int, str],
    end_day: Union[int, str],
) -> tuple[str, str, str, str, str, str]:
    """
    Validates start and end date components, accepting ints or strings.

    This function converts all inputs to integers, checks if they form valid
    calendar dates, and formats the month/day with a leading zero if needed.

    Args:
        start_year (int or str): The year of the start date.
        start_month (int or str): The month of the start date.
        start_day (int or str): The day of the start date.
        end_year (int or str): The year of the end date.
        end_month (int or str): The month of the end date.
        end_day (int or str): The day of the end date.

    Returns:
        A tuple containing the formatted string values in the same order:
        (start_year, start_month, start_day, end_year, end_month, end_day).

    Raises:
        ValueError: If any input cannot be converted to an integer or if
                    the date is invalid (e.g., month=13).
    """
    try:
        # Step 1: Coerce all start date inputs to integers.
        # This will raise a ValueError if the input is not a valid number.
        s_year = int(start_year)
        s_month = int(start_month)
        s_day = int(start_day)

        # Step 2: Validate by creating a datetime object.
        start_date_obj = datetime.date(s_year, s_month, s_day)
    except (ValueError, TypeError) as e:
        # Catches errors from both int() conversion and datetime.date()
        raise ValueError(f"Invalid start date component: {e}") from e

    try:
        # Step 1: Coerce all end date inputs to integers.
        e_year = int(end_year)
        e_month = int(end_month)
        e_day = int(end_day)

        # Step 2: Validate by creating a datetime object.
        end_date_obj = datetime.date(e_year, e_month, e_day)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid end date component: {e}") from e

    # Step 3: Format components into strings. strftime handles the zfill.
    s_year_str = start_date_obj.strftime("%Y")
    s_month_str = start_date_obj.strftime("%m")
    s_day_str = start_date_obj.strftime("%d")

    e_year_str = end_date_obj.strftime("%Y")
    e_month_str = end_date_obj.strftime("%m")
    e_day_str = end_date_obj.strftime("%d")

    return (
        s_year_str,
        s_month_str,
        s_day_str,
        e_year_str,
        e_month_str,
        e_day_str,
    )
