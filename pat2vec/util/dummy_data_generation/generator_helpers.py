"""Shared utilities for dummy data generation.

This module contains helper functions used across multiple data generators.
"""

import calendar
import random
import re
import string
from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple

import numpy as np

# Random state alias for backward compatibility
random_state = 42


def is_safe_host(h: str) -> bool:
    """Checks if a host is local or part of a private network to permit dummy data population.

    Args:
        h: The hostname or IP address to check.

    Returns:
        True if the host is safe for dummy data operations, False otherwise.
    """
    if h in [
        "localhost",
        "127.0.0.1",
        "0.0.0.0",
        "::1",
        "elasticsearch",
        "es01",
        "host.docker.internal",
    ]:
        return True

    # Allow private IP ranges (common for Docker bridge/internal networks)
    return bool(
        re.match(
            r"^(127\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|172\.17\.0\.1|192\.168\.)", h
        )
    )


def maybe_nan(value: Any, probability: float = 0.2) -> Any:
    """Returns a value or NaN based on a probability.

    Args:
        value: The value to potentially return.
        probability: The probability of returning `np.nan` instead of the value.
            Defaults to 0.2.

    Returns:
        The original value or `np.nan`.
    """
    return value if random.random() > probability else np.nan


def create_random_date_from_globals(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    start_day: Optional[int] = None,
    end_day: Optional[int] = None,
) -> datetime:
    """Generates a random datetime within a given month-level range.

    Args:
        start_year: The starting year.
        start_month: The starting month.
        end_year: The ending year.
        end_month: The ending month.
        start_day: The optional starting day (defaults to 1 of start month).
        end_day: The optional ending day (defaults to last day of end month).

    Returns:
        A random datetime object within the specified range.
    """
    # Convert day params to int for robustness (handles string "01" from config)
    start_day_val = int(start_day) if start_day is not None else 1
    end_day_val = (
        int(end_day)
        if end_day is not None
        else calendar.monthrange(end_year, int(end_month))[1]
    )

    start_dt = datetime(start_year, start_month, start_day_val, 0, 0, 0)

    _, num_days_in_end_month = calendar.monthrange(end_year, int(end_month))
    end_day_final = min(end_day_val, num_days_in_end_month)
    end_dt = datetime(end_year, end_month, end_day_final, 23, 59, 59)

    time_difference = end_dt - start_dt
    total_seconds = int(time_difference.total_seconds())

    if total_seconds <= 0:
        return start_dt

    random_second = random.randrange(total_seconds)
    return start_dt + timedelta(seconds=random_second)


def generate_uuid(prefix: str, length: int = 7) -> str:
    """Generates a UUID-like string with a given prefix."""

    chars = string.ascii_uppercase + string.digits
    random_chars = "".join(random.choices(chars, k=length))
    return f"{prefix}{random_chars}"


def generate_uuid_list(n: int, prefix: str, length: int = 7) -> List[str]:
    """Generates a list of n UUID-like strings.

    Args:
        n: The number of UUIDs to generate.
        prefix: The prefix for each UUID.
        length: The length of the random part of each UUID. Defaults to 7.

    Returns:
        A list of generated UUID-like strings.
    """
    return [generate_uuid(prefix, length) for _ in range(n)]


def extract_date_range(
    date_string: str,
) -> Optional[Tuple[int, int, int, int, int, int]]:
    """Extracts a date range from a string.

    The expected format is "YYYY-MM-DD TO YYYY-MM-DD".
    This function is now more robust to handle cases where the search string
    might not contain a date range, returning None in such scenarios.

    Args:
        date_string: The string containing the date range.

    Returns:
        A tuple of six integers (start_year, start_month, start_day,
        end_year, end_month, end_day), or None if the pattern is not found.
    """
    pattern = r"(\d{4})-(\d{2})-(\d{2}) TO (\d{4})-(\d{2})-(\d{2})"
    match = re.search(pattern, date_string)
    if not match:
        return None

    global_start_year = int(match.group(1))
    global_start_month = int(match.group(2))
    _start_day = int(match.group(3))
    global_end_year = int(match.group(4))
    global_end_month = int(match.group(5))
    _end_day = int(match.group(6))

    return (
        global_start_year,
        global_start_month,
        1,  # placeholder start day (ignored)
        global_end_year,
        global_end_month,
        31,  # placeholder end day (ignored)
    )


def extract_search_term_obscatalogmasteritem_displayname(
    search_string: str,
) -> str:
    """Extracts a search term from an 'obscatalogmasteritem_displayname' query.

    This function uses a regular expression to find a term enclosed in
    parentheses following 'obscatalogmasteritem_displayname:'. It cleans the
    term by removing quotes and stripping any trailing 'AND' or 'OR' clauses.

    Args:
        search_string: The input query string.

    Returns:
        The extracted search term, or the original string if no match is found.
    """
    match = re.search(r"obscatalogmasteritem_displayname:\((.*?)\)", search_string)
    if match:
        search_term = match.group(1).replace('"', "").replace("'", "").strip()
        search_term = search_term.split("AND", 1)[0].split("OR", 1)[0].strip()
        return search_term
    else:
        return search_string
