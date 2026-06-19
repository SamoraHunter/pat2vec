import datetime
from typing import Any, Tuple, Union, List
import pandas as pd


def get_start_end_year_month(
    target_date_range: Union[
        Tuple[int, int, int], Tuple[Any, Any], Tuple[Any], List[Any]
    ],
    config_obj: Any = None,
) -> Tuple[int, int, int, int, int, int]:
    """Calculates start and end date components based on a time interval.

    This function extracts date components from various input formats for target_date_range.
    It handles component tuples (year, month, day), explicit date object ranges (start, end),
    and single date objects where the end is calculated using the config's interval.

    Args:
        target_date_range: The date range input. Supported formats:
            - Tuple[int, int, int]: (year, month, day)
            - Tuple[date/datetime/Timestamp, date/datetime/Timestamp]: (start, end)
            - Tuple[date/datetime/Timestamp]: (start,)
            - Lists of the above.
        config_obj: A configuration object that must contain
            the `time_window_interval_delta` attribute. This delta is added
            to the start date to calculate the end date. Defaults to None.

    Returns:
        A tuple of six integers in the order:
        (start_year, start_month, end_year, end_month, start_day, end_day).

        Note: This interleaved order (Year/Month grouping) is intentional to match
         the positional arguments expected by `filter_dataframe_by_timestamp`.

    Raises:
        ValueError: If `config_obj` is not provided.
        TypeError: If target_date_range is not a tuple or list.
        AttributeError: If `config_obj` does not have `time_window_interval_delta`.
    """

    if config_obj is None:
        raise ValueError("config_obj cannot be None")

    if not isinstance(target_date_range, (tuple, list)):
        raise TypeError(
            f"target_date_range must be a tuple or list, got {type(target_date_range)}"
        )

    time_window_interval_delta = config_obj.time_window_interval_delta

    # 1. Handle object-based target_date_range (date, datetime, or Timestamp objects)
    if len(target_date_range) > 0 and isinstance(
        target_date_range[0], (datetime.datetime, datetime.date, pd.Timestamp)
    ):
        start_date = target_date_range[0]
        if len(target_date_range) >= 2 and isinstance(
            target_date_range[1], (datetime.datetime, datetime.date, pd.Timestamp)
        ):
            # Case: explicit (start_dt, end_dt) range
            end_date = target_date_range[1]
        else:
            # Case: starting point (start_dt,), calculate end based on interval
            end_date = start_date + time_window_interval_delta

    # 2. Handle component-based target_date_range (year, month, day)
    elif len(target_date_range) >= 3:
        try:
            start_year, start_month, start_day = target_date_range[0:3]

            # When IPW+lookback is enabled, use the overall patient window instead of sliding windows
            if getattr(config_obj, "individual_patient_window", False) and getattr(
                config_obj, "lookback", False
            ):
                start_date = datetime.date(
                    int(config_obj.global_start_year),
                    int(config_obj.global_start_month),
                    int(config_obj.global_start_day),
                )
                end_date = datetime.date(
                    int(config_obj.global_end_year),
                    int(config_obj.global_end_month),
                    int(config_obj.global_end_day),
                )
            else:
                start_date = datetime.date(
                    int(start_year), int(start_month), int(start_day)
                )
                end_date = start_date + time_window_interval_delta
        except (ValueError, TypeError, IndexError) as e:
            raise ValueError(f"Invalid date components in {target_date_range}: {e}")

    else:
        raise ValueError(
            f"target_date_range must have at least 3 components (Y, M, D) or contain date objects. "
            f"Got length {len(target_date_range)}: {target_date_range}"
        )

    if isinstance(start_date, (datetime.datetime, pd.Timestamp)):
        start_date = start_date.date()
    if isinstance(end_date, (datetime.datetime, pd.Timestamp)):
        end_date = end_date.date()

    return (
        start_date.year,
        start_date.month,
        end_date.year,
        end_date.month,
        start_date.day,
        end_date.day,
    )
