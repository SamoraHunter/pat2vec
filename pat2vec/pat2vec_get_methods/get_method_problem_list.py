import pandas as pd
from typing import Any, Tuple
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp


def get_current_pat_problem_list(
    current_pat_client_id_code: str,
    target_date_range: Tuple[Any, Any],
    pat_batch: pd.DataFrame,
    config_obj: Any,
) -> pd.DataFrame:
    """Extracts features for the patient's problem list.

    Args:
        current_pat_client_id_code: The patient's ID code.
        target_date_range: A tuple of (start_datetime, end_datetime).
        pat_batch: DataFrame containing a batch of problem list records.
        config_obj: The configuration object.

    Returns:
        pd.DataFrame: A single-row DataFrame containing problem list features.
    """
    if pat_batch is None or pat_batch.empty:
        return pd.DataFrame()

    df = pat_batch[pat_batch["client_idcode"] == current_pat_client_id_code].copy()

    if df.empty:
        return pd.DataFrame()

    # Temporal filtering
    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )
    time_field = getattr(config_obj, "problem_list_time_field", "updatetime")  # type: ignore

    df = filter_dataframe_by_timestamp(
        df,
        start_year,
        start_month,
        end_year,
        end_month,
        start_day,
        end_day,
        timestamp_string=time_field,
    )

    if df.empty:
        return pd.DataFrame()

    # Binary indicators for statuses
    res = pd.get_dummies(df["problem_status"], prefix="problem_status", dtype=float)
    res["client_idcode"] = current_pat_client_id_code

    # Aggregate by patient
    res = res.groupby("client_idcode").sum().reset_index()

    # Add count of total problems
    res["problem_list_count"] = float(len(df))

    return res
