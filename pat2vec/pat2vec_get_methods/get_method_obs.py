import pandas as pd
from typing import Any, Tuple
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.post_processing_dataframe import aggregate_dataframe_mean


def get_current_pat_obs(
    current_pat_client_id_code: str,
    target_date_range: Tuple[Any, Any],
    pat_batch: pd.DataFrame,
    search_term: str,
    config_obj: Any,
) -> pd.DataFrame:
    """Extracts features for generic observations based on a search term.

    Args:
        current_pat_client_id_code: The patient's ID code.
        target_date_range: A tuple of (start_datetime, end_datetime).
        pat_batch: DataFrame containing a batch of observations.
        search_term: The specific observation type to filter for.
        config_obj: The configuration object.

    Returns:
        pd.DataFrame: A single-row DataFrame containing observation features.
    """
    if pat_batch is None or pat_batch.empty:
        return pd.DataFrame()

    # Filter for specific patient and search term
    df = pat_batch[
        (pat_batch["client_idcode"] == current_pat_client_id_code)
        & (pat_batch["obscatalogmasteritem_displayname"] == search_term)
    ].copy()

    if df.empty:
        return pd.DataFrame()

    # Temporal filtering
    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )
    time_field = getattr(config_obj, "obs_time_field", "updatetime")

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

    # Extract numeric values
    df["observation_valuetext_analysed"] = pd.to_numeric(
        df["observation_valuetext_analysed"], errors="coerce"
    )

    # Group by patient and aggregate
    res = aggregate_dataframe_mean(df)

    # Rename columns to be specific to the search term
    safe_term = "".join(e for e in search_term if e.isalnum() or e == "_").lower()
    res.columns = [
        f"obs_{safe_term}_{c}" if c != "client_idcode" else c for c in res.columns
    ]

    return res
