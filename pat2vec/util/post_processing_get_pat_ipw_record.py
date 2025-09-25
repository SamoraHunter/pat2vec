import os
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging
import pandas as pd
from IPython.display import display

from pat2vec.util.post_processing import (filter_and_select_rows,
                                          filter_annot_dataframe2)

logger = logging.getLogger(__name__)


def _get_source_record(
    pat_id: str,
    base_path: str,
    time_column: str,
    necessary_columns: List[str],
    annot_filter_arguments: Optional[Dict[str, Any]],
    filter_codes: Optional[List[Any]],
    mode: str,
    verbose: int,
) -> pd.DataFrame:
    """Reads, cleans, and filters records from a single data source file.

    This helper function is designed to process a single patient's annotation
    file from a specific source (e.g., EPR, MCT). It handles file reading,
    data cleaning by dropping rows with missing necessary values, and applying
    various filters.

    Args:
        pat_id: The patient identifier.
        base_path: The base directory path where the patient's CSV file is located.
        time_column: The name of the column containing the timestamp.
        necessary_columns: A list of columns that must have non-NaN values.
        annot_filter_arguments: A dictionary of filters to apply
            to the annotations.
        filter_codes: A list of CUI codes to filter for.
        mode: The mode for `filter_and_select_rows` ('earliest' or 'latest').
        verbose: The verbosity level for logging.

    Returns:
        A DataFrame containing the filtered records from the source.
            Returns an empty DataFrame if the file is not found, is empty, or if
            no rows remain after filtering.
    """
    file_path = f"{base_path}/{pat_id}.csv"
    if not os.path.exists(file_path):
        if verbose >= 10:
            logger.debug(f"File not found for patient {pat_id} at {base_path}")
        return pd.DataFrame()

    if verbose >= 10:
        logger.debug(f"Reading annotations from {base_path}...")

    try:
        df = pd.read_csv(file_path)

        if df.empty:
            if verbose >= 10:
                logger.warning(f"Empty CSV file for patient {pat_id} at {base_path}")
            return pd.DataFrame()

    except Exception as e:
        if verbose >= 10:
            logger.error(
                f"Error reading CSV for patient {pat_id} at {base_path}: {e}")
        return pd.DataFrame()

    if verbose > 12:
        logger.debug(f"Sample annotations from {base_path}...")
        logger.debug(df.head())

    # Check if necessary columns exist before dropping NaNs
    existing_necessary_columns = [
        col for col in necessary_columns if col in df.columns]
    missing_columns = [
        col for col in necessary_columns if col not in df.columns]

    if missing_columns:
        if verbose >= 10:
            logger.warning(
                f"Missing necessary columns in {base_path}: {missing_columns}")
        # Create missing columns with appropriate default values
        for col in missing_columns:
            if col == time_column:
                df[col] = pd.NaT  # Will be filtered out by dropna
            else:
                df[col] = None  # Will be filtered out by dropna

    # Drop rows with NaN in necessary columns
    df = df.dropna(subset=necessary_columns)

    if df.empty:
        if verbose >= 10:
            logger.debug(f"No valid rows after dropping NaNs in {base_path}")
        return pd.DataFrame()

    # Apply annotation filters
    if annot_filter_arguments:
        if verbose >= 10:
            logger.debug(f"Filtering annotations from {base_path}...")
        try:
            df = filter_annot_dataframe2(df, annot_filter_arguments)
            if df.empty:
                if verbose >= 10:
                    logger.debug(f"No rows after annotation filtering in {base_path}")
                return pd.DataFrame()
        except Exception as e:
            if verbose >= 10:
                logger.error(f"Error in annotation filtering for {base_path}: {e}")
            return pd.DataFrame()

    # Apply code filtering
    if not df.empty and filter_codes is not None:
        if verbose >= 10:
            logger.debug(f"Filtering by filter_codes from {base_path}...")
        try:
            filtered_df = filter_and_select_rows(
                df,
                filter_codes,
                verbosity=verbose > 0,
                time_column=time_column,
                filter_column="cui",
                mode=mode,
                n_rows=1,
            )
            return filtered_df
        except Exception as e:
            if verbose >= 10:
                logger.error(f"Error in filter_and_select_rows for {base_path}: {e}")
            return pd.DataFrame()

    return df


def get_pat_ipw_record(
    current_pat_idcode: str,
    config_obj: Optional[Any] = None,
    annot_filter_arguments: Optional[Dict[str, Any]] = None,
    filter_codes: Optional[List[Any]] = None,
    mode: str = "earliest",
    verbose: int = 0,
    include_mct: bool = True,
    include_textual_obs: bool = True,
) -> pd.DataFrame:
    """Retrieves a patient's Individual Patient Window (IPW) record.

    This function finds the most relevant "index" record for a single patient
    by searching across multiple annotation sources (EPR, MCT, textual_obs).
    The index record is determined by `filter_codes` and the `mode` (e.g., the
    'earliest' occurrence of a specific diagnosis CUI).

    If no record is found, a fallback record is created based on the global
    date settings in the configuration.

    Args:
        current_pat_idcode: The unique identifier for the patient.
        config_obj: The configuration object containing
            paths and settings. Defaults to None.
        annot_filter_arguments:
            A dictionary of filters to apply to annotations before selecting the
            IPW record. Defaults to None.
        filter_codes: A list of CUI codes to identify the
            relevant clinical events. Defaults to None.
        mode: Determines whether to find the 'earliest' or 'latest'
            record for the patient. Defaults to "earliest".
        verbose: Verbosity level for logging. Defaults to 0.
        include_mct: If True, includes annotations from MCT
            (MRC clinical notes) in the search. Defaults to True.
        include_textual_obs: If True, includes annotations from
            textual observations. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame containing the single IPW record for the patient.
    """

    # Use config verbosity if available
    if config_obj and hasattr(config_obj, "verbosity") and config_obj.verbosity >= 0:
        verbose = config_obj.verbosity

    if verbose >= 1:
        logger.info(f"Getting IPW record for patient: {current_pat_idcode}")

    # Define common necessary columns
    base_necessary_columns = [
        "client_idcode",
        "pretty_name",
        "cui",
        "type_ids",
        "types",
        "source_value",
        "detected_name",
        "acc",
        "id",
        "Time_Value",
        "Time_Confidence",
        "Presence_Value",
        "Presence_Confidence",
        "Subject_Value",
        "Subject_Confidence",
    ]

    # Get EPR record
    fsr = _get_source_record(
        current_pat_idcode,
        config_obj.pre_document_annotation_batch_path,
        "updatetime",
        ["updatetime"] + base_necessary_columns,
        annot_filter_arguments,
        filter_codes,
        mode,
        verbose,
    )

    # Get MCT record
    fsr_mct = pd.DataFrame()
    if include_mct:
        fsr_mct = _get_source_record(
            current_pat_idcode,
            config_obj.pre_document_annotation_batch_path_mct,
            "observationdocument_recordeddtm",
            ["observationdocument_recordeddtm"] + base_necessary_columns,
            annot_filter_arguments,
            filter_codes,
            mode,
            verbose,
        )

    # Get Textual Obs record
    fsr_textual_obs = pd.DataFrame()
    if include_textual_obs:
        fsr_textual_obs = _get_source_record(
            current_pat_idcode,
            config_obj.pre_textual_obs_annotation_batch_path,
            "basicobs_entered",
            ["basicobs_entered"] + base_necessary_columns,
            annot_filter_arguments,
            filter_codes,
            mode,
            verbose,
        )

    # Prepare dataframes for comparison
    dfs_to_compare = []

    if not fsr.empty:
        fsr = fsr.copy()
        fsr["source"] = "EPR"
        dfs_to_compare.append(fsr)

    if include_mct and not fsr_mct.empty:
        fsr_mct = fsr_mct.copy()
        fsr_mct["source"] = "MCT"
        dfs_to_compare.append(fsr_mct)

    if include_textual_obs and not fsr_textual_obs.empty:
        fsr_textual_obs = fsr_textual_obs.copy()
        fsr_textual_obs["source"] = "textual_obs"
        dfs_to_compare.append(fsr_textual_obs)

    # Standardize timestamp column names
    for df in dfs_to_compare:
        if "observationdocument_recordeddtm" in df.columns:
            df.rename(
                columns={"observationdocument_recordeddtm": "updatetime"}, inplace=True
            )
        elif "basicobs_entered" in df.columns:
            # Drop existing updatetime column if it exists
            if "updatetime" in df.columns:
                df.drop(columns=["updatetime"], inplace=True)
            df.rename(columns={"basicobs_entered": "updatetime"}, inplace=True)
        elif "updatetime" not in df.columns:
            if verbose > 10:
                logger.warning(
                    f"No timestamp column found in DataFrame with source {df.iloc[0].get('source', 'unknown')}"
                )
            continue

    # Convert 'updatetime' to datetime objects before comparison
    for df in dfs_to_compare:
        if "updatetime" in df.columns:
            # Use errors='coerce' to turn unparseable dates into NaT (Not a Time)
            df["updatetime"] = pd.to_datetime(
                df["updatetime"], errors="coerce", utc=True
            )

    # Filter out dataframes that don't have valid updatetime
    valid_dfs = []
    for df in dfs_to_compare:
        if "updatetime" in df.columns and not df["updatetime"].isna().all():
            valid_dfs.append(df)
        elif verbose > 10:
            source = df.iloc[0].get(
                "source", "unknown") if not df.empty else "unknown"
            logger.debug(
                f"Excluding DataFrame from {source} due to invalid updatetime.")

    # Find the earliest record among valid DataFrames
    if valid_dfs:
        try:
            earliest_df = min(
                valid_dfs, key=lambda df: df.iloc[0]["updatetime"])
            if verbose >= 10:
                source = earliest_df.iloc[0].get("source", "unknown")
                timestamp = earliest_df.iloc[0]["updatetime"]
                logger.debug(
                    f"Selected earliest record from {source} with timestamp {timestamp}."
                )
        except Exception as e:
            if verbose > 10:
                logger.error(f"Error finding earliest record: {e}")
            earliest_df = pd.DataFrame()
    else:
        earliest_df = pd.DataFrame()

    # Handle case where no valid records found
    if earliest_df.empty or len(earliest_df) == 0:
        if verbose >= 1:
            logger.info("No valid annotations available from EPR, MCT, or textual_obs. Creating fallback.")

        # Create a fallback record
        earliest_df = pd.DataFrame()
        earliest_df["client_idcode"] = [current_pat_idcode]

        # Set other required columns with appropriate defaults
        for col in base_necessary_columns:
            if col not in ["client_idcode"]:
                earliest_df[col] = [None]  # or appropriate default values

        # Set timestamp based on config
        start_datetime = datetime(
            int(config_obj.global_start_year),
            int(config_obj.global_start_month),
            int(config_obj.global_start_day),
        )
        end_datetime = datetime(
            int(config_obj.global_end_year),
            int(config_obj.global_end_month),
            int(config_obj.global_end_day),
        )

        if not config_obj.lookback:
            earliest_df["updatetime"] = [
                pd.to_datetime(start_datetime, utc=True)]
        else:
            earliest_df["updatetime"] = [
                pd.to_datetime(end_datetime, utc=True)]

        earliest_df["source"] = ["fallback"]

    # Ensure we return a copy to avoid SettingWithCopyWarning
    result_df = earliest_df.copy()

    # Final validation - remove any rows that are completely NaN
    if not result_df.empty:
        # Check if any row has all NaN values (except client_idcode and updatetime)
        check_columns = [
            col
            for col in result_df.columns
            if col not in ["client_idcode", "updatetime", "source"]
        ]
        if check_columns:
            all_nan_mask = result_df[check_columns].isna().all(axis=1)
            if all_nan_mask.any() and verbose > 10:
                logger.warning(
                    f"Found {all_nan_mask.sum()} rows with all NaN values.")

        # Ensure client_idcode is not NaN
        result_df.loc[result_df["client_idcode"].isna(), "client_idcode"] = (
            current_pat_idcode
        )

    if verbose >= 5:
        logger.debug(f"Final DataFrame columns: {list(result_df.columns)}")
        logger.debug(f"Final DataFrame shape: {result_df.shape}")
        if not result_df.empty:
            logger.debug("Sample of final DataFrame:\n%s", result_df.iloc[0])

    return result_df
