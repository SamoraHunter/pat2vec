import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Union
from pat2vec.util.post_processing import filter_and_select_rows, filter_annot_dataframe2
import os
from IPython.display import display


def _get_source_record(
    pat_id: str,
    base_path: str,
    time_column: str,
    necessary_columns: List[str],
    annot_filter_arguments: Optional[Dict],
    filter_codes: Optional[List[int]],
    mode: str,
    verbose: int,
) -> pd.DataFrame:
    """Helper to read, clean, and filter records from a single data source."""
    file_path = f"{base_path}/{pat_id}.csv"
    if not os.path.exists(file_path):
        if verbose > 10:
            print(f"File not found for patient {pat_id} at {base_path}")
        return pd.DataFrame()

    if verbose > 10:
        print(f"Reading annotations from {base_path}...")

    try:
        df = pd.read_csv(file_path)

        if df.empty:
            if verbose > 10:
                print(f"Empty CSV file for patient {pat_id} at {base_path}")
            return pd.DataFrame()

    except Exception as e:
        if verbose > 10:
            print(f"Error reading CSV for patient {pat_id} at {base_path}: {e}")
        return pd.DataFrame()

    if verbose > 12:
        print(f"Sample annotations from {base_path}...")
        display(df.head())

    # Check if necessary columns exist before dropping NaNs
    existing_necessary_columns = [col for col in necessary_columns if col in df.columns]
    missing_columns = [col for col in necessary_columns if col not in df.columns]

    if missing_columns:
        if verbose > 10:
            print(f"Missing necessary columns in {base_path}: {missing_columns}")
        # Create missing columns with appropriate default values
        for col in missing_columns:
            if col == time_column:
                df[col] = pd.NaT  # Will be filtered out by dropna
            else:
                df[col] = None  # Will be filtered out by dropna

    # Drop rows with NaN in necessary columns
    df = df.dropna(subset=necessary_columns)

    if df.empty:
        if verbose > 10:
            print(f"No valid rows after dropping NaNs in {base_path}")
        return pd.DataFrame()

    # Apply annotation filters
    if annot_filter_arguments:
        if verbose > 10:
            print(f"Filtering annotations from {base_path}...")
        try:
            df = filter_annot_dataframe2(df, annot_filter_arguments)
            if df.empty:
                if verbose > 10:
                    print(f"No rows after annotation filtering in {base_path}")
                return pd.DataFrame()
        except Exception as e:
            if verbose > 10:
                print(f"Error in annotation filtering for {base_path}: {e}")
            return pd.DataFrame()

    # Apply code filtering
    if not df.empty and filter_codes is not None:
        if verbose > 10:
            print(f"Filtering by filter_codes from {base_path}...")
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
            if verbose > 10:
                print(f"Error in filter_and_select_rows for {base_path}: {e}")
            return pd.DataFrame()

    return df


def get_pat_ipw_record(
    current_pat_idcode: str,
    config_obj=None,
    annot_filter_arguments: Optional[Dict[str, Union[int, str, List[str]]]] = None,
    filter_codes: Optional[List[int]] = None,
    mode: str = "earliest",
    verbose: int = 0,
    include_mct: bool = True,
    include_textual_obs: bool = True,
) -> pd.DataFrame:
    """
    Retrieve patient IPW record.

    This function retrieves the earliest relevant records, based on the filter_codes,
    from the EPR, MCT, and textual_obs annotation dataframes.
    """

    # Use config verbosity if available
    if config_obj and hasattr(config_obj, "verbosity") and config_obj.verbosity >= 0:
        verbose = config_obj.verbosity

    if verbose >= 10:
        print(f"Getting IPW record for patient: {current_pat_idcode}")

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
                display(
                    f"Warning: No timestamp column found in DataFrame with source {df.iloc[0].get('source', 'unknown')}"
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
            source = df.iloc[0].get("source", "unknown") if not df.empty else "unknown"
            print(f"Excluding DataFrame from {source} due to invalid updatetime")

    # Find the earliest record among valid DataFrames
    if valid_dfs:
        try:
            earliest_df = min(valid_dfs, key=lambda df: df.iloc[0]["updatetime"])
            if verbose >= 10:
                source = earliest_df.iloc[0].get("source", "unknown")
                timestamp = earliest_df.iloc[0]["updatetime"]
                print(
                    f"Selected earliest record from {source} with timestamp {timestamp}"
                )
        except Exception as e:
            if verbose > 10:
                print(f"Error finding earliest record: {e}")
            earliest_df = pd.DataFrame()
    else:
        earliest_df = pd.DataFrame()

    # Handle case where no valid records found
    if earliest_df.empty or len(earliest_df) == 0:
        if verbose > 10:
            print("No valid annotations available from EPR, MCT, or textual_obs...")

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
            earliest_df["updatetime"] = [pd.to_datetime(start_datetime, utc=True)]
        else:
            earliest_df["updatetime"] = [pd.to_datetime(end_datetime, utc=True)]

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
                print(f"Warning: Found {all_nan_mask.sum()} rows with all NaN values")

        # Ensure client_idcode is not NaN
        result_df.loc[result_df["client_idcode"].isna(), "client_idcode"] = (
            current_pat_idcode
        )

    if verbose >= 10:
        print(f"Final DataFrame columns: {list(result_df.columns)}")
        print(f"Final DataFrame shape: {result_df.shape}")
        if not result_df.empty:
            print("Sample of final DataFrame:")
            display(result_df.iloc[0])

    return result_df
