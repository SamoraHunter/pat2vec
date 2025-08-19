from IPython.display import display
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Union
from pat2vec.util.post_processing import filter_and_select_rows, filter_annot_dataframe2
import os


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

    df = pd.read_csv(file_path)

    if verbose > 12:
        print(f"Sample annotations from {base_path}...")
        display(df.head())

    df = df.dropna(subset=necessary_columns)

    if annot_filter_arguments:
        if verbose > 10:
            print(f"Filtering annotations from {base_path}...")
        df = filter_annot_dataframe2(df, annot_filter_arguments)

    if not df.empty:
        if verbose > 10:
            print(f"Filtering by filter_codes from {base_path}...")
        return filter_and_select_rows(
            df,
            filter_codes,
            verbosity=verbose > 0,
            time_column=time_column,
            filter_column="cui",
            mode=mode,
            n_rows=1,
        )

    return pd.DataFrame()


def get_pat_ipw_record(
    current_pat_idcode: str,
    config_obj=None,
    annot_filter_arguments: Optional[Dict[str, Union[int, str, List[str]]]] = None,
    filter_codes: Optional[List[int]] = None,
    mode: str = "earliest",
    verbose: int = 0,  # Added default verbosity level
    include_mct: bool = True,  # Boolean argument to include MCT
    include_textual_obs: bool = True,  # Boolean argument to include textual_obs
) -> pd.DataFrame:
    """
    Retrieve patient IPW record.

    This function retrieves the earliest relevant records, based on the filter_codes,
    from the EPR, MCT, and textual_obs annotation dataframes.

    Parameters:
    - current_pat_idcode (str): Patient ID code.
    - config_obj (Optional[pat2vec config obj]): Configuration object (default: None).
    - annot_filter_arguments (Optional[YourFilterArgType]): Annotation filter arguments (default: None).
    - filter_codes (Optional[int]): Filter codes (default: None).
    - verbose (int): Verbosity level for printing messages (default: 1).
    - include_mct (bool): Whether to include MCT annotations (default: True).
    - include_textual_obs (bool): Whether to include textual_obs annotations (default: True).

    Returns:
    pd.DataFrame: DataFrame containing the earliest relevant records.
    """

    if config_obj.verbosity >= 0:
        verbose = config_obj.verbosity

    if verbose >= 10:
        print(f"Getting IPW record for patient: {current_pat_idcode}")

    # Define common necessary columns, excluding the timestamp column which varies.
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

    # EPR record
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

    # MCT record
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

    # Textual Obs record
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

    # Combine results from EPR, MCT, and textual_obs
    dfs_to_compare = []
    if not fsr.empty:
        fsr["source"] = "EPR"
        dfs_to_compare.append(fsr)
    if include_mct and not fsr_mct.empty:
        fsr_mct["source"] = "MCT"
        dfs_to_compare.append(fsr_mct)
    if include_textual_obs and not fsr_textual_obs.empty:
        fsr_textual_obs["source"] = "textual_obs"
        dfs_to_compare.append(fsr_textual_obs)

    # Standardize the timestamp column name to 'updatetime' for comparison
    for df in dfs_to_compare:
        if "observationdocument_recordeddtm" in df.columns:
            df.rename(
                columns={"observationdocument_recordeddtm": "updatetime"}, inplace=True
            )
        elif "basicobs_entered" in df.columns:
            # handle existing updatetime we need to drop as we use basic obs entered for time here
            if "updatetime" in df.columns:
                df.drop(columns=["updatetime"], axis=1, inplace=True)

            df.rename(columns={"basicobs_entered": "updatetime"}, inplace=True)
        elif "updatetime" not in df.columns:
            raise KeyError("Timestamp column not found in DataFrame.")

    # Find the earliest record among non-empty DataFrames
    if dfs_to_compare:
        earliest_df = min(dfs_to_compare, key=lambda df: df.iloc[0]["updatetime"])
    else:
        # All DataFrames are empty
        if verbose > 10:
            print("No annotations available from EPR, MCT, or textual_obs...")
        earliest_df = pd.DataFrame(columns=base_necessary_columns + ["updatetime"])
        earliest_df["client_idcode"] = [current_pat_idcode]
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

        if config_obj.lookback == False:
            earliest_df["updatetime"] = [start_datetime]
        else:
            earliest_df["updatetime"] = [end_datetime]

    earliest_df = earliest_df.copy()

    if len(earliest_df) == 0:
        if verbose > 10:
            print("No earliest IPW records available...")

    return earliest_df
