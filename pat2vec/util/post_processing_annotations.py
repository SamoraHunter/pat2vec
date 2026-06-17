import pandas as pd
import os
from typing import Any, Dict, List, Optional
from tqdm import tqdm
from sqlalchemy import text

from pat2vec.util.helper_functions import get_df_from_db
import logging

logger = logging.getLogger(__name__)

EMPTY_ANNOT_COLS = [
    "client_idcode",
    "pretty_name",
    "cui",
    "type_ids",
    "types",
    "source_value",
    "detected_name",
    "acc",
    "context_similarity",
    "start",
    "end",
    "icd10",
    "ontologies",
    "snomed",
    "opcs4",
    "id",
    "Time_Value",
    "Time_Confidence",
    "Presence_Value",
    "Presence_Confidence",
    "Subject_Value",
    "Subject_Confidence",
    "updatetime",
    "annotation_batch_source",
    # New columns added by annot_processor
    "document_guid",
    "annotation_description",
    "observationannotation_recordeddtm",
]


def filter_annot_dataframe2(
    dataframe: pd.DataFrame, filter_args: Dict[str, Any]
) -> pd.DataFrame:
    """Filter a DataFrame based on specified filter arguments.

    Args:
        dataframe: The DataFrame to filter.
        filter_args: A dictionary containing filter arguments.
            Keys are column names, and values are the filter criteria.
            Special handling for 'types', 'Time_Value', 'Presence_Value', 'Subject_Value',
            'Time_Confidence', 'Presence_Confidence', 'Subject_Confidence', and 'acc'.

    Returns:
        The filtered DataFrame.
    """

    # Initialize a boolean mask with True values for all rows
    mask = pd.Series(True, index=dataframe.index)  # Keep this line

    # Apply filters based on the provided arguments
    for column, value in filter_args.items():
        if column in dataframe.columns:
            # Special case for 'types' column
            if column == "types":
                mask &= dataframe[column].apply(
                    lambda x: any(item.lower() in str(x).lower() for item in value)
                )
            elif column in ["Time_Value", "Presence_Value", "Subject_Value"]:
                # Include rows where the column is in the specified list of values
                mask &= (
                    dataframe[column].astype(str).isin(value)
                    if isinstance(value, list)
                    else (dataframe[column] == value)
                )
            elif column in [
                "Time_Confidence",
                "Presence_Confidence",
                "Subject_Confidence",
            ]:
                # Include rows where the column is greater than or equal to the specified confidence threshold
                # Ensure column is numeric before comparison to avoid string comparison issues
                dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
                mask &= dataframe[column] >= value
            elif column in ["acc"]:
                # Include rows where the column is greater than or equal to the specified confidence threshold
                dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
                mask &= dataframe[column] >= value
            else:
                # Attempt to convert to numeric if the value is numeric, to handle mixed types in CSV chunks
                if isinstance(value, (int, float)):
                    dataframe[column] = pd.to_numeric(
                        dataframe[column], errors="ignore"
                    )
                mask &= dataframe[column] >= value

    # Return the filtered DataFrame
    return dataframe[mask]


def produce_filtered_annotation_dataframe(
    cui_filter: bool = False,
    meta_annot_filter: bool = False,
    pat_list: Optional[List[str]] = None,
    config_obj: Optional[Any] = None,
    filter_custom_args: Optional[Dict[str, Any]] = None,
    cui_code_list: Optional[List[int]] = None,
    mct: bool = False,
) -> pd.DataFrame:
    """Filter annotation dataframe based on specified criteria.

    Args:
        cui_filter: Whether to filter by CUI codes.
        meta_annot_filter: Whether to apply meta annotation filtering.
        pat_list: List of patient identifiers. If None, uses `config_obj.all_patient_list`.
        config_obj: Configuration object containing necessary parameters.
        filter_custom_args: Custom filter arguments. If None, uses `config_obj.filter_arguments`.
        cui_code_list: List of CUI codes for filtering.
        mct: If True, processes MCT annotation batches; otherwise, processes EPR.

    Returns:
        pd.DataFrame: Filtered annotation dataframe.
    """

    if meta_annot_filter:
        if filter_custom_args is None:
            logger.info("Using config obj filter arguments..")
            filter_args = config_obj.filter_arguments
        else:
            filter_args = filter_custom_args

    super_result = pd.DataFrame()

    if config_obj and getattr(config_obj, "storage_backend", "file") == "database":
        logger.info("Reading annotations from database...")
        try:
            if mct:
                table_name = "ann_mct_docs"
            elif filter_custom_args and "epic" in str(filter_custom_args):
                # If custom args suggest an Epic source, try to determine which one.
                # Defaulting to standard EPR for safety if ambiguous.
                table_name = "ann_epr_docs"
            else:
                table_name = "ann_epr_docs"

            # For the DB path, we want to optionally support multiple tables
            # but usually this function is called with a specific context.

            logger.info(f"Reading from annotations.{table_name}")
            super_result = get_df_from_db(
                config_obj, "annotations", table_name, patient_ids=pat_list
            )

        except Exception as e:
            logger.error(f"Error reading annotations from database: {e}")
            return pd.DataFrame(columns=EMPTY_ANNOT_COLS)

    else:  # File-based logic
        results = []
        if pat_list is None:
            if hasattr(config_obj, "all_patient_list"):
                logger.info(
                    f"Using all patient list of length {len(config_obj.all_patient_list)}"
                )
                pat_list = config_obj.all_patient_list
            else:
                logger.error(
                    "pat_list is None and config_obj.all_patient_list is not available."
                )
                return pd.DataFrame(columns=EMPTY_ANNOT_COLS)

        for i in tqdm(range(len(pat_list)), desc="Loading patient annotation batches"):
            current_pat_client_idcode = str(pat_list[i])

            path_attr = (
                "pre_document_annotation_batch_path_mct"
                if mct
                else "pre_document_annotation_batch_path"
            )
            base_path = getattr(config_obj, path_attr)
            current_pat_annot_batch_path = os.path.join(
                base_path, f"{current_pat_client_idcode}.csv"
            )

            if os.path.exists(current_pat_annot_batch_path):
                try:
                    current_pat_annot_batch = pd.read_csv(current_pat_annot_batch_path)
                    results.append(current_pat_annot_batch)
                except Exception as e:
                    logger.warning(
                        f"Could not read or process {current_pat_annot_batch_path}: {e}"
                    )

        if not results:
            return pd.DataFrame(columns=EMPTY_ANNOT_COLS)
        super_result = pd.concat(results, ignore_index=True)

    # Apply patient list filter if provided (for both DB and file backends)
    if pat_list and not super_result.empty:
        super_result = super_result[super_result["client_idcode"].isin(pat_list)]

    if super_result.empty:
        return pd.DataFrame(columns=EMPTY_ANNOT_COLS)

    results = []

    if pat_list is None:
        logger.info(
            f"Using all patient list of length {len(config_obj.all_patient_list)}"
        )
        pat_list = config_obj.all_patient_list

    # Common filtering logic for both DB and file paths
    necessary_columns = [
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
    time_col = "observationdocument_recordeddtm" if mct else "updatetime"
    if time_col not in necessary_columns:
        necessary_columns.append(time_col)

    super_result = super_result.dropna(
        subset=[col for col in necessary_columns if col in super_result.columns]
    )

    if meta_annot_filter:
        super_result = filter_annot_dataframe2(super_result, filter_args)

    if cui_filter:
        super_result = super_result[super_result["cui"].isin(cui_code_list)]

    return super_result


def extract_types_from_csv(directory: str) -> List[str]:
    """Extracts all unique 'types' from CSV files within a given directory and its subdirectories.

    Args:
        directory: The path to the directory to search for CSV files.

    Returns:
        A list of all unique 'types' found in the 'types' column of the CSV files.
    """

    all_types = set()

    # Traverse the directory and its subdirectories
    for root, dirs, files in os.walk(directory):
        logger.debug(f"Scanning files in {root}: {files}")
        for file in files:
            if file.endswith(".csv"):
                # Construct the full file path
                file_path = os.path.join(root, file)

                # Read the CSV file using pandas
                df = pd.read_csv(file_path)
                if "types" not in df.columns:
                    continue

                # Extract the "types" column and add unique values to the set
                types_column = df["types"]
                all_types.update(types_column.unique())

    return list(all_types)


def join_icd10_codes_to_annot(df: pd.DataFrame, inner: bool = False) -> pd.DataFrame:
    """Joins ICD-10 codes to an annotation DataFrame.

    This function merges the input DataFrame `df` with a predefined ICD-10 mapping
    DataFrame based on the 'cui' column in `df` and 'referencedComponentId' in the mapping.

    Args:
        df: The annotation DataFrame.
        inner: If True, performs an inner merge; otherwise, performs a left merge.

    Returns:
        The DataFrame with ICD-10 codes joined.
    """

    mfp = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "..",
        "snomed_methods",
        "snomed_icd10_map",
        "data",
        "tls_Icd10cmHumanReadableMap_US1000124_20230901.tsv",
    )

    mdf = pd.read_csv(mfp, sep="\t")

    # Prevent column clashing by dropping existing placeholders from the annotation DataFrame
    cols_to_drop = [
        c
        for c in mdf.columns
        if c in df.columns and c not in ["cui", "referencedComponentId"]
    ]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    if inner:
        result = pd.merge(
            df, mdf, left_on="cui", right_on="referencedComponentId", how="inner"
        )

    else:
        result = pd.merge(
            df, mdf, left_on="cui", right_on="referencedComponentId", how="left"
        )

    return result


def join_icd10_OPC4S_codes_to_annot(
    df: pd.DataFrame, inner: bool = False
) -> pd.DataFrame:
    """Joins ICD-10 and OPCS-4 codes to an annotation DataFrame.

    This function merges the input DataFrame `df` with a predefined ICD-10/OPCS-4 mapping
    DataFrame based on the 'cui' column in `df` and 'conceptId' in the mapping.

    Args:
        df: The annotation DataFrame.
        inner: If True, performs an inner merge; otherwise, performs a left merge.

    Returns:
        The DataFrame with ICD-10 and OPCS-4 codes joined.
    """

    # ../home/cogstack/samora/_data/gloabl_files/
    mfp = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "..",
        "snomed_methods",
        "snomed_to_icd10_opcs4",
        "map.csv",
    )

    mdf = pd.read_csv(mfp)

    # Prevent column clashing by dropping existing placeholders from the annotation DataFrame
    cols_to_drop = [
        c for c in mdf.columns if c in df.columns and c not in ["cui", "conceptId"]
    ]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    if inner:
        result = pd.merge(df, mdf, left_on="cui", right_on="conceptId", how="inner")

    else:
        result = pd.merge(df, mdf, left_on="cui", right_on="conceptId", how="left")

    return result


def filter_and_select_rows(
    dataframe: pd.DataFrame,
    filter_list: List[Any],
    verbosity: int = 0,
    time_column: str = "updatetime",
    filter_column: str = "cui",
    mode: str = "earliest",
    n_rows: int = 1,
) -> pd.DataFrame:
    """Filter a dataframe based on a filter_column and filter_list, and return either the earliest or latest rows.

    Args:
        dataframe: Input dataframe.
        filter_list: List of values to filter the dataframe.
        verbosity: If > 0, print additional information during execution.
        time_column: Column representing time, used for sorting if specified.
        filter_column: Column used for filtering based on filter_list.
        mode: Either 'earliest' or 'latest' to specify the rows to return.
        n_rows: Number of rows to return if they exist.

    Returns:
        pd.DataFrame: Filtered and selected rows from the input dataframe.

    """
    if not all(arg is not None for arg in [dataframe, filter_list, filter_column]):
        raise ValueError(
            "Please provide a valid dataframe, filter_list, and filter_column."
        )

    if filter_column not in dataframe.columns:
        raise ValueError(f"{filter_column} not found in the dataframe columns.")

    filtered_df = dataframe[dataframe[filter_column].isin(filter_list)]

    if time_column:
        filtered_df = filtered_df.sort_values(by=time_column)

    if mode == "earliest":
        selected_rows = filtered_df.head(n_rows)
    elif mode == "latest":
        selected_rows = filtered_df.tail(n_rows)
    else:
        raise ValueError("Invalid mode. Please choose 'earliest' or 'latest'.")

    if verbosity > 10:
        logger.debug("Filtered DataFrame:")
        # display(filtered_df) # Commented out as display is not available in all environments
        logger.debug(f"Selected {mode} {n_rows} row(s):")
        # display(selected_rows) # Commented out as display is not available in all environments

    return selected_rows


def filter_dataframe_by_cui(
    dataframe: pd.DataFrame,
    filter_list: List[int],
    filter_column: str = "cui",
    mode: str = "earliest",
    temporal: str = "before",
    verbosity: int = 0,
    time_column: str = "updatetime",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Filter an annotation DataFrame based on a list of CUI codes and a specified mode.

    Args:
        dataframe: The input DataFrame.
        filter_list: List of CUI codes to filter the DataFrame.
        filter_column: The column containing filter.
        mode: Specifies whether to consider the earliest or latest entry for each filter.
        temporal: Specifies whether to retain entries before or after the selected mode entry.
        verbosity: Verbosity level. 0 for no debug statements, higher values for more verbosity.
        time_column: The column containing time information.

    Returns:
        pd.DataFrame: Filtered DataFrame based on the specified criteria.
    """

    # Ensure the time column is in datetime format
    dataframe[time_column] = pd.to_datetime(dataframe[time_column], utc=True)

    # Ensure filter_list contains integers
    filter_list = [int(cui) for cui in filter_list]

    # Filter the DataFrame based on the given CUI codes
    filtered_df = dataframe[dataframe[filter_column].isin(filter_list)]

    # Debug statement for verbosity
    if verbosity > 0:
        logger.debug(f"Filtered DataFrame based on {filter_column} codes:\n")
        # display(filtered_df.head()) # Commented out as display is not available in all environments

    # Find the earliest or latest entry for each CUI code
    if mode == "earliest":
        result_df = filtered_df.groupby(filter_column, as_index=False)[
            time_column
        ].min()
    elif mode == "latest":
        result_df = filtered_df.groupby(filter_column, as_index=False)[
            time_column
        ].max()
    else:
        raise ValueError("Invalid mode. Use 'earliest' or 'latest'")

    filter_row = result_df.copy()  # preserve row used for filter
    # Debug statement for verbosity
    if verbosity > 0:
        logger.debug(f"Result DataFrame based on {mode} mode:\n")
        # display(result_df.head()) # Commented out as display is not available in all environments

    # Merge with the original DataFrame to get the full rows
    result_df = pd.merge(
        result_df, dataframe, on=[filter_column, time_column], how="inner"
    )

    # Filter the original DataFrame based on the earliest or latest entry
    if temporal == "before":
        filtered_original_df = dataframe[
            dataframe[time_column] <= result_df[time_column].min()
        ]
    elif temporal == "after":
        filtered_original_df = dataframe[
            dataframe[time_column] >= result_df[time_column].max()
        ]
    else:
        raise ValueError("Invalid temporal value. Use 'before' or 'after'")

    # Debug statement for verbosity
    if verbosity > 0:
        logger.debug(f"Filtered original DataFrame based on {temporal} temporal:\n")
        # display(filtered_original_df.head()) # Commented out as display is not available in all environments

    return filtered_original_df, filter_row, filtered_df


def check_list_presence(df, column, lst, annot_filter_arguments=None):
    """Checks if any string in a list is present in a specified DataFrame column,
    optionally after applying annotation filters.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column (str): The name of the column to check for string presence.
        lst (list): A list of strings to search for.
        annot_filter_arguments (dict, optional): Arguments to filter the DataFrame
            before checking for list presence. Defaults to None.

    Returns:
        bool: True if any string from `lst` is found in `column` (case-insensitive), False otherwise.
    """

    if annot_filter_arguments is not None:
        df = filter_annot_dataframe2(df, annot_filter_arguments)

    str_lst = list(map(str, lst))  # Convert elements to strings
    return any(
        df[column].astype(str).str.contains("|".join(str_lst), case=False, na=False)
    )


def filter_dataframe_n_lists(
    df: pd.DataFrame, column_name: str, n_lists: List[List[Any]]
) -> pd.DataFrame:
    """Filters a DataFrame to include rows where the value in a specified column
    is present in *all* of the provided lists.

    Args:
        df: The input DataFrame.
        column_name: The name of the column to filter.
        n_lists: A list of lists. A row is kept only if the value
            in `column_name` is present in every sublist within `n_lists`.

    Returns:
        The filtered DataFrame.
    """
    # Create a mask for each list in n_lists
    masks = [df[column_name].isin(lst) for lst in n_lists]

    # Combine masks with logical AND to get the final mask
    final_mask = pd.concat(masks, axis=1).all(axis=1)

    # Apply the mask to the DataFrame
    filtered_df = df[final_mask]

    return filtered_df


def get_all_target_annots(
    all_pat_list: List[str],
    n_lists: List[List[int]],
    config_obj: Optional[Any] = None,
    annot_filter_arguments: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Retrieves and filters target annotations for a list of patients.

    This function iterates through a list of patient IDs, retrieves their annotations,
    applies optional annotation filters, and then filters the annotations to include
    only those where the 'cui' (Concept Unique Identifier) is present in all of the
    provided `n_lists`. The results are concatenated into a single DataFrame and saved.

    Args:
        all_pat_list: A list of patient IDs to process.
        n_lists: A list of lists of CUI codes. Annotations are kept if their CUI is in all sublists.
        config_obj: A configuration object.
        annot_filter_arguments: Arguments to filter annotations.

    Returns:
        pd.DataFrame: A DataFrame containing all target annotations.
    """
    results_df = pd.DataFrame()

    for sublist in n_lists:
        for i, element in enumerate(sublist):
            sublist[i] = int(element)

    for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):

        current_pat_idcode = all_pat_list[i]

        all_annots = retrieve_pat_annots_mct_epr(current_pat_idcode, config_obj)

        all_annots.dropna(subset="acc", inplace=True)

        if annot_filter_arguments is not None:
            all_annots = filter_annot_dataframe2(all_annots, annot_filter_arguments)

        annots_to_return = filter_dataframe_n_lists(all_annots, "cui", n_lists)

        if not annots_to_return.empty:
            results_df = pd.concat([results_df, annots_to_return])

    if config_obj and hasattr(config_obj, "root_path"):
        out_path = os.path.join(config_obj.root_path, "all_target_annots.csv")
    else:
        out_path = "all_target_annots.csv"
    results_df.to_csv(out_path)

    return results_df


def retrieve_pat_annots_mct_epr(
    client_idcode: str,
    config_obj: Any,
    columns_epr: Optional[List[str]] = None,
    columns_mct: Optional[List[str]] = None,
    columns_to: Optional[List[str]] = None,
    columns_report: Optional[List[str]] = None,
    columns_epic_imaging_reports: Optional[List[str]] = None,
    columns_epic_medical_history: Optional[List[str]] = None,
    columns_epic_orders: Optional[List[str]] = None,
    columns_epic_clinical_notes: Optional[List[str]] = None,
    columns_epic_clinical_notes_appointments: Optional[List[str]] = None,
    merge_columns: bool = True,
) -> pd.DataFrame:
    """Retrieves and merges annotation data for a single patient from multiple sources (files or database).

    This function reads annotation data for a specified patient from four potential
    sources: EPR annotations, MCT annotations, textual observations annotations, and reports annotations.
    It loads the corresponding CSV files, optionally selecting specific columns,
    and concatenates them into a single DataFrame. It can also merge related
    columns (e.g., timestamps, content) to create a more unified dataset.

    Args:
        client_idcode: The unique identifier for the patient.
        config_obj: A configuration object containing paths to the
            various annotation batch files.
        columns_epr: A list of columns to load from the EPR annotations CSV.
        columns_mct: A list of columns to load from the MCT annotations CSV.
        columns_to: A list of columns to load from the textual observations annotations CSV.
        columns_report: A list of columns to load from the reports annotations CSV.
        columns_epic_imaging_reports: A list of columns to load from the Epic imaging reports annotations CSV.
        columns_epic_medical_history: A list of columns to load from the Epic medical history annotations CSV.
        columns_epic_clinical_notes: A list of columns to load from the Epic clinical notes annotations CSV.
        columns_epic_clinical_notes_appointments: A list of columns to load from the Epic clinical notes appointments annotations CSV.
        merge_columns (bool, optional): If True, attempts to merge corresponding
            columns (e.g., timestamps, content) from the different sources into a unified set of
            columns. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame containing the concatenated and optionally
            merged annotation data for the patient. Returns an empty
            DataFrame if no data is found for the patient in any of the sources.
    """
    all_annots_dfs = []

    if config_obj.storage_backend == "database":
        source_map = {
            "ann_epr_docs": ("epr", columns_epr),
            "ann_mct_docs": ("mct", columns_mct),
            "ann_textual_obs": ("textual_obs", columns_to),
            "ann_reports": ("report", columns_report),
            "ann_epic_clinical_notes": (
                "epic_clinical_notes",
                (
                    columns_epic_clinical_notes
                    if columns_epic_clinical_notes
                    else columns_epr
                ),
            ),
            "ann_epic_clinical_notes_appointments": (
                "epic_clinical_notes_appointments",
                (
                    columns_epic_clinical_notes_appointments
                    if columns_epic_clinical_notes_appointments
                    else columns_epr
                ),
            ),
            "ann_epic_imaging_reports": (
                "epic_imaging_reports",
                (
                    columns_epic_imaging_reports
                    if columns_epic_imaging_reports
                    else columns_epr
                ),
            ),
            "ann_epic_medical_history": (
                "epic_medical_history",
                (
                    columns_epic_medical_history
                    if columns_epic_medical_history
                    else columns_epr
                ),
            ),
            "ann_epic_orders": (
                "epic_orders",
                columns_epic_orders if columns_epic_orders else columns_epr,
            ),
        }
        for table, (source_name, cols) in source_map.items():
            df = get_df_from_db(
                config_obj,
                "annotations",
                table,
                patient_ids=[client_idcode],
                columns=cols,
            )
            if not df.empty:
                df["annotation_batch_source"] = source_name
                all_annots_dfs.append(df)
    else:
        path_map = {
            "epr": (config_obj.pre_document_annotation_batch_path, columns_epr),
            "mct": (config_obj.pre_document_annotation_batch_path_mct, columns_mct),
            "textual_obs": (
                config_obj.pre_textual_obs_annotation_batch_path,
                columns_to,
            ),
            "epic_clinical_notes": (
                config_obj.pre_epic_clinical_notes_annotation_batch_path,
                (
                    columns_epic_clinical_notes
                    if columns_epic_clinical_notes
                    else columns_epr
                ),
            ),
            "epic_clinical_notes_appointments": (
                config_obj.pre_epic_clinical_notes_appointments_annotation_batch_path,
                (
                    columns_epic_clinical_notes_appointments
                    if columns_epic_clinical_notes_appointments
                    else columns_epr
                ),
            ),
            "epic_imaging_reports": (
                config_obj.pre_epic_imaging_reports_annotation_batch_path,
                (
                    columns_epic_imaging_reports
                    if columns_epic_imaging_reports
                    else columns_epr
                ),
            ),
            "epic_medical_history": (
                config_obj.pre_epic_medical_history_annotation_batch_path,
                (
                    columns_epic_medical_history
                    if columns_epic_medical_history
                    else columns_epr
                ),
            ),
            "epic_orders": (
                config_obj.pre_epic_orders_annotation_batch_path,
                columns_epic_orders if columns_epic_orders else columns_epr,
            ),
            "report": (
                config_obj.pre_document_annotation_batch_path_reports,
                columns_report,
            ),
        }
        for source_name, (base_path, cols) in path_map.items():
            file_path = f"{base_path}/{client_idcode}.csv"
            if os.path.exists(file_path):
                try:
                    avail = pd.read_csv(file_path, nrows=0).columns
                    use_cols = [c for c in cols if c in avail] if cols else None
                    df = pd.read_csv(file_path, usecols=use_cols)
                    df["annotation_batch_source"] = source_name
                    all_annots_dfs.append(df)
                except Exception as e:
                    logger.warning(f"Could not read or process {file_path}: {e}")

    if not all_annots_dfs:
        return pd.DataFrame(columns=EMPTY_ANNOT_COLS)

    all_annots = pd.concat(all_annots_dfs, ignore_index=True)

    if not all_annots.empty and "cui" in all_annots.columns:
        all_annots["cui"] = pd.to_numeric(all_annots["cui"], errors="ignore")

    if merge_columns and not all_annots.empty:
        # Load data if files exist
        if "observationannotation_recordeddtm" in all_annots.columns:
            all_annots["updatetime"] = all_annots["updatetime"].fillna(
                all_annots["observationannotation_recordeddtm"]
            )

            all_annots["observationannotation_recordeddtm"] = all_annots[
                "observationannotation_recordeddtm"
            ].fillna(all_annots["updatetime"])

        if "basicobs_entered" in all_annots.columns:

            all_annots["updatetime"] = all_annots["updatetime"].fillna(
                all_annots["basicobs_entered"]
            )

        if "observationdocument_recordeddtm" in all_annots.columns:

            all_annots["updatetime"] = all_annots["updatetime"].fillna(
                all_annots["observationdocument_recordeddtm"]
            )

        if "basicobs_guid" in all_annots.columns:

            if "document_guid" in all_annots.columns:
                # Merge observation_guid to document_guid
                all_annots["document_guid"] = all_annots["document_guid"].fillna(
                    all_annots["basicobs_guid"]
                )
            else:
                all_annots["document_guid"] = all_annots["basicobs_guid"]

        if "observation_guid" in all_annots.columns:
            all_annots["document_guid"] = all_annots["document_guid"].fillna(
                all_annots["observation_guid"]
            )

        if "obscatalogmasteritem_displayname" in all_annots.columns:
            # Add obscatalogmasteritem_displayname to annotation_description
            all_annots["annotation_description"] = all_annots[
                "annotation_description"
            ].fillna(all_annots["obscatalogmasteritem_displayname"])

        if "observation_valuetext_analysed" in all_annots.columns:
            all_annots["body_analysed"] = all_annots["body_analysed"].fillna(
                all_annots["observation_valuetext_analysed"]
            )

        if "document_Content" in all_annots.columns:
            all_annots["body_analysed"] = all_annots["body_analysed"].fillna(
                all_annots["document_Content"]
            )

        if "document_Comment" in all_annots.columns:
            all_annots["body_analysed"] = all_annots["body_analysed"].fillna(
                all_annots["document_Comment"]
            )

        if "document_CreatedWhen" in all_annots.columns:
            all_annots["updatetime"] = all_annots["updatetime"].fillna(
                all_annots["document_CreatedWhen"]
            )

        if "id" in all_annots.columns:
            if "document_guid" in all_annots.columns:
                all_annots["document_guid"] = all_annots["document_guid"].fillna(
                    all_annots["id"]
                )
            else:
                all_annots["document_guid"] = all_annots["id"]

    return all_annots


def remove_file_from_paths(
    current_pat_idcode: str,
    project_name: str = "new_project",
    verbosity: int = 0,
    config_obj: Optional[Any] = None,
) -> None:
    """Removes patient-specific data from various predefined project paths or database tables.

    If `storage_backend` is 'database', it removes records for the patient from relevant tables.
    Otherwise, it removes CSV files.

    Args:
        current_pat_idcode: The unique identifier of the patient whose files are to be removed.
        project_name: The name of the project. Used if `config_obj` is None.
        verbosity: Verbosity level for printing messages.
        config_obj: A configuration object containing project paths.
            If provided, `project_name` is overridden by `config_obj.proj_name`. Defaults to None.
    """
    if config_obj and getattr(config_obj, "storage_backend", "file") == "database":
        try:
            effective_verbosity = max(verbosity, getattr(config_obj, "verbosity", 0))
            if effective_verbosity > 0:
                logger.info(
                    f"Removing data for patient {current_pat_idcode} from database..."
                )

            engine = config_obj.db_engine
            if not engine:
                logger.error("Database engine not initialized in config_obj.")
                return

            with engine.begin() as connection:
                is_sqlite = engine.name == "sqlite"

                t_features = (
                    '"features_features"' if is_sqlite else '"features"."features"'
                )
                try:
                    connection.execute(
                        text(
                            f'DELETE FROM {t_features} WHERE "client_idcode" = :pat_id'
                        ),
                        {"pat_id": current_pat_idcode},
                    )
                except Exception:
                    pass

                raw_tables = [
                    "raw_epr_docs",
                    "raw_mct_docs",
                    "raw_bloods",
                    "raw_drugs",
                    "raw_diagnostics",
                    "raw_news",
                    "raw_bmi",
                    "raw_demographics",
                    "raw_textual_obs",
                    "raw_reports",
                    "raw_covid",
                    "raw_smoking",
                    "raw_vte",
                    "raw_resus",
                    "raw_core_02",
                    "raw_bed",
                    "raw_hospsite",
                    # Epic Tables
                    "raw_epic_encounters",
                    "raw_epic_clinical_notes",
                    "raw_epic_medical_history",
                    "raw_epic_orders",
                    "raw_epic_lab_results",
                    "raw_epic_patients",
                    "raw_epic_imaging_reports",
                    "raw_epic_clinical_notes_appointments",
                ]
                for t in raw_tables:
                    t_name = f'"raw_data_{t}"' if is_sqlite else f'"raw_data"."{t}"'
                    try:
                        connection.execute(
                            text(
                                f'DELETE FROM {t_name} WHERE "client_idcode" = :pat_id'
                            ),
                            {"pat_id": current_pat_idcode},
                        )
                    except Exception:
                        pass

                t_app = (
                    '"raw_data_raw_appointments"'
                    if is_sqlite
                    else '"raw_data"."raw_appointments"'
                )
                try:
                    connection.execute(
                        text(f'DELETE FROM {t_app} WHERE "HospitalID" = :pat_id'),
                        {"pat_id": current_pat_idcode},
                    )
                except Exception:
                    pass

                ann_tables = [
                    "ann_epr_docs",
                    "ann_mct_docs",
                    "ann_textual_obs",
                    "ann_reports",
                    "ann_epic_clinical_notes",
                    "ann_epic_clinical_notes_appointments",
                    "ann_epic_imaging_reports",
                    "ann_epic_medical_history",
                    "ann_epic_orders",
                ]
                for t in ann_tables:
                    t_ann = (
                        f'"annotations_{t}"' if is_sqlite else f'"annotations"."{t}"'
                    )
                    try:
                        connection.execute(
                            text(
                                f'DELETE FROM {t_ann} WHERE "client_idcode" = :pat_id'
                            ),
                            {"pat_id": current_pat_idcode},
                        )
                    except Exception:
                        pass
            if effective_verbosity > 0:
                logger.info("Database cleanup complete.")
        except Exception as e:
            logger.error(f"Error during database cleanup: {e}")

        if not getattr(config_obj, "testing", False):
            return

    if config_obj is None:
        pat_file_paths = [f"{project_name}/current_pat_document_batches/"]
    else:
        pat_file_paths = [
            config_obj.pre_document_batch_path,
            config_obj.pre_document_batch_path_mct,
            config_obj.pre_document_annotation_batch_path,
            config_obj.pre_document_annotation_batch_path_mct,
        ]

    for path in pat_file_paths:
        file_path = os.path.join(path, f"{current_pat_idcode}.csv")
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            if verbosity > 0:
                logger.error(f"Error removing {file_path}: {e}")
