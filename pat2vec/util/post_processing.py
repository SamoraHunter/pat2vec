import csv
import os
import pickle
import shutil
import time
from datetime import datetime
from itertools import chain
from typing import List, Optional

import matplotlib.pyplot as plt
import pandas as pd
from IPython.display import display
from tqdm import tqdm


def count_files(path: str) -> int:
    """Recursively counts the number of files in a directory.

    Args:
        path (str): The path to the directory.

    Returns:
        int: The total number of files in the directory and its subdirectories.
    """
    count = 0
    for root, dirs, files in os.walk(path):
        count += len(files)
    return count


def extract_datetime_to_column(df, drop=True):
    """Extracts datetime information from specified columns and creates a new column.

    Args:
        df (pandas.DataFrame): The DataFrame containing the datetime information in specific columns.
        drop (bool, optional): If True, drop the original date_time_stamp columns after extraction.
            Defaults to True.

    Returns:
        pandas.DataFrame: The DataFrame with a new column 'extracted_datetime_stamp' containing the extracted datetime values.
    """

    # Initialize the new column
    df["extracted_datetime_stamp"] = pd.to_datetime("")

    # Iterate through rows using tqdm for progress bar
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Extracting datetime"):
        # Iterate through columns
        for column in df.columns:
            # Check if the column contains '_date_time_stamp' and the value is 1
            if "_date_time_stamp" in column and row[column] == 1:
                # Extract date from column name and convert to datetime
                date_str = column.replace("_date_time_stamp", "")
                datetime_obj = pd.to_datetime(date_str, format="(%Y, %m, %d)")

                # Assign the datetime value to the new column
                df.at[index, "extracted_datetime_stamp"] = datetime_obj

    # Display the count of extracted datetime values
    print("Extracted datetime values:")
    print(df["extracted_datetime_stamp"].value_counts())

    if drop:
        columns_to_drop = [
            col for col in df.columns if "date_time_stamp" in col]
        if columns_to_drop:
            print(f"Dropping {len(columns_to_drop)} date_time_stamp columns")
            df = df.drop(columns=columns_to_drop)

    return df


def filter_annot_dataframe2(dataframe, filter_args):
    """Filter a DataFrame based on specified filter arguments.

    Args:
        dataframe (pandas.DataFrame): The DataFrame to filter.
        filter_args (dict): A dictionary containing filter arguments.
            Keys are column names, and values are the filter criteria.
            Special handling for 'types', 'Time_Value', 'Presence_Value', 'Subject_Value',
            'Time_Confidence', 'Presence_Confidence', 'Subject_Confidence', and 'acc'.

    Returns:
        pandas.DataFrame: The filtered DataFrame.
    """
    # Initialize a boolean mask with True values for all rows
    mask = pd.Series(True, index=dataframe.index)

    # Apply filters based on the provided arguments
    for column, value in filter_args.items():
        if column in dataframe.columns:
            # Special case for 'types' column
            if column == "types":
                mask &= dataframe[column].apply(
                    lambda x: any(item.lower() in x for item in value)
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
                mask &= dataframe[column] >= value
            elif column in ["acc"]:
                # Include rows where the column is greater than or equal to the specified confidence threshold
                mask &= dataframe[column] >= value
            else:
                mask &= dataframe[column] >= value

    # Return the filtered DataFrame
    return dataframe[mask]


def produce_filtered_annotation_dataframe(
    cui_filter=False,
    meta_annot_filter=False,
    pat_list=None,
    config_obj=None,
    filter_custom_args=None,
    cui_code_list=None,
    mct=False,
):
    """Filter annotation dataframe based on specified criteria.

    Args:
        cui_filter (bool, optional): Whether to filter by CUI codes. Defaults to False.
        meta_annot_filter (bool, optional): Whether to apply meta annotation filtering. Defaults to False.
        pat_list (list, optional): List of patient identifiers. If None, uses `config_obj.all_patient_list`. Defaults to None.
        config_obj (ConfigObject, optional): Configuration object containing necessary parameters. Defaults to None.
        filter_custom_args (dict, optional): Custom filter arguments. If None, uses `config_obj.filter_arguments`. Defaults to None.
        cui_code_list (list, optional): List of CUI codes for filtering. Defaults to None.
        mct (bool, optional): If True, processes MCT annotation batches; otherwise, processes EPR. Defaults to False.

    Returns:
        pd.DataFrame: Filtered annotation dataframe.
    """

    if meta_annot_filter:
        if filter_custom_args is None:
            print("Using config obj filter arguments..")
            filter_args = config_obj.filter_arguments
        else:
            filter_args = filter_custom_args

    results = []

    if pat_list is None:

        print("Using all patient list", len(config_obj.all_patient_list))
        pat_list = config_obj.all_patient_list

    for i in tqdm(range(len(pat_list))):
        current_pat_client_idcode = str(pat_list[i])

        if mct == False:
            current_pat_annot_batch_path = (
                config_obj.pre_document_annotation_batch_path
                + current_pat_client_idcode
                + ".csv"
            )
        else:
            current_pat_annot_batch_path = (
                config_obj.pre_document_annotation_batch_path_mct
                + current_pat_client_idcode
                + ".csv"
            )

        if os.path.exists(current_pat_annot_batch_path):
            current_pat_annot_batch = pd.read_csv(current_pat_annot_batch_path)

            # drop nan on any col:
            necessary_columns = [
                "client_idcode",
                "updatetime",
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

            current_pat_annot_batch = current_pat_annot_batch.dropna(
                subset=necessary_columns
            )

            if meta_annot_filter:
                try:
                    current_pat_annot_batch = filter_annot_dataframe2(
                        current_pat_annot_batch, filter_args
                    )
                except Exception as e:
                    print(e, i)
                    display(current_pat_annot_batch)
                    raise e

            if cui_filter:
                current_pat_annot_batch = current_pat_annot_batch[
                    current_pat_annot_batch["cui"].isin(cui_code_list)
                ]

            results.append(current_pat_annot_batch)

    super_result = pd.concat(results)

    return super_result


def extract_types_from_csv(directory):
    """Extracts all unique 'types' from CSV files within a given directory and its subdirectories.

    Args:
        directory (str): The path to the directory to search for CSV files.

    Returns:
        list: A list of all unique 'types' found in the 'types' column of the CSV files.
    """

    all_types = set()

    # Traverse the directory and its subdirectories
    for root, dirs, files in os.walk(directory):
        print(files)
        for file in files:
            if file.endswith(".csv"):
                # Construct the full file path
                file_path = os.path.join(root, file)

                # Read the CSV file using pandas
                df = pd.read_csv(file_path)

                # Extract the "types" column and add unique values to the set
                types_column = df["types"]
                all_types.update(types_column.unique())

    return list(all_types)


def remove_file_from_paths(
    current_pat_idcode: str,
    project_name: str = "new_project",
    verbosity: int = 0,
    config_obj: Optional[object] = None,
) -> None:
    """Removes patient-specific CSV files from various predefined project paths.

    Args:
        current_pat_idcode (str): The unique identifier of the patient whose files are to be removed.
        project_name (str, optional): The name of the project. Used if `config_obj` is None.
            Defaults to "new_project".
        verbosity (int, optional): Verbosity level for printing messages. Defaults to 0.
        config_obj (Optional[object], optional): A configuration object containing project paths.
            If provided, `project_name` is overridden by `config_obj.proj_name`. Defaults to None.
    """

    if config_obj == None:
        pat_file_paths = [
            f"{project_name}/current_pat_document_batches/",
            f"{project_name}/current_pat_document_batches_mct/",
            f"{project_name}/current_pat_documents_annotations_batches/",
            f"{project_name}/current_pat_documents_annotations_batches_mct/",
            f"{project_name}/current_pat_bloods_batches/",
            f"{project_name}/current_pat_drugs_batches/",
            f"{project_name}/current_pat_diagnostics_batches/",
            f"{project_name}/current_pat_news_batches/",
            f"{project_name}/current_pat_obs_batches/",
            f"{project_name}/current_pat_bmi_batches/",
            f"{project_name}/current_pat_demo_batches/",
            f"{project_name}/current_pat_document_batches_reports/",
            f"{project_name}/current_pat_documents_annotations_batches_reports/",
            f"{project_name}/current_pat_textual_obs_document_batches/",
            f"{project_name}/current_pat_textual_obs_annotation_batches/",
            f"{project_name}/current_pat_appointments_batches/",
        ]
    else:
        project_name = config_obj.proj_name

        pat_file_paths = [
            config_obj.pre_document_batch_path,
            config_obj.pre_document_batch_path_mct,
            config_obj.pre_document_annotation_batch_path,
            config_obj.pre_document_annotation_batch_path_mct,
            config_obj.pre_bloods_batch_path,
            config_obj.pre_drugs_batch_path,
            config_obj.pre_diagnostics_batch_path,
            config_obj.pre_news_batch_path,
            config_obj.pre_obs_batch_path,
            config_obj.pre_bmi_batch_path,
            config_obj.pre_demo_batch_path,
            config_obj.pre_document_batch_path_reports,
            config_obj.pre_document_annotation_batch_path_reports,
            config_obj.pre_textual_obs_annotation_batch_path,
            config_obj.pre_textual_obs_document_batch_path,
            config_obj.pre_appointments_batch_path,
        ]

    # Print debug messages:

    print(f"Removing files for patient {current_pat_idcode}...")
    print("Project_name: ", project_name)
    print("Searching for files in the following paths:")
    print(pat_file_paths)

    for path in pat_file_paths:
        file_path = path + current_pat_idcode + ".csv"
        try:
            os.remove(file_path)
            if verbosity > 0:
                print(f"{file_path} successfully removed")
        except FileNotFoundError:
            if verbosity > 0:
                print(f"{file_path} not found")
        except Exception as e:
            if verbosity > 0:
                print(f"Error removing {file_path}: {e}")


def process_chunk(args):
    """Processes a chunk of CSV files, concatenating their data into a dictionary.

    This helper function is designed for multiprocessing. It reads a specified
    range of files, extracts data for a given set of unique columns, and
    returns a dictionary where keys are column names and values are lists of
    data from those columns.

    Args:
        args (tuple): A tuple containing (part_chunk, all_files, part_size, unique_columns).

    Returns:
        dict: A dictionary with concatenated data for the specified unique columns.
    """
    part_chunk, all_files, part_size, unique_columns = args
    concatenated_data = {column: [] for column in unique_columns}
    for file in all_files[part_chunk: part_chunk + part_size]:
        if file.endswith(".csv"):
            with open(file, "r", newline="") as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    for column in unique_columns:
                        concatenated_data[column].append(row.get(column, ""))
    return concatenated_data


def join_icd10_codes_to_annot(df, inner=False):
    """Joins ICD-10 codes to an annotation DataFrame.

    This function merges the input DataFrame `df` with a predefined ICD-10 mapping
    DataFrame based on the 'cui' column in `df` and 'referencedComponentId' in the mapping.

    Args:
        df (pd.DataFrame): The annotation DataFrame.
        inner (bool, optional): If True, performs an inner merge; otherwise, performs a left merge. Defaults to False.

    Returns:
        pd.DataFrame: The DataFrame with ICD-10 codes joined.
    """

    mfp = (
        "../../snomed_icd10_map/data/tls_Icd10cmHumanReadableMap_US1000124_20230901.tsv"
    )

    mdf = pd.read_csv(mfp, sep="\t")

    if inner == True:
        result = pd.merge(
            df, mdf, left_on="cui", right_on="referencedComponentId", how="inner"
        )

    else:
        result = pd.merge(
            df, mdf, left_on="cui", right_on="referencedComponentId", how="left"
        )

    return result


def join_icd10_OPC4S_codes_to_annot(df, inner=False):
    """Joins ICD-10 and OPCS-4 codes to an annotation DataFrame.

    This function merges the input DataFrame `df` with a predefined ICD-10/OPCS-4 mapping
    DataFrame based on the 'cui' column in `df` and 'conceptId' in the mapping.

    Args:
        df (pd.DataFrame): The annotation DataFrame.
        inner (bool, optional): If True, performs an inner merge; otherwise, performs a left merge. Defaults to False.

    Returns:
        pd.DataFrame: The DataFrame with ICD-10 and OPCS-4 codes joined.
    """

    # ../home/cogstack/samora/_data/gloabl_files/
    mfp = "../../snomed_to_icd10_opcs4/map.csv"

    mdf = pd.read_csv(mfp)

    if inner == True:
        result = pd.merge(df, mdf, left_on="cui",
                          right_on="conceptId", how="inner")

    else:
        result = pd.merge(df, mdf, left_on="cui",
                          right_on="conceptId", how="left")

    return result


def filter_and_select_rows(
    dataframe,
    filter_list,
    verbosity=0,
    time_column="updatetime",
    filter_column="cui",
    mode="earliest",
    n_rows=1,
):
    """Filter a dataframe based on a filter_column and filter_list, and return either the earliest or latest rows.

    Args:
        dataframe (pd.DataFrame): Input dataframe.
        filter_list (list): List of values to filter the dataframe.
        verbosity (int, optional): If > 0, print additional information during execution. Defaults to 0.
        time_column (str, optional): Column representing time, used for sorting if specified. Defaults to "updatetime".
        filter_column (str, optional): Column used for filtering based on filter_list. Defaults to "cui".
        mode (str, optional): Either 'earliest' or 'latest' to specify the rows to return. Defaults to "earliest".
        n_rows (int, optional): Number of rows to return if they exist. Defaults to 1.

    Returns:
        pd.DataFrame: Filtered and selected rows from the input dataframe.

    """
    if not all(arg is not None for arg in [dataframe, filter_list, filter_column]):
        raise ValueError(
            "Please provide a valid dataframe, filter_list, and filter_column."
        )

    if filter_column not in dataframe.columns:
        raise ValueError(
            f"{filter_column} not found in the dataframe columns.")

    filtered_df = dataframe[dataframe[filter_column].isin(filter_list)]

    if time_column:
        filtered_df.sort_values(by=time_column, inplace=True)

    if mode == "earliest":
        selected_rows = filtered_df.head(n_rows)
    elif mode == "latest":
        selected_rows = filtered_df.tail(n_rows)
    else:
        raise ValueError("Invalid mode. Please choose 'earliest' or 'latest'.")

    if verbosity > 10:
        print("Filtered DataFrame:")
        display(filtered_df)
        print(f"Selected {mode} {n_rows} row(s):")
        display(selected_rows)

    return selected_rows


def filter_dataframe_by_cui(
    dataframe,
    filter_list,
    filter_column="cui",
    mode="earliest",
    temporal="before",
    verbosity=0,
    time_column="updatetime",
):
    """Filter an annotation DataFrame based on a list of CUI codes and a specified mode.

    Args:
        dataframe (pd.DataFrame): The input DataFrame.
        filter_list (list): List of CUI codes to filter the DataFrame.
        filter_column (str, optional): The column containing filter. Defaults to 'cui'.
        mode (str, optional): Specifies whether to consider the earliest or latest entry for each filter. Defaults to "earliest".
        temporal (str, optional): Specifies whether to retain entries before or after the selected mode entry. Defaults to "before".
        verbosity (int, optional): Verbosity level. 0 for no debug statements, higher values for more verbosity.
        time_column (str, optional): The column containing time information. Defaults to 'updatetime'.

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
        print(f"Filtered DataFrame based on {filter_column} codes:\n")
        display(filtered_df.head())

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
        print(f"Result DataFrame based on {mode} mode:\n")
        display(result_df.head())

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
        print(f"Filtered original DataFrame based on {temporal} temporal:\n")
        display(filtered_original_df.head())

    return filtered_original_df, filter_row, filtered_df


# Example usage:
# filter_codes = [109989006]
#
# Returns all rows after the earliest cui code match.
# filter_dataframe_by_cui(res, filter_list=filter_codes, filter_column = 'cui', mode="earliest", temporal = 'after', verbosity=3)


def copy_files_and_dirs(
    source_root: str,
    source_name: str,
    destination: str,
    items_to_copy: List[str] = None,
    loose_files: List[str] = None,
) -> None:
    """Copies specified directories and files from a source project location to a new destination.

    This function is useful for porting project files to a new location while preserving
    the directory structure. It can copy specific subdirectories and individual files.

    Args:
        source_root (str): The root directory of the source project.
        source_name (str): The name of the source project directory (e.g., "new_project").
        destination (str): The destination directory where the project will be copied.
        items_to_copy (List[str], optional): A list of directory or file names (relative to `source_name`)
            to copy. If None, a default set of common project directories is copied. Defaults to None.
        loose_files (List[str], optional): A list of file names (relative to `source_root`)
            to copy directly to the `destination` root. If None, a default set of common
            loose files is copied. Defaults to None.

    Usage:
        project_root_source = "/home/cogstack/%USERNAME%/_data/HFE_5"
        project_name_source = "new_project"
        project_destination = "."

        copy_files_and_dirs(project_root_source, project_name_source, project_destination)
    """
    source_dir = os.path.join(source_root, source_name)

    # Create the destination directory if it doesn't exist
    destination_dir = os.path.join(destination, source_name)
    os.makedirs(destination_dir, exist_ok=True)

    if items_to_copy is None:
        # List of directories/files to copy
        items_to_copy = [
            "current_pat_annots_parts",
            "current_pat_annots_mrc_parts",
            "outputs",
            "current_pat_document_batches",
            "current_pat_document_batches_mct",
            "current_pat_documents_annotations_batches",
            "current_pat_documents_annotations_batches_mct",
            "current_pat_lines_parts",
        ]

    # Get all paths from the source directory
    all_source_paths = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            all_source_paths.append(
                os.path.relpath(os.path.join(root, file), source_dir)
            )
        for dir in dirs:
            all_source_paths.append(
                os.path.relpath(os.path.join(root, dir), source_dir)
            )

    # Filter paths based on items_to_copy
    paths_to_copy = [
        path for path in all_source_paths if any(item in path for item in items_to_copy)
    ]

    # Copy each path to the destination preserving structure
    for path in tqdm(paths_to_copy, desc="Copying"):
        source_path = os.path.join(source_dir, path)
        destination_path = os.path.join(destination_dir, path)

        if os.path.isdir(source_path):
            os.makedirs(destination_path, exist_ok=True)
        else:
            shutil.copy2(source_path, destination_path)

    if loose_files is None:
        # Look for loose files specifically in the root directory of the source
        loose_files = ["treatment_docs.csv", "control_path.pkl"]

    # Copy loose files to the root directory of the destination
    for loose_file in tqdm(loose_files, desc="Copying loose files"):
        source_loose_path = os.path.join(source_root, loose_file)
        destination_loose_path = os.path.join(destination, loose_file)
        shutil.copy2(source_loose_path, destination_loose_path)

    # Example usage:
    # if __name__ == "__main__":
    # project_root_source = "/home/cogstack/%USERNAME%/_data/HFE_5"
    # project_name_source = "new_project"
    # project_destination = "."

    # copy_files_and_dirs(project_root_source, project_name_source, project_destination)


def filter_and_update_csv(
    target_directory, ipw_dataframe, filter_type="after", verbosity=False
):
    """Filters and updates CSV files in a target directory based on patient IPW records.

    This function iterates through each patient record in the `ipw_dataframe`,
    finds corresponding CSV files in the `target_directory` (and its subdirectories),
    and filters the rows in those CSV files based on a timestamp column and a filter date.

    Args:
        target_directory (str): The root directory containing the CSV files to be filtered.
        ipw_dataframe (pd.DataFrame): A DataFrame containing patient IPW records, including 'client_idcode' and a timestamp column (e.g., 'updatetime').
        filter_type (str, optional): The type of filtering to apply: "after" (keep records after filter_date) or "before" (keep records before filter_date). Defaults to "after".
        verbosity (bool, optional): If True, print verbose messages during processing. Defaults to False.
    """
    for _, row in ipw_dataframe.iterrows():
        client_idcode = row["client_idcode"]
        # print(client_idcode, row['updatetime'])
        # filter_date = pd.to_datetime(row['updatetime']).tz_convert('UTC')  # Convert filter_date to UTC
        filter_date = pd.to_datetime(
            row["updatetime"], utc=True, errors="coerce")

        if verbosity:
            print(f"Processing client_idcode: {client_idcode}")

        # Recursively walk through the target directory
        for root, dirs, files in os.walk(target_directory):
            for file in files:
                if file.startswith(client_idcode) and file.endswith(".csv"):
                    file_path = os.path.join(root, file)

                    if verbosity:
                        print(f"Found CSV file: {file_path}")

                    df = pd.read_csv(file_path)

                    # Check if 'updatetime' is in columns
                    if "updatetime" in df.columns:
                        df["updatetime"] = pd.to_datetime(
                            df["updatetime"], utc=True, errors="coerce"
                        )
                        update_column = "updatetime"
                    elif "observationdocument_recordeddtm" in df.columns:
                        df["observationdocument_recordeddtm"] = pd.to_datetime(
                            df["observationdocument_recordeddtm"],
                            utc=True,
                            errors="coerce",
                        )
                        update_column = "observationdocument_recordeddtm"
                    elif "order_entered" in df.columns:
                        df["order_entered"] = pd.to_datetime(
                            df["order_entered"], utc=True, errors="coerce"
                        )
                        update_column = "order_entered"
                    elif "basicobs_entered" in df.columns:
                        df["basicobs_entered"] = pd.to_datetime(
                            df["basicobs_entered"], utc=True, errors="coerce"
                        )
                        update_column = "basicobs_entered"
                    else:
                        print(
                            f"Warning: Neither 'updatetime', 'observationdocument_recordeddtm', 'order_entered', nor 'basicobs_entered' found in {file_path}"
                        )

                    # Drop rows with NaT values in the updated column
                    df = df.dropna(subset=[update_column])

                    if verbosity:
                        print(f"Updating CSV file based on {update_column}")

                    df[update_column] = pd.to_datetime(
                        df[update_column], utc=True)
                    filter_condition = (
                        df[update_column] > filter_date
                        if filter_type == "after"
                        else df[update_column] < filter_date
                    )
                    filtered_df = df[filter_condition]
                    filtered_df.to_csv(file_path, index=False)

                    if verbosity:
                        print("CSV file updated successfully")


def retrieve_pat_annots_mct_epr(
    client_idcode,
    config_obj,
    columns_epr=None,
    columns_mct=None,
    columns_to=None,
    columns_report=None,
    merge_columns=True,
):
    """Retrieves and merges annotation data for a single patient from multiple sources.

    This function reads annotation data for a specified patient from four potential
    sources: EPR annotations, MCT annotations, textual observations annotations, and reports annotations.
    It loads the corresponding CSV files, optionally selecting specific columns,
    and concatenates them into a single DataFrame. It can also merge related
    columns (e.g., timestamps, content) to create a more unified dataset.

    Args:
        client_idcode (str): The unique identifier for the patient.
        config_obj (object): A configuration object containing paths to the
            various annotation batch files.
        columns_epr (list, optional): A list of columns to load from the EPR annotations CSV. Defaults to None (all columns).
        columns_mct (list, optional): A list of columns to load from the MCT annotations CSV. Defaults to None (all columns).
        columns_to (list, optional): A list of columns to load from the textual observations annotations CSV. Defaults to None (all columns).
        columns_report (list, optional): A list of columns to load from the reports annotations CSV. Defaults to None (all columns).
        merge_columns (bool, optional): If True, attempts to merge corresponding
            columns (e.g., timestamps, content) from the different sources into a unified set of
            columns. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame containing the concatenated and optionally
            merged annotation data for the patient. Returns an empty
            DataFrame if no data is found for the patient in any of the sources.
    """
    # Define file paths based on the client_idcode and config paths
    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path
    pre_document_annotation_batch_path_mct = (
        config_obj.pre_document_annotation_batch_path_mct
    )
    pre_textual_obs_annotation_batch_path = (
        config_obj.pre_textual_obs_annotation_batch_path
    )
    pre_document_annotation_batch_path_reports = (
        config_obj.pre_document_annotation_batch_path_reports
    )

    epr_file_path = f"{pre_document_annotation_batch_path}/{client_idcode}.csv"
    mct_file_path = f"{pre_document_annotation_batch_path_mct}/{client_idcode}.csv"
    textual_obs_files_path = (
        f"{pre_textual_obs_annotation_batch_path}/{client_idcode}.csv"
    )
    report_file_path = (
        f"{pre_document_annotation_batch_path_reports}/{client_idcode}.csv"
    )

    # Initialize DataFrames
    dfa = pd.DataFrame()
    dfa_mct = pd.DataFrame()
    dfa_to = pd.DataFrame()
    dfr = pd.DataFrame()

    # Load data if files exist
    if os.path.exists(epr_file_path):
        dfa = pd.read_csv(epr_file_path, usecols=columns_epr)
        dfa["annotation_batch_source"] = "epr"

    if os.path.exists(mct_file_path):
        dfa_mct = pd.read_csv(mct_file_path, usecols=columns_mct)
        dfa_mct["annotation_batch_source"] = "mct"

    if os.path.exists(textual_obs_files_path):
        dfa_to = pd.read_csv(textual_obs_files_path, usecols=columns_to)
        dfa_to["annotation_batch_source"] = "textual_obs"

    if os.path.exists(report_file_path):
        dfr = pd.read_csv(report_file_path, usecols=columns_report)
        dfr["annotation_batch_source"] = "report"

    # Concatenate all dataframes
    all_annots = pd.concat([dfa, dfa_mct, dfa_to, dfr],
                           axis=0, ignore_index=True)

    # Merge columns if required
    if merge_columns and not all_annots.empty:
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

    return all_annots


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
        df[column].astype(str).str.contains(
            "|".join(str_lst), case=False, na=False)
    )


def filter_dataframe_n_lists(df, column_name, n_lists):
    """Filters a DataFrame to include rows where the value in a specified column
    is present in *all* of the provided lists.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to filter.
        n_lists (list of list): A list of lists. A row is kept only if the value
            in `column_name` is present in every sublist within `n_lists`.

    Returns:
        pd.DataFrame: The filtered DataFrame.
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
    config_obj: Optional[object] = None,
    annot_filter_arguments: Optional[dict] = None,
):
    """Retrieves and filters target annotations for a list of patients.

    This function iterates through a list of patient IDs, retrieves their annotations,
    applies optional annotation filters, and then filters the annotations to include
    only those where the 'cui' (Concept Unique Identifier) is present in all of the
    provided `n_lists`. The results are concatenated into a single DataFrame and saved.

    Args:
        all_pat_list (List[str]): A list of patient IDs to process.
        n_lists (List[List[int]]): A list of lists of CUI codes. Annotations are kept if their CUI is in all sublists.
        config_obj (Optional[object], optional): A configuration object. Defaults to None.
        annot_filter_arguments (Optional[dict], optional): Arguments to filter annotations. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing all target annotations.
    """
    results_df = pd.DataFrame()

    for sublist in n_lists:
        for i, element in enumerate(sublist):
            sublist[i] = int(element)

    for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):

        current_pat_idcode = all_pat_list[i]

        all_annots = retrieve_pat_annots_mct_epr(
            current_pat_idcode, config_obj)

        all_annots.dropna(subset="acc", inplace=True)

        if annot_filter_arguments is not None:
            all_annots = filter_annot_dataframe2(
                all_annots, annot_filter_arguments)

        annots_to_return = filter_dataframe_n_lists(all_annots, "cui", n_lists)

        if annots_to_return:
            filtered_df = all_annots[all_annots["cui"].isin(
                list(chain(*n_lists)))]
            results_df = pd.concat([results_df, filtered_df])

    results_df.to_csv("all_target_annots.csv")

    return results_df


def extract_datetime_from_binary_columns(df):
    """Extracts datetime values from binary columns representing dates in a DataFrame.

    Binary columns are expected to have names like `(YYYY, MM, DD)_date_time_stamp`,
    where a value of 1 indicates the presence of that date.

    Args:
        df (pd.DataFrame): The DataFrame containing the binary columns with '_date_time_stamp' in column names.

    Returns:
        list: A list of datetime values extracted from the binary columns.
    """
    # Extracting date columns with '_date_time_stamp' in their names
    date_columns = [
        col.strip("()").split(")")[0] for col in df.columns if "_date_time_stamp" in col
    ]

    date_columns_raw = [col for col in df.columns if "_date_time_stamp" in col]

    date_time_column_values = []

    # Iterate over each row in the DataFrame
    for index, row in tqdm(df.iterrows(), total=len(df)):

        # Iterate over each date column
        for i in range(len(date_columns)):
            if row[date_columns_raw[i]] == 1:  # Check if the value in the column is 1
                date_string = date_columns[i]

                # Split the date string and convert each part to integer
                date_parts = [int(part) for part in date_string.split(", ")]

                # Unpack date_parts and create a datetime object
                formatted_date = datetime(*date_parts)

                # Append the datetime object to the list
                date_time_column_values.append(formatted_date)

    df["datetime"] = date_time_column_values
    return df


# Example usage:
# Assuming df_copy is defined
# df = extract_datetime_from_binary_columns(df)


def extract_datetime_from_binary_columns_chunk_reader(filepath):
    """Extracts datetime values from binary columns representing dates from a CSV file.

    This function reads a CSV file in chunks, extracts datetime values from binary columns
    (e.g., `(YYYY, MM, DD)_date_time_stamp`), and appends a 'datetime' column to the DataFrame.

    Args:
        filepath (str): The file path to the CSV file containing the binary columns with '_date_time_stamp' in column names.

    Returns:
        pd.DataFrame: The DataFrame with the 'datetime' column appended.
    """
    chunk_size = 1000  # Adjust this value based on your RAM capacity
    date_time_column_values = []

    # Iterate over DataFrame chunks
    for chunk in pd.read_csv(filepath, chunksize=chunk_size):
        # Extracting date columns with '_date_time_stamp' in their names
        date_columns = [
            col.strip("()").split(")")[0]
            for col in chunk.columns
            if "_date_time_stamp" in col
        ]
        date_columns_raw = [
            col for col in chunk.columns if "_date_time_stamp" in col]

        # Iterate over each row in the chunk
        for index, row in tqdm(chunk.iterrows(), total=len(chunk)):
            # Iterate over each date column
            for i in range(len(date_columns)):
                if (
                    row[date_columns_raw[i]] == 1
                ):  # Check if the value in the column is 1
                    date_string = date_columns[i]

                    # Split the date string and convert each part to integer
                    date_parts = [int(part)
                                  for part in date_string.split(", ")]

                    # Unpack date_parts and create a datetime object
                    formatted_date = datetime(*date_parts)

                    # Append the datetime object to the list
                    date_time_column_values.append(formatted_date)

    # Add 'datetime' column to the chunk DataFrame
    chunk["datetime"] = date_time_column_values
    return chunk


def drop_columns_with_all_nan(df):
    """Drops columns from a DataFrame where all values are NaN or None.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        tuple: A tuple containing:
            - pd.DataFrame: The DataFrame with columns containing all NaNs dropped.
            - pd.Index: An Index of the column names that were dropped.
    """
    # Identify columns where all values are NaN or None
    nan_columns = df.columns[df.isna().all()]

    # Drop columns with all NaN or None values
    df.drop(columns=nan_columns, inplace=True)

    return df, nan_columns


def save_missing_values_pickle(df, out_file_path, overwrite=False):
    """Calculates the percentage of missing values for each column in a DataFrame
    and saves the result as a pickle file.

    Args:
        df (pd.DataFrame): The input DataFrame.
        out_file_path (str): The full path to the output file (e.g., "path/to/data.csv").
            The pickle file will be saved in the same directory with a `_missing_dict.pickle` suffix.
        overwrite (bool, optional): If True, overwrites the pickle file if it already exists. Defaults to False.

    Returns:
        None
    """
    # Calculate percentage of missing values for each column
    missing_percentages = (df.isnull().sum() / len(df)) * 100

    # Create dictionary with column names as keys and percentage missing as values
    missing_dict = missing_percentages.to_dict()

    # Extracting the directory and filename from out_file_path
    output_dir = os.path.dirname(out_file_path)
    filename = os.path.basename(out_file_path)

    # Constructing the output pickle file path
    pickle_filename = os.path.splitext(filename)[0] + "_missing_dict.pickle"
    pickle_file_path = os.path.join(output_dir, pickle_filename)

    # Check if the pickle file already exists
    if os.path.exists(pickle_file_path) and not overwrite:
        print(
            f"Skipping saving as '{pickle_filename}' already exists and overwrite is set to False."
        )
    else:
        # Write the dictionary to the pickle file
        with open(pickle_file_path, "wb") as f:
            pickle.dump(missing_dict, f)

        print(f"Missing values dictionary written to: {pickle_file_path}")


# Example usage:
# Provide the path to your CSV file as an argument to the function

# save_missing_values_pickle(df, out_file_path)


def convert_true_to_float(
    df,
    columns=[
        "census_black_african_caribbean_or_black_british",
        "census_mixed_or_multiple_ethnic_groups",
        "census_white",
        "census_asian_or_asian_british",
        "census_other_ethnic_group",
    ],
):
    """Converts 'True' strings to float 1.0 in specified columns and
    ensures those columns are of float datatype.

    Args:
        df (pandas.DataFrame): The DataFrame to operate on.
        columns (list, optional): List of column names to convert. Defaults to a predefined list of census columns.

    Returns:
        pandas.DataFrame: DataFrame with specified columns converted.
    """
    # Replace 'True' with 1.0 in the specified columns
    df[columns] = df[columns].replace("True", 1.0)

    # Convert the columns to float datatype
    df[columns] = df[columns].astype(float)

    return df


def impute_datetime(
    df,
    datetime_column="datetime",
    patient_column="client_idcode",
    forward=True,
    backward=True,
    mean_impute=True,
    verbose=False,
):
    """Imputes missing datetime values in a DataFrame based on patient ID and temporal order.

    This function sorts the DataFrame by patient ID and datetime, then applies
    forward-fill, backward-fill, and mean imputation (for remaining NaNs) to the
    datetime column.

    Args:
        df (pd.DataFrame): The input DataFrame.
        datetime_column (str, optional): The name of the datetime column to impute. Defaults to "datetime".
        patient_column (str, optional): The name of the patient ID column for grouping. Defaults to "client_idcode".
        forward (bool, optional): If True, performs forward-fill imputation. Defaults to True.
        backward (bool, optional): If True, performs backward-fill imputation. Defaults to True.
        mean_impute (bool, optional): If True, performs mean imputation for any remaining NaNs. Defaults to True.
        verbose (bool, optional): If True, prints verbose messages. Defaults to False.
    """
    start_time = time.time()
    if verbose:
        print("Converting datetime column to datetime type...")
    df[datetime_column] = pd.to_datetime(df[datetime_column])

    if verbose:
        print("Sorting DataFrame by patient_column and datetime_column...")
    df = df.sort_values(by=[patient_column, datetime_column])
    end_time = time.time()
    if verbose:
        print(
            "Sorting complete. Time taken: {:.2f} seconds.".format(
                end_time - start_time
            )
        )

    if forward:
        start_time = time.time()
        if verbose:
            print("Forward filling missing values per patient_column...")
        df = df.groupby(patient_column).ffill()
        end_time = time.time()
        if verbose:
            print(
                "Forward filling complete. Time taken: {:.2f} seconds.".format(
                    end_time - start_time
                )
            )

    if backward:
        start_time = time.time()
        if verbose:
            print("Backward filling missing values per patient_column...")
        df = df.groupby(patient_column).bfill()
        end_time = time.time()
        if verbose:
            print(
                "Backward filling complete. Time taken: {:.2f} seconds.".format(
                    end_time - start_time
                )
            )

    if mean_impute:
        start_time = time.time()
        if verbose:
            print("Mean imputing missing values...")
        df = df.fillna(df.mean())
        end_time = time.time()
        if verbose:
            print(
                "Mean imputing complete. Time taken: {:.2f} seconds.".format(
                    end_time - start_time
                )
            )

    if verbose:
        print("Imputation complete.")

    return df


def impute_dataframe(
    df,
    verbose=True,
    datetime_column="datetime",
    patient_column="client_idcode",
    forward=True,
    backward=True,
    mean_impute: bool = True,
):
    """Imputes missing numeric values in a DataFrame based on patient ID and temporal order.

    This function sorts the DataFrame by patient ID and datetime, then applies
    forward-fill, backward-fill, and mean imputation (for remaining NaNs) to all
    numeric columns.

    Args:
        df (pd.DataFrame): The input DataFrame.
        verbose (bool, optional): If True, prints verbose messages. Defaults to True.
        datetime_column (str, optional): The name of the datetime column for sorting. Defaults to "datetime".
        patient_column (str, optional): The name of the patient ID column for grouping. Defaults to "client_idcode".
        forward (bool, optional): If True, performs forward-fill imputation. Defaults to True.
        backward (bool, optional): If True, performs backward-fill imputation. Defaults to True.
        mean_impute (bool, optional): If True, performs mean imputation for any remaining NaNs. Defaults to True.
    """
    start_time = time.time()

    numeric_columns = df.select_dtypes(include="number").columns.tolist()

    df = df.sort_values(by=[patient_column, datetime_column])

    for i in tqdm(range(0, len(numeric_columns))):
        if forward:
            df[numeric_columns[i]] = df.groupby(patient_column, as_index=True)[
                numeric_columns[i]
            ].ffill()
        if backward:
            df[numeric_columns[i]] = df.groupby(patient_column, as_index=True)[
                numeric_columns[i]
            ].bfill()
        if mean_impute:
            df[numeric_columns[i]] = df[numeric_columns[i]].fillna(
                df[numeric_columns[i]].mean()
            )

    if verbose:
        print("Preprocessing took: %s seconds" % (time.time() - start_time))

    return df


def missing_percentage_df(dataframe):
    """Calculate the percentage of missing values in each column of a DataFrame.

    Args:
        dataframe (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing two columns: 'Column' (column names) and 'MissingPercentage' (percentage of missing values).
    """
    # Calculate percentage of missing values
    missing_percentage = dataframe.isnull().mean() * 100

    # Create a new DataFrame to store the missing percentage
    missing_df = pd.DataFrame(
        {
            "Column": missing_percentage.index,
            "MissingPercentage": missing_percentage.values,
        }
    )

    return missing_df


def aggregate_dataframe_mean(
    df: pd.DataFrame, group_by_column: str = "client_idcode"
) -> pd.DataFrame:
    """Aggregates a DataFrame by a grouping column.

    For each group, it calculates the mean for numeric columns and takes the
    first value for non-numeric columns.

    Args:
        df (pd.DataFrame): The input DataFrame to aggregate.
        group_by_column (str, optional): The column to group by.
            Defaults to "client_idcode".

    Returns:
        pd.DataFrame: The aggregated DataFrame.
    """

    def custom_aggregation(x):
        agg_values = {}
        for col in x.columns:
            if pd.api.types.is_numeric_dtype(x[col].dtype):
                agg_values[col] = x[col].mean()
            else:
                agg_values[col] = x[col].iloc[0]
        return pd.Series(agg_values)

    grouped = df.groupby(group_by_column)
    tqdm.pandas(desc="Aggregating")
    aggregated_df = grouped.progress_apply(custom_aggregation)

    return aggregated_df


def collapse_df_to_mean(
    df, output_filename="output.csv", client_idcode_string="client_idcode"
):
    """Collapses a DataFrame to calculate mean values for numeric columns and retains the first non-numeric value for non-numeric columns
    for each unique client_idcode.

    Args:
        df (pd.DataFrame): Input DataFrame containing client_idcode and other columns.
        output_filename (str, optional): Name of the output file to save the processed DataFrame. Defaults to 'output.csv'.
        client_idcode_string (str, optional): Name of the client_idcode column in the DataFrame. Defaults to 'client_idcode'.

    Returns:
        None: Saves the processed DataFrame to the specified output file.
    """
    # Check if the output file exists
    if os.path.exists(output_filename):
        # Read the output DataFrame from the existing file
        output_df = pd.read_csv(output_filename)
    else:
        # Initialize an empty DataFrame for output
        output_df = pd.DataFrame(columns=df.columns)
        # Write the header to the output file
        # output_df.to_csv(output_filename, index=False)

    # Function to check if a row exists in the output DataFrame
    def row_exists(client_idcode):
        return any(output_df[client_idcode_string] == client_idcode)

    len_out_df = len(output_df)
    started = False
    # Iterate over unique client_idcodes with tqdm progress bar
    for client_idcode in tqdm(
        df[client_idcode_string].unique(),
        desc="Processing",
        total=len(df[client_idcode_string].unique()),
    ):
        # Check if the row already exists in output_df
        if not row_exists(client_idcode):
            # Filter rows for the current client_idcode
            client_df = df[df[client_idcode_string] == client_idcode]

            # Initialize a dictionary to store mean values and first non-numeric values
            mean_values = {}
            first_non_numeric_values = {}

            # Iterate over columns
            for column in df.columns:
                # Check if the column is numeric
                if pd.api.types.is_numeric_dtype(df[column]):
                    mean_values[column] = client_df[column].mean()
                else:
                    first_non_numeric_values[column] = client_df[column].iloc[0]

            # Append mean values and first non-numeric values to output_df
            row_data = {
                client_idcode_string: client_idcode,
                **mean_values,
                **first_non_numeric_values,
            }
            if started == False and len_out_df == 0:
                pd.DataFrame([row_data]).to_csv(
                    output_filename, mode="a", index=False, header=True
                )
                started = True
            else:
                pd.DataFrame([row_data]).to_csv(
                    output_filename, mode="a", index=False, header=False
                )


def plot_missing_pattern_bloods(dfb):
    """Plots the number of missing client_idcodes for the top 50 most frequent
    'basicobs_itemname_analysed' values in the given dataframe dfb.

    Args:
        dfb (pd.DataFrame): DataFrame containing the columns 'client_idcode' and 'basicobs_itemname_analysed'.

    """

    # Step 1: Identify the top 50 basicobs_itemname_analysed by frequency
    top_items = dfb["basicobs_itemname_analysed"].value_counts().nlargest(
        50).index

    # Filter the data for these top items
    filtered_dfb = dfb[dfb["basicobs_itemname_analysed"].isin(top_items)]

    # Step 2: Group by basicobs_itemname_analysed and get the unique client_idcodes
    clients_per_item = filtered_dfb.groupby("basicobs_itemname_analysed")[
        "client_idcode"
    ].apply(set)

    # Get all unique client_idcodes
    all_clients = set(dfb["client_idcode"])

    # Step 3: Calculate missing client_idcodes for each top basicobs_itemname_analysed
    missing_clients_counts = {
        item: len(all_clients - clients) for item, clients in clients_per_item.items()
    }

    # Convert to DataFrame for plotting
    missing_clients_df = pd.DataFrame(
        list(missing_clients_counts.items()),
        columns=["basicobs_itemname_analysed", "missing_client_count"],
    ).sort_values(by="missing_client_count", ascending=False)

    # Step 4: Plot the results
    plt.figure(figsize=(10, 8))
    plt.barh(
        missing_clients_df["basicobs_itemname_analysed"],
        missing_clients_df["missing_client_count"],
        color="skyblue",
    )
    plt.xlabel("Number of Missing Client ID Codes")
    plt.ylabel("Basicobs Item Name Analysed")
    plt.title("Missing Client ID Codes per Top 50 Basicobs Item Names")
    plt.gca().invert_yaxis()  # Invert y-axis for better readability
    plt.tight_layout()
    plt.show()


# Example usage:
# plot_missing_pattern_bloods(dfb)
