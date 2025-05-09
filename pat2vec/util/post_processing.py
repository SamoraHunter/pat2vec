import csv
import os
import pickle
import shutil
import sys
from datetime import datetime
from itertools import chain
from multiprocessing import Pool, cpu_count
from pathlib import Path
import time
from typing import Dict, List, Optional, Union
import matplotlib.pyplot as plt

import pandas as pd
from IPython.display import display
from tqdm import tqdm


def count_files(path):
    count = 0
    for root, dirs, files in os.walk(path):
        count += len(files)
    return count


def process_csv_files(
    input_path,
    out_folder="outputs",
    output_filename_suffix="concatenated_output",
    part_size=336,
    sample_size=None,
    append_timestamp_column=False,
):
    """
    Concatenate multiple CSV files from a given input path and save the result to a specified output path.

    Parameters:
    - input_path (str): The path where the CSV files are located.
    - output_path (str): The path to save the concatenated CSV file.
    - out_folder (str): The folder name for the output CSV file. Default is 'outputs'.
    - output_filename_suffix (str): The suffix for the output CSV file name. Default is 'concatenated_output'.
    - curate_columns (bool): If True, use a curated list of columns. Default is False.
    - sample_size (int): Number of files to sample. If None, use all files. Default is None.
    - part_size (int): Size of parts for processing files in chunks. Default is 336.

    Returns:
    - None: The function saves the concatenated data to the specified output path.
    """

    curate_columns = False

    # Specify the directory where your CSV files are located
    all_file_paths = [
        os.path.join(dp, f)
        for dp, dn, filenames in os.walk(input_path)
        for f in filenames
        if os.path.splitext(f)[1] == ".csv"
    ]
    if type(sample_size) == str or sample_size == None:
        if sample_size == None or sample_size.lower() == "all":
            sample_size = len(all_file_paths)

    # Create an output CSV file to hold the concatenated data
    output_file = os.path.join(
        out_folder, f"concatenated_data_{output_filename_suffix}.csv"
    )

    # Keep track of all unique column names found across all CSV files
    unique_columns = set()

    # Sample files if sample_size is provided
    all_files = all_file_paths if sample_size is None else all_file_paths[:sample_size]

    # Create a dictionary to hold the concatenated data with the unique columns as keys
    concatenated_data = {column: [] for column in unique_columns}

    # Loop through each CSV file and read its data
    if not curate_columns:
        for file in all_files:
            if file.endswith(".csv"):
                with open(file, "r", newline="") as infile:
                    reader = csv.reader(infile)
                    try:
                        # Get the header of the current CSV file
                        header = next(reader)
                        # Add all column names to the unique_columns set
                        unique_columns.update(header)
                    except StopIteration:
                        pass

    # Check if the output file already exists
    if os.path.exists(output_file):
        # If it exists, append datetime stamp and "overwritten" to the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name, extension = os.path.splitext(output_file)
        new_output_file = f"{base_name}_{timestamp}_overwritten{extension}"
        print(
            f"Warning: Output file already exists. Renaming {output_file} to {new_output_file}"
        )
        os.rename(output_file, new_output_file)
    else:
        new_output_file = output_file

    # Create a header and write it to the output CSV file
    with open(output_file, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=unique_columns)
        writer.writeheader()

    # Loop through each CSV file again and concatenate its data to the dictionary
    for part_chunk in tqdm(range(0, len(all_files), part_size)):
        # Reset the concatenated_data dictionary for each part chunk
        concatenated_data = {column: [] for column in unique_columns}

        # Loop through each CSV file again and concatenate its data to the dictionary
        for file in all_files[part_chunk : part_chunk + part_size]:
            if file.endswith(".csv"):
                with open(file, "r", newline="") as infile:
                    reader = csv.DictReader(infile)
                    # Loop through each row in the current CSV file
                    for row in reader:
                        # Add each value to the appropriate column in the dictionary
                        for column in unique_columns:
                            concatenated_data[column].append(row.get(column, ""))

        # Append the concatenated data to the output CSV file
        with open(output_file, "a", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=unique_columns)
            for i in range(len(concatenated_data[next(iter(concatenated_data))])):
                writer.writerow(
                    {column: concatenated_data[column][i] for column in unique_columns}
                )

    if append_timestamp_column:
        print("Reading results and appending updatetime column")
        df = pd.read_csv(output_file)

        df = extract_datetime_to_column(df)

        df.to_csv(output_file)

    print(f"Concatenated data saved to {output_file}")

    return output_file


# Example Usage:
# concatenate_csv_files('/home/cogstack/samora/_data/HAEM_AG11193_3/new_project/current_pat_lines_parts', 'output_path_here')


def extract_datetime_to_column(df, drop=True):
    """
    Extracts datetime information from specified columns and creates a new column.

    Parameters:
    - df (pandas.DataFrame): The DataFrame containing the datetime information in specific columns.

    Returns:
    - pandas.DataFrame: The DataFrame with a new column 'extracted_datetime_stamp' containing the extracted datetime values.
    """

    # Initialize the new column
    df["extracted_datetime_stamp"] = pd.to_datetime("")

    # Iterate through rows using tqdm for progress bar
    for index, row in tqdm(df.iterrows(), total=len(df)):
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
    print(df["extracted_datetime_stamp"].value_counts())

    if drop:
        columns_to_drop = [col for col in df.columns if "date_time_stamp" in col]

        # Drop columns
        df = df.drop(columns=columns_to_drop)

    return df


def filter_annot_dataframe2(dataframe, filter_args):
    """
    Filter a DataFrame based on specified filter arguments.

    Parameters:
    - dataframe: pandas DataFrame
    - filter_args: dict
        A dictionary containing filter arguments.

    Returns:
    - pandas DataFrame
        The filtered DataFrame.
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
                    dataframe[column].isin(value)
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
    """
    Filter annotation dataframe based on specified criteria.

    Parameters:
    - cui_filter (bool): Whether to filter by CUI codes.
    - meta_annot_filter (bool): Whether to apply meta annotation filtering.
    - pat_list (list): List of patient identifiers.
    - config_obj (ConfigObject): Configuration object containing necessary parameters.
    - filter_custom_args (dict): Custom filter arguments.
    - cui_code_list (list): List of CUI codes for filtering.

    Returns:
    - pd.DataFrame: Filtered annotation dataframe.
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
    current_pat_idcode: str, project_name="new_project", verbosity=0, config_obj=None
) -> None:

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
    part_chunk, all_files, part_size, unique_columns = args
    concatenated_data = {column: [] for column in unique_columns}
    for file in all_files[part_chunk : part_chunk + part_size]:
        if file.endswith(".csv"):
            with open(file, "r", newline="") as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    for column in unique_columns:
                        concatenated_data[column].append(row.get(column, ""))
    return concatenated_data


def process_csv_files_multi(
    input_path,
    out_folder="outputs",
    output_filename_suffix="concatenated_output",
    part_size=336,
    sample_size=None,
    append_timestamp_column=False,
    n_proc=None,
):
    curate_columns = False

    all_file_paths = [
        os.path.join(dp, f)
        for dp, dn, filenames in os.walk(input_path)
        for f in filenames
        if os.path.splitext(f)[1] == ".csv"
    ]

    if type(sample_size) == str or sample_size is None:
        if sample_size is None or sample_size.lower() == "all":
            sample_size = len(all_file_paths)

    output_file = os.path.join(
        out_folder, f"concatenated_data_{output_filename_suffix}.csv"
    )

    unique_columns = set()

    all_files = all_file_paths if sample_size is None else all_file_paths[:sample_size]

    print("all files size", len(all_files))

    if not curate_columns:
        for file in tqdm(all_files):
            if file.endswith(".csv"):
                with open(file, "r", newline="") as infile:
                    reader = csv.reader(infile)
                    try:
                        header = next(reader)
                        unique_columns.update(header)
                    except StopIteration:
                        pass

    if os.path.exists(output_file):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name, extension = os.path.splitext(output_file)
        new_output_file = f"{base_name}_{timestamp}_overwritten{extension}"
        print(
            f"Warning: Output file already exists. Renaming {output_file} to {new_output_file}"
        )
        os.rename(output_file, new_output_file)
    else:
        new_output_file = output_file

    unique_columns = list(unique_columns)
    unique_columns.sort(key=lambda x: x != "client_idcode")

    with open(output_file, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=unique_columns)
        writer.writeheader()

    # Get the number of available CPU cores
    available_cores = cpu_count()

    # Set the desired number of processes (e.g., half of the available cores)
    desired_half_processes = available_cores // 2

    if n_proc != None:
        if n_proc == "all":
            n_proc_val = available_cores
        if n_proc == "half":
            n_proc_val = desired_half_processes
        elif type(n_proc) == int:
            n_proc_val = n_proc
    print("desried cores:", n_proc_val)

    with Pool(processes=n_proc_val) as pool:
        args_list = [
            (i, all_files, part_size, unique_columns)
            for i in range(0, len(all_files), part_size)
        ]
        results = list(
            tqdm(pool.imap(process_chunk, args_list), total=len(all_files) // part_size)
        )

    with open(output_file, "a", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=unique_columns)
        for result in tqdm(results, desc="Writing lines..."):
            for i in range(len(result[next(iter(result))])):
                writer.writerow(
                    {column: result[column][i] for column in unique_columns}
                )

    if append_timestamp_column:
        print("Reading results and appending updatetime column")
        df = pd.read_csv(output_file)
        df = extract_datetime_to_column(df)
        df.to_csv(output_file)

    print(f"Concatenated data saved to {output_file}")

    return output_file


def join_icd10_codes_to_annot(df, inner=False):

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

    mfp = "../../snomed_to_icd10_opcs4/map.csv"  # ../home/cogstack/samora/_data/gloabl_files/

    mdf = pd.read_csv(mfp)

    if inner == True:
        result = pd.merge(df, mdf, left_on="cui", right_on="conceptId", how="inner")

    else:
        result = pd.merge(df, mdf, left_on="cui", right_on="conceptId", how="left")

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
    """
    Filter a dataframe based on a filter_column and filter_list, and return either the earliest or latest rows.

    Parameters:
    - dataframe (pd.DataFrame): Input dataframe.
    - filter_list (list): List of values to filter the dataframe.
    - verbosity (int): If True, print additional information during execution.
    - time_column (str): Column representing time, used for sorting if specified.
    - filter_column (str): Column used for filtering based on filter_list.
    - mode (str): Either 'earliest' or 'latest' to specify the rows to return.
    - n_rows (int): Number of rows to return if they exist.

    Returns:
    - pd.DataFrame: Filtered and selected rows from the input dataframe.
    """
    if not all(arg is not None for arg in [dataframe, filter_list, filter_column]):
        raise ValueError(
            "Please provide a valid dataframe, filter_list, and filter_column."
        )

    if filter_column not in dataframe.columns:
        raise ValueError(f"{filter_column} not found in the dataframe columns.")

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
    """
    Filter an annotation DataFrame based on a list of CUI codes and a specified mode.

    Parameters:
    - dataframe (pd.DataFrame): The input DataFrame.
    - filter_list (list): List of CUI codes to filter the DataFrame.
    - filter_column (str): The column containing filter. Default is 'cui'.
    - mode (str): Specifies whether to consider the earliest or latest entry for each filter. Default is "earliest".
    - temporal (str): Specifies whether to retain entries before or after the selected mode entry. Default is "before".
    - verbosity (int): Verbosity level. 0 for no debug statements, higher values for more verbosity.
    - time_column (str): The column containing time information. Default is 'updatetime'.

    Returns:
    - pd.DataFrame: Filtered DataFrame based on the specified criteria.
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
    """
    Useful for porting project files to a new project location.

    Copy specified directories and files from the source directory to the destination directory.

    Parameters:
        source_root (str): The root directory of the source project.
        source_name (str): The name of the source project directory.
        destination (str): The destination directory.
        items_to_copy (List[str], optional): List of directories/files to copy. Defaults to None.

    Returns:
        None

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


from typing import Dict, List, Union, Optional
import pandas as pd

# pre_textual_obs_document_batch_path

# pre_textual_obs_annotation_batch_path


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

    # Retrieve necessary paths
    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path
    pre_document_annotation_batch_path_mct = (
        config_obj.pre_document_annotation_batch_path_mct
    )
    pre_textual_obs_document_batch_path = config_obj.pre_textual_obs_document_batch_path
    pre_textual_obs_annotation_batch_path = (
        config_obj.pre_textual_obs_annotation_batch_path
    )

    # Initialize dataframes
    fsr = pd.DataFrame()  # Filters DataFrame
    fsr_mct = pd.DataFrame()  # Filters MCT DataFrame
    fsr_textual_obs = pd.DataFrame()  # Filters textual_obs DataFrame

    # Read EPR annotations
    if verbose > 10:
        print("Reading EPR annotations...")
    dfa = pd.read_csv(f"{pre_document_annotation_batch_path}/{current_pat_idcode}.csv")

    if verbose > 12:
        print("Sample EPR annotations...")
        display(dfa.head())

    # Drop NaNs in columns that are necessary for filtering
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

    if verbose > 11:
        print(f"Dropping NaNs in columns: {necessary_columns}...")
    dfa = dfa.dropna(subset=necessary_columns)

    if verbose > 12:
        print("Sample of filtered EPR DataFrame:")
        display(dfa.head())

    # Apply annotation filter if provided
    if annot_filter_arguments is not None:
        if verbose > 10:
            print("Filtering EPR annotations...")
        dfa = filter_annot_dataframe2(dfa, annot_filter_arguments)

    # Filter based on filter_codes if provided
    if len(dfa) > 0:
        if verbose > 10:
            print("Filtering EPR annotations based on filter_codes...")
        fsr = filter_and_select_rows(
            dfa,
            filter_codes,
            verbosity=verbose > 0,
            time_column="updatetime",
            filter_column="cui",
            mode=mode,
            n_rows=1,
        )

    # Read MCT annotations if include_mct is True
    if include_mct:
        if verbose > 10:
            print("Reading MCT annotations...")
        dfa_mct = pd.read_csv(
            f"{pre_document_annotation_batch_path_mct}/{current_pat_idcode}.csv"
        )

        # Drop NaNs in columns that are necessary for filtering
        necessary_columns_mct = [
            "client_idcode",
            "observationdocument_recordeddtm",
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

        if verbose > 11:
            print(f"Dropping NaNs in columns: {necessary_columns_mct}...")
        dfa_mct = dfa_mct.dropna(subset=necessary_columns_mct)

        if verbose > 12:
            print("Sample of filtered MCT DataFrame:")
            display(dfa_mct.head())

        # Apply annotation filter if provided
        if annot_filter_arguments is not None:
            if verbose > 10:
                print("Filtering MCT annotations...")
            dfa_mct = filter_annot_dataframe2(dfa_mct, annot_filter_arguments)

        # Filter based on filter_codes if provided
        if len(dfa_mct) > 0:
            if verbose > 10:
                print("Filtering MCT annotations based on filter_codes...")
            fsr_mct = filter_and_select_rows(
                dfa_mct,
                filter_codes,
                verbosity=verbose > 0,
                time_column="observationdocument_recordeddtm",
                filter_column="cui",
                mode=mode,
                n_rows=1,
            )

    # Read textual_obs annotations if include_textual_obs is True
    if include_textual_obs:
        if verbose > 10:
            print("Reading textual_obs annotations...")
        dfa_textual_obs = pd.read_csv(
            f"{pre_textual_obs_annotation_batch_path}/{current_pat_idcode}.csv"
        )

        # Drop NaNs in columns that are necessary for filtering
        necessary_columns_textual_obs = [
            "client_idcode",
            "basicobs_entered",
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

        if verbose > 11:
            print(f"Dropping NaNs in columns: {necessary_columns_textual_obs}...")
        dfa_textual_obs = dfa_textual_obs.dropna(subset=necessary_columns_textual_obs)

        if verbose > 12:
            print("Sample of filtered textual_obs DataFrame:")
            display(dfa_textual_obs.head())

        # Apply annotation filter if provided
        if annot_filter_arguments is not None:
            if verbose > 10:
                print("Filtering textual_obs annotations...")
            dfa_textual_obs = filter_annot_dataframe2(
                dfa_textual_obs, annot_filter_arguments
            )

        # Filter based on filter_codes if provided
        if len(dfa_textual_obs) > 0:
            if verbose > 10:
                print("Filtering textual_obs annotations based on filter_codes...")
            fsr_textual_obs = filter_and_select_rows(
                dfa_textual_obs,
                filter_codes,
                verbosity=verbose > 0,
                time_column="basicobs_entered",
                filter_column="cui",
                mode=mode,
                n_rows=1,
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
        earliest_df = pd.DataFrame(columns=necessary_columns)
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


def filter_and_update_csv(
    target_directory, ipw_dataframe, filter_type="after", verbosity=False
):
    for _, row in ipw_dataframe.iterrows():
        client_idcode = row["client_idcode"]
        # print(client_idcode, row['updatetime'])
        # filter_date = pd.to_datetime(row['updatetime']).tz_convert('UTC')  # Convert filter_date to UTC
        filter_date = pd.to_datetime(row["updatetime"], utc=True, errors="coerce")

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

                    df[update_column] = pd.to_datetime(df[update_column], utc=True)
                    filter_condition = (
                        df[update_column] > filter_date
                        if filter_type == "after"
                        else df[update_column] < filter_date
                    )
                    filtered_df = df[filter_condition]
                    filtered_df.to_csv(file_path, index=False)

                    if verbosity:
                        print("CSV file updated successfully")


def build_ipw_dataframe(
    annot_filter_arguments=None,
    filter_codes=None,
    config_obj=None,
    mode="earliest",
    include_mct=True,
    include_textual_obs=True,
):

    df = pd.DataFrame()

    pat_list = os.listdir(config_obj.pre_document_batch_path)

    pat_list_stripped = [
        os.path.splitext(file)[0] for file in pat_list if file.endswith(".csv")
    ]

    for pat in pat_list_stripped:

        res = get_pat_ipw_record(
            current_pat_idcode=pat,
            annot_filter_arguments=annot_filter_arguments,
            filter_codes=filter_codes,
            config_obj=config_obj,
            mode=mode,
            include_mct=include_mct,  # Boolean argument to include MCT
            include_textual_obs=include_textual_obs,  # Boolean argument to include textual_obs
        )

        df = pd.concat([df, res], ignore_index=True)

    if "observationdocument_recordeddtm" in df.columns:
        df["updatetime"] = df["updatetime"].fillna(
            df["observationdocument_recordeddtm"]
        )

        if "updatetime" in df.columns:
            # Fill missing values in 'observationdocument_recordeddtm' column with values from 'updatetime'
            df["observationdocument_recordeddtm"] = df[
                "observationdocument_recordeddtm"
            ].fillna(df["updatetime"])

    return df


def retrieve_pat_annots_mct_epr(
    client_idcode,
    config_obj,
    columns_epr=None,
    columns_mct=None,
    columns_to=None,
    columns_report=None,
    merge_columns=True,
):
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
    all_annots = pd.concat([dfa, dfa_mct, dfa_to, dfr], axis=0, ignore_index=True)

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


# def retrieve_pat_annots_mct_epr(
#     client_idcode,
#     config_obj,
#     columns_epr=None,
#     columns_mct=None,
#     merge_time_columns=True,
# ):
#     """
#     Retrieve patient annotations for Electronic Patient Record (EPR) and Medical Chart (MCT).

#     Args:
#         client_idcode (str): The client's unique identifier code.
#         config_obj: The configuration object containing paths for annotation batches.
#         columns_epr (list, optional): List of columns to be used from the EPR annotation batch CSV.
#         columns_mct (list, optional): List of columns to be used from the MCT annotation batch CSV.
#         merge_time_columns (bool, optional): Whether to merge time columns. Default is True.

#     Returns:
#         pandas.DataFrame: A DataFrame containing combined patient annotations from EPR and MCT.

#     Note:
#         The function assumes that the EPR and MCT annotation documents are stored in separate CSV files
#         and are accessible via the provided paths in the `config_obj`.
#     """
#     pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path
#     pre_document_annotation_batch_path_mct = (
#         config_obj.pre_document_annotation_batch_path_mct
#     )

#     epr_file_path = f"{pre_document_annotation_batch_path}/{client_idcode}.csv"
#     mct_file_path = f"{pre_document_annotation_batch_path_mct}/{client_idcode}.csv"

#     dfa = pd.DataFrame()
#     dfa_mct = pd.DataFrame()

#     if os.path.exists(epr_file_path):
#         dfa = pd.read_csv(epr_file_path, usecols=columns_epr)
#     else:
#         pass
#         # print(
#         #     f"EPR file for client_idcode {client_idcode} does not exist. Skipping EPR data."
#         # )

#     if os.path.exists(mct_file_path):
#         dfa_mct = pd.read_csv(mct_file_path, usecols=columns_mct)
#     else:
#         pass
#         # print(
#         #     f"MCT file for client_idcode {client_idcode} does not exist. Skipping MCT data."
#         # )

#     all_annots = pd.concat([dfa, dfa_mct], ignore_index=True)

#     if merge_time_columns and not all_annots.empty:
#         try:
#             all_annots["updatetime"] = all_annots["updatetime"].fillna(
#                 all_annots["observationdocument_recordeddtm"]
#             )
#         except Exception as e:
#             print(e)

#         try:
#             all_annots["observationdocument_recordeddtm"] = all_annots[
#                 "observationdocument_recordeddtm"
#             ].fillna(all_annots["updatetime"])
#         except Exception as e:
#             print(e)

#         try:
#             all_annots["document_guid"] = all_annots["document_guid"].fillna(
#                 all_annots["observation_guid"]
#             )
#         except Exception as e:
#             print(e)

#     return all_annots


# def retrieve_pat_annots_mct_epr(
#     client_idcode,
#     config_obj,
#     columns_epr=None,
#     columns_mct=None,
#     merge_time_columns=True,
# ):
#     """
#     Retrieve patient annotations for Electronic Patient Record (EPR) and Medical Chart (MCT).

#     Args:
#         client_idcode (str): The client's unique identifier code.
#         config_obj: The configuration object containing paths for annotation batches.
#         columns_epr (list, optional): List of columns to be used from the EPR annotation batch CSV.
#         columns_mct (list, optional): List of columns to be used from the MCT annotation batch CSV.
#         merge_time_columns (bool, optional): Whether to merge time columns. Default is True.

#     Returns:
#         pandas.DataFrame: A DataFrame containing combined patient annotations from EPR and MCT.

#     Note:
#         The function assumes that the EPR and MCT annotation documents are stored in separate CSV files
#         and are accessible via the provided paths in the `config_obj`.
#     """
#     pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path
#     pre_document_annotation_batch_path_mct = (
#         config_obj.pre_document_annotation_batch_path_mct
#     )

#     dfa = pd.read_csv(
#         f"{pre_document_annotation_batch_path}/{client_idcode}.csv", usecols=columns_epr
#     )

#     dfa_mct = pd.read_csv(
#         f"{pre_document_annotation_batch_path_mct}/{client_idcode}.csv",
#         usecols=columns_mct,
#     )

#     all_annots = pd.concat([dfa, dfa_mct], ignore_index=True)

#     if merge_time_columns:
#         all_annots["updatetime"] = all_annots["updatetime"].fillna(
#             all_annots["observationdocument_recordeddtm"]
#         )
#         all_annots["observationdocument_recordeddtm"] = all_annots[
#             "observationdocument_recordeddtm"
#         ].fillna(all_annots["updatetime"])

#         all_annots["document_guid"] = all_annots["document_guid"].fillna(
#             all_annots["observation_guid"]
#         )

#     return all_annots


# def retrieve_pat_docs_mct_epr(
#     client_idcode,
#     config_obj,
#     columns_epr=None,
#     columns_mct=None,
#     columns_to=None,
#     columns_report=None,
#     merge_columns=True,
# ):
#     # Define file paths based on the client_idcode and config paths
#     """
#     Retrieve patient documents for Electronic Patient Record (EPR), Medical Chart (MCT), textual observations and reports.

#     Args:
#         client_idcode (str): The client's unique identifier code.
#         config_obj: The configuration object containing paths for document batches.
#         columns_epr (list, optional): List of columns to be used from the EPR document batch CSV.
#         columns_mct (list, optional): List of columns to be used from the MCT document batch CSV.
#         columns_to (list, optional): List of columns to be used from the textual observations document batch CSV.
#         columns_report (list, optional): List of columns to be used from the reports document batch CSV.
#         merge_columns (bool, optional): Whether to merge time columns. Default is True.

#     Returns:
#         pandas.DataFrame: A DataFrame containing combined patient documents from EPR, MCT, textual observations and reports.

#     Note:
#         The function assumes that the EPR, MCT, textual observations and reports documents are stored in separate CSV files
#         and are accessible via the provided paths in the `config_obj`.
#     """

#     pre_document_batch_path = config_obj.pre_document_batch_path
#     pre_document_batch_path_mct = config_obj.pre_document_batch_path_mct
#     pre_textual_obs_document_batch_path = config_obj.pre_textual_obs_document_batch_path
#     pre_document_batch_path_reports = config_obj.pre_document_batch_path_reports

#     epr_file_path = f"{pre_document_batch_path}/{client_idcode}.csv"
#     mct_file_path = f"{pre_document_batch_path_mct}/{client_idcode}.csv"
#     textual_obs_files_path = (
#         f"{pre_textual_obs_document_batch_path}/{client_idcode}.csv"
#     )
#     report_file_path = f"{pre_document_batch_path_reports}/{client_idcode}.csv"

#     # Initialize DataFrames
#     dfa = pd.DataFrame()
#     dfa_mct = pd.DataFrame()
#     dfa_to = pd.DataFrame()
#     dfr = pd.DataFrame()

#     # Load data if files exist
#     if os.path.exists(epr_file_path):
#         dfa = pd.read_csv(epr_file_path, usecols=columns_epr)
#         dfa["document_batch_source"] = "epr"
#         dfa.reset_index(inplace=True, drop=True)

#     if os.path.exists(mct_file_path):
#         dfa_mct = pd.read_csv(mct_file_path, usecols=columns_mct)
#         dfa_mct["document_batch_source"] = "mct"
#         dfa_mct.reset_index(inplace=True, drop=True)

#     if os.path.exists(textual_obs_files_path):
#         dfa_to = pd.read_csv(textual_obs_files_path, usecols=columns_to)
#         dfa_to["document_batch_source"] = "textual_obs"
#         dfa_to.dropna(subset=["textualObs"], inplace=True)
#         dfa_to.reset_index(inplace=True, drop=True)

#     if os.path.exists(report_file_path):
#         dfr = pd.read_csv(report_file_path, usecols=columns_report)
#         dfr["document_batch_source"] = "report"
#         dfr.reset_index(inplace=True, drop=True)

#     dfs = [dfa, dfa_mct, dfa_to, dfr]

#     # Function to ensure each DataFrame is 2D
#     def ensure_2d(dataframes):
#         for i, df in enumerate(dataframes):
#             if df.ndim > 2:
#                 print(
#                     f"DataFrame at index {i} had more than two dimensions. It has been reduced to 2D."
#                 )

#                 print(
#                     "offending df:", ["dfa", "dfa_mct", "dfa_to", "dfr"][i]
#                 )  # Reduce the DataFrame to 2D (in this case, we just keep it as-is)
#                 dataframes[i] = df.iloc[:, :]

#             # After checking, you can print or handle the DataFrame
#             # print(f"DataFrame at index {i}:\n{dataframes[i]}\n")

#     # Apply the function to the list of DataFrames
#     ensure_2d(dfs)
#     # Step 1: Get the union of all columns from all DataFrames
#     all_columns = pd.Index([])  # Start with an empty Index
#     for df in dfs:
#         all_columns = all_columns.union(df.columns)

#     # Step 2: Reindex each DataFrame to have the same columns
#     dfs_reindexed = [df.reindex(columns=all_columns) for df in dfs]

#     # Concatenate all dataframes
#     all_docs = pd.concat(dfs_reindexed, ignore_index=True)

#     # Merge columns if required
#     if merge_columns and not all_docs.empty:
#         all_docs["updatetime"] = all_docs["updatetime"].fillna(
#             all_docs["observationdocument_recordeddtm"]
#         )
#         all_docs["observationdocument_recordeddtm"] = all_docs[
#             "observationdocument_recordeddtm"
#         ].fillna(all_docs["updatetime"])

#         # Merge observation_guid to document_guid
#         all_docs["document_guid"] = all_docs["document_guid"].fillna(
#             all_docs["observation_guid"]
#         )

#         # Add obscatalogmasteritem_displayname to document_description
#         all_docs["document_description"] = all_docs["document_description"].fillna(
#             all_docs["obscatalogmasteritem_displayname"]
#         )

#         all_docs["body_analysed"] = all_docs["body_analysed"].fillna(
#             all_docs["observation_valuetext_analysed"]
#         )

#     return all_docs


# def retrieve_pat_docs_mct_epr(
#     client_idcode, config_obj, columns_epr=None, columns_mct=None, merge_columns=True
# ):
#     """
#     Retrieve patient documents for Electronic Patient Record (EPR) and Medical Chart (MCT).

#     Args:
#         client_idcode (str): The client's unique identifier code.
#         config_obj: The configuration object containing paths for document batches.
#         columns_epr (list, optional): List of columns to be used from the EPR document batch CSV.
#         columns_mct (list, optional): List of columns to be used from the MCT document batch CSV.
#         merge_columns (bool, optional): Whether to merge time columns. Default is True.

#     Returns:
#         pandas.DataFrame: A DataFrame containing combined patient documents from EPR and MCT.

#     Note:
#         The function assumes that the EPR and MCT documents are stored in separate CSV files
#         and are accessible via the provided paths in the `config_obj`.
#     """
#     pre_document_batch_path = config_obj.pre_document_batch_path
#     pre_document_batch_path_mct = config_obj.pre_document_batch_path_mct

#     epr_file_path = f"{pre_document_batch_path}/{client_idcode}.csv"
#     mct_file_path = f"{pre_document_batch_path_mct}/{client_idcode}.csv"

#     dfa = pd.DataFrame()
#     dfa_mct = pd.DataFrame()

#     if os.path.exists(epr_file_path):
#         dfa = pd.read_csv(epr_file_path, usecols=columns_epr)
#     else:
#         pass
#         # print(f"EPR file for client_idcode {client_idcode} does not exist. Skipping EPR data.")

#     if os.path.exists(mct_file_path):
#         dfa_mct = pd.read_csv(mct_file_path, usecols=columns_mct)
#     else:
#         pass
#         # print(f"MCT file for client_idcode {client_idcode} does not exist. Skipping MCT data.")

#     all_docs = pd.concat([dfa, dfa_mct], ignore_index=True)

#     if merge_columns and not all_docs.empty:
#         all_docs["updatetime"] = all_docs["updatetime"].fillna(
#             all_docs["observationdocument_recordeddtm"]
#         )
#         all_docs["observationdocument_recordeddtm"] = all_docs[
#             "observationdocument_recordeddtm"
#         ].fillna(all_docs["updatetime"])

#         # Merge observation_guid to document_guid
#         all_docs["document_guid"] = all_docs["document_guid"].fillna(
#             all_docs["observation_guid"]
#         )

#         # Add obscatalogmasteritem_displayname to document_description
#         all_docs["document_description"] = all_docs["document_description"].fillna(
#             all_docs["obscatalogmasteritem_displayname"]
#         )

#         all_docs["body_analysed"] = all_docs["body_analysed"].fillna(
#             all_docs["observation_valuetext_analysed"]
#         )

#     # Check if '_id' column exists in the DataFrame
#     if "_id" in all_docs.columns:
#         print("Debug: '_id' column exists in the DataFrame.")
#         # Fill NaN values in '_id' with values from 'id' if 'id' exists
#         if "id" in all_docs.columns:
#             all_docs["_id"] = all_docs["_id"].fillna(all_docs["id"])
#         else:
#             print("Debug: 'id' does not exist, cannot fill NaN values in '_id'.")

#         # Create 'id' column from '_id' if 'id' does not exist
#         if "id" not in all_docs.columns:
#             print("Debug: 'id' not in all_docs.columns, creating 'id' from '_id'.")
#             all_docs["id"] = all_docs["_id"]

#     else:
#         print("Debug: '_id' column does not exist in the DataFrame.")
#         # If 'id' exists, create '_id' from 'id'
#         if "id" in all_docs.columns:
#             print("Debug: Creating '_id' from 'id'.")
#             all_docs["_id"] = all_docs["id"]
#         else:
#             print("Debug: 'id' does not exist either. Cannot create '_id' or 'id'.")

#         # If '_id' is created and 'id' does not exist, create 'id' from '_id'
#         if "_id" in all_docs.columns and "id" not in all_docs.columns:
#             print("Debug: Creating 'id' from '_id'.")
#             all_docs["id"] = all_docs["_id"]

#     # Drop the specified columns
#     columns_to_drop = [
#         "observation_guid",
#         "obscatalogmasteritem_displayname",
#         "observation_valuetext_analysed",
#         "observationdocument_recordeddtm",
#         # "_id",
#     ]
#     try:
#         all_docs.drop(columns_to_drop, axis=1, inplace=True)
#     except Exception as e:
#         pass
#     # print("Debug: Specified columns dropped from the DataFrame.")

#     return all_docs


# def retrieve_pat_docs_mct_epr(
#     client_idcode, config_obj, columns_epr=None, columns_mct=None, merge_columns=True
# ):
#     """
#     Retrieve patient documents for Electronic Patient Record (EPR) and Medical Chart (MCT).

#     Args:
#         client_idcode (str): The client's unique identifier code.
#         config_obj: The configuration object containing paths for document batches.
#         columns_epr (list, optional): List of columns to be used from the EPR document batch CSV.
#         columns_mct (list, optional): List of columns to be used from the MCT document batch CSV.
#         merge_time_columns (bool, optional): Whether to merge time columns. Default is True.

#     Returns:
#         pandas.DataFrame: A DataFrame containing combined patient documents from EPR and MCT.

#     Note:
#         The function assumes that the EPR and MCT documents are stored in separate CSV files
#         and are accessible via the provided paths in the `config_obj`.
#     """
#     pre_document_batch_path = config_obj.pre_document_batch_path
#     pre_document_batch_path_mct = config_obj.pre_document_batch_path_mct

#     dfa = pd.read_csv(
#         f"{pre_document_batch_path}/{client_idcode}.csv", usecols=columns_epr
#     )

#     dfa_mct = pd.read_csv(
#         f"{pre_document_batch_path_mct}/{client_idcode}.csv", usecols=columns_mct
#     )

#     all_docs = pd.concat([dfa, dfa_mct], ignore_index=True)

#     if merge_columns:
#         all_docs["updatetime"] = all_docs["updatetime"].fillna(
#             all_docs["observationdocument_recordeddtm"]
#         )
#         all_docs["observationdocument_recordeddtm"] = all_docs[
#             "observationdocument_recordeddtm"
#         ].fillna(all_docs["updatetime"])

#     # Merge observation_guid to document_guid
#     all_docs["document_guid"] = all_docs["document_guid"].fillna(
#         all_docs["observation_guid"]
#     )

#     # Add obscatalogmasteritem_displayname to document_description
#     all_docs["document_description"] = all_docs["document_description"].fillna(
#         all_docs["obscatalogmasteritem_displayname"]
#     )

#     all_docs["body_analysed"] = all_docs["body_analysed"].fillna(
#         all_docs["observation_valuetext_analysed"]
#     )

#     # Check if '_id' column exists
#     if "_id" in all_docs.columns:
#         print("Debug: '_id' column exists in the DataFrame.")
#         # Fill NaN values in '_id' with values from 'id'
#         all_docs["_id"] = all_docs["_id"].fillna(all_docs["_id"])
#         if('id' not in all_docs.columns):
#             print("id not in all columns, making one from _id")
#             all_docs['id'] = all_docs['_id']


#     else:
#         print(
#             "Debug: '_id' column does not exist in the DataFrame. Creating '_id' from 'id'."
#         )
#         # Create '_id' column from 'id'
#         all_docs["_id"] = all_docs["id"]

#     # Drop the specified columns
#     columns_to_drop = [
#         "observation_guid",
#         "obscatalogmasteritem_displayname",
#         "observation_valuetext_analysed",
#         "observationdocument_recordeddtm",
#         "_id",
#     ]

#     all_docs.drop(columns_to_drop, axis=1, inplace=True)
#     # print("Debug: Specified columns dropped from the DataFrame.")

#     return all_docs


def check_list_presence(df, column, lst, annot_filter_arguments=None):

    if annot_filter_arguments is not None:
        df = filter_annot_dataframe2(df, annot_filter_arguments)

    str_lst = list(map(str, lst))  # Convert elements to strings
    return any(
        df[column].astype(str).str.contains("|".join(str_lst), case=False, na=False)
    )


# Check if at least one element from each list is present in the 'cui' column
# result = all(check_list_presence(all_annots, 'cui', lst) for lst in n_lists)


def filter_dataframe_n_lists(df, column_name, n_lists):
    # Create a mask for each list in n_lists
    masks = [df[column_name].isin(lst) for lst in n_lists]

    # Combine masks with logical AND to get the final mask
    final_mask = pd.concat(masks, axis=1).all(axis=1)

    # Apply the mask to the DataFrame
    filtered_df = df[final_mask]

    return filtered_df


def get_all_target_annots(
    all_pat_list, n_lists, config_obj=None, annot_filter_arguments=None
):
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

        if annots_to_return:
            filtered_df = all_annots[all_annots["cui"].isin(list(chain(*n_lists)))]
            results_df = pd.concat([results_df, filtered_df])

    results_df.to_csv("all_target_annots.csv")

    return results_df


# deprecated use post_processing_build_methods
# def build_merged_epr_mct_annot_df(config_obj):
#     directory_path = config_obj.proj_name + "/" + "merged_batches/"
#     Path(directory_path).mkdir(parents=True, exist_ok=True)

#     output_file_path = directory_path + "annots_mct_epr.csv"

#     all_pat_list = config_obj.all_pat_list

#     for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):
#         current_pat_idcode = all_pat_list[i]
#         all_annots = retrieve_pat_annots_mct_epr(current_pat_idcode, config_obj)

#         if i == 0:
#             # Create the output file and write the first batch directly
#             all_annots.to_csv(output_file_path, index=False)
#         else:
#             # Append each result to the output file
#             all_annots.to_csv(output_file_path, mode="a", header=False, index=False)


def extract_datetime_from_binary_columns(df):
    """
    Extracts datetime values from binary columns representing dates in a DataFrame.

    Parameters:
        df (DataFrame): The DataFrame containing the binary columns with '_date_time_stamp' in column names.

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
    """
    Extracts datetime values from binary columns representing dates in a DataFrame.

    Parameters:
        filepath (str): The file path to the DataFrame containing the binary columns with '_date_time_stamp' in column names.

    Returns:
        DataFrame: The DataFrame with the 'datetime' column appended.
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
        date_columns_raw = [col for col in chunk.columns if "_date_time_stamp" in col]

        # Iterate over each row in the chunk
        for index, row in tqdm(chunk.iterrows(), total=len(chunk)):
            # Iterate over each date column
            for i in range(len(date_columns)):
                if (
                    row[date_columns_raw[i]] == 1
                ):  # Check if the value in the column is 1
                    date_string = date_columns[i]

                    # Split the date string and convert each part to integer
                    date_parts = [int(part) for part in date_string.split(", ")]

                    # Unpack date_parts and create a datetime object
                    formatted_date = datetime(*date_parts)

                    # Append the datetime object to the list
                    date_time_column_values.append(formatted_date)

    # Add 'datetime' column to the chunk DataFrame
    chunk["datetime"] = date_time_column_values
    return chunk


def drop_columns_with_all_nan(df):
    # Identify columns where all values are NaN or None
    nan_columns = df.columns[df.isna().all()]

    # Drop columns with all NaN or None values
    df.drop(columns=nan_columns, inplace=True)

    return df, nan_columns


def save_missing_values_pickle(df, out_file_path, overwrite=False):
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
    """
    Convert 'True' strings to float 1.0 in specified columns and
    ensure those columns are of float datatype.

    Parameters:
        df (pandas.DataFrame): The DataFrame to operate on.
        columns (list): List of column names to convert.

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
    mean_impute=True,
):
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
    """
    Calculate the percentage of missing values in each column of a DataFrame.

    Parameters:
    dataframe (DataFrame): The input DataFrame.

    Returns:
    DataFrame: A DataFrame containing two columns: 'Column' (column names) and 'MissingPercentage' (percentage of missing values).
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


def aggregate_dataframe_mean(df, group_by_column="client_idcode"):
    # Convert non-float columns to string
    # non_float_columns = df.select_dtypes(include=['object']).columns
    # df[non_float_columns] = df[non_float_columns].astype(str)

    # Define aggregation function
    def custom_aggregation(x):
        agg_values = {}
        for col in x.columns:
            if pd.api.types.is_numeric_dtype(x[col].dtype):
                agg_values[col] = x[col].mean()
            else:
                agg_values[col] = x[col].iloc[0]
        return pd.Series(agg_values)

    # Group by specified column and aggregate
    grouped = df.groupby(group_by_column)

    # Use tqdm for progress tracking
    tqdm.pandas(desc="Aggregating")
    aggregated_df = grouped.progress_apply(custom_aggregation)

    return aggregated_df


def collapse_df_to_mean(
    df, output_filename="output.csv", client_idcode_string="client_idcode"
):
    """
    Collapse a DataFrame to calculate mean values for numeric columns and retain the first non-numeric value for non-numeric columns
    for each unique client_idcode.

    Args:
        df (DataFrame): Input DataFrame containing client_idcode and other columns.
        output_filename (str, optional): Name of the output file to save the processed DataFrame. Default is 'output.csv'.
        client_idcode_string (str, optional): Name of the client_idcode column in the DataFrame. Default is 'client_idcode'.

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
    """
    Plots the number of missing client_idcodes for the top 50 most frequent
    'basicobs_itemname_analysed' values in the given dataframe dfb.

    Parameters:
    dfb (pd.DataFrame): DataFrame containing the columns 'client_idcode' and 'basicobs_itemname_analysed'.
    """

    # Step 1: Identify the top 50 basicobs_itemname_analysed by frequency
    top_items = dfb["basicobs_itemname_analysed"].value_counts().nlargest(50).index

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
