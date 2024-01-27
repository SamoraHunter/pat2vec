from typing import Union
import os
import pandas as pd
from tqdm import tqdm
import shutil


def retrieve_pat_annotations(current_pat_client_idcode: str, config_obj= None) -> pd.DataFrame:
    """
    Concatenates data from two CSV files (EPR and MCT) into a single dataframe.
    Maps values from 'observationdocument_recordeddtm' to a new column 'updatetime' in the MCT dataframe.

    Parameters:
    - current_pat_client_idcode (str): The client ID code.
    - config_obj (Union[YourConfigObjectType, dict]): The configuration object containing paths.

    Returns:
    pd.DataFrame: Concatenated dataframe with the 'updatetime' column added.
    """
    # Specify the file paths
    current_pat_docs_epr = os.path.join(config_obj.pre_document_annotation_batch_path, current_pat_client_idcode + '.csv')
    current_pat_docs_mct = os.path.join(config_obj.pre_document_annotation_batch_path_mct, current_pat_client_idcode + '.csv')

    # Read CSV files into dataframes
    df_epr = pd.read_csv(current_pat_docs_epr)
    df_mct = pd.read_csv(current_pat_docs_mct)

    # Check if 'updatetime' column exists in df_mct, if not, create it and map values
    if 'updatetime' not in df_mct.columns:
        df_mct['updatetime'] = df_mct['observationdocument_recordeddtm'].map(lambda x: pd.to_datetime(x, errors='coerce'))

    # Concatenate dataframes
    result_df = pd.concat([df_epr, df_mct], axis=0, ignore_index=True)

    return result_df


def copy_project_folders_with_substring_match(pat2vec_obj, substrings_to_match=None):
    if substrings_to_match is None:
        substrings_to_match = ['batches', 'annots']

    base_project_name = pat2vec_obj.config_obj.proj_name
    suffix = 1
    new_project_name = f"{base_project_name}_{suffix}"

    while os.path.exists(new_project_name):
        suffix += 1
        new_project_name = f"{base_project_name}_{suffix}"

    os.makedirs(new_project_name)

    old_project_folders = os.listdir(base_project_name)

    for folder in tqdm(old_project_folders, desc="Copying folders"):
        if any(substring in folder for substring in substrings_to_match):
            src_path = os.path.join(base_project_name, folder)
            dest_path = os.path.join(new_project_name, folder)
            shutil.copytree(src_path, dest_path)

    print("Folders copied successfully.")
    

import warnings

def check_csv_integrity(file_path, verbosity=0):
    try:
        df = pd.read_csv(file_path)
        # Perform integrity checks on the DataFrame
        # Check for missing values in columns specified in the list
        non_nullable_columns = ['client_idcode']  # Add more columns as needed
        for column in non_nullable_columns:
            if column in df.columns and df[column].isnull().any():
                warning_message = f"Column {column} contains missing values in CSV: {file_path}"
                warnings.warn(warning_message, UserWarning)
            else:
                if verbosity >1:
                    warning_message = f"Column {column} in CSV file has no missing values: {file_path}"
                    warnings.warn(warning_message, UserWarning)
                else:
                    continue

        if verbosity == 2:
            print("CSV file integrity is good.")
    except pd.errors.EmptyDataError:
        warning_message = f"CSV file is empty: {file_path}"
        warnings.warn(warning_message, UserWarning)
    except pd.errors.ParserError:
        warning_message = f"Error parsing CSV file: {file_path}"
        warnings.warn(warning_message, UserWarning)
    except FileNotFoundError:
        warning_message = f"File not found: {file_path}"
        warnings.warn(warning_message, UserWarning)

from tqdm import tqdm

def check_csv_files_in_directory(directory, verbosity=0, ignore_outputs=True, ignore_output_vectors=True):
    total_files = sum(1 for _ in os.walk(directory) for _ in os.listdir(directory))

    # Initialize tqdm progress bar
    progress_bar = tqdm(total=total_files, unit="file", desc=f"Checking CSV files in {directory}")

    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if ignore_outputs and 'output' in file_path.lower():
                continue  # Skip files with 'output' in the path
            if ignore_output_vectors and 'current_pat_lines_parts' in file_path.lower():
                continue  # Skip files with 'current_pat_lines_parts' in the path
            if file_path.lower().endswith('.csv'):
                progress_bar.set_description(f"Checking CSV integrity for: {file_path}")
                check_csv_integrity(file_path, verbosity)
                progress_bar.update(1)

    progress_bar.close()


