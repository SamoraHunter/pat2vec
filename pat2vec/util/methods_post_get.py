import warnings
from typing import Any, List, Optional
import os
import logging
import pandas as pd
from tqdm import tqdm
import shutil
from pat2vec.util.post_processing import remove_file_from_paths

logger = logging.getLogger(__name__)

def retrieve_pat_annotations(
    current_pat_client_idcode: str, config_obj: Any = None
) -> pd.DataFrame:
    """Concatenates EPR and MCT annotation data for a single patient.

    This function reads a patient's annotation data from two separate CSV
    files—one for EPR documents and one for MCT documents. It then standardizes
    the timestamp column and concatenates them into a single DataFrame.

    Args:
        current_pat_client_idcode: The client ID code for the patient.
        config_obj: The configuration object containing file paths for
            EPR and MCT annotation batches.

    Returns:
        A concatenated DataFrame containing annotations from both EPR and MCT
        sources, with a unified 'updatetime' column.
    """
    # Specify the file paths
    current_pat_docs_epr = os.path.join(
        config_obj.pre_document_annotation_batch_path, current_pat_client_idcode + '.csv')
    current_pat_docs_mct = os.path.join(
        config_obj.pre_document_annotation_batch_path_mct, current_pat_client_idcode + '.csv')

    # Read CSV files into dataframes
    df_epr = pd.read_csv(current_pat_docs_epr)
    df_mct = pd.read_csv(current_pat_docs_mct)

    # Check if 'updatetime' column exists in df_mct, if not, create it and map values
    if 'updatetime' not in df_mct.columns:
        df_mct['updatetime'] = df_mct['observationdocument_recordeddtm'].map(
            lambda x: pd.to_datetime(x, errors='coerce'))

    # Concatenate dataframes
    result_df = pd.concat([df_epr, df_mct], axis=0, ignore_index=True)

    return result_df


def copy_project_folders_with_substring_match(
    pat2vec_obj: Any, substrings_to_match: Optional[List[str]] = None
) -> None:
    """Copies project subfolders that match given substrings to a new versioned directory.

    This is useful for creating a snapshot or a new version of a project's
    outputs before running a new experiment. It finds the next available
    version number (e.g., `my_project_1`, `my_project_2`) and copies only the
    subfolders whose names contain one of the specified substrings.

    Args:
        pat2vec_obj: The main pat2vec object, containing the `config_obj`.
        substrings_to_match: A list of substrings to identify which folders
            to copy (e.g., ['batches', 'annots']).
    """
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

    logger.info("Folders copied successfully.")


def check_csv_integrity(
    file_path: str, verbosity: int = 0, delete_broken: bool = False, config_obj: Any = None
) -> None:
    """Checks the integrity of a single CSV file.

    This function attempts to read a CSV file and performs basic integrity
    checks, such as ensuring it's not empty and that key columns do not
    contain null values. If `delete_broken` is True, it will remove files
    that fail these checks.

    Args:
        file_path: The path to the CSV file to check.
        verbosity: The level of detail for logging warnings.
        delete_broken: If True, deletes files that fail integrity checks.
        config_obj: The configuration object, required if `delete_broken` is
            True to pass to `remove_file_from_paths`.

    Raises:
        UserWarning: If the CSV file is empty, cannot be parsed, a key column
            contains null values, or the file is not found. These warnings
            are issued to inform the user of potential data integrity issues.

    """
    try:
        df = pd.read_csv(file_path)

        def _delete_and_log(reason: str):
            """Helper to remove a file and log the action."""
            warnings.warn(f"{reason}: {file_path}", UserWarning)
            _, filename_ext = os.path.split(file_path)
            filename, _ = os.path.splitext(filename_ext)
            remove_file_from_paths(filename, config_obj=config_obj)
            logger.info(f"Deleted broken file: {filename} : {file_path}")

        # Perform integrity checks on the DataFrame
        non_nullable_columns = ['client_idcode']  # Add more columns as needed

        for column in non_nullable_columns:
            if column in df.columns and df[column].isnull().any():
                warning_message = f"Column {column} contains missing values in CSV: {file_path}"
                warnings.warn(warning_message, UserWarning)
                if delete_broken:
                    _delete_and_log(f"Column {column} contains missing values")
                    return
            elif verbosity > 1:
                warning_message = f"Column {column} in CSV file has no missing values: {file_path}"
                warnings.warn(warning_message, UserWarning)

        if verbosity == 2:
            logger.info("CSV file integrity is good.")

    except pd.errors.EmptyDataError:
        if delete_broken:
            _delete_and_log("CSV file is empty")

    except pd.errors.ParserError:
        if delete_broken:
            _delete_and_log("Error parsing CSV file")

    except FileNotFoundError:
        warning_message = f"File not found: {file_path}"
        warnings.warn(warning_message, UserWarning)


def check_csv_files_in_directory(
    directory: str,
    verbosity: int = 0,
    ignore_outputs: bool = True,
    ignore_output_vectors: bool = True, # type: ignore
    delete_broken: bool = False,
) -> None:
    """Recursively checks the integrity of all CSV files in a directory.

    This function walks through a directory and its subdirectories, applying
    `check_csv_integrity` to every CSV file found. It provides options to
    ignore certain common output directories.

    Args:
        directory: The root directory to start the search from.
        verbosity: The verbosity level passed to `check_csv_integrity`.
        ignore_outputs: If True, skips any path containing 'output'.
        ignore_output_vectors: If True, skips paths for 'current_pat_lines_parts'.
        delete_broken: If True, deletes files that fail integrity checks.
    """
    total_files = sum(1 for _ in os.walk(directory)
                      for _ in os.listdir(directory))

    # Initialize tqdm progrcommitess bar
    progress_bar = tqdm(total=total_files, unit="file",
                        desc=f"Checking CSV files in {directory}")

    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if ignore_outputs and 'output' in file_path.lower():
                continue  # Skip files with 'output' in the path
            if ignore_output_vectors and 'current_pat_lines_parts' in file_path.lower():
                continue  # Skip files with 'current_pat_lines_parts' in the path
            if file_path.lower().endswith('.csv'):
                progress_bar.set_description(
                    f"Checking CSV integrity for: {file_path}")
                check_csv_integrity(file_path, verbosity, delete_broken, config_obj=None) # type: ignore
                progress_bar.update(1)

    progress_bar.close()
