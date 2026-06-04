import csv
import os
import shutil
import logging
from typing import List, Optional, Dict
import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)


def count_files(path: str) -> int:
    """Recursively counts the number of files in a directory."""
    count = 0
    for root, dirs, files in os.walk(path):
        count += len(files)
    return count


def process_chunk(args: tuple) -> Dict[str, List[str]]:
    """Processes a chunk of CSV files, concatenating their data into a dictionary.

    This helper function is designed for multiprocessing. It reads a specified
    range of files, extracts data for a given set of unique columns, and
    returns a dictionary where keys are column names and values are lists of
    data from those columns.

    Args:
        args: A tuple containing (part_chunk, all_files, part_size, unique_columns).

    Returns:
        A dictionary with concatenated data for the specified unique columns.
    """
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


def copy_files_and_dirs(
    source_root: str,
    source_name: str,
    destination: str,
    items_to_copy: Optional[List[str]] = None,
    loose_files: Optional[List[str]] = None,
) -> None:
    """Copies specified directories and files from a source location to a new destination."""
    source_dir = os.path.join(source_root, source_name)
    if items_to_copy is None:
        items_to_copy = [
            "current_pat_annots_parts",
            "current_pat_annots_mrc_parts",
            "outputs",
            "current_pat_document_batches",
            "current_pat_document_batches_mct",
            "current_pat_documents_annotations_batches",
            "current_pat_documents_annotations_batches_mct",
            "current_pat_lines_parts",
            "pre_document_annotation_batch_path",
            "pre_document_annotation_batch_path_mct",
            "pre_textual_obs_annotation_batch_path",
            "pre_document_annotation_batch_path_reports",
        ]

    all_source_paths = []
    if os.path.exists(source_dir):
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                all_source_paths.append(
                    os.path.relpath(os.path.join(root, file), source_dir)
                )
            for dir in dirs:
                all_source_paths.append(
                    os.path.relpath(os.path.join(root, dir), source_dir)
                )

    destination_dir = os.path.join(destination, source_name)
    os.makedirs(destination_dir, exist_ok=True)

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
        loose_files = ["treatment_docs.csv", "control_path.pkl"]

    # Copy loose files to the root directory of the destination
    for loose_file in tqdm(loose_files, desc="Copying loose files"):
        source_loose_path = os.path.join(source_root, loose_file)
        if os.path.exists(source_loose_path):
            destination_loose_path = os.path.join(destination, loose_file)
            shutil.copy2(source_loose_path, destination_loose_path)


def filter_and_update_csv(
    target_directory: str,
    ipw_dataframe: pd.DataFrame,
    filter_type: str = "after",
    verbosity: bool = False,
) -> None:
    """Filters and updates CSV files in a target directory based on patient IPW records.

    This function iterates through each patient record in the `ipw_dataframe`,
    finds corresponding CSV files in the `target_directory` (and its subdirectories),
    and filters the rows in those CSV files based on a timestamp column and a filter date.

    Args:
        target_directory: The root directory containing the CSV files to be filtered.
        ipw_dataframe (pd.DataFrame): A DataFrame containing patient IPW records,
            including 'client_idcode' and a timestamp column (e.g., 'updatetime').
        filter_type (str, optional): The type of filtering to apply: "after"
            (keep records after filter_date) or "before" (keep records before
            filter_date). Defaults to "after".
        verbosity (bool, optional): If True, print verbose messages during processing.
    """
    for _, row in ipw_dataframe.iterrows():
        client_idcode = str(row["client_idcode"])
        filter_date = pd.to_datetime(row["updatetime"], utc=True, errors="coerce")

        if pd.isna(filter_date):
            continue

        if verbosity:
            logger.info(f"Processing client_idcode: {client_idcode}")

        # Recursively walk through the target directory
        for root, _, files in os.walk(target_directory):
            for file in files:
                if file.startswith(client_idcode) and file.endswith(".csv"):
                    file_path = os.path.join(root, file)

                    if verbosity:
                        logger.info(f"Found CSV file: {file_path}")

                    try:
                        df = pd.read_csv(file_path)
                        if df.empty:
                            continue

                        # Check for various possible timestamp columns
                        update_column = None
                        for col in [
                            "updatetime",
                            "observationdocument_recordeddtm",
                            "order_entered",
                            "basicobs_entered",
                        ]:
                            if col in df.columns:
                                df[col] = pd.to_datetime(
                                    df[col], utc=True, errors="coerce"
                                )
                                update_column = col
                                break

                        if update_column is None:
                            continue

                        filter_condition = (
                            df[update_column] > filter_date
                            if filter_type == "after"
                            else df[update_column] < filter_date
                        )
                        filtered_df = df[filter_condition]
                        filtered_df.to_csv(file_path, index=False)

                    except Exception as e:
                        logger.error(f"Error updating CSV file {file_path}: {e}")
