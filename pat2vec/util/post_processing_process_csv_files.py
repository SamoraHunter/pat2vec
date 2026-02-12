from multiprocessing import Pool, cpu_count
from pat2vec.util.post_processing import extract_datetime_to_column, process_chunk
import logging
import pandas as pd
from tqdm import tqdm
import csv
import os
from datetime import datetime
from typing import Optional, Union

logger = logging.getLogger(__name__)


def process_csv_files(
    input_path: str,
    out_folder: str = "outputs",
    output_filename_suffix: str = "concatenated_output",
    part_size: int = 336,
    sample_size: Optional[Union[int, str]] = None,
    append_timestamp_column: bool = False,
) -> str:
    """Concatenates multiple CSV files from a directory into a single file.

    This function scans a directory for CSV files, determines a union of all
    column headers, and then reads each file to append its content into a
    single, large CSV file. It handles cases where CSVs have different columns
    and can process files in chunks.

    Args:
        input_path: The path to the directory containing the CSV files.
        out_folder: The folder name for the output CSV file.
        output_filename_suffix: The suffix for the output CSV file name.
        part_size: The number of files to process in each chunk.
        sample_size: The number of files to sample. If 'all' or None, all
            files are used.
        append_timestamp_column: If True, processes the final concatenated
            file to extract a datetime column from binary date columns.

    Returns:
        The path to the saved concatenated CSV file.
    """

    # Ensure output folder exists
    os.makedirs(out_folder, exist_ok=True)

    # Find all CSV files in the input path
    all_file_paths = [
        os.path.join(dp, f)
        for dp, dn, filenames in os.walk(input_path)
        for f in filenames
        if os.path.splitext(f)[1].lower() == ".csv"
    ]

    if not all_file_paths:
        raise ValueError(f"No CSV files found in {input_path}")

    # Handle sample_size parameter
    if isinstance(sample_size, str):
        if sample_size.lower() == "all":
            sample_size = len(all_file_paths)
        else:
            try:
                sample_size = int(sample_size)
            except ValueError:
                raise ValueError(
                    f"Invalid sample_size: {sample_size}. Must be an integer or 'all'"
                )

    if sample_size is None:
        sample_size = len(all_file_paths)

    sample_size = min(sample_size, len(all_file_paths))

    # Create output file path
    output_file = os.path.join(
        out_folder, f"concatenated_data_{output_filename_suffix}.csv"
    )

    # Handle existing output file
    if os.path.exists(output_file):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name, extension = os.path.splitext(output_file)
        backup_file = f"{base_name}_{timestamp}_backup{extension}"  # type: ignore
        logger.warning(f"Output file already exists. Creating backup: {backup_file}")
        os.rename(output_file, backup_file)

    # Keep track of all unique column names found across all CSV files
    unique_columns = set()

    # Sample files if needed
    sampled_files = all_file_paths[:sample_size]

    logger.info(f"Processing {len(sampled_files)} CSV files...")

    # First pass: collect all unique column names
    for file in tqdm(sampled_files, desc="Analyzing columns"):
        try:
            with open(file, "r", newline="", encoding="utf-8") as infile:
                reader = csv.reader(infile)
                try:
                    header = next(reader)
                    # Strip whitespace from column names and add to unique_columns
                    unique_columns.update([col.strip() for col in header])
                except StopIteration:
                    logger.warning(f"Empty file skipped: {file}")
        except Exception as e:
            logger.warning(f"Could not read file {file}: {e}")

    if not unique_columns:
        raise ValueError("No valid columns found in any CSV files")

    # Convert to sorted list for consistent ordering
    unique_columns = sorted(list(unique_columns))

    # Create output file with header
    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=unique_columns)
        writer.writeheader()

    # Second pass: process files in chunks and append data
    total_rows_processed = 0

    for part_chunk in tqdm(
        range(0, len(sampled_files), part_size), desc="Processing chunks"
    ):
        chunk_files = sampled_files[part_chunk : part_chunk + part_size]

        # Collect data for this chunk
        chunk_data = []

        for file in chunk_files:
            try:
                with open(file, "r", newline="", encoding="utf-8") as infile:
                    reader = csv.DictReader(infile)
                    for row in reader:
                        # Create a clean row with all columns, filling missing ones with empty strings
                        clean_row = {}
                        for column in unique_columns:
                            # Strip whitespace from both keys and values
                            value = ""
                            for key, val in row.items():
                                if key.strip() == column:
                                    value = str(val).strip() if val is not None else ""
                                    break
                            clean_row[column] = value
                        chunk_data.append(clean_row)

            except Exception as e:
                logger.warning(f"Error processing file {file}: {e}")

        # Append chunk data to output file
        if chunk_data:
            with open(output_file, "a", newline="", encoding="utf-8") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=unique_columns)
                writer.writerows(chunk_data)

            total_rows_processed += len(chunk_data)

    logger.info(
        f"Processed {total_rows_processed} total rows from {len(sampled_files)} files"
    )

    # Add timestamp column if requested
    if append_timestamp_column:
        logger.info("Reading results and appending timestamp column...")
        try:
            df = pd.read_csv(output_file)
            df = extract_datetime_to_column(df)
            df.to_csv(output_file, index=False)
            logger.info("Timestamp column added successfully")
        except Exception as e:
            logger.warning(f"Could not add timestamp column: {e}")

    logger.info(f"Concatenated data saved to {output_file}")
    return output_file


def process_csv_files_multi(
    input_path: str,
    out_folder: str = "outputs",
    output_filename_suffix: str = "concatenated_output",
    part_size: int = 336,
    sample_size: Optional[Union[int, str]] = None,
    append_timestamp_column: bool = False,
    n_proc: Optional[Union[int, str]] = None,
) -> str:
    """Concatenates multiple CSV files using multiprocessing.

    This function is a multiprocessing version of `process_csv_files`. It
    distributes the file processing across multiple CPU cores to speed up the
    concatenation of a large number of CSV files.

    Args:
        input_path: The path to the directory containing the CSV files.
        out_folder: The folder name for the output CSV file.
        output_filename_suffix: The suffix for the output CSV file name.
        part_size: The number of files to process in each chunk per process.
        sample_size: The number of files to sample. If 'all' or None, all
            files are used.
        append_timestamp_column: If True, processes the final file to extract
            a datetime column.
        n_proc: The number of processes to use. Can be an integer, 'all', or 'half'.
    """
    curate_columns = False

    all_file_paths = [
        os.path.join(dp, f)
        for dp, dn, filenames in os.walk(input_path)
        for f in filenames
        if os.path.splitext(f)[1] == ".csv"
    ]

    if isinstance(sample_size, str) or sample_size is None:
        if sample_size is None or sample_size.lower() == "all":
            sample_size = len(all_file_paths)

    output_file = os.path.join(
        out_folder, f"concatenated_data_{output_filename_suffix}.csv"
    )

    unique_columns = set()

    all_files = all_file_paths if sample_size is None else all_file_paths[:sample_size]

    logger.info(f"all files size: {len(all_files)}")

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
        new_output_file = f"{base_name}_{timestamp}_overwritten{extension}"  # type: ignore
        logger.warning(
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

    if n_proc is not None:
        if n_proc == "all":
            n_proc_val = available_cores
        if n_proc == "half":
            n_proc_val = desired_half_processes
        elif isinstance(n_proc, int):
            n_proc_val = n_proc
    logger.info(f"Desired cores for multiprocessing: {n_proc_val}")

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
        logger.info("Reading results and appending updatetime column")
        df = pd.read_csv(output_file)
        df = extract_datetime_to_column(df)
        df.to_csv(output_file)

    logger.info(f"Concatenated data saved to {output_file}")

    return output_file
