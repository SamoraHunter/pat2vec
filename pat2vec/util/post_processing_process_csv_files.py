from multiprocessing import Pool, cpu_count
from pat2vec.util.post_processing import extract_datetime_to_column, process_chunk
import pandas as pd
from tqdm import tqdm
import csv
import os
from datetime import datetime


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
    - out_folder (str): The folder name for the output CSV file. Default is 'outputs'.
    - output_filename_suffix (str): The suffix for the output CSV file name. Default is 'concatenated_output'.
    - part_size (int): Size of parts for processing files in chunks. Default is 336.
    - sample_size (int): Number of files to sample. If None, use all files. Default is None.
    - append_timestamp_column (bool): If True, append a timestamp column. Default is False.

    Returns:
    - str: The path to the saved concatenated CSV file.
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
        backup_file = f"{base_name}_{timestamp}_backup{extension}"
        print(f"Warning: Output file already exists. Creating backup: {backup_file}")
        os.rename(output_file, backup_file)

    # Keep track of all unique column names found across all CSV files
    unique_columns = set()

    # Sample files if needed
    sampled_files = all_file_paths[:sample_size]

    print(f"Processing {len(sampled_files)} CSV files...")

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
                    print(f"Warning: Empty file skipped: {file}")
        except Exception as e:
            print(f"Warning: Could not read file {file}: {e}")

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
                print(f"Warning: Error processing file {file}: {e}")

        # Append chunk data to output file
        if chunk_data:
            with open(output_file, "a", newline="", encoding="utf-8") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=unique_columns)
                writer.writerows(chunk_data)

            total_rows_processed += len(chunk_data)

    print(
        f"Processed {total_rows_processed} total rows from {len(sampled_files)} files"
    )

    # Add timestamp column if requested
    if append_timestamp_column:
        print("Reading results and appending timestamp column...")
        try:
            df = pd.read_csv(output_file)
            df = extract_datetime_to_column(df)
            df.to_csv(output_file, index=False)
            print("Timestamp column added successfully")
        except Exception as e:
            print(f"Warning: Could not add timestamp column: {e}")

    print(f"Concatenated data saved to {output_file}")
    return output_file


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
