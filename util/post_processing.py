import csv
import os
from datetime import datetime

from tqdm import tqdm


def process_csv_files(input_dir, output_file):
    """
    Merge multiple CSV files from a given directory into a single CSV file.

    Parameters:
    - input_dir (str): The path to the directory containing CSV files to be merged.
    - output_file (str): The path to the output CSV file where the merged data will be stored.

    The function recursively searches for CSV files in the specified input directory,
    extracts column names from each file, and writes a merged CSV file with dynamic column names.

    Returns:
    None

    Example:
    ```python
    process_csv_files('/path/to/csv_files', '/path/to/merged_output.csv')
    ```

    Note:
    - The merged CSV file will have a header row with unique column names derived from all input files.
    - The function uses the 'tqdm' library to display a progress bar while merging files.
    """
    #print("proc debug")
    
    # Initialize a dictionary to store column names
    column_names_dict = {}

    # Recursively get all CSV files in the input directory
    for root, _, files in os.walk(input_dir):
        for csv_file in [f for f in files if f.endswith('.csv')]:
            file_path = os.path.join(root, csv_file)

            # Read the first row to get the column names
            with open(file_path, 'r') as file:
                reader = csv.reader(file)
                columns = next(reader)

            # Store the column names in the dictionary with file path as key
            column_names_dict[file_path] = columns

    # Check if the output file already exists
    if os.path.exists(output_file):
        # If it exists, append datetime stamp and "overwritten" to the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name, extension = os.path.splitext(output_file)
        new_output_file = f"{base_name}_{timestamp}_overwritten{extension}"
        print(f"Warning: Output file already exists. Renaming to {new_output_file}")
        os.rename(output_file, new_output_file)
    else:
        new_output_file = output_file

    # Write the merged CSV file with dynamic column names
    with open(output_file, 'w', newline='') as output_csv:
        writer = csv.writer(output_csv)

        # Write the header with unique column names
        unique_columns = set(column for columns in column_names_dict.values() for column in columns)
        writer.writerow(unique_columns)

        # Iterate over each CSV file
        for file_path, columns in tqdm(column_names_dict.items(), desc=f"Merging CSV files {file_path.rsplit('/', 1)[1]}", unit="file"):
            # Read the CSV file and write to the merged file
            with open(file_path, 'r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip the header row
                for row in reader:
                    writer.writerow(row)
