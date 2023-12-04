import csv
import os
from datetime import datetime

import pandas as pd
from tqdm import tqdm


def count_files(path):
    count = 0
    for root, dirs, files in os.walk(path):
        count += len(files)
    return count

# def process_csv_files(input_dir, output_file):
#     """
#     Merge multiple CSV files from a given directory into a single CSV file.

#     Parameters:
#     - input_dir (str): The path to the directory containing CSV files to be merged.
#     - output_file (str): The path to the output CSV file where the merged data will be stored.

#     The function recursively searches for CSV files in the specified input directory,
#     extracts column names from each file, and writes a merged CSV file with dynamic column names.

#     Returns:
#     None

#     Example:
#     ```python
#     process_csv_files('/path/to/csv_files', '/path/to/merged_output.csv')
#     ```

#     Note:
#     - The merged CSV file will have a header row with unique column names derived from all input files.
#     - The function uses the 'tqdm' library to display a progress bar while merging files.
#     """
    
    
#     print(f"Processing input directory '{input_dir}'. The directory contains {len(os.listdir(input_dir))} patient directories, and a total of {count_files(input_dir)} date vectors. Results will be written to '{output_file}'.")

    
#     # Initialize a dictionary to store column names
#     column_names_dict = {}

#     # Recursively get all CSV files in the input directory
#     for root, _, files in os.walk(input_dir):
#         for csv_file in [f for f in files if f.endswith('.csv')]:
#             file_path = os.path.join(root, csv_file)

#             # Read the first row to get the column names
#             with open(file_path, 'r') as file:
#                 reader = csv.reader(file)
#                 columns = next(reader)

#             # Store the column names in the dictionary with file path as key
#             column_names_dict[file_path] = columns

#     # Check if the output file already exists
#     if os.path.exists(output_file):
#         # If it exists, append datetime stamp and "overwritten" to the filename
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#         base_name, extension = os.path.splitext(output_file)
#         new_output_file = f"{base_name}_{timestamp}_overwritten{extension}"
#         print(f"Warning: Output file already exists. Renaming to {new_output_file}")
#         os.rename(output_file, new_output_file)
#     else:
#         new_output_file = output_file

#     # Write the merged CSV file with dynamic column names
#     with open(output_file, 'w', newline='') as output_csv:
#         writer = csv.writer(output_csv)

#         # Write the header with unique column names
#         unique_columns = set(column for columns in column_names_dict.values() for column in columns)
#         writer.writerow(unique_columns)

#         # Iterate over each CSV file
#         for file_path, columns in tqdm(column_names_dict.items(), desc=f"Merging CSV files {file_path.rsplit('/', 1)[1]}", unit="file"):
#             # Read the CSV file and write to the merged file
#             with open(file_path, 'r') as file:
#                 reader = csv.reader(file)
#                 next(reader)  # Skip the header row
#                 for row in reader:
#                     writer.writerow(row)




# def process_csv_files(root_directory, output_csv_path):
#     """
#     Concatenate multiple CSV files with potentially different columns from a given directory
#     and save the result to a new CSV file.

#     Parameters:
#     - root_directory (str): The root directory containing the CSV files.
#     - output_csv_path (str): The path to the output CSV file.

#     Returns:
#     None
#     """
#     # Initialize a set to store unique column names
#     unique_columns = set()

#     # Count the total number of files to track progress
#     total_files = sum([len(files) for _, _, files in os.walk(root_directory)])

#     # Initialize tqdm to track progress
#     progress_bar = tqdm(total=total_files, desc="Processing Files")

#     # Loop through all subdirectories and files
#     for subdir, dirs, files in os.walk(root_directory):
#         for file in files:
#             # Check if the file is a CSV file
#             if file.endswith(".csv"):
#                 file_path = os.path.join(subdir, file)

#                 with open(file_path, 'r') as csv_file:
#                     # Use csv.reader to read the CSV file
#                     csv_reader = csv.reader(csv_file)

#                     # Read the header
#                     header = next(csv_reader, None)

#                     # Add unique column names to the set
#                     if header:
#                         unique_columns.update(header)

#                 # Update the progress bar
#                 progress_bar.update(1)

#     # Close the progress bar for processing files
#     progress_bar.close()

#     # Write the concatenated rows to a new CSV file
#     with open(output_csv_path, 'w', newline='') as output_csv:
#         csv_writer = csv.writer(output_csv)

#         # Write the header with unique column names
#         csv_writer.writerow(sorted(unique_columns))

#         # Reset the progress bar for writing rows
#         progress_bar.reset(total=total_files)
#         progress_bar.set_description("Writing Rows")

#         # Loop through all files again to write rows with standardized columns
#         for subdir, dirs, files in os.walk(root_directory):
#             for file in files:
#                 if file.endswith(".csv"):
#                     file_path = os.path.join(subdir, file)

#                     with open(file_path, 'r') as csv_file:
#                         csv_reader = csv.reader(csv_file)
#                         next(csv_reader)  # Skip the header

#                         # Write rows with standardized columns
#                         for row in csv_reader:
#                             # Create a dictionary to map each column to its value
#                             row_dict = {column: '' for column in sorted(unique_columns)}
#                             row_dict.update(zip(sorted(row), row))

#                             # Write the row with standardized columns
#                             csv_writer.writerow([row_dict[column] for column in sorted(unique_columns)])

#                     # Update the progress bar
#                     progress_bar.update(1)

#     # Close the progress bar for writing rows
#     progress_bar.close()

#     print(f'Concatenated CSV saved to: {output_csv_path}')





def process_csv_files(input_path, out_folder='outputs', output_filename_suffix='concatenated_output',  part_size=336, sample_size = None):
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
    
    curate_columns=False
    

    # Specify the directory where your CSV files are located
    all_file_paths = [os.path.join(dp, f) for dp, dn, filenames in os.walk(input_path) for f in filenames if os.path.splitext(f)[1] == '.csv']
    if type(sample_size) == str or sample_size == None:
        if(sample_size == None or sample_size.lower() == 'all'):
            sample_size= len(all_file_paths)

    # Create an output CSV file to hold the concatenated data
    output_file = os.path.join(out_folder, f'concatenated_data_{output_filename_suffix}.csv')

    # Keep track of all unique column names found across all CSV files
    unique_columns = set()

    # Sample files if sample_size is provided
    all_files = all_file_paths if sample_size is None else all_file_paths[:sample_size]

    # Create a dictionary to hold the concatenated data with the unique columns as keys
    concatenated_data = {column: [] for column in unique_columns}

    # Loop through each CSV file and read its data
    if not curate_columns:
        for file in tqdm(all_files):
            if file.endswith('.csv'):
                with open(file, 'r', newline='') as infile:
                    reader = csv.reader(infile)
                    try:
                        # Get the header of the current CSV file
                        header = next(reader)
                        # Add all column names to the unique_columns set
                        unique_columns.update(header)
                    except StopIteration:
                        pass
    
    #Check if the output file already exists
    if os.path.exists(output_file):
        # If it exists, append datetime stamp and "overwritten" to the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name, extension = os.path.splitext(output_file)
        new_output_file = f"{base_name}_{timestamp}_overwritten{extension}"
        print(f"Warning: Output file already exists. Renaming to {new_output_file}")
        os.rename(output_file, new_output_file)
    else:
        new_output_file = output_file




    # Create a header and write it to the output CSV file
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=unique_columns)
        writer.writeheader()

    # Loop through each CSV file again and concatenate its data to the dictionary
    for part_chunk in tqdm(range(0, len(all_files), part_size)):
        # Reset the concatenated_data dictionary for each part chunk
        concatenated_data = {column: [] for column in unique_columns}

        # Loop through each CSV file again and concatenate its data to the dictionary
        for file in all_files[part_chunk:part_chunk + part_size]:
            if file.endswith('.csv'):
                with open(file, 'r', newline='') as infile:
                    reader = csv.DictReader(infile)
                    # Loop through each row in the current CSV file
                    for row in reader:
                        # Add each value to the appropriate column in the dictionary
                        for column in unique_columns:
                            concatenated_data[column].append(row.get(column, ''))

        # Append the concatenated data to the output CSV file
        with open(output_file, 'a', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=unique_columns)
            for i in range(len(concatenated_data[next(iter(concatenated_data))])):
                writer.writerow({column: concatenated_data[column][i] for column in unique_columns})

    print(f"Concatenated data saved to {output_file}")

# Example Usage:
# concatenate_csv_files('/home/cogstack/samora/_data/HAEM_AG11193_3/new_project/current_pat_lines_parts', 'output_path_here')



def extract_datetime_to_column(df):
    """
    Extracts datetime information from specified columns and creates a new column.

    Parameters:
    - df (pandas.DataFrame): The DataFrame containing the datetime information in specific columns.

    Returns:
    - pandas.DataFrame: The DataFrame with a new column 'extracted_datetime_stamp' containing the extracted datetime values.
    """

    # Initialize the new column
    df['extracted_datetime_stamp'] = pd.to_datetime('')

    # Iterate through rows using tqdm for progress bar
    for index, row in tqdm(df.iterrows(), total=len(df)):
        # Iterate through columns
        for column in df.columns:
            # Check if the column contains '_date_time_stamp' and the value is 1
            if '_date_time_stamp' in column and row[column] == 1:
                # Extract date from column name and convert to datetime
                date_str = column.replace('_date_time_stamp', '')
                datetime_obj = pd.to_datetime(date_str, format='(%Y, %m, %d)')

                # Assign the datetime value to the new column
                df.at[index, 'extracted_datetime_stamp'] = datetime_obj

    # Display the count of extracted datetime values
    print(df['extracted_datetime_stamp'].value_counts())

    return df