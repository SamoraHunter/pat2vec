import csv
import os


def process_csv_files(input_directory, output_file):
    
    """
    Process CSV files in the specified input directory and its subdirectories.
    Extracts unique column names and appends data to the output CSV file.

    Parameters:
    - input_directory (str): Path to the input directory containing CSV files.
    - output_file (str): Path to the output CSV file.

    Returns:
    None
    """
    # Dictionary to store unique column names
    column_names_dict = {}

    # Iterate through all directories and subdirectories
    for root, dirs, files in os.walk(input_directory):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)

                # Read column names from the CSV file
                with open(file_path, 'r', newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    headers = next(reader)

                    # Update the dictionary with unique column names
                    for header in headers:
                        column_names_dict[header] = True

                # Append data to the output CSV file
                with open(output_file, 'a', newline='') as output_csvfile:
                    # Write headers only if the file is empty
                    write_headers = os.path.getsize(output_file) == 0
                    writer = csv.DictWriter(output_csvfile, fieldnames=column_names_dict.keys())

                    if write_headers:
                        writer.writeheader()

                    # Read and write data row by row
                    with open(file_path, 'r', newline='') as csvfile:
                        reader = csv.DictReader(csvfile)
                        for row in reader:
                            writer.writerow(row)