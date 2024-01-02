import csv
import os
import sys
from datetime import datetime
from multiprocessing import Pool

import pandas as pd
from tqdm import tqdm

sys.path.insert(0,'/home/aliencat/samora/gloabl_files')
sys.path.insert(0,'/data/AS/Samora/gloabl_files')
sys.path.insert(0,'/home/jovyan/work/gloabl_files')
sys.path.insert(0,'/home/cogstack/samora/_data/gloabl_files')


def count_files(path):
    count = 0
    for root, dirs, files in os.walk(path):
        count += len(files)
    return count





def process_csv_files(input_path, out_folder='outputs', output_filename_suffix='concatenated_output',  part_size=336, sample_size = None, append_timestamp_column=False):
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
        for file in (all_files):
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
        print(f"Warning: Output file already exists. Renaming {output_file} to {new_output_file}")
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


    if(append_timestamp_column):
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
    
    if(drop):
        columns_to_drop = [col for col in df.columns if 'date_time_stamp' in col]

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
            if column == 'types':
                mask &= dataframe[column].apply(lambda x: any(item.lower() in x for item in value))
            elif column in ['Time_Value', 'Presence_Value', 'Subject_Value']:
                # Include rows where the column is in the specified list of values
                mask &= dataframe[column].isin(value) if isinstance(value, list) else (dataframe[column] == value)
            elif column in ['Time_Confidence', 'Presence_Confidence', 'Subject_Confidence']:
                # Include rows where the column is greater than or equal to the specified confidence threshold
                mask &= dataframe[column] >= value
            elif column in ['acc']:
                # Include rows where the column is greater than or equal to the specified confidence threshold
                mask &= dataframe[column] >= value
            else:
                mask &= dataframe[column] >= value

    # Return the filtered DataFrame
    return dataframe[mask]

# def produce_filtered_annotation_dataframe(cui_filter = False, meta_annot_filter = False, pat_list = None, config_obj = None, filter_custom_args =None, cui_code_list=None):
    
#     if(meta_annot_filter):
#         if(filter_custom_args ==None):
#             print("using config obj filter arguments..")
#             filter_args = config_obj.filter_arguments
#         else:
#             filter_args = filter_custom_args
    
#     results = []
    
#     if(pat_list ==None):
#         print("using all patient list", len(config_obj.all_patient_list))
#         pat_list = config_obj.all_patient_list
        
#         for i in tqdm(range(0, len(pat_list))):
            
#             current_pat_client_idcode = pat_list[i]
            
#             current_pat_annot_batch_path = config_obj.pre_document_annotation_batch_path + current_pat_client_idcode
            
#             current_pat_annot_batch = pd.read_csv(current_pat_annot_batch_path)
            
#             if(meta_annot_filter):
                
#                 current_pat_annot_batch = filter_annot_dataframe2(current_pat_annot_batch, filter_args)
            
            
#             if(cui_filter):
#                 current_pat_annot_batch = current_pat_annot_batch[current_pat_annot_batch['cui'].isin(cui_code_list)]
                
                
#     return current_pat_annot_batch



# def produce_filtered_annotation_dataframe(cui_filter=False, meta_annot_filter=False, pat_list=None, config_obj=None, filter_custom_args=None, cui_code_list=None):
#     """
#     Filter annotation dataframe based on specified criteria.

#     Parameters:
#     - cui_filter (bool): Whether to filter by CUI codes.
#     - meta_annot_filter (bool): Whether to apply meta annotation filtering.
#     - pat_list (list): List of patient identifiers.
#     - config_obj (ConfigObject): Configuration object containing necessary parameters.
#     - filter_custom_args (dict): Custom filter arguments.
#     - cui_code_list (list): List of CUI codes for filtering.

#     Returns:
#     - pd.DataFrame: Filtered annotation dataframe.
#     """

#     if meta_annot_filter:
#         if filter_custom_args is None:
#             print("Using config obj filter arguments..")
#             filter_args = config_obj.filter_arguments
#         else:
#             filter_args = filter_custom_args

#     results = []

#     if pat_list is None:
#         print("Using all patient list", len(config_obj.all_patient_list))
#         pat_list = config_obj.all_patient_list

#     for i in tqdm(range(len(pat_list))):
#         current_pat_client_idcode = str(pat_list[i])
#         current_pat_annot_batch_path = config_obj.pre_document_annotation_batch_path + current_pat_client_idcode + ".csv"
#         current_pat_annot_batch = pd.read_csv(current_pat_annot_batch_path)

#         if meta_annot_filter:
#             current_pat_annot_batch = filter_annot_dataframe2(current_pat_annot_batch, filter_args)

#         if cui_filter:
#             current_pat_annot_batch = current_pat_annot_batch[current_pat_annot_batch['cui'].isin(cui_code_list)]

#         results.append(current_pat_annot_batch)
        
#     super_result = pd.concat(results)

#     return super_result


def produce_filtered_annotation_dataframe(cui_filter=False, meta_annot_filter=False, pat_list=None, config_obj=None, filter_custom_args=None, cui_code_list=None, mct=False):
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
        
        if(mct==False):
            current_pat_annot_batch_path = config_obj.pre_document_annotation_batch_path + current_pat_client_idcode + ".csv"
        else:
            current_pat_annot_batch_path = config_obj.pre_document_annotation_batch_path_mct + current_pat_client_idcode + ".csv"
            
        current_pat_annot_batch = pd.read_csv(current_pat_annot_batch_path)

        if meta_annot_filter:
            current_pat_annot_batch = filter_annot_dataframe2(current_pat_annot_batch, filter_args)

        if cui_filter:
            current_pat_annot_batch = current_pat_annot_batch[current_pat_annot_batch['cui'].isin(cui_code_list)]

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




def remove_file_from_paths(current_pat_idcode: str, project_name='new_project' ) -> None:
    #project_name = 'new_project'
    #current_pat_idcode = 'D886526'
    pat_file_paths = [
        f"{project_name}/current_pat_document_batches/",
        f"{project_name}/current_pat_document_batches_mct/",
        f"{project_name}/current_pat_documents_annotations_batches/",
        f"{project_name}/current_pat_documents_annotations_batches_mct/"
    ]
    for path in pat_file_paths:
        try:
            os.remove(path + current_pat_idcode + ".csv")
            print(path + current_pat_idcode + ".csv", "successfully removed")
        except:
            print(path + current_pat_idcode + ".csv", "not found")



def process_chunk(args):
    part_chunk, all_files, part_size, unique_columns = args
    concatenated_data = {column: [] for column in unique_columns}
    for file in all_files[part_chunk:part_chunk + part_size]:
        if file.endswith('.csv'):
            with open(file, 'r', newline='') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    for column in unique_columns:
                        concatenated_data[column].append(row.get(column, ''))
    return concatenated_data

def process_csv_files_multi(input_path, out_folder='outputs', output_filename_suffix='concatenated_output', part_size=336, sample_size=None, append_timestamp_column=False):
    curate_columns = False

    all_file_paths = [os.path.join(dp, f) for dp, dn, filenames in os.walk(input_path) for f in filenames if os.path.splitext(f)[1] == '.csv']

    if type(sample_size) == str or sample_size is None:
        if sample_size is None or sample_size.lower() == 'all':
            sample_size = len(all_file_paths)

    output_file = os.path.join(out_folder, f'concatenated_data_{output_filename_suffix}.csv')

    unique_columns = set()

    all_files = all_file_paths if sample_size is None else all_file_paths[:sample_size]

    print("all files size", len(all_files))

    if not curate_columns:
        for file in all_files:
            if file.endswith('.csv'):
                with open(file, 'r', newline='') as infile:
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
        print(f"Warning: Output file already exists. Renaming {output_file} to {new_output_file}")
        os.rename(output_file, new_output_file)
    else:
        new_output_file = output_file

    unique_columns = list(unique_columns)
    unique_columns.sort(key=lambda x: x != 'client_idcode')

    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=unique_columns)
        writer.writeheader()

    with Pool() as pool:
        args_list = [(i, all_files, part_size, unique_columns) for i in range(0, len(all_files), part_size)]
        results = list(tqdm(pool.imap(process_chunk, args_list), total=len(all_files)//part_size))

    with open(output_file, 'a', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=unique_columns)
        for result in tqdm(results, desc='Writing lines...'):
            for i in range(len(result[next(iter(result))])):
                writer.writerow({column: result[column][i] for column in unique_columns})

    if append_timestamp_column:
        print("Reading results and appending updatetime column")
        df = pd.read_csv(output_file)
        df = extract_datetime_to_column(df)
        df.to_csv(output_file)

    print(f"Concatenated data saved to {output_file}")

    return output_file
