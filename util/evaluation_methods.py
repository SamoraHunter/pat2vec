from ydata_profiling import ProfileReport
from typing import Optional, List
from IPython.display import clear_output
import csv
import os
import sys
from datetime import datetime
from multiprocessing import Pool, cpu_count
import pandas as pd
from IPython.display import display
from tqdm import tqdm
import shutil
from typing import List, Union


sys.path.insert(0, '/home/aliencat/samora/gloabl_files')
sys.path.insert(0, '/data/AS/Samora/gloabl_files')
sys.path.insert(0, '/home/jovyan/work/gloabl_files')
sys.path.insert(0, '/home/cogstack/samora/_data/gloabl_files')


def compare_ipw_annotation_rows(dataframes: List[pd.DataFrame],
                                columns_to_print: Union[List[str], None] = None) -> None:
    """
    Compare rows with the same 'client_idcode' across multiple individual patient window annotation dataframes and print specified columns 
    when differences are found in the 'text_sample' column. Example usage: I have a dataframe with the earliest annotation for a CUI,
    I have another dataframe with the earliest annotation but filtered by meta annotations. I want to evaluate the application of the meta
    annotation filter. 

    Parameters:
    - dataframes (List[pd.DataFrame]): A list of pandas DataFrames to compare.
    - columns_to_print (Union[List[str], None]): A list of column names to print when differences are found.
      If None, it defaults to columns:
      ['updatetime', 'pretty_name', 'cui', 'types', 'source_value', 'detected_name', 'acc',
       'context_similarity', 'Time_Value', 'Time_Confidence', 'Presence_Value', 'Presence_Confidence',
       'Subject_Value', 'Subject_Confidence']

    Returns:
    - None
    """
    if columns_to_print is None:
        # Default columns to print
        columns_to_print = ['updatetime', 'pretty_name', 'cui', 'types', 'source_value',
                            'detected_name', 'acc', 'context_similarity', 'Time_Value',
                            'Time_Confidence', 'Presence_Value', 'Presence_Confidence',
                            'Subject_Value', 'Subject_Confidence']

    # Iterate over unique client_idcode values
    unique_client_ids = set()
    for df in dataframes:
        unique_client_ids = unique_client_ids.union(
            set(df['client_idcode'].unique()))

    for client_id in unique_client_ids:
        # Initialize a list to store rows for each dataframe
        rows = [df[df['client_idcode'] == client_id].iloc[0]
                for df in dataframes]

        # Check if the 'text_sample' column is not the same across all dataframes
        if not all(rows[0]['text_sample'] == row['text_sample'] for row in rows):
            clear_output(wait=True)  # Clear the output in Jupyter Notebook

            # Print 'text_sample' column from each dataframe
            for i, df in enumerate(dataframes):
                print(f"{df.name}['text_sample']: {rows[i]['text_sample']}")

            # Print specified columns
            for column in columns_to_print:
                print(f"{column}:")
                for i, df in enumerate(dataframes):
                    print(f"{df.name}: {rows[i][column]}")
                print("\n")

            # Wait for user input to proceed
            input("Press Enter to continue...")


def create_profile_reports(epr_batchs_fp: str, prefix: Optional[str] = None, cols: Optional[List[str]] = None, icd10_opc4s: bool = False) -> None:
    """
    Generate Pandas Profiling Reports for CSV files in a directory.

    Parameters:
    - epr_batchs_fp (str): Path to the directory containing CSV files.
    - prefix (str, optional): Prefix to be added to the generated report files.
    - cols (List[str], optional): List of columns to be used in profiling. If not provided, default columns are used.
    - icd10_opc4s (bool): Flag to indicate whether to filter rows based on the 'targetId' column.

    Returns:
    None
    """
    # Default columns
    default_cols = ['client_idcode', 'pretty_name', 'cui', 'type_ids', 'types', 'acc', 'context_similarity', 'icd10', 'ontologies', 'snomed', 'Time_Value',
                    'Time_Confidence', 'Presence_Value', 'Presence_Confidence', 'Subject_Value', 'Subject_Confidence', 'conceptId', 'targetId', 'updatetime']

    # Use default columns if not provided
    cols = cols or default_cols

    # Create a directory for profile reports if it doesn't exist
    profile_reports_dir = 'profile_reports'
    os.makedirs(profile_reports_dir, exist_ok=True)

    for csv_file in tqdm(os.listdir(epr_batchs_fp)):
        file_path = os.path.join(epr_batchs_fp, csv_file)
        try:
            # Check if 'updatetime' is in the columns before using it
            csv_columns = pd.read_csv(file_path, nrows=1).columns
            if 'updatetime' not in csv_columns:
                try:
                    cols.remove('updatetime')
                except:
                    pass
                if ('observationdocument_recordeddtm') not in cols:
                    cols.append('observationdocument_recordeddtm')

            # Check if 'targetId' is in the columns before using it
            if 'targetId' not in csv_columns:
                if 'targetId' in cols:
                    cols.remove('targetId')
                continue

            if not icd10_opc4s:
                df = pd.read_csv(file_path, usecols=cols).sample(100)
            else:
                df = pd.read_csv(file_path, usecols=cols)
                if 'targetId' in df.columns:
                    df = df[df['targetId'].notna()]

            profile = ProfileReport(
                df, title=f'Pandas Profiling Report {csv_file}_{prefix}', explorative=True, config_file='')

            # Save the profiling report to the profile_reports directory
            report_name = f"{prefix}_{csv_file.split('.')[0]}_profile_report.html"
            report_path = os.path.join(profile_reports_dir, report_name)
            profile.to_file(report_path)

            print(f"Profile report for {csv_file} created at: {report_path}")
        except Exception as e:
            print(f"Error processing {csv_file}: {type(e).__name__}")
            print(e)
            import traceback
            traceback.print_exc()

# Example usage:
# create_profile_reports('/path/to/csv/files', prefix='my_prefix', cols=['column1', 'column2'], icd10_opc4s=True)
