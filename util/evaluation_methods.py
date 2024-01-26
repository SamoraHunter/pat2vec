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


sys.path.insert(0,'/home/aliencat/samora/gloabl_files')
sys.path.insert(0,'/data/AS/Samora/gloabl_files')
sys.path.insert(0,'/home/jovyan/work/gloabl_files')
sys.path.insert(0,'/home/cogstack/samora/_data/gloabl_files')


from typing import List, Union
import pandas as pd

from typing import List, Union
import pandas as pd
from IPython.display import clear_output

from typing import List, Union
import pandas as pd
from IPython.display import clear_output

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
        unique_client_ids = unique_client_ids.union(set(df['client_idcode'].unique()))

    for client_id in unique_client_ids:
        # Initialize a list to store rows for each dataframe
        rows = [df[df['client_idcode'] == client_id].iloc[0] for df in dataframes]

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