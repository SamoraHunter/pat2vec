import pandas as pd
from typing import Optional


def read_test_data(file_path: str) -> Optional[pd.DataFrame]:
    """Reads data from a CSV file into a pandas DataFrame.

    This function is a simple wrapper around `pd.read_csv` with added
    error handling for file not found and other exceptions. It also prints
    a warning if the loaded DataFrame is empty.

    Args:
        file_path: The path to the CSV file.

    Returns:
        A pandas DataFrame containing the data from the CSV file, or None
        if an error occurs or the file is not found.
    """
    try:
        df = pd.read_csv(file_path)
        if len(df) == 0:
            print(f"Warning: Test data file is empty: {file_path}")
        return df
    except FileNotFoundError:
        print(f"Error: Test data file not found at: {file_path}")
        return None
    except Exception as e:
        print(f"An error occurred while reading {file_path}: {e}")
        return None
