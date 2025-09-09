import pandas as pd


def read_test_data(file_path: str):
    """
    Read data from a CSV file and return as a pandas DataFrame.

    Parameters:
    file_path (str): Path to the CSV file.

    Returns:
    pandas.DataFrame: DataFrame containing the data from the CSV file, or None on error.
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