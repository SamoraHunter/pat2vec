import os

import pandas as pd


def read_test_data():
    """
    Read data from a CSV file and return as a pandas DataFrame.

    Parameters:
    file_path (str): Path to the CSV file.

    Returns:
    pandas.DataFrame: DataFrame containing the data from the CSV file.
    """

    # Get the directory of the current module
    current_dir = os.path.dirname(__file__)

    # Construct the file path relative to the current directory
    file_path = os.path.join(current_dir, "treatment_docs.csv")

    try:
        df = pd.read_csv(file_path)
        if len(df) == 0:
            print("failed")
        return df
    except FileNotFoundError:
        print("File not found.")
        return None
    except Exception as e:
        print("An error occurred:", e)
        return None
