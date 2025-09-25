import pandas as pd
from typing import Optional
import logging

logger = logging.getLogger(__name__)


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
            logger.warning(f"Test data file is empty: {file_path}")
        return df
    except FileNotFoundError:
        logger.error(f"Test data file not found at: {file_path}")
        return None
    except Exception as e:
        logger.error(f"An error occurred while reading {file_path}: {e}")
        return None
