import pandas as pd
from fuzzywuzzy import process


def filter_dataframe_by_fuzzy_terms(df, filter_term_list, column_name='document_description', verbose=0):
    """
    Filter DataFrame by fuzzy matching terms in the specified column.

    Args:
        df (pd.DataFrame): DataFrame to filter.
        filter_term_list (list): List of terms to filter by.
        column_name (str): Name of the column to perform fuzzy matching.
        verbose (int): Verbosity level (0: no verbose, 1: moderate verbose, 2: high verbose).

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    if verbose >= 1:
        print("Filtering DataFrame by fuzzy terms...")

    filtered_df = pd.DataFrame()
    for term in filter_term_list:
        if verbose >= 1:
            print(f"Processing term: {term}")
        matches = process.extractBests(term, df[column_name], score_cutoff=80)
        for match, score, idx in matches:
            if verbose >= 2:
                print(f"Found match: {match} with similarity score: {score}")
            filtered_df = pd.concat([filtered_df, df.iloc[[idx]]])

    if verbose >= 1:
        print("Filtering complete.")

    return filtered_df
