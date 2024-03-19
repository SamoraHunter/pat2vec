import pandas as pd
from fuzzywuzzy import process


def filter_dataframe_by_fuzzy_terms(
    df, filter_term_list, column_name="document_description", verbose=0
):
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

    matched_indices = set()
    for term in filter_term_list:
        if verbose >= 1:
            print(f"Processing term: {term}")
        matches = process.extractBests(term, df[column_name], score_cutoff=80)
        for match, score, idx in matches:
            if verbose >= 2:
                print(f"Found match: {match} with similarity score: {score}")
            matched_indices.add(idx)

    if verbose >= 1:
        print("Filtering complete.")

    filtered_df = df[df.index.isin(matched_indices)]
    return filtered_df
