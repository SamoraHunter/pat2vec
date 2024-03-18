import pandas as pd
import re


def append_regex_term_counts(df, terms, text_column="body_analysed"):
    """
    Count occurrences of terms in a DataFrame's text column using regular expressions.

    Args:
        df (pd.DataFrame): DataFrame containing the text column.
        terms (list): List of terms to count occurrences of.
        text_column (str): Name of the text column in the DataFrame. Default is 'body_analysed'.

    Returns:
        pd.DataFrame: DataFrame with columns for the terms and counts for each match in each text row.
    """
    # Initialize an empty DataFrame to store counts
    counts_df = pd.DataFrame()

    # Iterate through each term
    for term in terms:
        # Compile the regular expression pattern for the term
        pattern = re.compile(term, flags=re.IGNORECASE)
        # Count occurrences of the term in each row of the text column
        counts = df[text_column].apply(lambda x: len(re.findall(pattern, x)))
        # Add the counts to the counts DataFrame
        counts_df[term] = counts

    return counts_df
