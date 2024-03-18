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
        pd.DataFrame: Original DataFrame with columns for the counts of each term in each text row.
    """
    # Iterate through each term
    for term in terms:
        # Compile the regular expression pattern for the term
        pattern = re.compile(term, flags=re.IGNORECASE)
        # Create a new column for the counts of the current term
        df[term] = df[text_column].apply(lambda x: len(re.findall(pattern, x)))

    return df
