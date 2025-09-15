import re
import pandas as pd
from typing import List


def append_regex_term_counts(
    df: pd.DataFrame,
    terms: List[str],
    text_column: str = "body_analysed",
    debug: bool = False,
) -> pd.DataFrame:
    """Counts occurrences of regex patterns in a DataFrame's text column.

    For each term (regex pattern) in the `terms` list, this function counts
    its case-insensitive occurrences in each row of the specified `text_column`.
    A new column is added to the DataFrame for each term, containing the count.

    Args:
        df: The DataFrame to process.
        terms: A list of regex patterns to search for.
        text_column: The name of the column containing the text to search.
        debug: If True, prints debugging information about the DataFrame.

    Returns:
        The original DataFrame with new columns for the counts of each term.
    """
    if debug:
        print("append_regex_term_counts df:")
        print(df.columns)
        print(df.head())

    # Iterate through each term
    for term in terms:
        # Compile the regular expression pattern for the term
        pattern = re.compile(term, flags=re.IGNORECASE)
        # Create a new column for the counts of the current term
        df[term] = df[text_column].apply(lambda x: len(re.findall(pattern, x)))

    return df
