import re
from typing import Any, Dict, Iterable, List

import pandas as pd


def stringlist2searchlist(string_list: str, output_name: str) -> None:
    """Converts a newline-separated string into an Elasticsearch OR-separated search string.

    The resulting string is saved to a text file. For example, a string
    "term1\nterm2" becomes ""term1" OR "term2"".

    Args:
        string_list: A string where items are separated by newlines.
        output_name: The base name for the output text file ('.txt' will be appended).
    """
    list_string = string_list.replace("\n", '" OR "')
    textfile = open(output_name + ".txt", "w")
    textfile.write(f'"{list_string}"')
    textfile.close()
    print("List processed!")


def pylist2searchlist(list_name: List[str], output_name: str) -> None:
    """Converts a Python list into an Elasticsearch OR-separated search string.

    The resulting string is saved to a text file. For example, a list
    ['term1', 'term2'] becomes ""term1" OR "term2"".

    Args:
        list_name: A list of strings to be joined.
        output_name: The base name for the output text file ('.txt' will be appended).
    """
    test_str = '" OR "'.join(list_name)
    textfile = open(output_name + ".txt", "w")
    textfile.write(f'"{test_str}"')
    textfile.close()


def stringlist2pylist(string_list: str, var_name: str) -> None:
    """Converts a newline-separated string into a Python list and assigns it to a global variable.

    Note:
        This function uses `globals()` to create a variable in the global
        scope, which is generally not recommended.

    Args:
        string_list: A string where items are separated by newlines.
        var_name: The name of the global variable to which the resulting list will be assigned.
    """
    globals()[var_name] = string_list.replace("\n", ",").split(",")
    print("List generated!")


def date_cleaner(df: pd.DataFrame, cols: List[str], date_format: str) -> None:
    """Formats specified datetime columns in a DataFrame to a given string format.

    This function modifies the DataFrame in-place.

    Args:
        df: The DataFrame to modify.
        cols: A list of column names to format.
        date_format: The target string format for the dates (e.g., '%Y-%m-%d').
    """
    for col in cols:
        df[col] = pd.to_datetime(df[col], utc=True).dt.strftime(date_format)
    print("dates formatted!")


def bulk_str_findall(
    target_colname_regex_pairs: Dict[str, str], source_colname: str, df_name: pd.DataFrame
) -> None:
    """Applies multiple regex `findall` operations to a source column.

    For each key-value pair in `target_colname_regex_pairs`, this function
    finds all occurrences of the regex (value) in the `source_colname` and
    stores the joined results in a new column named after the key. This
    modifies the DataFrame in-place.

    Args:
        target_colname_regex_pairs: A dictionary mapping new column names to regex patterns.
        source_colname: The name of the column to search within.
        df_name: The DataFrame to modify.
    """
    for key, value in target_colname_regex_pairs.items():
        df_name[key] = (
            df_name[source_colname].str.lower().str.findall(value).str.join(",\n")
        )


def without_keys(d: Dict[Any, Any], keys: Iterable[Any]) -> Dict[Any, Any]:
    """Returns a new dictionary excluding the specified keys.

    Args:
        d: The original dictionary.
        keys: An iterable of keys to exclude.

    Returns:
        A new dictionary without the specified keys.
    """
    return {k: v for k, v in d.items() if k not in keys}


def bulk_str_extract(
    target_colname_regex_pairs: Dict[str, str],
    source_colname: str,
    df_name: pd.DataFrame,
    expand: bool,
) -> None:
    """Applies multiple regex `extract` operations to a source column.

    For each key-value pair in `target_colname_regex_pairs`, this function
    extracts the first match of the regex (value) from the `source_colname`
    and stores it in a new column named after the key. This modifies the
    DataFrame in-place.

    Args:
        target_colname_regex_pairs: A dictionary mapping new column names to regex patterns.
        source_colname: The name of the column to search within.
        df_name: The DataFrame to modify.
        expand: The `expand` parameter for `pd.Series.str.extract`.
    """
    for key, value in target_colname_regex_pairs.items():
        df_name[key] = (
            df_name[source_colname]
            .str.lower()
            .str.extract(pat=value, expand=expand, flags=re.IGNORECASE)
        )


def bulk_str_extract_round_robin(
    target_dict: Dict[str, str],
    df_name: pd.DataFrame,
    source_colname: str,
    expand: bool,
) -> None:
    """Extracts text between a series of regex patterns in a round-robin fashion.

    For each pattern in the `target_dict`, this function attempts to extract the
    text that appears *after* that pattern but *before* any of the other patterns
    in the dictionary. This modifies the DataFrame in-place.

    Args:
        target_dict: A dictionary mapping new column names to regex patterns.
        df_name: The DataFrame to modify.
        source_colname: The name of the column to search within.
        expand: The `expand` parameter for `pd.Series.str.extract`.
    """
    for key, value in target_dict.items():
        remaining_dict = without_keys(target_dict, key)
        remaining_strings = "|".join(list(remaining_dict.values()))
        df_name[key] = (
            df_name[source_colname]
            .str.lower()
            .str.extract(
                pat=f"{value}(.*?)({remaining_strings})",
                expand=expand,
                flags=re.IGNORECASE,
            )[0]
        )
