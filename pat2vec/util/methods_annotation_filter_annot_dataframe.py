import pandas as pd
from typing import Any, Dict
import ast


def filter_annot_dataframe(
    dataframe: pd.DataFrame, filter_args: Dict[str, Any]
) -> pd.DataFrame:
    """Filters an annotation DataFrame based on specified criteria.

    This function applies a series of filters to a MedCAT annotation DataFrame.
    It supports filtering by:
    -   Meta-annotation values (e.g., `Time_Value`, `Presence_Value`).
    -   Confidence scores for meta-annotations (e.g., `Time_Confidence`).
    -   Annotation accuracy (`acc`).
    -   Annotation types (e.g., 'disorder', 'procedure').

    Args:
        dataframe: The annotation DataFrame to filter.
        filter_args: A dictionary where keys are column names and values are
            the criteria to filter by. For confidence/accuracy scores, the
            value is a minimum threshold. For value columns, it's a list
            of allowed values.

    Returns:
        The filtered DataFrame.
    """
    # Initialize a boolean mask with True values for all rows
    mask = pd.Series(True, index=dataframe.index)

    # Apply filters based on the provided arguments
    for column, value in filter_args.items():
        if column in dataframe.columns:
            # Special case for 'types' column
            if column == "types":
                # This function safely parses the string representation of a list
                def check_type(row_str):
                    try:
                        # Parse the string like "['disease', 'finding']" into a list
                        type_list = ast.literal_eval(row_str)
                        # Check if any of the desired types are in the parsed list
                        return any(
                            item.lower() in [t.lower() for t in type_list]
                            for item in value
                        )
                    except (ValueError, SyntaxError):
                        # If the string is malformed, it's not a match
                        return False

                mask &= dataframe[column].apply(check_type)
            elif column in ["Time_Value", "Presence_Value", "Subject_Value"]:
                # Include rows where the column is in the specified list of values
                mask &= (
                    dataframe[column].isin(value)
                    if isinstance(value, list)
                    else (dataframe[column] == value)
                )
            elif column in [
                "Time_Confidence",
                "Presence_Confidence",
                "Subject_Confidence",
            ]:
                # Include rows where the column is greater than or equal to the specified confidence threshold
                mask &= dataframe[column] >= value
            elif column in ["acc"]:
                # Include rows where the column is greater than or equal to the specified confidence threshold
                mask &= dataframe[column] >= value

            else:
                mask &= dataframe[column] >= value

    # Return the filtered DataFrame
    return dataframe[mask]
