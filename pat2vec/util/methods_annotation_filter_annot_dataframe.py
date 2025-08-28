import pandas as pd


def filter_annot_dataframe(dataframe, filter_args):
    """
    Filter a DataFrame based on specified filter arguments.

    Parameters:
    - dataframe: pandas DataFrame
    - filter_args: dict
        A dictionary containing filter arguments.

    Returns:
    - pandas DataFrame
        The filtered DataFrame.
    """
    # Initialize a boolean mask with True values for all rows
    mask = pd.Series(True, index=dataframe.index)

    # Apply filters based on the provided arguments
    for column, value in filter_args.items():
        if column in dataframe.columns:
            # Special case for 'types' column
            if column == "types":
                mask &= dataframe[column].apply(
                    lambda x: any(item.lower() in x for item in value)
                )
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
