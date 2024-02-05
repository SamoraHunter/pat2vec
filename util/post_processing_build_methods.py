

from pathlib import Path

import pandas as pd
from tqdm import tqdm

from pat2vec.util.post_processing import retrieve_pat_annots_mct_epr


def filter_annot_dataframe(df, annot_filter_arguments):
    """
    Filter a DataFrame based on the given inclusion criteria.

    Parameters:
    - df: DataFrame to be filtered
    - annot_filter_arguments: Dictionary containing inclusion criteria

    Returns:
    - Filtered DataFrame
    """
    # Define a function to handle non-numeric values in float columns
    def filter_float_column(df, column_name, threshold):
        if column_name in df.columns and column_name in annot_filter_arguments:
            # Convert the column to numeric values (assuming it contains only convertible values)
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
            df = df[df[column_name] > threshold]
            return df

    # Apply the function for each float column
    df = filter_float_column(df, 'acc', annot_filter_arguments['acc'])
    df = filter_float_column(df, 'Time_Confidence',
                             annot_filter_arguments['Time_Confidence'])
    df = filter_float_column(df, 'Presence_Confidence',
                             annot_filter_arguments['Presence_Confidence'])
    df = filter_float_column(df, 'Subject_Confidence',
                             annot_filter_arguments['Subject_Confidence'])

    # Check if 'types' column exists in the DataFrame
    if 'types' in df.columns and 'types' in annot_filter_arguments:
        df = df[df['types'].isin(annot_filter_arguments['types'])]

    # Check if 'Time_Value' column exists in the DataFrame
    if 'Time_Value' in df.columns and 'Time_Value' in annot_filter_arguments:
        df = df[df['Time_Value'].isin(annot_filter_arguments['Time_Value'])]

    # Check if 'Presence_Value' column exists in the DataFrame
    if 'Presence_Value' in df.columns and 'Presence_Value' in annot_filter_arguments:
        df = df[df['Presence_Value'].isin(
            annot_filter_arguments['Presence_Value'])]

    # Check if 'Subject_Value' column exists in the DataFrame
    if 'Subject_Value' in df.columns and 'Subject_Value' in annot_filter_arguments:
        df = df[df['Subject_Value'].isin(
            annot_filter_arguments['Subject_Value'])]

    return df

# Example usage:
# Assuming df and annot_filter_arguments are defined with appropriate values
# filtered_df = filter_annot_dataframe(df, annot_filter_arguments)


from pathlib import Path


def build_merged_epr_mct_annot_df(all_pat_list, config_obj, overwrite=False):
    """
    Build a merged DataFrame containing annotations for multiple patients using MCT and EPR data.

    Parameters:
    - config_obj (ConfigObject): An object containing configuration settings, including project name,
                                patient list, etc.
    - overwrite (bool): If True, overwrite the existing output file. Default is False.

    Returns:
    File path to output

    This function creates a directory for merged batches, retrieves annotations for each patient,
    and writes the merged annotations to a CSV file named 'annots_mct_epr.csv'.
    If the output file already exists and overwrite is False, subsequent batches are appended to it.

    Example usage:
    ```
    config = ConfigObject(...)  # Create or load your configuration object
    build_merged_epr_mct_annot_df(config, overwrite=True)
    ```

    """
    directory_path = config_obj.proj_name + "/" + "merged_batches/"
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = directory_path + 'annots_mct_epr.csv'

    if not overwrite and Path(output_file_path).is_file():
        print("Output file already exists. Appending to the existing file.")
    else:
        for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):
            current_pat_idcode = all_pat_list[i]
            all_annots = retrieve_pat_annots_mct_epr(
                current_pat_idcode, config_obj)

            if i == 0:
                # Create the output file and write the first batch directly
                all_annots.to_csv(output_file_path, index=False)
            else:
                # Append each result to the output file
                all_annots.to_csv(output_file_path, mode='a',
                                  header=False, index=False)

    return output_file_path
