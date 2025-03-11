from pathlib import Path
import os
import pandas as pd
from tqdm import tqdm

from pat2vec.util.post_processing import (
    retrieve_pat_annots_mct_epr,
    #    retrieve_pat_docs_mct_epr,
)


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
            df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
            df = df[df[column_name] > threshold]
            return df

    # Apply the function for each float column
    df = filter_float_column(df, "acc", annot_filter_arguments["acc"])
    df = filter_float_column(
        df, "Time_Confidence", annot_filter_arguments["Time_Confidence"]
    )
    df = filter_float_column(
        df, "Presence_Confidence", annot_filter_arguments["Presence_Confidence"]
    )
    df = filter_float_column(
        df, "Subject_Confidence", annot_filter_arguments["Subject_Confidence"]
    )

    # Check if 'types' column exists in the DataFrame
    if "types" in df.columns and "types" in annot_filter_arguments:
        df = df[df["types"].isin(annot_filter_arguments["types"])]

    # Check if 'Time_Value' column exists in the DataFrame
    if "Time_Value" in df.columns and "Time_Value" in annot_filter_arguments:
        df = df[df["Time_Value"].isin(annot_filter_arguments["Time_Value"])]

    # Check if 'Presence_Value' column exists in the DataFrame
    if "Presence_Value" in df.columns and "Presence_Value" in annot_filter_arguments:
        df = df[df["Presence_Value"].isin(annot_filter_arguments["Presence_Value"])]

    # Check if 'Subject_Value' column exists in the DataFrame
    if "Subject_Value" in df.columns and "Subject_Value" in annot_filter_arguments:
        df = df[df["Subject_Value"].isin(annot_filter_arguments["Subject_Value"])]

    return df


# Example usage:
# Assuming df and annot_filter_arguments are defined with appropriate values
# filtered_df = filter_annot_dataframe(df, annot_filter_arguments)


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

    output_file_path = directory_path + "annots_mct_epr.csv"

    if not overwrite and Path(output_file_path).is_file():
        print("Output file already exists. Appending to the existing file.")
    else:
        for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):
            current_pat_idcode = all_pat_list[i]
            all_annots = retrieve_pat_annots_mct_epr(current_pat_idcode, config_obj)

            if i == 0:
                # Create the output file and write the first batch directly
                all_annots.to_csv(output_file_path, index=False)
            else:
                # Append each result to the output file
                all_annots.to_csv(output_file_path, mode="a", header=False, index=False)

    return output_file_path


# def build_merged_epr_mct_doc_df(all_pat_list, config_obj, overwrite=False):
#     """
#     Build a merged DataFrame containing annotations for multiple patients using MCT and EPR data.

#     Parameters:
#     - config_obj (ConfigObject): An object containing configuration settings, including project name,
#                                 patient list, etc.
#     - overwrite (bool): If True, overwrite the existing output file. Default is False.

#     Returns:
#     File path to output

#     This function creates a directory for merged batches, retrieves annotations for each patient,
#     and writes the merged annotations to a CSV file named 'annots_mct_epr.csv'.
#     If the output file already exists and overwrite is False, subsequent batches are appended to it.

#     Example usage:
#     ```
#     config = ConfigObject(...)  # Create or load your configuration object
#     build_merged_epr_mct_annot_df(config, overwrite=True)
#     ```

#     """
#     directory_path = config_obj.proj_name + "/" + "merged_batches/"
#     Path(directory_path).mkdir(parents=True, exist_ok=True)

#     output_file_path = directory_path + "docs_mct_epr.csv"

#     if not overwrite and Path(output_file_path).is_file():
#         print("Output file already exists. Appending to the existing file.")
#     else:
#         for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):
#             current_pat_idcode = all_pat_list[i]
#             try:
#                 all_docs = retrieve_pat_docs_mct_epr(current_pat_idcode, config_obj)
#             except Exception as e:
#                 print(e, current_pat_idcode)

#             if i == 0:
#                 # Create the output file and write the first batch directly
#                 all_docs.to_csv(output_file_path, index=False)
#             else:
#                 # Append each result to the output file
#                 all_docs.to_csv(output_file_path, mode="a", header=False, index=False)

#     return output_file_path


def build_merged_bloods(all_pat_list, config_obj, overwrite=False):

    directory_path = config_obj.proj_name + "/" + "merged_batches/"
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = directory_path + "bloods_batches.csv"
    file_exists = os.path.isfile(output_file_path)

    if not overwrite and file_exists:
        print("Output file already exists. Appending to the existing file.")
    else:
        file_exists = False  # Reset to False to trigger overwrite if needed
        print("Creating a new output file.")

    for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):
        current_pat_idcode = all_pat_list[i]
        try:
            all_bloods = retrieve_pat_bloods(current_pat_idcode, config_obj)
        except Exception as e:
            print("Error retrieving patient bloods for:", current_pat_idcode)
            print(e)
            continue  # Skip to the next patient in case of an error

        if file_exists:
            # Read the existing columns from the file
            existing_columns = pd.read_csv(output_file_path, nrows=0).columns
            # Ensure the new DataFrame has the same column order
            all_bloods = all_bloods.reindex(columns=existing_columns)

        # Write or append the DataFrame to the CSV
        all_bloods.to_csv(
            output_file_path,
            mode="a" if file_exists else "w",
            header=not file_exists,
            index=False,
            float_format="%.6f",
        )

        # After the first successful write, set file_exists to True
        file_exists = True

    return output_file_path


def build_merged_epr_mct_doc_df(all_pat_list, config_obj, overwrite=False):
    directory_path = config_obj.proj_name + "/" + "merged_batches/"
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = directory_path + "docs_mct_epr.csv"
    file_exists = os.path.isfile(output_file_path)

    if not overwrite and file_exists:
        print("Output file already exists. Appending to the existing file.")
    else:
        file_exists = False  # Reset to False to trigger overwrite if needed
        print("Creating a new output file.")

    for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):
        current_pat_idcode = all_pat_list[i]
        try:
            all_docs = retrieve_pat_docs_mct_epr(current_pat_idcode, config_obj)
        except Exception as e:
            print("Error retrieving patient documents for:", current_pat_idcode)
            print(e)
            continue  # Skip to the next patient in case of an error

        if file_exists:
            # Read the existing columns from the file
            existing_columns = pd.read_csv(output_file_path, nrows=0).columns
            # Ensure the new DataFrame has the same column order
            all_docs = all_docs.reindex(columns=existing_columns)

        # Write or append the DataFrame to the CSV
        all_docs.to_csv(
            output_file_path,
            mode="a" if file_exists else "w",
            header=not file_exists,
            index=False,
        )

        # After the first successful write, set file_exists to True
        file_exists = True

    return output_file_path


def retrieve_pat_bloods(client_idcode, config_obj):
    """
    Retrieve bloods data for the given client_idcode.

    Parameters
    ----------
    client_idcode : str
        Unique identifier for the patient.
    config_obj : ConfigObject
        Configuration object containing necessary paths and parameters.

    Returns
    -------
    pat_bloods : pd.DataFrame
        Bloods data for the given client_idcode, if available.
    """
    pre_bloods_batch_path = config_obj.pre_bloods_batch_path

    pat_bloods_path = f"{pre_bloods_batch_path}/{client_idcode}.csv"

    try:
        pat_bloods = pd.read_csv(pat_bloods_path)
    except Exception as e:
        print(e)
        pat_bloods = pd.DataFrame()

    return pat_bloods


def retrieve_pat_docs_mct_epr(
    client_idcode,
    config_obj,
    columns_epr=None,
    columns_mct=None,
    columns_to=None,
    columns_report=None,
    merge_columns=True,
):
    pre_document_batch_path = config_obj.pre_document_batch_path
    pre_document_batch_path_mct = config_obj.pre_document_batch_path_mct
    pre_textual_obs_document_batch_path = config_obj.pre_textual_obs_document_batch_path
    pre_document_batch_path_reports = config_obj.pre_document_batch_path_reports

    epr_file_path = f"{pre_document_batch_path}/{client_idcode}.csv"
    mct_file_path = f"{pre_document_batch_path_mct}/{client_idcode}.csv"
    textual_obs_files_path = (
        f"{pre_textual_obs_document_batch_path}/{client_idcode}.csv"
    )
    report_file_path = f"{pre_document_batch_path_reports}/{client_idcode}.csv"

    dfs = []
    if os.path.exists(epr_file_path):
        dfa = pd.read_csv(epr_file_path, usecols=columns_epr)
        dfa["document_batch_source"] = "epr"
        dfs.append(dfa)

    if os.path.exists(mct_file_path):
        dfa_mct = pd.read_csv(mct_file_path, usecols=columns_mct)
        dfa_mct["document_batch_source"] = "mct"
        dfs.append(dfa_mct)

    if os.path.exists(textual_obs_files_path):
        dfa_to = pd.read_csv(textual_obs_files_path, usecols=columns_to)
        dfa_to["document_batch_source"] = "textual_obs"
        dfs.append(dfa_to)

    if os.path.exists(report_file_path):
        dfr = pd.read_csv(report_file_path, usecols=columns_report)
        dfr["document_batch_source"] = "report"
        dfs.append(dfr)

    if not dfs:
        return pd.DataFrame()  # Return an empty DataFrame if no data was loaded

    all_docs = pd.concat(dfs, ignore_index=True)

    if merge_columns and not all_docs.empty:

        for col1, col2 in [
            ("updatetime", "observationdocument_recordeddtm"),
            ("observationdocument_recordeddtm", "updatetime"),
            ("document_guid", "observation_guid"),
            ("document_description", "obscatalogmasteritem_displayname"),
            ("body_analysed", "observation_valuetext_analysed"),
        ]:
            if col1 in all_docs.columns and col2 in all_docs.columns:
                all_docs[col1] = all_docs[col1].fillna(all_docs[col2])

    return all_docs.reset_index(drop=True)


def join_docs_to_annots(annots_df, docs_temp, drop_duplicates=True):
    """
    Merge two DataFrames based on the 'document_guid' column.

    Parameters:
    annots_df (DataFrame): The DataFrame containing annotations.
    docs_temp (DataFrame): The DataFrame containing documents.

    Returns:
    DataFrame: A merged DataFrame.
    """

    if drop_duplicates:
        # Get the sets of column names
        annots_columns_set = set(annots_df.columns)
        docs_columns_set = set(docs_temp.columns)

        # Identify duplicated column names
        duplicated_columns = annots_columns_set.intersection(docs_columns_set)
        # Assuming 'document_guid' is a unique identifier
        duplicated_columns.remove("document_guid")
        if duplicated_columns:
            print("Duplicated columns found:", duplicated_columns)
            # Drop duplicated columns from docs_temp
            docs_temp_dropped = docs_temp.drop(columns=duplicated_columns)
        else:
            docs_temp_dropped = docs_temp

    # Merge the DataFrames on 'document_guid' column
    merged_df = pd.merge(annots_df, docs_temp_dropped, on="document_guid", how="left")

    return merged_df


def get_annots_joined_to_docs(config_obj, pat2vec_obj):

    pre_path = config_obj.proj_name

    build_merged_epr_mct_doc_df(pat2vec_obj.all_patient_list, config_obj)

    build_merged_epr_mct_annot_df(pat2vec_obj.all_patient_list, config_obj)

    annots_df = pd.read_csv(f"{pre_path}/merged_batches/annots_mct_epr.csv")

    docs_temp = pd.read_csv(f"{pre_path}/merged_batches/docs_mct_epr.csv")

    res = join_docs_to_annots(annots_df, docs_temp, drop_duplicates=True)

    return res


def merge_demographics_csv(all_pat_list, config_obj, overwrite=False):
    """
    Merge all CSV files in the demographics folder that match the patient list.

    Parameters:
    - all_pat_list (list): List of patient IDs to include.
    - config_obj (ConfigObject): Configuration object containing project settings.
    - overwrite (bool): If True, overwrite the existing output file. Default is False.

    Returns:
    - File path to the merged output CSV.
    """
    directory_path = os.path.join(config_obj.proj_name, "merged_batches")
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = os.path.join(directory_path, "merged_demographics.csv")
    demographics_folder = config_obj.pre_demo_batch_path

    if not overwrite and Path(output_file_path).is_file():
        print("Output file already exists. Overwrite is set to False.")
        return output_file_path

    merged_df = pd.DataFrame()

    for pat_id in tqdm(all_pat_list, total=len(all_pat_list)):
        file_path = os.path.join(demographics_folder, f"{pat_id}.csv")
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        else:
            print(f"Warning: File {file_path} not found.")

    merged_df.to_csv(output_file_path, index=False)
    print(f"Merged CSV saved to {output_file_path}")
    return output_file_path


def merge_bmi_csv(all_pat_list, config_obj, overwrite=False):
    """
    Merge all CSV files in the BMI folder that match the patient list.

    Parameters:
    - all_pat_list (list): List of patient IDs to include.
    - config_obj (ConfigObject): Configuration object containing project settings.
    - overwrite (bool): If True, overwrite the existing output file. Default is False.

    Returns:
    - File path to the merged output CSV.
    """
    directory_path = os.path.join(config_obj.proj_name, "merged_batches")
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = os.path.join(directory_path, "merged_bmi.csv")
    bmi_folder = config_obj.pre_bmi_batch_path

    if not overwrite and Path(output_file_path).is_file():
        print("Output file already exists. Overwrite is set to False.")
        return output_file_path

    merged_df = pd.DataFrame()

    for pat_id in tqdm(all_pat_list, total=len(all_pat_list)):
        file_path = os.path.join(bmi_folder, f"{pat_id}.csv")
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        else:
            print(f"Warning: File {file_path} not found.")

    merged_df.to_csv(output_file_path, index=False)
    print(f"Merged CSV saved to {output_file_path}")
    return output_file_path


def merge_news_csv(all_pat_list, config_obj, overwrite=False):
    """
    Merge all CSV files in the News folder that match the patient list.

    Parameters:
    - all_pat_list (list): List of patient IDs to include.
    - config_obj (ConfigObject): Configuration object containing project settings.
    - overwrite (bool): If True, overwrite the existing output file. Default is False.

    Returns:
    - File path to the merged output CSV.
    """
    directory_path = os.path.join(config_obj.proj_name, "merged_batches")
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = os.path.join(directory_path, "merged_news.csv")
    news_folder = config_obj.pre_news_batch_path

    if not overwrite and Path(output_file_path).is_file():
        print("Output file already exists. Overwrite is set to False.")
        return output_file_path

    merged_df = pd.DataFrame()

    for pat_id in tqdm(all_pat_list, total=len(all_pat_list)):
        file_path = os.path.join(news_folder, f"{pat_id}.csv")
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        else:
            print(f"Warning: File {file_path} not found.")

    merged_df.to_csv(output_file_path, index=False)
    print(f"Merged CSV saved to {output_file_path}")
    return output_file_path


def merge_diagnostics_csv(all_pat_list, config_obj, overwrite=False):
    """
    Merge all CSV files in the Diagnostics folder that match the patient list.

    Parameters:
    - all_pat_list (list): List of patient IDs to include.
    - config_obj (ConfigObject): Configuration object containing project settings.
    - overwrite (bool): If True, overwrite the existing output file. Default is False.

    Returns:
    - File path to the merged output CSV.
    """
    directory_path = os.path.join(config_obj.proj_name, "merged_batches")
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = os.path.join(directory_path, "merged_diagnostics.csv")
    diagnostics_folder = config_obj.pre_diagnostics_batch_path

    if not overwrite and Path(output_file_path).is_file():
        print("Output file already exists. Overwrite is set to False.")
        return output_file_path

    merged_df = pd.DataFrame()

    for pat_id in tqdm(all_pat_list, total=len(all_pat_list)):
        file_path = os.path.join(diagnostics_folder, f"{pat_id}.csv")
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        else:
            print(f"Warning: File {file_path} not found.")

    merged_df.to_csv(output_file_path, index=False)
    print(f"Merged CSV saved to {output_file_path}")
    return output_file_path


def merge_drugs_csv(all_pat_list, config_obj, overwrite=False):
    """
    Merge all CSV files in the Drugs folder that match the patient list.

    Parameters:
    - all_pat_list (list): List of patient IDs to include.
    - config_obj (ConfigObject): Configuration object containing project settings.
    - overwrite (bool): If True, overwrite the existing output file. Default is False.

    Returns:
    - File path to the merged output CSV.
    """
    # Define the directory for saving the merged file
    directory_path = os.path.join(config_obj.proj_name, "merged_batches")
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = os.path.join(directory_path, "merged_drugs.csv")
    drugs_folder = config_obj.pre_drugs_batch_path

    # If overwrite is False and the file already exists, return early
    if not overwrite and Path(output_file_path).is_file():
        print("Output file already exists. Overwrite is set to False.")
        return output_file_path

    # Initialize an empty DataFrame to store merged data
    merged_df = pd.DataFrame()

    # Loop through the patient IDs and merge the data
    for pat_id in tqdm(all_pat_list, total=len(all_pat_list)):
        file_path = os.path.join(drugs_folder, f"{pat_id}.csv")

        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        else:
            print(f"Warning: File {file_path} not found.")

    # Save the merged DataFrame to a CSV file
    merged_df.to_csv(output_file_path, index=False)
    print(f"Merged CSV saved to {output_file_path}")

    # Return the path to the merged CSV
    return output_file_path


def merge_appointments_csv(all_pat_list, config_obj, overwrite=False):
    """
    Merge all CSV files in the Appointments folder that match the patient list.

    Parameters:
    - all_pat_list (list): List of patient IDs to include.
    - config_obj (ConfigObject): Configuration object containing project settings.
    - overwrite (bool): If True, overwrite the existing output file. Default is False.

    Returns:
    - File path to the merged output CSV.
    """
    # Define the directory for saving the merged file
    directory_path = os.path.join(config_obj.proj_name, "merged_batches")
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = os.path.join(directory_path, "merged_appointments.csv")
    appointments_folder = config_obj.pre_appointments_batch_path

    # If overwrite is False and the file already exists, return early
    if not overwrite and Path(output_file_path).is_file():
        print("Output file already exists. Overwrite is set to False.")
        return output_file_path

    # Initialize an empty DataFrame to store merged data
    merged_df = pd.DataFrame()

    # Loop through the patient IDs and merge the data
    for pat_id in tqdm(all_pat_list, total=len(all_pat_list)):
        file_path = os.path.join(appointments_folder, f"{pat_id}.csv")

        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        else:
            print(f"Warning: File {file_path} not found.")

    # Save the merged DataFrame to a CSV file
    merged_df.to_csv(output_file_path, index=False)
    print(f"Merged CSV saved to {output_file_path}")

    # Return the path to the merged CSV
    return output_file_path
