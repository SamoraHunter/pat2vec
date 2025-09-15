import os
from pathlib import Path

from typing import Any, Dict, List, Optional
import pandas as pd
from tqdm import tqdm

from pat2vec.util.post_processing import retrieve_pat_annots_mct_epr


def filter_annot_dataframe(
    df: pd.DataFrame, annot_filter_arguments: Dict[str, Any]
) -> pd.DataFrame:
    """Filters a DataFrame based on the given inclusion criteria.

    Args:
        df: DataFrame to be filtered.
        annot_filter_arguments: Dictionary containing inclusion criteria.

    Returns:
        The filtered DataFrame.
    """

    # Define a function to handle non-numeric values in float columns
    def filter_float_column(df: pd.DataFrame, column_name: str, threshold: float) -> pd.DataFrame:
        if column_name in df.columns and column_name in annot_filter_arguments:
            # Convert the column to numeric values (assuming it contains only convertible values)
            df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
            df = df[df[column_name] > threshold]  # type: ignore
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
        df = df[df["Presence_Value"].isin(
            annot_filter_arguments["Presence_Value"])]

    # Check if 'Subject_Value' column exists in the DataFrame
    if "Subject_Value" in df.columns and "Subject_Value" in annot_filter_arguments:
        df = df[df["Subject_Value"].isin(
            annot_filter_arguments["Subject_Value"])]

    return df


# Example usage:
# Assuming df and annot_filter_arguments are defined with appropriate values
# filtered_df = filter_annot_dataframe(df, annot_filter_arguments)


def build_merged_epr_mct_annot_df(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> Optional[str]:
    """Builds a merged DataFrame of annotations from EPR and MCT sources.

    This function iterates through a list of patient IDs, retrieves their
    respective annotation data from both EPR and MCT sources using the
    `retrieve_pat_annots_mct_epr` function, and then
    concatenates it into a single large DataFrame. The final merged DataFrame
    is saved to a CSV file.

    Args:
        all_pat_list: A list of patient client ID codes to process.
        config_obj: A configuration object containing project settings.
        overwrite: If True, any existing merged file will be
                                    overwritten. If False, the function will skip
                                    the process if the file already exists.
                                    Defaults to False.

    Returns:
        Optional[str]: The file path to the merged annotations CSV file.
            Returns None if no annotation data is found for any patient.
    """
    directory_path = config_obj.proj_name + "/" + "merged_batches/"
    Path(directory_path).mkdir(parents=True, exist_ok=True)
    output_file_path = directory_path + "annots_mct_epr.csv"

    if not overwrite and Path(output_file_path).is_file():
        print(f"File already exists at {output_file_path}. Skipping.")
        return output_file_path  # Return existing path

    # --- Step 1: Collect all patient DataFrames in a list ---
    all_patient_dfs = []
    print("Reading and collecting all patient annotation batches...")
    for current_pat_idcode in tqdm(all_pat_list):
        # This function reads the individual patient CSV
        pat_annots_df = retrieve_pat_annots_mct_epr(
            current_pat_idcode, config_obj)

        # --- Step 2: Clean each DataFrame as we get it ---
        # Robustly handle the inconsistent index column
        if "Unnamed: 0" in pat_annots_df.columns:
            pat_annots_df = pat_annots_df.drop("Unnamed: 0", axis=1)

        if not pat_annots_df.empty:
            all_patient_dfs.append(pat_annots_df)

    # --- Step 3: Merge, Save, and Return ---
    if not all_patient_dfs:
        print("No annotation data found for any patient. No file will be created.")
        return None  # Return None if there's nothing to save

    print(f"\nConcatenating data for {len(all_patient_dfs)} patients...")
    # Create one large, final DataFrame from the list of clean DataFrames
    final_merged_df = pd.concat(all_patient_dfs, ignore_index=True)

    print(f"Saving merged file to {output_file_path}...")
    # Save the final, clean DataFrame to a CSV in a single operation
    final_merged_df.to_csv(output_file_path, index=False)

    print("Done.")
    return output_file_path


def build_merged_bloods(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Builds a merged CSV file of bloods data from patient batch files.

    This function iterates through a list of patient IDs, reads the corresponding
    bloods batch CSV for each patient, and appends the data to a single merged
    CSV file. It handles file existence and overwriting logic.

    Args:
        all_pat_list: A list of patient client ID codes to process.
        config_obj: A configuration object containing project settings.
        overwrite: If True, any existing merged file will be overwritten.
            If False, data will be appended to the existing file.

    Returns:
        str: The file path to the merged bloods CSV file.
    """

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


def build_merged_epr_mct_doc_df(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Builds a merged CSV of documents from EPR and MCT sources.

    This function iterates through a list of patient IDs, retrieves their
    respective document data from both EPR and MCT sources using the
    `retrieve_pat_docs_mct_epr` function, and appends the data to a single
    merged CSV file.

    Args:
        all_pat_list: A list of patient client ID codes to process.
        config_obj: A configuration object containing project settings.
        overwrite: If True, any existing merged file will be
                                    overwritten. If False, data will be appended
                                    to the existing file. Defaults to False.

    Returns:
        str: The file path to the merged documents CSV file.
    """
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
            all_docs = retrieve_pat_docs_mct_epr(
                current_pat_idcode, config_obj)
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


def retrieve_pat_bloods(client_idcode: str, config_obj: Any) -> pd.DataFrame:
    """Retrieve bloods data for the given client_idcode.

    Args:
        client_idcode: Unique identifier for the patient.
        config_obj: Configuration object containing necessary paths and parameters.

    Returns:
        Bloods data for the given client_idcode, or an empty DataFrame if not found.
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
    client_idcode: str,
    config_obj: Any,
    columns_epr: Optional[List[str]] = None,
    columns_mct: Optional[List[str]] = None,
    columns_to: Optional[List[str]] = None,
    columns_report: Optional[List[str]] = None,
    merge_columns: bool = True,
) -> pd.DataFrame:
    """Retrieves and merges document data for a patient from multiple sources.

    This function reads document data for a specified patient from four potential
    sources: EPR documents, MCT documents, textual observations, and reports.
    It loads the corresponding CSV files, optionally selecting specific columns,
    and concatenates them into a single DataFrame. It can also merge related

    columns (like timestamps and content) to create a more unified dataset.

    Args:
        client_idcode: The unique identifier for the patient.
        config_obj: A configuration object containing paths to document batches.
        columns_epr: A list of columns to load from the EPR documents CSV.
        columns_mct: A list of columns to load from the MCT documents CSV.
        columns_to: A list of columns to load from the textual observations CSV.
        columns_report: A list of columns to load from the reports CSV.
        merge_columns: If True, attempts to merge corresponding columns
            (e.g., timestamps, content) from the different sources into a
            unified set of columns.

    Returns:
        A DataFrame containing the concatenated and optionally
                      merged document data for the patient. Returns an empty
                      DataFrame if no data is found for the patient in any
                      of the sources.
    """
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


def join_docs_to_annots(
    annots_df: pd.DataFrame, docs_temp: pd.DataFrame, drop_duplicates: bool = True
) -> pd.DataFrame:
    """Merge two DataFrames based on the 'document_guid' column.

    Args:
        annots_df: The DataFrame containing annotations.
        docs_temp: The DataFrame containing documents.
        drop_duplicates: If True, drops duplicated columns from `docs_temp`
            before merging.

    Returns:
        A merged DataFrame.
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
    merged_df = pd.merge(annots_df, docs_temp_dropped,
                         on="document_guid", how="left")

    return merged_df


def get_annots_joined_to_docs(
    config_obj: Any, pat2vec_obj: Any
) -> pd.DataFrame:
    """Builds and merges document and annotation dataframes, then joins them.

    This function orchestrates the process of creating comprehensive, patient-level
    data by first building merged dataframes for both documents (from EPR and MCT
    sources) and their corresponding annotations. It then joins these two
    dataframes based on a common document identifier.

    Args:
        config_obj: A configuration object containing project settings,
            including `proj_name` and paths to data batches.
        pat2vec_obj: The main pat2vec object, which contains the
                              `all_patient_list` and other necessary components.

    Returns:
        A DataFrame containing the annotations joined with their
                      corresponding document information.
    """

    pre_path = config_obj.proj_name

    build_merged_epr_mct_doc_df(pat2vec_obj.all_patient_list, config_obj)

    build_merged_epr_mct_annot_df(pat2vec_obj.all_patient_list, config_obj)

    annots_df = pd.read_csv(f"{pre_path}/merged_batches/annots_mct_epr.csv")

    docs_temp = pd.read_csv(f"{pre_path}/merged_batches/docs_mct_epr.csv")

    res = join_docs_to_annots(annots_df, docs_temp, drop_duplicates=True)

    return res


def merge_demographics_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all demographics CSV files that match the patient list.

    Args:
        all_pat_list: List of patient IDs to include.
        config_obj: Configuration object containing project settings.
        overwrite: If True, overwrite the existing output file.

    Returns:
        str: File path to the merged output CSV.
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


def merge_bmi_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all BMI CSV files that match the patient list.

    Args:
        all_pat_list: List of patient IDs to include.
        config_obj: Configuration object containing project settings.
        overwrite: If True, overwrite the existing output file.

    Returns:
        str: File path to the merged output CSV.
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


def merge_news_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all NEWS CSV files that match the patient list.

    Args:
        all_pat_list: List of patient IDs to include.
        config_obj: Configuration object containing project settings.
        overwrite: If True, overwrite the existing output file.

    Returns:
        str: File path to the merged output CSV.
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


def merge_diagnostics_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all diagnostics CSV files that match the patient list.

    Args:
        all_pat_list: List of patient IDs to include.
        config_obj: Configuration object containing project settings.
        overwrite: If True, overwrite the existing output file.

    Returns:
        str: File path to the merged output CSV.
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


def merge_drugs_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all drugs CSV files that match the patient list.

    Args:
        all_pat_list: List of patient IDs to include.
        config_obj: Configuration object containing project settings.
        overwrite: If True, overwrite the existing output file.

    Returns:
        str: File path to the merged output CSV.
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


def merge_appointments_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all appointments CSV files that match the patient list.

    Args:
        all_pat_list: List of patient IDs to include.
        config_obj: Configuration object containing project settings.
        overwrite: If True, overwrite the existing output file.

    Returns:
        str: File path to the merged output CSV.
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
