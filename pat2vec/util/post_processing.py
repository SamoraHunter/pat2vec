import csv
import os
import pickle
import shutil
from datetime import datetime
from itertools import chain
import time
from typing import List
import matplotlib.pyplot as plt
import pandas as pd
from IPython.display import display
from tqdm import tqdm


def count_files(path):
    count = 0
    for root, dirs, files in os.walk(path):
        count += len(files)
    return count


import os
import csv
import pandas as pd
from datetime import datetime
from tqdm import tqdm


import os
import csv
import pandas as pd
from datetime import datetime
from tqdm import tqdm


def extract_datetime_to_column(df, drop=True):
    """
    Extracts datetime information from specified columns and creates a new column.

    Parameters:
    - df (pandas.DataFrame): The DataFrame containing the datetime information in specific columns.
    - drop (bool): If True, drop the original date_time_stamp columns after extraction.

    Returns:
    - pandas.DataFrame: The DataFrame with a new column 'extracted_datetime_stamp' containing the extracted datetime values.
    """

    # Initialize the new column
    df["extracted_datetime_stamp"] = pd.to_datetime("")

    # Iterate through rows using tqdm for progress bar
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Extracting datetime"):
        # Iterate through columns
        for column in df.columns:
            # Check if the column contains '_date_time_stamp' and the value is 1
            if "_date_time_stamp" in column and row[column] == 1:
                # Extract date from column name and convert to datetime
                date_str = column.replace("_date_time_stamp", "")
                datetime_obj = pd.to_datetime(date_str, format="(%Y, %m, %d)")

                # Assign the datetime value to the new column
                df.at[index, "extracted_datetime_stamp"] = datetime_obj

    # Display the count of extracted datetime values
    print("Extracted datetime values:")
    print(df["extracted_datetime_stamp"].value_counts())

    if drop:
        columns_to_drop = [col for col in df.columns if "date_time_stamp" in col]
        if columns_to_drop:
            print(f"Dropping {len(columns_to_drop)} date_time_stamp columns")
            df = df.drop(columns=columns_to_drop)

    return df


def filter_annot_dataframe2(dataframe, filter_args):
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
                    dataframe[column].astype(str).isin(value)
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


def produce_filtered_annotation_dataframe(
    cui_filter=False,
    meta_annot_filter=False,
    pat_list=None,
    config_obj=None,
    filter_custom_args=None,
    cui_code_list=None,
    mct=False,
):
    """
    Filter annotation dataframe based on specified criteria.

    Parameters:
    - cui_filter (bool): Whether to filter by CUI codes.
    - meta_annot_filter (bool): Whether to apply meta annotation filtering.
    - pat_list (list): List of patient identifiers.
    - config_obj (ConfigObject): Configuration object containing necessary parameters.
    - filter_custom_args (dict): Custom filter arguments.
    - cui_code_list (list): List of CUI codes for filtering.

    Returns:
    - pd.DataFrame: Filtered annotation dataframe.
    """

    if meta_annot_filter:
        if filter_custom_args is None:
            print("Using config obj filter arguments..")
            filter_args = config_obj.filter_arguments
        else:
            filter_args = filter_custom_args

    results = []

    if pat_list is None:

        print("Using all patient list", len(config_obj.all_patient_list))
        pat_list = config_obj.all_patient_list

    for i in tqdm(range(len(pat_list))):
        current_pat_client_idcode = str(pat_list[i])

        if mct == False:
            current_pat_annot_batch_path = (
                config_obj.pre_document_annotation_batch_path
                + current_pat_client_idcode
                + ".csv"
            )
        else:
            current_pat_annot_batch_path = (
                config_obj.pre_document_annotation_batch_path_mct
                + current_pat_client_idcode
                + ".csv"
            )

        if os.path.exists(current_pat_annot_batch_path):
            current_pat_annot_batch = pd.read_csv(current_pat_annot_batch_path)

            # drop nan on any col:
            necessary_columns = [
                "client_idcode",
                "updatetime",
                "pretty_name",
                "cui",
                "type_ids",
                "types",
                "source_value",
                "detected_name",
                "acc",
                "id",
                "Time_Value",
                "Time_Confidence",
                "Presence_Value",
                "Presence_Confidence",
                "Subject_Value",
                "Subject_Confidence",
            ]

            current_pat_annot_batch = current_pat_annot_batch.dropna(
                subset=necessary_columns
            )

            if meta_annot_filter:
                try:
                    current_pat_annot_batch = filter_annot_dataframe2(
                        current_pat_annot_batch, filter_args
                    )
                except Exception as e:
                    print(e, i)
                    display(current_pat_annot_batch)
                    raise e

            if cui_filter:
                current_pat_annot_batch = current_pat_annot_batch[
                    current_pat_annot_batch["cui"].isin(cui_code_list)
                ]

            results.append(current_pat_annot_batch)

    super_result = pd.concat(results)

    return super_result


def extract_types_from_csv(directory):
    all_types = set()

    # Traverse the directory and its subdirectories
    for root, dirs, files in os.walk(directory):
        print(files)
        for file in files:
            if file.endswith(".csv"):
                # Construct the full file path
                file_path = os.path.join(root, file)

                # Read the CSV file using pandas
                df = pd.read_csv(file_path)

                # Extract the "types" column and add unique values to the set
                types_column = df["types"]
                all_types.update(types_column.unique())

    return list(all_types)


def remove_file_from_paths(
    current_pat_idcode: str, project_name="new_project", verbosity=0, config_obj=None
) -> None:

    if config_obj == None:
        pat_file_paths = [
            f"{project_name}/current_pat_document_batches/",
            f"{project_name}/current_pat_document_batches_mct/",
            f"{project_name}/current_pat_documents_annotations_batches/",
            f"{project_name}/current_pat_documents_annotations_batches_mct/",
            f"{project_name}/current_pat_bloods_batches/",
            f"{project_name}/current_pat_drugs_batches/",
            f"{project_name}/current_pat_diagnostics_batches/",
            f"{project_name}/current_pat_news_batches/",
            f"{project_name}/current_pat_obs_batches/",
            f"{project_name}/current_pat_bmi_batches/",
            f"{project_name}/current_pat_demo_batches/",
            f"{project_name}/current_pat_document_batches_reports/",
            f"{project_name}/current_pat_documents_annotations_batches_reports/",
            f"{project_name}/current_pat_textual_obs_document_batches/",
            f"{project_name}/current_pat_textual_obs_annotation_batches/",
            f"{project_name}/current_pat_appointments_batches/",
        ]
    else:
        project_name = config_obj.proj_name

        pat_file_paths = [
            config_obj.pre_document_batch_path,
            config_obj.pre_document_batch_path_mct,
            config_obj.pre_document_annotation_batch_path,
            config_obj.pre_document_annotation_batch_path_mct,
            config_obj.pre_bloods_batch_path,
            config_obj.pre_drugs_batch_path,
            config_obj.pre_diagnostics_batch_path,
            config_obj.pre_news_batch_path,
            config_obj.pre_obs_batch_path,
            config_obj.pre_bmi_batch_path,
            config_obj.pre_demo_batch_path,
            config_obj.pre_document_batch_path_reports,
            config_obj.pre_document_annotation_batch_path_reports,
            config_obj.pre_textual_obs_annotation_batch_path,
            config_obj.pre_textual_obs_document_batch_path,
            config_obj.pre_appointments_batch_path,
        ]

    # Print debug messages:

    print(f"Removing files for patient {current_pat_idcode}...")
    print("Project_name: ", project_name)
    print("Searching for files in the following paths:")
    print(pat_file_paths)

    for path in pat_file_paths:
        file_path = path + current_pat_idcode + ".csv"
        try:
            os.remove(file_path)
            if verbosity > 0:
                print(f"{file_path} successfully removed")
        except FileNotFoundError:
            if verbosity > 0:
                print(f"{file_path} not found")
        except Exception as e:
            if verbosity > 0:
                print(f"Error removing {file_path}: {e}")


def process_chunk(args):
    part_chunk, all_files, part_size, unique_columns = args
    concatenated_data = {column: [] for column in unique_columns}
    for file in all_files[part_chunk : part_chunk + part_size]:
        if file.endswith(".csv"):
            with open(file, "r", newline="") as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    for column in unique_columns:
                        concatenated_data[column].append(row.get(column, ""))
    return concatenated_data


def join_icd10_codes_to_annot(df, inner=False):

    mfp = (
        "../../snomed_icd10_map/data/tls_Icd10cmHumanReadableMap_US1000124_20230901.tsv"
    )

    mdf = pd.read_csv(mfp, sep="\t")

    if inner == True:
        result = pd.merge(
            df, mdf, left_on="cui", right_on="referencedComponentId", how="inner"
        )

    else:
        result = pd.merge(
            df, mdf, left_on="cui", right_on="referencedComponentId", how="left"
        )

    return result


def join_icd10_OPC4S_codes_to_annot(df, inner=False):

    mfp = "../../snomed_to_icd10_opcs4/map.csv"  # ../home/cogstack/samora/_data/gloabl_files/

    mdf = pd.read_csv(mfp)

    if inner == True:
        result = pd.merge(df, mdf, left_on="cui", right_on="conceptId", how="inner")

    else:
        result = pd.merge(df, mdf, left_on="cui", right_on="conceptId", how="left")

    return result


def filter_and_select_rows(
    dataframe,
    filter_list,
    verbosity=0,
    time_column="updatetime",
    filter_column="cui",
    mode="earliest",
    n_rows=1,
):
    """
    Filter a dataframe based on a filter_column and filter_list, and return either the earliest or latest rows.

    Parameters:
    - dataframe (pd.DataFrame): Input dataframe.
    - filter_list (list): List of values to filter the dataframe.
    - verbosity (int): If True, print additional information during execution.
    - time_column (str): Column representing time, used for sorting if specified.
    - filter_column (str): Column used for filtering based on filter_list.
    - mode (str): Either 'earliest' or 'latest' to specify the rows to return.
    - n_rows (int): Number of rows to return if they exist.

    Returns:
    - pd.DataFrame: Filtered and selected rows from the input dataframe.
    """
    if not all(arg is not None for arg in [dataframe, filter_list, filter_column]):
        raise ValueError(
            "Please provide a valid dataframe, filter_list, and filter_column."
        )

    if filter_column not in dataframe.columns:
        raise ValueError(f"{filter_column} not found in the dataframe columns.")

    filtered_df = dataframe[dataframe[filter_column].isin(filter_list)]

    if time_column:
        filtered_df.sort_values(by=time_column, inplace=True)

    if mode == "earliest":
        selected_rows = filtered_df.head(n_rows)
    elif mode == "latest":
        selected_rows = filtered_df.tail(n_rows)
    else:
        raise ValueError("Invalid mode. Please choose 'earliest' or 'latest'.")

    if verbosity > 10:
        print("Filtered DataFrame:")
        display(filtered_df)
        print(f"Selected {mode} {n_rows} row(s):")
        display(selected_rows)

    return selected_rows


def filter_dataframe_by_cui(
    dataframe,
    filter_list,
    filter_column="cui",
    mode="earliest",
    temporal="before",
    verbosity=0,
    time_column="updatetime",
):
    """
    Filter an annotation DataFrame based on a list of CUI codes and a specified mode.

    Parameters:
    - dataframe (pd.DataFrame): The input DataFrame.
    - filter_list (list): List of CUI codes to filter the DataFrame.
    - filter_column (str): The column containing filter. Default is 'cui'.
    - mode (str): Specifies whether to consider the earliest or latest entry for each filter. Default is "earliest".
    - temporal (str): Specifies whether to retain entries before or after the selected mode entry. Default is "before".
    - verbosity (int): Verbosity level. 0 for no debug statements, higher values for more verbosity.
    - time_column (str): The column containing time information. Default is 'updatetime'.

    Returns:
    - pd.DataFrame: Filtered DataFrame based on the specified criteria.
    """

    # Ensure the time column is in datetime format
    dataframe[time_column] = pd.to_datetime(dataframe[time_column], utc=True)

    # Ensure filter_list contains integers
    filter_list = [int(cui) for cui in filter_list]

    # Filter the DataFrame based on the given CUI codes
    filtered_df = dataframe[dataframe[filter_column].isin(filter_list)]

    # Debug statement for verbosity
    if verbosity > 0:
        print(f"Filtered DataFrame based on {filter_column} codes:\n")
        display(filtered_df.head())

    # Find the earliest or latest entry for each CUI code
    if mode == "earliest":
        result_df = filtered_df.groupby(filter_column, as_index=False)[
            time_column
        ].min()
    elif mode == "latest":
        result_df = filtered_df.groupby(filter_column, as_index=False)[
            time_column
        ].max()
    else:
        raise ValueError("Invalid mode. Use 'earliest' or 'latest'")

    filter_row = result_df.copy()  # preserve row used for filter
    # Debug statement for verbosity
    if verbosity > 0:
        print(f"Result DataFrame based on {mode} mode:\n")
        display(result_df.head())

    # Merge with the original DataFrame to get the full rows
    result_df = pd.merge(
        result_df, dataframe, on=[filter_column, time_column], how="inner"
    )

    # Filter the original DataFrame based on the earliest or latest entry
    if temporal == "before":
        filtered_original_df = dataframe[
            dataframe[time_column] <= result_df[time_column].min()
        ]
    elif temporal == "after":
        filtered_original_df = dataframe[
            dataframe[time_column] >= result_df[time_column].max()
        ]
    else:
        raise ValueError("Invalid temporal value. Use 'before' or 'after'")

    # Debug statement for verbosity
    if verbosity > 0:
        print(f"Filtered original DataFrame based on {temporal} temporal:\n")
        display(filtered_original_df.head())

    return filtered_original_df, filter_row, filtered_df


# Example usage:
# filter_codes = [109989006]
#
# Returns all rows after the earliest cui code match.
# filter_dataframe_by_cui(res, filter_list=filter_codes, filter_column = 'cui', mode="earliest", temporal = 'after', verbosity=3)


def copy_files_and_dirs(
    source_root: str,
    source_name: str,
    destination: str,
    items_to_copy: List[str] = None,
    loose_files: List[str] = None,
) -> None:
    """
    Useful for porting project files to a new project location.

    Copy specified directories and files from the source directory to the destination directory.

    Parameters:
        source_root (str): The root directory of the source project.
        source_name (str): The name of the source project directory.
        destination (str): The destination directory.
        items_to_copy (List[str], optional): List of directories/files to copy. Defaults to None.

    Returns:
        None

    Usage:
        project_root_source = "/home/cogstack/%USERNAME%/_data/HFE_5"
        project_name_source = "new_project"
        project_destination = "."

        copy_files_and_dirs(project_root_source, project_name_source, project_destination)
    """
    source_dir = os.path.join(source_root, source_name)

    # Create the destination directory if it doesn't exist
    destination_dir = os.path.join(destination, source_name)
    os.makedirs(destination_dir, exist_ok=True)

    if items_to_copy is None:
        # List of directories/files to copy
        items_to_copy = [
            "current_pat_annots_parts",
            "current_pat_annots_mrc_parts",
            "outputs",
            "current_pat_document_batches",
            "current_pat_document_batches_mct",
            "current_pat_documents_annotations_batches",
            "current_pat_documents_annotations_batches_mct",
            "current_pat_lines_parts",
        ]

    # Get all paths from the source directory
    all_source_paths = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            all_source_paths.append(
                os.path.relpath(os.path.join(root, file), source_dir)
            )
        for dir in dirs:
            all_source_paths.append(
                os.path.relpath(os.path.join(root, dir), source_dir)
            )

    # Filter paths based on items_to_copy
    paths_to_copy = [
        path for path in all_source_paths if any(item in path for item in items_to_copy)
    ]

    # Copy each path to the destination preserving structure
    for path in tqdm(paths_to_copy, desc="Copying"):
        source_path = os.path.join(source_dir, path)
        destination_path = os.path.join(destination_dir, path)

        if os.path.isdir(source_path):
            os.makedirs(destination_path, exist_ok=True)
        else:
            shutil.copy2(source_path, destination_path)

    if loose_files is None:
        # Look for loose files specifically in the root directory of the source
        loose_files = ["treatment_docs.csv", "control_path.pkl"]

    # Copy loose files to the root directory of the destination
    for loose_file in tqdm(loose_files, desc="Copying loose files"):
        source_loose_path = os.path.join(source_root, loose_file)
        destination_loose_path = os.path.join(destination, loose_file)
        shutil.copy2(source_loose_path, destination_loose_path)

    # Example usage:
    # if __name__ == "__main__":
    # project_root_source = "/home/cogstack/%USERNAME%/_data/HFE_5"
    # project_name_source = "new_project"
    # project_destination = "."

    # copy_files_and_dirs(project_root_source, project_name_source, project_destination)


def filter_and_update_csv(
    target_directory, ipw_dataframe, filter_type="after", verbosity=False
):
    for _, row in ipw_dataframe.iterrows():
        client_idcode = row["client_idcode"]
        # print(client_idcode, row['updatetime'])
        # filter_date = pd.to_datetime(row['updatetime']).tz_convert('UTC')  # Convert filter_date to UTC
        filter_date = pd.to_datetime(row["updatetime"], utc=True, errors="coerce")

        if verbosity:
            print(f"Processing client_idcode: {client_idcode}")

        # Recursively walk through the target directory
        for root, dirs, files in os.walk(target_directory):
            for file in files:
                if file.startswith(client_idcode) and file.endswith(".csv"):
                    file_path = os.path.join(root, file)

                    if verbosity:
                        print(f"Found CSV file: {file_path}")

                    df = pd.read_csv(file_path)

                    # Check if 'updatetime' is in columns
                    if "updatetime" in df.columns:
                        df["updatetime"] = pd.to_datetime(
                            df["updatetime"], utc=True, errors="coerce"
                        )
                        update_column = "updatetime"
                    elif "observationdocument_recordeddtm" in df.columns:
                        df["observationdocument_recordeddtm"] = pd.to_datetime(
                            df["observationdocument_recordeddtm"],
                            utc=True,
                            errors="coerce",
                        )
                        update_column = "observationdocument_recordeddtm"
                    elif "order_entered" in df.columns:
                        df["order_entered"] = pd.to_datetime(
                            df["order_entered"], utc=True, errors="coerce"
                        )
                        update_column = "order_entered"
                    elif "basicobs_entered" in df.columns:
                        df["basicobs_entered"] = pd.to_datetime(
                            df["basicobs_entered"], utc=True, errors="coerce"
                        )
                        update_column = "basicobs_entered"
                    else:
                        print(
                            f"Warning: Neither 'updatetime', 'observationdocument_recordeddtm', 'order_entered', nor 'basicobs_entered' found in {file_path}"
                        )

                    # Drop rows with NaT values in the updated column
                    df = df.dropna(subset=[update_column])

                    if verbosity:
                        print(f"Updating CSV file based on {update_column}")

                    df[update_column] = pd.to_datetime(df[update_column], utc=True)
                    filter_condition = (
                        df[update_column] > filter_date
                        if filter_type == "after"
                        else df[update_column] < filter_date
                    )
                    filtered_df = df[filter_condition]
                    filtered_df.to_csv(file_path, index=False)

                    if verbosity:
                        print("CSV file updated successfully")


def retrieve_pat_annots_mct_epr(
    client_idcode,
    config_obj,
    columns_epr=None,
    columns_mct=None,
    columns_to=None,
    columns_report=None,
    merge_columns=True,
):
    # Define file paths based on the client_idcode and config paths
    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path
    pre_document_annotation_batch_path_mct = (
        config_obj.pre_document_annotation_batch_path_mct
    )
    pre_textual_obs_annotation_batch_path = (
        config_obj.pre_textual_obs_annotation_batch_path
    )
    pre_document_annotation_batch_path_reports = (
        config_obj.pre_document_annotation_batch_path_reports
    )

    epr_file_path = f"{pre_document_annotation_batch_path}/{client_idcode}.csv"
    mct_file_path = f"{pre_document_annotation_batch_path_mct}/{client_idcode}.csv"
    textual_obs_files_path = (
        f"{pre_textual_obs_annotation_batch_path}/{client_idcode}.csv"
    )
    report_file_path = (
        f"{pre_document_annotation_batch_path_reports}/{client_idcode}.csv"
    )

    # Initialize DataFrames
    dfa = pd.DataFrame()
    dfa_mct = pd.DataFrame()
    dfa_to = pd.DataFrame()
    dfr = pd.DataFrame()

    # Load data if files exist
    if os.path.exists(epr_file_path):
        dfa = pd.read_csv(epr_file_path, usecols=columns_epr)
        dfa["annotation_batch_source"] = "epr"

    if os.path.exists(mct_file_path):
        dfa_mct = pd.read_csv(mct_file_path, usecols=columns_mct)
        dfa_mct["annotation_batch_source"] = "mct"

    if os.path.exists(textual_obs_files_path):
        dfa_to = pd.read_csv(textual_obs_files_path, usecols=columns_to)
        dfa_to["annotation_batch_source"] = "textual_obs"

    if os.path.exists(report_file_path):
        dfr = pd.read_csv(report_file_path, usecols=columns_report)
        dfr["annotation_batch_source"] = "report"

    # Concatenate all dataframes
    all_annots = pd.concat([dfa, dfa_mct, dfa_to, dfr], axis=0, ignore_index=True)

    # Merge columns if required
    if merge_columns and not all_annots.empty:
        if "observationannotation_recordeddtm" in all_annots.columns:
            all_annots["updatetime"] = all_annots["updatetime"].fillna(
                all_annots["observationannotation_recordeddtm"]
            )

            all_annots["observationannotation_recordeddtm"] = all_annots[
                "observationannotation_recordeddtm"
            ].fillna(all_annots["updatetime"])

        if "basicobs_entered" in all_annots.columns:

            all_annots["updatetime"] = all_annots["updatetime"].fillna(
                all_annots["basicobs_entered"]
            )

        if "observationdocument_recordeddtm" in all_annots.columns:

            all_annots["updatetime"] = all_annots["updatetime"].fillna(
                all_annots["observationdocument_recordeddtm"]
            )

        if "basicobs_guid" in all_annots.columns:

            if "document_guid" in all_annots.columns:
                # Merge observation_guid to document_guid
                all_annots["document_guid"] = all_annots["document_guid"].fillna(
                    all_annots["basicobs_guid"]
                )
            else:
                all_annots["document_guid"] = all_annots["basicobs_guid"]

        if "observation_guid" in all_annots.columns:
            all_annots["document_guid"] = all_annots["document_guid"].fillna(
                all_annots["observation_guid"]
            )

        if "obscatalogmasteritem_displayname" in all_annots.columns:
            # Add obscatalogmasteritem_displayname to annotation_description
            all_annots["annotation_description"] = all_annots[
                "annotation_description"
            ].fillna(all_annots["obscatalogmasteritem_displayname"])

        if "observation_valuetext_analysed" in all_annots.columns:
            all_annots["body_analysed"] = all_annots["body_analysed"].fillna(
                all_annots["observation_valuetext_analysed"]
            )

    return all_annots


def check_list_presence(df, column, lst, annot_filter_arguments=None):

    if annot_filter_arguments is not None:
        df = filter_annot_dataframe2(df, annot_filter_arguments)

    str_lst = list(map(str, lst))  # Convert elements to strings
    return any(
        df[column].astype(str).str.contains("|".join(str_lst), case=False, na=False)
    )


def filter_dataframe_n_lists(df, column_name, n_lists):
    # Create a mask for each list in n_lists
    masks = [df[column_name].isin(lst) for lst in n_lists]

    # Combine masks with logical AND to get the final mask
    final_mask = pd.concat(masks, axis=1).all(axis=1)

    # Apply the mask to the DataFrame
    filtered_df = df[final_mask]

    return filtered_df


def get_all_target_annots(
    all_pat_list, n_lists, config_obj=None, annot_filter_arguments=None
):
    results_df = pd.DataFrame()

    for sublist in n_lists:
        for i, element in enumerate(sublist):
            sublist[i] = int(element)

    for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):

        current_pat_idcode = all_pat_list[i]

        all_annots = retrieve_pat_annots_mct_epr(current_pat_idcode, config_obj)

        all_annots.dropna(subset="acc", inplace=True)

        if annot_filter_arguments is not None:
            all_annots = filter_annot_dataframe2(all_annots, annot_filter_arguments)

        annots_to_return = filter_dataframe_n_lists(all_annots, "cui", n_lists)

        if annots_to_return:
            filtered_df = all_annots[all_annots["cui"].isin(list(chain(*n_lists)))]
            results_df = pd.concat([results_df, filtered_df])

    results_df.to_csv("all_target_annots.csv")

    return results_df


def extract_datetime_from_binary_columns(df):
    """
    Extracts datetime values from binary columns representing dates in a DataFrame.

    Parameters:
        df (DataFrame): The DataFrame containing the binary columns with '_date_time_stamp' in column names.

    Returns:
        list: A list of datetime values extracted from the binary columns.
    """
    # Extracting date columns with '_date_time_stamp' in their names
    date_columns = [
        col.strip("()").split(")")[0] for col in df.columns if "_date_time_stamp" in col
    ]

    date_columns_raw = [col for col in df.columns if "_date_time_stamp" in col]

    date_time_column_values = []

    # Iterate over each row in the DataFrame
    for index, row in tqdm(df.iterrows(), total=len(df)):

        # Iterate over each date column
        for i in range(len(date_columns)):
            if row[date_columns_raw[i]] == 1:  # Check if the value in the column is 1
                date_string = date_columns[i]

                # Split the date string and convert each part to integer
                date_parts = [int(part) for part in date_string.split(", ")]

                # Unpack date_parts and create a datetime object
                formatted_date = datetime(*date_parts)

                # Append the datetime object to the list
                date_time_column_values.append(formatted_date)

    df["datetime"] = date_time_column_values
    return df


# Example usage:
# Assuming df_copy is defined
# df = extract_datetime_from_binary_columns(df)


def extract_datetime_from_binary_columns_chunk_reader(filepath):
    """
    Extracts datetime values from binary columns representing dates in a DataFrame.

    Parameters:
        filepath (str): The file path to the DataFrame containing the binary columns with '_date_time_stamp' in column names.

    Returns:
        DataFrame: The DataFrame with the 'datetime' column appended.
    """
    chunk_size = 1000  # Adjust this value based on your RAM capacity
    date_time_column_values = []

    # Iterate over DataFrame chunks
    for chunk in pd.read_csv(filepath, chunksize=chunk_size):
        # Extracting date columns with '_date_time_stamp' in their names
        date_columns = [
            col.strip("()").split(")")[0]
            for col in chunk.columns
            if "_date_time_stamp" in col
        ]
        date_columns_raw = [col for col in chunk.columns if "_date_time_stamp" in col]

        # Iterate over each row in the chunk
        for index, row in tqdm(chunk.iterrows(), total=len(chunk)):
            # Iterate over each date column
            for i in range(len(date_columns)):
                if (
                    row[date_columns_raw[i]] == 1
                ):  # Check if the value in the column is 1
                    date_string = date_columns[i]

                    # Split the date string and convert each part to integer
                    date_parts = [int(part) for part in date_string.split(", ")]

                    # Unpack date_parts and create a datetime object
                    formatted_date = datetime(*date_parts)

                    # Append the datetime object to the list
                    date_time_column_values.append(formatted_date)

    # Add 'datetime' column to the chunk DataFrame
    chunk["datetime"] = date_time_column_values
    return chunk


def drop_columns_with_all_nan(df):
    # Identify columns where all values are NaN or None
    nan_columns = df.columns[df.isna().all()]

    # Drop columns with all NaN or None values
    df.drop(columns=nan_columns, inplace=True)

    return df, nan_columns


def save_missing_values_pickle(df, out_file_path, overwrite=False):
    # Calculate percentage of missing values for each column
    missing_percentages = (df.isnull().sum() / len(df)) * 100

    # Create dictionary with column names as keys and percentage missing as values
    missing_dict = missing_percentages.to_dict()

    # Extracting the directory and filename from out_file_path
    output_dir = os.path.dirname(out_file_path)
    filename = os.path.basename(out_file_path)

    # Constructing the output pickle file path
    pickle_filename = os.path.splitext(filename)[0] + "_missing_dict.pickle"
    pickle_file_path = os.path.join(output_dir, pickle_filename)

    # Check if the pickle file already exists
    if os.path.exists(pickle_file_path) and not overwrite:
        print(
            f"Skipping saving as '{pickle_filename}' already exists and overwrite is set to False."
        )
    else:
        # Write the dictionary to the pickle file
        with open(pickle_file_path, "wb") as f:
            pickle.dump(missing_dict, f)

        print(f"Missing values dictionary written to: {pickle_file_path}")


# Example usage:
# Provide the path to your CSV file as an argument to the function

# save_missing_values_pickle(df, out_file_path)


def convert_true_to_float(
    df,
    columns=[
        "census_black_african_caribbean_or_black_british",
        "census_mixed_or_multiple_ethnic_groups",
        "census_white",
        "census_asian_or_asian_british",
        "census_other_ethnic_group",
    ],
):
    """
    Convert 'True' strings to float 1.0 in specified columns and
    ensure those columns are of float datatype.

    Parameters:
        df (pandas.DataFrame): The DataFrame to operate on.
        columns (list): List of column names to convert.

    Returns:
        pandas.DataFrame: DataFrame with specified columns converted.
    """
    # Replace 'True' with 1.0 in the specified columns
    df[columns] = df[columns].replace("True", 1.0)

    # Convert the columns to float datatype
    df[columns] = df[columns].astype(float)

    return df


def impute_datetime(
    df,
    datetime_column="datetime",
    patient_column="client_idcode",
    forward=True,
    backward=True,
    mean_impute=True,
    verbose=False,
):
    start_time = time.time()
    if verbose:
        print("Converting datetime column to datetime type...")
    df[datetime_column] = pd.to_datetime(df[datetime_column])

    if verbose:
        print("Sorting DataFrame by patient_column and datetime_column...")
    df = df.sort_values(by=[patient_column, datetime_column])
    end_time = time.time()
    if verbose:
        print(
            "Sorting complete. Time taken: {:.2f} seconds.".format(
                end_time - start_time
            )
        )

    if forward:
        start_time = time.time()
        if verbose:
            print("Forward filling missing values per patient_column...")
        df = df.groupby(patient_column).ffill()
        end_time = time.time()
        if verbose:
            print(
                "Forward filling complete. Time taken: {:.2f} seconds.".format(
                    end_time - start_time
                )
            )

    if backward:
        start_time = time.time()
        if verbose:
            print("Backward filling missing values per patient_column...")
        df = df.groupby(patient_column).bfill()
        end_time = time.time()
        if verbose:
            print(
                "Backward filling complete. Time taken: {:.2f} seconds.".format(
                    end_time - start_time
                )
            )

    if mean_impute:
        start_time = time.time()
        if verbose:
            print("Mean imputing missing values...")
        df = df.fillna(df.mean())
        end_time = time.time()
        if verbose:
            print(
                "Mean imputing complete. Time taken: {:.2f} seconds.".format(
                    end_time - start_time
                )
            )

    if verbose:
        print("Imputation complete.")

    return df


def impute_dataframe(
    df,
    verbose=True,
    datetime_column="datetime",
    patient_column="client_idcode",
    forward=True,
    backward=True,
    mean_impute=True,
):
    start_time = time.time()

    numeric_columns = df.select_dtypes(include="number").columns.tolist()

    df = df.sort_values(by=[patient_column, datetime_column])

    for i in tqdm(range(0, len(numeric_columns))):
        if forward:
            df[numeric_columns[i]] = df.groupby(patient_column, as_index=True)[
                numeric_columns[i]
            ].ffill()
        if backward:
            df[numeric_columns[i]] = df.groupby(patient_column, as_index=True)[
                numeric_columns[i]
            ].bfill()
        if mean_impute:
            df[numeric_columns[i]] = df[numeric_columns[i]].fillna(
                df[numeric_columns[i]].mean()
            )

    if verbose:
        print("Preprocessing took: %s seconds" % (time.time() - start_time))

    return df


def missing_percentage_df(dataframe):
    """
    Calculate the percentage of missing values in each column of a DataFrame.

    Parameters:
    dataframe (DataFrame): The input DataFrame.

    Returns:
    DataFrame: A DataFrame containing two columns: 'Column' (column names) and 'MissingPercentage' (percentage of missing values).
    """
    # Calculate percentage of missing values
    missing_percentage = dataframe.isnull().mean() * 100

    # Create a new DataFrame to store the missing percentage
    missing_df = pd.DataFrame(
        {
            "Column": missing_percentage.index,
            "MissingPercentage": missing_percentage.values,
        }
    )

    return missing_df


def aggregate_dataframe_mean(df, group_by_column="client_idcode"):
    # Convert non-float columns to string
    # non_float_columns = df.select_dtypes(include=['object']).columns
    # df[non_float_columns] = df[non_float_columns].astype(str)

    # Define aggregation function
    def custom_aggregation(x):
        agg_values = {}
        for col in x.columns:
            if pd.api.types.is_numeric_dtype(x[col].dtype):
                agg_values[col] = x[col].mean()
            else:
                agg_values[col] = x[col].iloc[0]
        return pd.Series(agg_values)

    # Group by specified column and aggregate
    grouped = df.groupby(group_by_column)

    # Use tqdm for progress tracking
    tqdm.pandas(desc="Aggregating")
    aggregated_df = grouped.progress_apply(custom_aggregation)

    return aggregated_df


def collapse_df_to_mean(
    df, output_filename="output.csv", client_idcode_string="client_idcode"
):
    """
    Collapse a DataFrame to calculate mean values for numeric columns and retain the first non-numeric value for non-numeric columns
    for each unique client_idcode.

    Args:
        df (DataFrame): Input DataFrame containing client_idcode and other columns.
        output_filename (str, optional): Name of the output file to save the processed DataFrame. Default is 'output.csv'.
        client_idcode_string (str, optional): Name of the client_idcode column in the DataFrame. Default is 'client_idcode'.

    Returns:
        None: Saves the processed DataFrame to the specified output file.
    """
    # Check if the output file exists
    if os.path.exists(output_filename):
        # Read the output DataFrame from the existing file
        output_df = pd.read_csv(output_filename)
    else:
        # Initialize an empty DataFrame for output
        output_df = pd.DataFrame(columns=df.columns)
        # Write the header to the output file
        # output_df.to_csv(output_filename, index=False)

    # Function to check if a row exists in the output DataFrame
    def row_exists(client_idcode):
        return any(output_df[client_idcode_string] == client_idcode)

    len_out_df = len(output_df)
    started = False
    # Iterate over unique client_idcodes with tqdm progress bar
    for client_idcode in tqdm(
        df[client_idcode_string].unique(),
        desc="Processing",
        total=len(df[client_idcode_string].unique()),
    ):
        # Check if the row already exists in output_df
        if not row_exists(client_idcode):
            # Filter rows for the current client_idcode
            client_df = df[df[client_idcode_string] == client_idcode]

            # Initialize a dictionary to store mean values and first non-numeric values
            mean_values = {}
            first_non_numeric_values = {}

            # Iterate over columns
            for column in df.columns:
                # Check if the column is numeric
                if pd.api.types.is_numeric_dtype(df[column]):
                    mean_values[column] = client_df[column].mean()
                else:
                    first_non_numeric_values[column] = client_df[column].iloc[0]

            # Append mean values and first non-numeric values to output_df
            row_data = {
                client_idcode_string: client_idcode,
                **mean_values,
                **first_non_numeric_values,
            }
            if started == False and len_out_df == 0:
                pd.DataFrame([row_data]).to_csv(
                    output_filename, mode="a", index=False, header=True
                )
                started = True
            else:
                pd.DataFrame([row_data]).to_csv(
                    output_filename, mode="a", index=False, header=False
                )


def plot_missing_pattern_bloods(dfb):
    """
    Plots the number of missing client_idcodes for the top 50 most frequent
    'basicobs_itemname_analysed' values in the given dataframe dfb.

    Parameters:
    dfb (pd.DataFrame): DataFrame containing the columns 'client_idcode' and 'basicobs_itemname_analysed'.
    """

    # Step 1: Identify the top 50 basicobs_itemname_analysed by frequency
    top_items = dfb["basicobs_itemname_analysed"].value_counts().nlargest(50).index

    # Filter the data for these top items
    filtered_dfb = dfb[dfb["basicobs_itemname_analysed"].isin(top_items)]

    # Step 2: Group by basicobs_itemname_analysed and get the unique client_idcodes
    clients_per_item = filtered_dfb.groupby("basicobs_itemname_analysed")[
        "client_idcode"
    ].apply(set)

    # Get all unique client_idcodes
    all_clients = set(dfb["client_idcode"])

    # Step 3: Calculate missing client_idcodes for each top basicobs_itemname_analysed
    missing_clients_counts = {
        item: len(all_clients - clients) for item, clients in clients_per_item.items()
    }

    # Convert to DataFrame for plotting
    missing_clients_df = pd.DataFrame(
        list(missing_clients_counts.items()),
        columns=["basicobs_itemname_analysed", "missing_client_count"],
    ).sort_values(by="missing_client_count", ascending=False)

    # Step 4: Plot the results
    plt.figure(figsize=(10, 8))
    plt.barh(
        missing_clients_df["basicobs_itemname_analysed"],
        missing_clients_df["missing_client_count"],
        color="skyblue",
    )
    plt.xlabel("Number of Missing Client ID Codes")
    plt.ylabel("Basicobs Item Name Analysed")
    plt.title("Missing Client ID Codes per Top 50 Basicobs Item Names")
    plt.gca().invert_yaxis()  # Invert y-axis for better readability
    plt.tight_layout()
    plt.show()


# Example usage:
# plot_missing_pattern_bloods(dfb)
