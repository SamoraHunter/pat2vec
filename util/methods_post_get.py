from typing import Union
import os
import pandas as pd
from tqdm import tqdm
import shutil


def retrieve_pat_annotations(current_pat_client_idcode: str, config_obj= None) -> pd.DataFrame:
    """
    Concatenates data from two CSV files (EPR and MCT) into a single dataframe.
    Maps values from 'observationdocument_recordeddtm' to a new column 'updatetime' in the MCT dataframe.

    Parameters:
    - current_pat_client_idcode (str): The client ID code.
    - config_obj (Union[YourConfigObjectType, dict]): The configuration object containing paths.

    Returns:
    pd.DataFrame: Concatenated dataframe with the 'updatetime' column added.
    """
    # Specify the file paths
    current_pat_docs_epr = os.path.join(config_obj.pre_document_annotation_batch_path, current_pat_client_idcode + '.csv')
    current_pat_docs_mct = os.path.join(config_obj.pre_document_annotation_batch_path_mct, current_pat_client_idcode + '.csv')

    # Read CSV files into dataframes
    df_epr = pd.read_csv(current_pat_docs_epr)
    df_mct = pd.read_csv(current_pat_docs_mct)

    # Check if 'updatetime' column exists in df_mct, if not, create it and map values
    if 'updatetime' not in df_mct.columns:
        df_mct['updatetime'] = df_mct['observationdocument_recordeddtm'].map(lambda x: pd.to_datetime(x, errors='coerce'))

    # Concatenate dataframes
    result_df = pd.concat([df_epr, df_mct], axis=0, ignore_index=True)

    return result_df


def copy_project_folders_with_substring_match(pat2vec_obj, substrings_to_match=None):
    if substrings_to_match is None:
        substrings_to_match = ['batches', 'annots']

    base_project_name = pat2vec_obj.config_obj.proj_name
    suffix = 1
    new_project_name = f"{base_project_name}_{suffix}"

    while os.path.exists(new_project_name):
        suffix += 1
        new_project_name = f"{base_project_name}_{suffix}"

    os.makedirs(new_project_name)

    old_project_folders = os.listdir(base_project_name)

    for folder in tqdm(old_project_folders, desc="Copying folders"):
        if any(substring in folder for substring in substrings_to_match):
            src_path = os.path.join(base_project_name, folder)
            dest_path = os.path.join(new_project_name, folder)
            shutil.copytree(src_path, dest_path)

    print("Folders copied successfully.")