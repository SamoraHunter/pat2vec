import os
import pickle
import random
import re
import sys
from typing import List

import pandas as pd

from notebooks.test_files.read_test_file import read_test_data


def extract_treatment_id_list_from_docs(config_obj):
    """
    Retrieves a list of unique client IDs from a treatment document specified in the configuration.

    Parameters:
    - config_obj (object): An object containing configuration parameters.
        - treatment_doc_filename (str): The filename of the treatment document (CSV or XLSX format).
        - patient_id_column_name (str): The column name for patient IDs. If 'auto', use regex to find the most likely column.

    Returns:
    - list: A list of unique client IDs from the treatment document.
    """

    # Extract the treatment document filename from the configuration object
    treatment_doc_filename = config_obj.treatment_doc_filename

    try:
        with open(config_obj.treatment_doc_filename, "r") as file:
            # Process the file here, for example, read its content into a list
            content = file.readlines()
    except FileNotFoundError:
        print("Warning: File doesn't exist. Returning an empty list.")
        content = []
        return []

    # Determine the file format based on the file extension
    file_extension = treatment_doc_filename.split(".")[-1].lower()

    # Read the treatment document into a pandas DataFrame based on the file format
    if file_extension == "csv":
        docs = pd.read_csv(treatment_doc_filename)
    elif file_extension in ["xlsx", "xls"]:
        docs = pd.read_excel(treatment_doc_filename)
    else:
        raise ValueError(
            f"Unsupported file format: {file_extension}. Please provide a CSV or XLSX file."
        )

    # If patient_id_column_name is 'auto', use regex to find the most likely column
    if config_obj.patient_id_column_name == "auto":
        # Define regex patterns for sample IDs
        sample_id_patterns = ["P\d{6}", "V\d{6}"]

        # Iterate through columns and find the one with the most matches to sample ID patterns
        best_match_column = None
        max_matches = 0
        for column in docs.columns:
            column_matches = sum(
                docs[column]
                .astype(str)
                .str.contains("|".join(sample_id_patterns), na=False)
            )
            if column_matches > max_matches:
                max_matches = column_matches
                best_match_column = column

        if best_match_column is not None:
            if config_obj.verbosity > 2:
                print("best_match_column:", best_match_column)
            config_obj.patient_id_column_name = best_match_column
        else:
            if config_obj.verbosity > 2:
                print("best_match_column: None, attempting default client_idcode")
            config_obj.patient_id_column_name = "client_idcode"

            # raise ValueError("Unable to automatically determine patient ID column.")

    # drop the nan in column
    docs[config_obj.patient_id_column_name].dropna(inplace=True)

    # Extract the unique client IDs from the document
    treatment_client_id_list = list(docs[config_obj.patient_id_column_name].unique())

    return treatment_client_id_list


def generate_control_list(
    treatment_client_id_list: List[str],
    treatment_control_ratio_n: int,
    control_list_path: str = "control_list.pkl",
    all_epr_patient_list_path: str = "none",
    verbosity: int = 0,
) -> None:
    """
    Generate a list of control patients for a given list of treatment patients.

    Args:
        treatment_client_id_list (List[str]): A list of client IDs for the treatment group.
        treatment_control_ratio_n (int): The ratio of control patients to treatment patients.
        control_list_path (str): The path to save the control list.
        verbosity (int, optional): The level of verbosity. Defaults to 0.

    Returns:
        all_patient_list_control
    """
    random.seed(42)

    # Get control docs default 1:1

    all_idcodes = pd.read_csv(all_epr_patient_list_path)["client_idcode"]
    # all_idcodes = pd.read_csv('/home/cogstack/samora/_data/gloabl_files/all_client_idcodes_epr_unique.csv')['client_idcode']

    full_control_client_id_list = list(set(all_idcodes) - set(treatment_client_id_list))
    full_control_client_id_list.sort()  # ensure sort for repeatability

    n_treatments = len(treatment_client_id_list) * treatment_control_ratio_n
    if verbosity > 0:
        print(
            f"{n_treatments} selected as controls"
        )  # Soft control selection, many treatments will be false positives

    treatment_control_sample = pd.DataFrame(full_control_client_id_list).sample(
        n_treatments, random_state=42
    )[0]
    all_patient_list_control = list(treatment_control_sample.values)

    with open(control_list_path, "wb") as f:
        pickle.dump(all_patient_list_control, f)

    if verbosity > 0:
        print(all_patient_list_control[0:10])

    return all_patient_list_control


def sanitize_hospital_ids(hospital_ids, config_obj):
    valid_format = re.compile(
        r"^[A-Z]\d{6}$"
    )  # Regular expression for one uppercase letter followed by 6 digits
    valid_count = 0
    uppercase_warning_count = 0
    digit_warning_count = 0
    changes_made = 0

    # Debug and warnings before sanitization
    for hospital_id in hospital_ids:
        if valid_format.match(hospital_id):
            valid_count += 1
        else:
            if not re.match(r"^[A-Z]", hospital_id):
                uppercase_warning_count += 1
            if not re.match(r"^\d{6}$", hospital_id[1:]):
                digit_warning_count += 1

    if config_obj.verbosity > 0:
        print(
            f"Debug: Number of hospital IDs conforming to the format before sanitization: {valid_count}"
        )

    if (
        config_obj.verbosity > 1
    ):  # Only print detailed warnings at a higher verbosity level
        if uppercase_warning_count > 0:
            print(
                f"Warning: Number of hospital IDs that do not start with an uppercase letter: {uppercase_warning_count}"
            )

        if digit_warning_count > 0:
            print(
                f"Warning: Number of hospital IDs that do not have exactly 6 digits following the letter: {digit_warning_count}"
            )

    if config_obj.sanitize_pat_list:
        sanitized_list = []
        for hospital_id in hospital_ids:
            new_id = hospital_id.upper()
            if new_id != hospital_id:
                changes_made += 1
            sanitized_list.append(new_id)

        # After sanitization
        if config_obj.verbosity > 0:
            print(f"Info: Number of hospital IDs changed to uppercase: {changes_made}")

        # Warning on irregular number of digits after sanitization
        irregular_count = sum(len(hospital_id) != 7 for hospital_id in sanitized_list)
        if irregular_count > 0 and config_obj.verbosity > 1:
            print(
                f"Warning: Number of hospital IDs that do not have exactly 7 characters: {irregular_count}"
            )

        # Assuming all_patient_list should be returned or assigned
        return sanitized_list
    else:
        return hospital_ids


def get_all_patients_list(config_obj):
    """
    Extracts a list of all patient IDs from the given configuration object.

    Args:
        config_obj: A configuration object containing the necessary parameters.

    Returns:
        A list of all patient IDs.
    """
    if config_obj.testing == False:

        patient_ids = extract_treatment_id_list_from_docs(config_obj)

    else:

        file_path = "notebooks/test_files/treatment_docs.csv"

        patient_ids = read_test_data()["client_idcode"]

    all_patient_list = patient_ids.copy()

    all_patient_list = pd.Series(all_patient_list).dropna().to_list()

    all_epr_patient_list_path = config_obj.all_epr_patient_list_path

    if config_obj.use_controls:

        control_ids = generate_control_list(
            treatment_client_id_list=patient_ids,
            treatment_control_ratio_n=config_obj.treatment_control_ratio_n,
            control_list_path=config_obj.control_list_path,
            verbosity=config_obj.verbosity,
            all_epr_patient_list_path=all_epr_patient_list_path,
        )
        all_patient_list.extend(control_ids)

    all_patient_list = sanitize_hospital_ids(
        hospital_ids=all_patient_list, config_obj=config_obj
    )

    # Propensity score matching here or in super function?

    return all_patient_list
