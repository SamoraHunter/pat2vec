import pickle
import random
import re
from typing import Any, Dict, List

import numpy as np
from collections import Counter
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import CountVectorizer

from pat2vec.util.testing_helpers import read_test_data


def extract_treatment_id_list_from_docs(config_obj: Any) -> List[str]:
    """Retrieves a list of unique client IDs from a treatment document.

    This function reads a CSV or XLSX file specified in the configuration,
    identifies the column containing patient IDs (either explicitly or via
    auto-detection), and returns a list of unique IDs. It can also sample
    the document to a smaller size if configured.

    Args:
        config_obj: A configuration object containing parameters like
            `treatment_doc_filename`, `patient_id_column_name`, and
            `sample_treatment_docs`.

    Returns:
        A list of unique client IDs from the treatment document.

    Raises:
        ValueError: If the file format is not CSV or XLSX.
    """
    random.seed(config_obj.random_seed_val)
    np.random.seed(config_obj.random_seed_val)

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

        # drop the nan in column
        docs[config_obj.patient_id_column_name].dropna(inplace=True)

        if config_obj.sample_treatment_docs > 0:
            # Determine the number of samples by taking the smaller of the requested
            # number and the total number of available rows.
            n_samples = min(config_obj.sample_treatment_docs, len(docs))

            if config_obj.verbosity >= 1:
                # The print statement now reflects the actual number of samples being taken.
                print(f"Sampling {n_samples} of {len(docs)} available treatment docs.")

            # Safely sample the DataFrame.
            docs = docs.sample(n_samples)

    # Extract the unique client IDs from the document
    treatment_client_id_list = list(docs[config_obj.patient_id_column_name].unique())

    return treatment_client_id_list


def generate_control_list(
    treatment_client_id_list: List[str],
    treatment_control_ratio_n: int,
    control_list_path: str = "control_list.pkl",
    all_epr_patient_list_path: str = "none",
    verbosity: int = 0,
) -> List[str]:
    """Generates and saves a list of control patients.

    This function creates a control group by taking a master list of all
    patient IDs, removing the IDs from the provided treatment list, and then
    randomly sampling from the remaining pool. The size of the control group
    is determined by the size of the treatment group and the specified ratio.
    The resulting list of control IDs is saved to a pickle file.

    Args:
        treatment_client_id_list: A list of client IDs for the treatment group.
        treatment_control_ratio_n: The desired ratio of control patients to
            treatment patients (e.g., 2 for a 2:1 ratio).
        control_list_path: The file path to save the generated control list pickle file.
        all_epr_patient_list_path: The file path to the CSV containing all possible
            patient IDs.
        verbosity: The level of verbosity for logging.

    Returns:
        A list of client IDs for the generated control group.
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


def sanitize_hospital_ids(hospital_ids: List[str], config_obj: Any) -> List[str]:
    """Sanitizes a list of hospital IDs by converting them to uppercase.

    This function iterates through a list of hospital IDs, converts each to
    uppercase, and provides warnings if the IDs do not conform to the expected
    format (e.g., one letter followed by six digits).

    Args:
        hospital_ids: A list of hospital IDs to be sanitized.
        config_obj: A configuration object containing the `verbosity` and
            `sanitize_pat_list` flags.

    Returns:
        The sanitized list of hospital IDs.
    """
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


def get_all_patients_list(config_obj: Any) -> List[str]:
    """Extracts and prepares the final list of all patient IDs for the pipeline.

    This function serves as the main entry point for generating the patient cohort.
    It orchestrates several steps:

    1.  Extracts the initial list of patient IDs, either from a treatment document,
        an individual patient window (IPW) DataFrame, or a test data file.
    2.  If `use_controls` is enabled in the config, it generates a corresponding
        list of control patient IDs and appends them to the main list.
    3.  Sanitizes the final list of IDs (e.g., converts to uppercase).
    4.  Optionally samples the final list down to a smaller size if
        `sample_treatment_docs` is configured.

    Args:
        config_obj: The main configuration object containing all necessary
            parameters.

    Returns:
        A list of all patient IDs to be processed by the pipeline.

    Raises:
        ValueError: If required configuration parameters are missing (e.g.,
            `test_data_path` in testing mode).
    """
    if config_obj.individual_patient_window:
        if config_obj.verbosity > 0:
            print("Using patient list from individual_patient_window_df")

        ipw_df = config_obj.individual_patient_window_df
        id_column = config_obj.individual_patient_id_column_name

        if ipw_df is None or id_column is None:
            raise ValueError(
                "For individual_patient_window, both 'individual_patient_window_df' and 'individual_patient_id_column_name' must be provided in config."
            )

        if id_column not in ipw_df.columns:
            raise ValueError(
                f"Column '{id_column}' not found in individual_patient_window_df."
            )

        patient_ids = ipw_df[id_column].unique().tolist()

    elif config_obj.testing == False:

        patient_ids = extract_treatment_id_list_from_docs(config_obj)

    else:

        if not hasattr(config_obj, "test_data_path") or not config_obj.test_data_path:
            raise ValueError(
                "In testing mode, 'test_data_path' must be set in the config object."
            )

        test_df = read_test_data(config_obj.test_data_path)

        if test_df is not None and "client_idcode" in test_df.columns:
            patient_ids = test_df["client_idcode"]
        else:
            # Return an empty Series if file is not found or malformed
            patient_ids = pd.Series([])

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

    try:
        analyze_client_codes(all_patient_list)
    except Exception as e:
        print("failed to analyze_client_codes")
        print(e)

    if config_obj.sample_treatment_docs > 0:
        random.seed(config_obj.random_seed_val)
        np.random.seed(config_obj.random_seed_val)

        n_samples = min(config_obj.sample_treatment_docs, len(all_patient_list))

        if config_obj.verbosity >= 0:
            # The print statement now reflects the actual number of samples being taken.
            print(
                f"Sampling {n_samples} of {len(all_patient_list)} available treatment docs."
            )

            # Safely sample the DataFrame.
            all_patient_list = pd.Series(all_patient_list).sample(n_samples).to_list()

            print("all_patient_list size now:", len(all_patient_list))

    return all_patient_list


def analyze_client_codes(
    client_idcode_list: List[str], min_val: int = 3
) -> Dict[str, Any]:
    """Analyzes and clusters client ID codes based on their structure.

    This function separates a list of client IDs into valid and invalid groups
    based on a regex pattern (e.g., 'A123456'). It then uses KMeans clustering
    on the valid codes to identify potential subgroups based on their prefix and
    the sum of their digits.

    Args:
        client_idcode_list: A list of client ID codes to analyze.
        min_val: The minimum number of clusters to create. Defaults to 3.

    Returns:
        A dictionary containing 'valid_codes', 'invalid_codes', and 'clusters'.
    """
    # Step 1: Separate valid and invalid codes based on the expected pattern
    expected_pattern = r"^[A-Z]\d{6}$"
    valid_codes = [
        code for code in client_idcode_list if re.match(expected_pattern, code)
    ]
    invalid_codes = [
        code for code in client_idcode_list if not re.match(expected_pattern, code)
    ]

    # Display warnings for large numbers of invalid codes
    if len(invalid_codes) > len(client_idcode_list) * 0.0001:  # If >10% are invalid
        print(
            f"Warning: invalid codes ({len(invalid_codes)} out of {len(client_idcode_list)})"
        )
        print(
            "Sample invalid codes:", invalid_codes[:15]
        )  # Show a sample of invalid codes

    # Step 2: Extract features for valid codes
    def extract_features(code):
        prefix = code[0]  # First character
        digit_sum = sum(int(digit) for digit in code[1:])  # Sum of digits
        return f"{prefix}-{digit_sum}"  # Feature combining prefix and digit sum

    features = [extract_features(code) for code in valid_codes]

    # Vectorize the features for clustering
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(features)

    # Perform clustering on valid codes
    n_clusters = min(
        min_val, len(valid_codes)
    )  # At most 3 clusters, or fewer if not enough codes
    if n_clusters > 1:
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        clusters = kmeans.fit_predict(X)

        # Group codes by clusters
        cluster_dict = {}
        for i, label in enumerate(clusters):
            cluster_dict.setdefault(label, []).append(valid_codes[i])

        # Check cluster sizes
        cluster_sizes = Counter(clusters)

        print("\nDiscovered Clusters:")
        for cluster, codes in cluster_dict.items():
            print(f"Cluster {cluster}: {codes}")
        print("\nCluster sizes:", dict(cluster_sizes))
    else:
        cluster_dict = {0: valid_codes}
        print(
            "Insufficient valid codes for clustering. All valid codes grouped in a single cluster."
        )

    # Step 3: Return results as a dictionary
    return {
        "valid_codes": valid_codes,
        "invalid_codes": invalid_codes,
        "clusters": cluster_dict,
    }


# # Example usage
# client_idcodes = ["A123456", "B654321", "Z000000", "INVALID1", "A12345"]  # Example input
# result = analyze_client_codes(client_idcodes)
# print("\nAnalysis Results:", result)
