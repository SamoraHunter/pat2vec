import pickle
import uuid
import pandas as pd  # Import pandas
from typing import Dict, List, Optional, Tuple


def anonymize_feature_names(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Anonymizes DataFrame column names, preserving prefixes and suffixes.

    The 'core' part of each feature name is replaced with a unique, generic
    identifier (e.g., 'concept_0'). This is useful for sharing data structures
    without revealing sensitive or proprietary feature names. The function
    returns both the anonymized DataFrame and a key to reverse the process.

    The function identifies prefixes and suffixes from predefined lists. These
    are sorted by length to ensure the longest possible match is found first,
    avoiding partial matches (e.g., matching '_count' before '_count_present').

    Args:
        df: The input pandas DataFrame whose columns need to be anonymized.

    Returns:
        A tuple containing:
            - pd.DataFrame: A new DataFrame with anonymized column names.
            - dict: A dictionary mapping anonymized names to their original
              names, for de-anonymization. Format: {anonymized_name: original_name}.
    """
    original_feature_names: List[str] = (
        df.columns.tolist()
    )  # Extract column names from the DataFrame

    anonymized_names_list: List[str] = []
    anonymization_key: Dict[str, str] = {}

    # Mapping to keep core concepts consistently anonymized if they appear multiple times
    core_concept_map: Dict[str, str] = {}
    next_concept_id: int = 0

    # Define prefixes and suffixes strictly based on the provided get_pertubation_columns function
    # Sorted by length in descending order to ensure longest match is found first

    # Prefixes derived from explicit string checks and definition lists in get_pertubation_columns
    PREDEFINED_PREFIXES = sorted(
        [
            "__index_level_",  # from index_level_list filter
            "Unnamed:",  # from Unnamed_list filter
            "client_idcode:",  # from client_idcode: filter
            "outcome_var_",  # from outcome_variable construction
            "ConsultantCode_",  # from appointments_substrings
            "ClinicCode_",  # from appointments_substrings
            "AppointmentType_",  # from appointments_substrings
            "date_time_stamp_",  # from date_time_stamp_list filter (added underscore for consistency)
            "core_resus_",  # from core_resus_list filter
            "news_resus_",  # from news_list filter
            "vte_status_",  # from vte_status_list filter
            "hosp_site_",  # from hosp_site_list filter
            "core_02_",  # from core_02_list filter
            "census_",  # from ethnicity_list filter
            "bed_",  # from bed_list filter
            "bmi_",  # from bmi_list filter
        ],
        key=len,
        reverse=True,
    )

    # Suffixes derived from explicit *_substrings lists and count filters in get_pertubation_columns
    PREDEFINED_SUFFIXES = sorted(
        [
            "_count_subject_present_mrc_cs",  # from meta_sp_annotation_mrc_count_list filter
            "_count_subject_present",  # from meta_sp_annotation_count_list filter
            "_days-since-last-diagnostic-order",  # from diagnostic_test_substrings
            "_days-between-first-last-diagnostic",  # from diagnostic_test_substrings
            "_days-between-first-last-drug",  # from drug_order_substrings
            "_days-since-last-drug-order",  # from drug_order_substrings
            "_days-since-last-test",  # from blood_test_substrings
            "_days-between-first-last",  # from blood_test_substrings
            "_contains-extreme-high",  # from blood_test_substrings
            "_contains-extreme-low",  # from blood_test_substrings
            "_num-diagnostic-order",  # from diagnostic_test_substrings
            "_num-drug-order",  # from drug_order_substrings
            "_earliest-test",  # from blood_test_substrings
            "_most-recent",  # from blood_test_substrings
            "_num-tests",  # from blood_test_substrings
            "_count_mrc_cs",  # from annotation_mrc_count_list filter
            "_mean",  # from blood_test_substrings
            "_median",  # from blood_test_substrings
            "_mode",  # from blood_test_substrings
            "_std",  # from blood_test_substrings
            "_max",  # from blood_test_substrings
            "_min",  # from blood_test_substrings
            "_count",  # from annotation_count_list filter
        ],
        key=len,
        reverse=True,
    )

    for original_name in original_feature_names:
        matched_prefix = ""
        matched_suffix = ""

        temp_name = original_name  # This will be progressively reduced

        # 1. Identify and extract the longest matching prefix
        for prefix in PREDEFINED_PREFIXES:
            if temp_name.startswith(prefix):
                matched_prefix = prefix
                temp_name = temp_name[len(prefix) :]
                break  # Found the longest prefix, move on

        # 2. Identify and extract the longest matching suffix from the remaining part
        for suffix in PREDEFINED_SUFFIXES:
            if temp_name.endswith(suffix):
                matched_suffix = suffix
                temp_name = temp_name[: -len(suffix)]
                break  # Found the longest suffix, move on

        # The remaining 'temp_name' is the core concept to anonymize
        core_concept = temp_name

        # 3. Anonymize the core concept
        if core_concept not in core_concept_map:
            core_concept_map[core_concept] = f"concept_{next_concept_id}"
            next_concept_id += 1

        anonymized_core_concept = core_concept_map[core_concept]

        # 4. Reconstruct the anonymized feature name
        anonymized_name = f"{matched_prefix}{anonymized_core_concept}{matched_suffix}"

        # Special handling for names that don't fit the prefix/suffix pattern
        # (e.g., "age", "male", "outcome_var_0" if not matched by outcome_var_)
        # If no prefix or suffix was matched, and the core concept is the original name itself,
        # we can use a more generic anonymization for simplicity.
        if not matched_prefix and not matched_suffix and original_name == core_concept:
            # If it's a simple, non-pattern-matching name, give it a simple feature_X ID
            # This ensures names like 'age' or 'male' don't become 'concept_0' or 'concept_1'
            # without a prefix/suffix.
            anonymized_name_base = (
                f"feature_{len(anonymization_key)}"  # Use len as a simple counter
            )
            anonymized_name = anonymized_name_base

            # Update core concept map for these simple cases too
            if original_name not in core_concept_map:
                core_concept_map[original_name] = anonymized_name_base
            anonymized_core_concept = core_concept_map[
                original_name
            ]  # ensures consistency if same simple name appears again

        # Store the full mapping for de-anonymization
        anonymization_key[anonymized_name] = original_name
        anonymized_names_list.append(anonymized_name)

    # Create a new DataFrame with anonymized columns
    anonymized_df = df.copy()
    anonymized_df.columns = anonymized_names_list

    return anonymized_df, anonymization_key


def deanonymize_feature_names(
    anonymized_feature_names: List[str], anonymization_key: Dict[str, str]
) -> List[Optional[str]]:
    """De-anonymizes a list of feature names using a provided key.

    Args:
        anonymized_feature_names: A list of anonymized feature names.
        anonymization_key: The dictionary mapping anonymized names back to
            original names. Format: {anonymized_name: original_name}.

    Returns:
        A list of the original feature names. If an anonymized name is not
        found in the key, the corresponding item in the list will be None.
    """
    deanonymized_names: List[Optional[str]] = []
    for anonymized_name in anonymized_feature_names:
        original_name = anonymization_key.get(anonymized_name, None)
        if original_name is None:
            print(f"Warning: Anonymized name '{anonymized_name}' not found in the key.")
        deanonymized_names.append(original_name)

    return deanonymized_names


## --- Example Usage ---
# if __name__ == '__main__':
#     # Sample feature names

#     sample_df = pd.read_csv('../notebooks/new_project/output_directory/concatenated_data_output_file.csv.csv')

#     sample_columns = sample_df.columns.tolist()

#     print("Original DataFrame Columns:")
#     print(sample_df.columns.tolist())
#     print("\n" + "="*50 + "\n")

#     # Anonymize the DataFrame columns
#     anonymized_df, anonymization_key = anonymize_feature_names(sample_df)

#     # --- Saving the anonymization key ---
#     key_filename = 'anonymization_key.pkl'
#     try:
#         with open(key_filename, 'wb') as f:
#             pickle.dump(anonymization_key, f)
#         print(f"Anonymization key successfully saved to '{key_filename}'")
#     except Exception as e:
#         print(f"Error saving anonymization key: {e}")
#     print("\n" + "="*50 + "\n")

#     # --- Loading the anonymization key ---
#     loaded_anonymization_key = {}
#     try:
#         with open(key_filename, 'rb') as f:
#             loaded_anonymization_key = pickle.load(f)
#         print(f"Anonymization key successfully loaded from '{key_filename}'")
#         print("Loaded key preview:", list(loaded_anonymization_key.items())[:2]) # Show first 2 items
#     except FileNotFoundError:
#         print(f"Error: Anonymization key file '{key_filename}' not found.")
#     except Exception as e:
#         print(f"Error loading anonymization key: {e}")
#     print("\n" + "="*50 + "\n")

#     print("Anonymized DataFrame Columns:")
#     print(anonymized_df.columns.tolist())
#     print("\n" + "="*50 + "\n")

#     print("Anonymization Key (Anonymized -> Original):")
#     for k, v in anonymization_key.items():
#         print(f"- {k}: {v}")
#     print("\n" + "="*50 + "\n")

#     # Demonstrate deanonymization of the anonymized DataFrame's columns
#     deanonymized_columns = deanonymize_feature_names(anonymized_df.columns.tolist(), anonymization_key)

#     print("De-anonymized DataFrame Columns (should match original list):")
#     print(deanonymized_columns)
#     print("\n" + "="*50 + "\n")

#     # Verify if fully deanonymized list matches original list
#     if deanonymized_columns == sample_columns:
#         print("Verification successful: Full de-anonymization of DataFrame columns matches original list!")
#     else:
#         print("Verification failed: De-anonymized DataFrame columns do NOT match original list.")
