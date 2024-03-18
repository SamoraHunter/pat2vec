import re


def extract_nhs_numbers(input_string):
    # Find all occurrences of "NHS" followed by a 10-digit number
    matches = re.findall(r"NHS\s*(\d{3}\s*\d{3}\s*\d{4})", input_string)
    # Remove spaces from each extracted number
    cleaned_numbers = [re.sub(r"\s+", "", number) for number in matches]
    return cleaned_numbers


import warnings


def get_search_client_idcode_list_from_nhs_number_list(nhs_numbers, pat2vec_obj):
    """
    Retrieve a unique list of hospital IDs associated with a list of NHS numbers.

    Args:
        nhs_numbers (list): A list of NHS numbers.
        pat2vec_obj: The pat2vec object used for cohort search.

    Returns:
        list: A unique list of hospital IDs.

    Raises:
        None.

    Warns:
        UserWarning: If any of the NHS numbers do not have an associated Hospital ID.
    """
    # Perform cohort search
    df = pat2vec_obj.cohort_searcher_with_terms_and_search(
        index_name="pims_apps*",
        fields_list=["PatNHSNo", "HospitalID"],
        term_name="PatNHSNo",
        entered_list=nhs_numbers,
        search_string="*",
    )

    # Extract unique hospital IDs
    unique_hospital_ids = list(df["HospitalID"].unique())

    # Check for missing hospital IDs
    missing_ids = df[df["HospitalID"].isna() | (df["HospitalID"] == "")][
        "PatNHSNo"
    ].tolist()
    if missing_ids:
        warnings.warn(
            f"The following NHS numbers do not have associated Hospital IDs: {missing_ids}"
        )

    return unique_hospital_ids
