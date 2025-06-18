import re
import warnings


def extract_nhs_numbers(input_string):
    # Find all occurrences of "NHS" followed by a 10-digit number
    """
    Extract all occurrences of "NHS" followed by a 10-digit number from a string.

    Parameters
    ----------
    input_string : str
        The string to search for NHS numbers.

    Returns
    -------
    list of str
        A list of all extracted NHS numbers without spaces.

    Examples
    --------
    >>> extract_nhs_numbers("NHS 123 456 7890")
    ['1234567890']
    >>> extract_nhs_numbers("NHS 123 456 7890 and NHS 098 765 4321")
    ['1234567890', '987654321']
    """
    matches = re.findall(r"NHS\s*(\d{3}\s*\d{3}\s*\d{4})", input_string)
    # Remove spaces from each extracted number
    cleaned_numbers = [re.sub(r"\s+", "", number) for number in matches]
    return cleaned_numbers


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
