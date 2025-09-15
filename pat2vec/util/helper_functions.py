import re
import warnings
from typing import Any, List


def extract_nhs_numbers(input_string: str) -> List[str]:
    """Extracts all occurrences of "NHS" followed by a 10-digit number.

    The function searches for the pattern "NHS" followed by a 10-digit number,
    which may contain spaces. It then cleans the extracted numbers by removing
    any spaces.

    Args:
        input_string: The string to search for NHS numbers.

    Returns:
        A list of all extracted 10-digit NHS numbers as strings.

    Examples:
        >>> extract_nhs_numbers("NHS 123 456 7890")
        ['1234567890']
        >>> extract_nhs_numbers("NHS 123 456 7890 and NHS 098 765 4321")
        ['1234567890', '0987654321']
    """
    # Find all occurrences of "NHS" followed by a 10-digit number
    matches = re.findall(r"NHS\s*(\d{3}\s*\d{3}\s*\d{4})", input_string)
    # Remove spaces from each extracted number
    cleaned_numbers = [re.sub(r"\s+", "", number) for number in matches]
    return cleaned_numbers


def get_search_client_idcode_list_from_nhs_number_list(
    nhs_numbers: List[str], pat2vec_obj: Any
) -> List[str]:
    """Retrieves a unique list of hospital IDs from a list of NHS numbers.

    This function uses a `pat2vec_obj` to perform a cohort search against an
    index (e.g., 'pims_apps*') to find the corresponding 'HospitalID' for each
    'PatNHSNo' in the provided list.

    Args:
        nhs_numbers: A list of NHS numbers to search for.
        pat2vec_obj: An object with a `cohort_searcher_with_terms_and_search`
            method for querying the data source.

    Returns:
        A unique list of hospital IDs found for the given NHS numbers.
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
