from typing import Callable, List, Optional, Tuple, Union

import pandas as pd
from IPython.display import display

from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

DEMOGRAPHICS_FIELDS = [
    "client_idcode",
    "client_firstname",
    "client_lastname",
    "client_dob",
    "client_gendercode",
    "client_racecode",
    "client_deceaseddtm",
    "updatetime",
]


def search_demographics(
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
    client_id_codes: Optional[Union[str, List[str]]] = None,
    demographics_time_field: str = "updatetime",
    start_year: str = "1995",
    start_month: str = "01",
    start_day: str = "01",
    end_year: str = "2025",
    end_month: str = "12",
    end_day: str = "12",
    additional_custom_search_string: Optional[str] = None,
) -> pd.DataFrame:
    """Searches for demographics data for patients within a date range.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        demographics_time_field (str): The timestamp field for filtering
            demographics. Defaults to 'updatetime'.
        start_year (str): Start year for the search. Defaults to '1995'.
        start_month (str): Start month for the search. Defaults to '01'.
        start_day (str): Start day for the search. Defaults to '01'.
        end_year (str): End year for the search. Defaults to '2025'.
        end_month (str): End month for the search. Defaults to '12'.
        end_day (str): End day for the search. Defaults to '12'.
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the raw demographics data.

    Raises:
        ValueError: If essential arguments are None.
    """
    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")
    if demographics_time_field is None:
        raise ValueError("demographics_time_field cannot be None.")
    if any(
        x is None
        for x in [start_year, start_month, start_day, end_year, end_month, end_day]
    ):
        raise ValueError("Date components cannot be None.")

    # Ensure client_id_codes is a list for the search function
    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = (
        validate_input_dates(
            start_year, start_month, start_day, end_year, end_month, end_day
        )
    )

    # Base search string for demographics
    search_string = f"{demographics_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    return cohort_searcher_with_terms_and_search(
        index_name="epr_documents",
        fields_list=DEMOGRAPHICS_FIELDS,
        # Note: using default, can be made configurable
        term_name="client_idcode.keyword",
        entered_list=client_id_codes,
        search_string=search_string,
    )


def process_demographics_data(
    demo_data: pd.DataFrame, patlist: List[str]
) -> pd.DataFrame:
    """Processes raw demographics data to return the most recent record per patient.

    Args:
        demo_data (pd.DataFrame): Raw demographics data from the search.
        patlist (List[str]): List of patient IDs that were requested.

    Returns:
        pd.DataFrame: Processed demographics data containing the single most
            recent record for the patient(s).
    """
    if len(demo_data) == 0:
        # No data found, return DataFrame with just patient IDs
        return pd.DataFrame({"client_idcode": patlist})

    # Convert updatetime to datetime and sort
    demo_data = demo_data.copy()
    demo_data["updatetime"] = pd.to_datetime(demo_data["updatetime"], utc=True)
    demo_data = demo_data.sort_values(["client_idcode", "updatetime"])

    if len(demo_data) > 1:
        try:
            # Get the most recent record (last row after sorting)
            # Use [[-1]] to keep DataFrame structure
            return demo_data.iloc[[-1]]
        except Exception as e:
            print(f"Error processing demographics data: {e}")
            # Fallback: return DataFrame with patient IDs
            return pd.DataFrame({"client_idcode": patlist})
    elif len(demo_data) == 1:
        return demo_data
    else:
        # This shouldn't happen given the len check above, but keeping for safety
        return pd.DataFrame({"client_idcode": patlist})


def get_demographics3(
    patlist: List[str],
    target_date_range: Tuple,
    cohort_searcher_with_terms_and_search: Callable,
    config_obj: Optional[object] = None,
) -> pd.DataFrame:
    """Gets demographics information for patients within a specified date range.

    Args:
        patlist (List[str]): List of patient IDs.
        target_date_range (Tuple): A tuple representing the target date range.
        cohort_searcher_with_terms_and_search (Callable): The function for cohort
            searching.
        config_obj (Optional[object]): Configuration object containing settings.
            Defaults to None.

    Returns:
        pd.DataFrame: Demographics information for the specified patients.

    Raises:
        ValueError: If `config_obj` or `cohort_searcher_with_terms_and_search` is
            None, or if `patlist` is empty.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration."
        )

    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")

    if not patlist:
        raise ValueError("patlist cannot be empty.")

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    # Search for demographics data
    demo_data = search_demographics(
        cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
        client_id_codes=patlist,
        demographics_time_field="updatetime",
        start_year=start_year,
        start_month=start_month,
        start_day=start_day,
        end_year=end_year,
        end_month=end_month,
        end_day=end_day,
    )

    # Process the demographics data
    processed_demo = process_demographics_data(demo_data, patlist)

    if config_obj.verbosity >= 6:
        display(processed_demo)

    return processed_demo


# Example usage (commented out):
# patlist_example = ["patient_id1", "patient_id2"]
# target_date_range = (2023, 1, 1, 2023, 12, 31)  # Proper tuple format
# result = get_demographics3(patlist_example, target_date_range, searcher_function, config_obj)
# print(result)
