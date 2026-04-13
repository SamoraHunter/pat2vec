import os
from typing import Union, Optional, List

import pandas as pd

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates

BED_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]


def search_bed_data(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    client_idcode_name="client_idcode.keyword",
    bed_time_field="observationdocument_recordeddtm",
    fields_override: Optional[List[str]] = None,
    start_year: Union[int, str] = 1995,
    start_month: Union[int, str] = 1,
    start_day: Union[int, str] = 1,
    end_year: Union[int, str] = 2025,
    end_month: Union[int, str] = 12,
    end_day: Union[int, str] = 12,
    search_term="CORE_BedNumber3",
    additional_custom_search_string=None,
    index_name: str = "observations",
    output_filename: Optional[str] = "bed_search_results.csv",
    overwrite: bool = False,
    config_obj: Optional[object] = None,
):
    """Searches for bed data for patients within a date range.

    Uses a cohort searcher to find bed data based on a search term and date range.

    Args:
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.
        client_id_codes (Optional[Union[str, List[str]]]): The client ID code(s) of
            the patient(s). Defaults to None.
        client_idcode_name (str): The name of the client ID code field in the
            index. Defaults to "client_idcode.keyword".
        bed_time_field (str): The timestamp field for filtering bed data.
            Defaults to 'observationdocument_recordeddtm'.
        fields_override (Optional[List[str]]): A list of fields to override the
            default `BED_FIELDS`. Defaults to None.
        start_year (Union[int, str]): Start year for the search. Defaults to 1995.
        start_month (Union[int, str]): Start month for the search. Defaults to 1.
        start_day (Union[int, str]): Start day for the search. Defaults to 1.
        end_year (Union[int, str]): End year for the search. Defaults to 2025.
        end_month (Union[int, str]): End month for the search. Defaults to 12.
        end_day (Union[int, str]): End day for the search. Defaults to 12.
        search_term (str): The search term for bed data. Defaults to "CORE_BedNumber3".
        additional_custom_search_string (Optional[str]): An additional string to
            append to the search query. Defaults to None.
        index_name (str): The name of the Elasticsearch index to search.
            Defaults to "observations".
        output_filename (Optional[str]): The filename or path to a CSV file to
            load from or save to. Defaults to "bed_search_results.csv".
        overwrite (bool): If True, perform the search even if `output_filename`
            exists. Defaults to False.
        config_obj (Optional[object]): Configuration object containing root_path.
            Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the raw bed data.
    """
    if (
        output_filename
        and config_obj
        and hasattr(config_obj, "root_path")
        and hasattr(config_obj, "proj_name")
    ):
        output_filename = os.path.join(
            config_obj.root_path, config_obj.proj_name, output_filename
        )

    if output_filename and os.path.exists(output_filename) and not overwrite:
        print(f"Loading existing bed data from {output_filename}")
        return pd.read_csv(output_filename)

    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")
    if bed_time_field is None:
        raise ValueError("bed_time_field cannot be None.")
    if any(
        x is None
        for x in [start_year, start_month, start_day, end_year, end_month, end_day]
    ):
        raise ValueError("Date components cannot be None.")

    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = (
        validate_input_dates(
            start_year, start_month, start_day, end_year, end_month, end_day
        )
    )

    search_string = f'obscatalogmasteritem_displayname:("{search_term}") AND {bed_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]'

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    fields_to_use = BED_FIELDS
    if fields_override:
        fields_to_use = fields_override

    results = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_to_use,
        term_name=client_idcode_name,
        entered_list=client_id_codes,
        search_string=search_string,
    )

    if output_filename:
        if os.path.dirname(output_filename):
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        print(f"Saving bed data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def get_bed(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieves CORE_BedNumber3 features for a patient within a date range.

    This function fetches bed data, either from a pre-loaded batch DataFrame or
    by searching, and then creates one-hot encoded features for each unique
    bed number found.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient.
        target_date_range (tuple): A tuple representing the target date range.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing CORE_BedNumber3 features for the
            specified patient. If no data is found, a DataFrame with only the
            'client_idcode' is returned.
    """
    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )
    search_term = "CORE_BedNumber3"
    bed_time_field = "observationdocument_recordeddtm"

    if pat_batch.empty:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    if batch_mode:
        current_pat_raw = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            bed_time_field,
        )
    else:
        current_pat_raw = search_bed_data(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            client_idcode_name=config_obj.client_idcode_term_name,
            bed_time_field=bed_time_field,
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            search_term=search_term,
            output_filename=None,
            config_obj=config_obj,
        )

    features = pd.DataFrame(
        data=[current_pat_client_id_code], columns=["client_idcode"]
    )

    if len(current_pat_raw) == 0:
        return features

    features_data = current_pat_raw[
        current_pat_raw["obscatalogmasteritem_displayname"] == search_term
    ].copy()

    features_data.dropna(subset=["observation_valuetext_analysed"], inplace=True)

    "bed".lower()

    if len(features_data) > 0:
        all_bed_terms = list(features_data["observation_valuetext_analysed"].unique())

        for bed_term in all_bed_terms:
            features[f"bed_{bed_term}"] = 1

    return features
