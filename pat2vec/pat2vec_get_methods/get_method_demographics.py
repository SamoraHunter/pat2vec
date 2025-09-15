from typing import Callable, List, Optional, Tuple

import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.pat2vec_get_methods.get_method_demo import search_demographics
from pat2vec.pat2vec_search.data_helper_functions import \
    append_age_at_record_series
# from COGStats import EthnicityAbstractor
# from COGStats import *
from pat2vec.util.ethnicity_abstractor import EthnicityAbstractor
from pat2vec.util.filter_dataframe_by_timestamp import \
    filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month


def get_demo(
    current_pat_client_id_code: str,
    target_date_range: Tuple,
    pat_batch: pd.DataFrame,
    config_obj: Optional[object] = None,
) -> pd.DataFrame:
    """Retrieves and processes demographic features for a patient.

    This function orchestrates the retrieval of the latest demographic record
    for a patient within a target date range and then processes it to extract
    features for age, sex, deceased status, and ethnicity.

    Args:
        current_pat_client_id_code (str): The client ID code for the patient.
        target_date_range (Tuple): The date range for which to get data.
        pat_batch (pd.DataFrame): The batch DataFrame containing demographic data.
        config_obj (Optional[object]): Configuration object. Defaults to None.

    Returns:
        pd.DataFrame: A single-row DataFrame with demographic features.
    """
    # Filters the raw pat batch of data to return the latest row of raw data within the target date range
    current_pat_demo = get_demographics3_batch(
        [current_pat_client_id_code],
        target_date_range,
        pat_batch,
        config_obj=config_obj,
    )
    current_pat_demo.reset_index(inplace=True)
    if config_obj.verbosity >= 1:
        print("Get demo: get_demographics3_batch:")

    # If the demo data is not empty
    if not current_pat_demo.drop(columns="client_idcode").isna().all().all():

        if not current_pat_demo["client_dob"].isna().any():

            current_pat_demo = _process_age(current_pat_demo)

        else:
            current_pat_demo["age"] = np.nan

        if not current_pat_demo["client_gendercode"].isna().any():

            current_pat_demo = _process_sex(current_pat_demo)

        else:
            current_pat_demo["male"] = np.nan

        if not current_pat_demo["client_deceaseddtm"].isna().any():

            current_pat_demo = _process_dead(current_pat_demo)

        else:
            current_pat_demo["dead"] = 0

        if not current_pat_demo["client_racecode"].isna().any():

            current_pat_demo = _process_ethnicity(current_pat_demo)

        else:
            current_pat_demo["census_white"] = np.nan
            current_pat_demo["census_asian_or_asian_british"] = np.nan
            current_pat_demo["census_black_african_caribbean_or_black_british"] = np.nan
            current_pat_demo["census_mixed_or_multiple_ethnic_groups"] = np.nan
            current_pat_demo["census_other_ethnic_group"] = np.nan

        current_pat_demo.reset_index(inplace=True)

        # Select necessary columns
        current_pat_demo = current_pat_demo[
            [
                "client_idcode",
                "male",
                "age",
                "dead",
                "census_white",
                "census_asian_or_asian_british",
                "census_black_african_caribbean_or_black_british",
                "census_mixed_or_multiple_ethnic_groups",
                "census_other_ethnic_group",
            ]
        ].copy()

        if config_obj and config_obj.verbosity >= 6:
            display(current_pat_demo)
    if config_obj.verbosity >= 1:
        print("Get demo: get_demographics3_batch: Done.")
        print(current_pat_demo)

    if len(current_pat_demo) > 1:
        display("error")
        display(current_pat_demo)
        raise Exception("more than one row process ethnicity")

    exclude_column = "client_idcode"
    current_pat_demo = current_pat_demo.astype(
        {col: "float" for col in current_pat_demo.columns if col != exclude_column}
    )

    return current_pat_demo.head(1)


def _process_age(demo_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Calculates and appends the patient's age to the DataFrame.

    Args:
        demo_dataframe (pd.DataFrame): DataFrame containing demographic information,
            including a 'client_dob' column.

    Returns:
        pd.DataFrame: The DataFrame with an added 'age' column.
    """
    demo_dataframe = append_age_at_record_series(demo_dataframe)

    if len(demo_dataframe) > 1:
        display("error")
        display(demo_dataframe)
        raise Exception("more than one row process _process_age")

    return demo_dataframe


def _process_ethnicity(demo_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Abstracts and one-hot encodes ethnicity information.

    Args:
        demo_dataframe (pd.DataFrame): DataFrame containing demographic information,
            including a 'client_racecode' column.

    Returns:
        pd.DataFrame: The DataFrame with added one-hot encoded census ethnicity columns.

    Raises:
        Exception: If the input DataFrame contains more than one row.
    """
    if len(demo_dataframe) > 1:
        raise Exception("more than one row process ethnicity")

    # Define the columns to ensure
    target_columns = [
        "census_white",
        "census_asian_or_asian_british",
        "census_black_african_caribbean_or_black_british",
        "census_mixed_or_multiple_ethnic_groups",
        "census_other_ethnic_group",
    ]

    # if latest:
    #     demo_dataframe.dropna(subset="client_racecode", inplace=True)
    #     # Select the latest row based on 'datetime_column'
    #     latest_row_index = demo_dataframe[datetime_column].idxmax()
    #     demo_dataframe = demo_dataframe.loc[[latest_row_index]]
    #     display(type(demo_dataframe))
    #     display(demo_dataframe)

    ethnicity_df = EthnicityAbstractor.abstractEthnicity(
        demo_dataframe,
        outputNameString="_census",
        ethnicityColumnString="client_racecode",
    )

    # One-hot encode the extracted ethnicity
    encoded_ethnicity = pd.get_dummies(ethnicity_df["census"], prefix="census")

    # Impute missing columns with zeros
    for col in target_columns:
        if col not in encoded_ethnicity.columns:
            encoded_ethnicity[col] = 0

    # Reorder columns to match the target order
    encoded_ethnicity = encoded_ethnicity[target_columns]

    processed_df = pd.DataFrame()

    processed_df["client_idcode"] = demo_dataframe["client_idcode"]
    processed_df["updatetime"] = demo_dataframe["updatetime"]
    processed_df["census_white"] = encoded_ethnicity["census_white"]
    processed_df["census_asian_or_asian_british"] = encoded_ethnicity[
        "census_asian_or_asian_british"
    ]
    processed_df["census_black_african_caribbean_or_black_british"] = encoded_ethnicity[
        "census_black_african_caribbean_or_black_british"
    ]
    processed_df["census_mixed_or_multiple_ethnic_groups"] = encoded_ethnicity[
        "census_mixed_or_multiple_ethnic_groups"
    ]
    processed_df["census_other_ethnic_group"] = encoded_ethnicity[
        "census_other_ethnic_group"
    ]
    for col in demo_dataframe.columns:
        if col not in processed_df.columns:
            processed_df[col] = demo_dataframe[col]

    if len(processed_df) > 1:
        print("error")
        raise Exception("more than one row process ethnicity")

    return processed_df


def _process_sex(demo_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Maps gender information to a binary 'male' column.

    Args:
        demo_dataframe (pd.DataFrame): DataFrame containing demographic information,
            including a 'client_gendercode' column.

    Returns:
        pd.DataFrame: The DataFrame with an added 'male' column (1 for Male, 0 for Female).
    """
    sex_map = {"Male": 1, "Female": 0, "male": 1, "female": 0}
    demo_dataframe["male"] = demo_dataframe["client_gendercode"].map(sex_map)

    if len(demo_dataframe) > 1:
        display("error")
        display(demo_dataframe)
        raise Exception("more than one row process _process_sex")
    return demo_dataframe


def _process_dead(demo_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Maps deceased status to a binary 'dead' column.

    Args:
        demo_dataframe (pd.DataFrame): DataFrame containing demographic information,
            including a 'client_deceaseddtm' column.

    Returns:
        pd.DataFrame: The DataFrame with an added 'dead' column (1 if deceased, 0 otherwise).
    """
    demo_dataframe["dead"] = demo_dataframe["client_deceaseddtm"].apply(
        lambda x: int(isinstance(x, str))
    )
    if len(demo_dataframe) > 1:
        display("error")
        display(demo_dataframe)
        raise Exception("more than one row process _process_dead")
    return demo_dataframe


def get_demographics3_batch(
    patlist: List[str],
    target_date_range: Tuple,
    pat_batch: pd.DataFrame,
    config_obj: Optional[object] = None,
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
) -> pd.DataFrame:
    """Retrieves the latest demographic record for patients within a date range.

    This function either filters a pre-loaded batch DataFrame or searches for
    demographic data. It forward-fills missing critical information and returns
    the single most recent record for each patient within the specified time window.

    Args:
        patlist (List[str]): A list of patient client ID codes.
        target_date_range (Tuple): The date range for which to retrieve data.
        pat_batch (pd.DataFrame): The DataFrame containing patient data for batch mode.
        config_obj (Optional[object]): Configuration object. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The function for
            cohort searching. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the most recent demographic record
            for the patient(s) in the date range.
    """
    batch_mode = config_obj.batch_mode

    # patlist = config_obj.patlist #is present?

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )
    pat_batch = pat_batch.sort_values(["client_idcode", "updatetime"])

    # if critical field is nan, lets impute from its most recent non nan
    pat_batch[
        [
            "client_firstname",
            "client_lastname",
            "client_dob",
            "client_gendercode",
            "client_racecode",
            "client_deceaseddtm",
        ]
    ] = (
        pat_batch.groupby("client_idcode")[
            [
                "client_firstname",
                "client_lastname",
                "client_dob",
                "client_gendercode",
                "client_racecode",
                "client_deceaseddtm",
            ]
        ]
        .ffill()
        .copy()
    )

    pat_batch.reset_index(drop=True, inplace=True)

    if batch_mode:

        demo = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "updatetime",
        )

    else:
        demo = search_demographics(
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

    demo["updatetime"] = pd.to_datetime(demo["updatetime"], utc=True)
    # .drop_duplicates(subset = ["client_idcode"], keep = "last", inplace = True)
    demo = demo.sort_values(["client_idcode", "updatetime"])

    # Reset index if necessary
    demo = demo.reset_index(drop=True)

    # if more than one in the range return the nearest the end of the period
    if len(demo) > 1:
        try:
            # print("case1")
            return demo.tail(1)
            # return demo.iloc[-1].to_frame()
        except Exception as e:
            print(e)

    # if only one return it
    elif len(demo) == 1:
        return demo

    # otherwise return only the client id
    else:
        if config_obj.verbosity >= 1:
            display(f"no demo data found for {patlist}")
            display(pat_batch)

        demo = pd.DataFrame(data=None, columns=None)
        demo["client_idcode"] = patlist
        # Define the columns to be set to NaN
        columns_to_set_nan = [
            "client_firstname",
            "client_lastname",
            "client_dob",
            "client_gendercode",
            "client_racecode",
            "client_deceaseddtm",
            "updatetime",
        ]

        # Add these columns to the DataFrame and set their values to NaN
        for column in columns_to_set_nan:
            demo[column] = np.nan

        return demo.head(1)
