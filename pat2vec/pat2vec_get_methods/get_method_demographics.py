import os
import sys

import numpy as np
import pandas as pd
from cogstack_search_methods.cogstack_v8_lite import append_age_at_record_series
from IPython.display import display

# from COGStats import EthnicityAbstractor
# from COGStats import *
from pat2vec.util.ethnicity_abstractor import EthnicityAbstractor
from pat2vec.util.methods_get import get_demographics3_batch

# individual elements:


def get_demo(current_pat_client_id_code, target_date_range, pat_batch, config_obj=None):
    """
    Retrieve demographic information for a patient based on the provided parameters.

    Parameters:
    - current_pat_client_id_code (str): The client ID code for the current patient.
    - target_date_range (tuple): A tuple representing the date range for which demographic information is required.
    - pat_batch (dataframe): The demo batch dataframe for the patient.
    - config_obj (Config,): Contains config options.

    Returns:
    - pd.DataFrame: A DataFrame containing the demographic information for the specified patient.
    """

    # Filters the raw pat batch of data to return the latest row of raw data within the target date range
    current_pat_demo = get_demographics3_batch(
        [current_pat_client_id_code],
        target_date_range,
        pat_batch,
        config_obj=config_obj,
    )
    current_pat_demo.reset_index(inplace=True)
    # print("pre current_pat_demo")
    # display(current_pat_demo)
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

        # print("post ethnicity")
        # print(current_pat_demo)
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
    # display(current_pat_demo)

    return current_pat_demo.head(1)


def _process_age(demo_dataframe):
    """
    Process age information for the patient.

    Parameters:
    - demo_dataframe (pd.DataFrame): DataFrame containing demographic information.

    Returns:
    - pd.DataFrame: DataFrame with processed age information.

    """

    demo_dataframe = append_age_at_record_series(demo_dataframe)

    if len(demo_dataframe) > 1:
        display("error")
        display(demo_dataframe)
        raise Exception("more than one row process _process_age")

    return demo_dataframe


def _process_ethnicity(demo_dataframe):
    """
    Process ethnicity information for the patient.

    Parameters:
    - demo_dataframe (pd.DataFrame): DataFrame containing demographic information.

    Returns:
    - pd.DataFrame: DataFrame with processed ethnicity information.
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


def _process_sex(demo_dataframe):
    """
    Process gender information for the patient.

    Parameters:
    - demo_dataframe (pd.DataFrame): DataFrame containing demographic information.

    Returns:
    - pd.DataFrame: DataFrame with processed gender information.
    """
    sex_map = {"Male": 1, "Female": 0, "male": 1, "female": 0}
    demo_dataframe["male"] = demo_dataframe["client_gendercode"].map(sex_map)

    if len(demo_dataframe) > 1:
        display("error")
        display(demo_dataframe)
        raise Exception("more than one row process _process_sex")
    return demo_dataframe


def _process_dead(demo_dataframe):
    """
    Process deceased status information for the patient.

    Parameters:
    - demo_dataframe (pd.DataFrame): DataFrame containing demographic information.

    Returns:
    - pd.DataFrame: DataFrame with processed deceased status information.
    """
    demo_dataframe["dead"] = demo_dataframe["client_deceaseddtm"].apply(
        lambda x: int(isinstance(x, str))
    )
    if len(demo_dataframe) > 1:
        display("error")
        display(demo_dataframe)
        raise Exception("more than one row process _process_dead")
    return demo_dataframe
