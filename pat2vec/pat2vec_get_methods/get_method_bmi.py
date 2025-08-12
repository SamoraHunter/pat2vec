import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import (
    get_start_end_year_month,
)


def get_bmi_features(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieves BMI-related features for a given patient within a specified date range.

    Parameters:
    - current_pat_client_id_code (str): The client ID code of the patient.
    - target_date_range (tuple): A tuple representing the target date range.
    - pat_batch (pd.DataFrame): The DataFrame containing patient data.
    - batch_mode (bool, optional): Indicates whether batch mode is enabled. Defaults to False.
    - cohort_searcher_with_terms_and_search (callable, optional): The function for cohort searching. Defaults to None.

    Returns:
    - pd.DataFrame: A DataFrame containing BMI-related features for the specified patient.
    """

    batch_mode = config_obj.batch_mode

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    if batch_mode:
        current_pat_raw_bmi = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "observationdocument_recordeddtm",
        )
    else:
        current_pat_raw_bmi = cohort_searcher_with_terms_and_search(
            index_name="observations",
            fields_list=[
                "observation_guid",
                "client_idcode",
                "obscatalogmasteritem_displayname",
                "observation_valuetext_analysed",
                "observationdocument_recordeddtm",
                "clientvisit_visitidcode",
            ],
            term_name=config_obj.client_idcode_term_name,
            entered_list=[current_pat_client_id_code],
            search_string='obscatalogmasteritem_displayname:("OBS BMI" OR "OBS Weight" OR "OBS Height") AND '
            + f"observationdocument_recordeddtm:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
        )

    if (
        len(
            current_pat_raw_bmi[
                current_pat_raw_bmi["obscatalogmasteritem_displayname"]
                == "OBS BMI Calculation"
            ]
        )
        == 0
    ):
        bmi_features = pd.DataFrame(
            data={"client_idcode": [current_pat_client_id_code]}
        )
    else:
        # Get BMI features
        bmi_sample = current_pat_raw_bmi[
            current_pat_raw_bmi["obscatalogmasteritem_displayname"]
            == "OBS BMI Calculation"
        ]
        bmi_sample = bmi_sample[
            (bmi_sample["observation_valuetext_analysed"].astype(float) < 200)
            & (bmi_sample["observation_valuetext_analysed"].astype(float) > 6)
        ]

        term = "bmi"
        bmi_features = pd.DataFrame(
            data={"client_idcode": [current_pat_client_id_code]}
        )

        if len(bmi_sample) > 0:
            value_array = bmi_sample["observation_valuetext_analysed"].astype(float)

            bmi_features[f"{term}_mean"] = value_array.mean()
            bmi_features[f"{term}_median"] = value_array.median()
            bmi_features[f"{term}_std"] = value_array.std()
            bmi_features[f"{term}_high"] = int(bool(value_array.median() > 24.9))
            bmi_features[f"{term}_low"] = int(bool(value_array.median() < 18.5))
            bmi_features[f"{term}_extreme"] = int(bool(value_array.median() > 30))
            bmi_features[f"{term}_max"] = max(value_array)
            bmi_features[f"{term}_min"] = min(value_array)

        elif config_obj.negate_biochem:
            bmi_features[f"{term}_mean"] = np.nan
            bmi_features[f"{term}_median"] = np.nan
            bmi_features[f"{term}_std"] = np.nan
            bmi_features[f"{term}_high"] = np.nan
            bmi_features[f"{term}_low"] = np.nan
            bmi_features[f"{term}_extreme"] = np.nan
            bmi_features[f"{term}_max"] = np.nan
            bmi_features[f"{term}_min"] = np.nan
        else:
            pass

        # Get height features
        height_sample = current_pat_raw_bmi[
            current_pat_raw_bmi["obscatalogmasteritem_displayname"] == "OBS Height"
        ]
        height_sample = height_sample[
            (height_sample["observation_valuetext_analysed"].astype(float) < 300)
            & (height_sample["observation_valuetext_analysed"].astype(float) > 30)
        ]

        term = "height"
        if len(height_sample) > 0:
            value_array = height_sample["observation_valuetext_analysed"].astype(float)

            bmi_features[f"{term}_mean"] = value_array.mean()
            bmi_features[f"{term}_median"] = value_array.median()
            bmi_features[f"{term}_std"] = value_array.std()

        elif config_obj.negate_biochem:
            bmi_features[f"{term}_mean"] = np.nan
            bmi_features[f"{term}_median"] = np.nan
            bmi_features[f"{term}_std"] = np.nan

        else:
            pass

        # Get weight features
        weight_sample = current_pat_raw_bmi[
            current_pat_raw_bmi["obscatalogmasteritem_displayname"] == "OBS Weight"
        ]
        weight_sample = weight_sample[
            (weight_sample["observation_valuetext_analysed"].astype(float) < 800)
            & (weight_sample["observation_valuetext_analysed"].astype(float) > 1)
        ]

        term = "weight"
        if len(weight_sample) > 0:
            value_array = weight_sample["observation_valuetext_analysed"].astype(float)

            bmi_features[f"{term}_mean"] = value_array.mean()
            bmi_features[f"{term}_median"] = value_array.median()
            bmi_features[f"{term}_std"] = value_array.std()
            bmi_features[f"{term}_max"] = max(value_array)
            bmi_features[f"{term}_min"] = min(value_array)

        elif config_obj.negate_biochem:

            bmi_features[f"{term}_mean"] = np.nan
            bmi_features[f"{term}_median"] = np.nan
            bmi_features[f"{term}_std"] = np.nan
            bmi_features[f"{term}_max"] = np.nan
            bmi_features[f"{term}_min"] = np.nan

        else:
            pass

    if config_obj.verbosity >= 6:
        display(bmi_features)

    return bmi_features
