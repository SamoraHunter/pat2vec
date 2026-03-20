from pat2vec.util.helper_functions import (
    get_df_from_db,
    save_raw_patient_batch,
    sanitize_for_path,
)
from pat2vec.util.methods_get import exist_check


import pandas as pd


import logging
import os
from typing import Any


def get_pat_batch_obs(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of specific observations for a patient.

    This function fetches observation data for a single patient, filtering by a
    specific `search_term` (e.g., 'CORE_SmokingStatus') within the globally
    defined time window. It includes logic to read from a cached file if it
    exists or query the data source otherwise.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The specific observation term to search for.
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of specified observations.
    """
    if not search_term:
        logging.warning(
            f"get_pat_batch_obs called with empty search_term for patient {current_pat_client_id_code}"
        )
        return pd.DataFrame()

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    batch_target = pd.DataFrame()

    if config_obj.storage_backend == "database":
        try:
            safe_search_term = "".join(
                e for e in search_term if e.isalnum() or e == "_"
            ).lower()
            table_name = f"raw_obs_{safe_search_term}"
            schema_name = "raw_data"

            if not config_obj.overwrite_stored_pat_observations:
                df = get_df_from_db(
                    config_obj,
                    schema_name,
                    table_name,
                    patient_ids=[current_pat_client_id_code],
                )
                if not df.empty:
                    return df
        except Exception as e:
            logging.error(
                f"Error with database backend for observation '{search_term}' for patient {current_pat_client_id_code}: {e}"
            )
            return pd.DataFrame()

    sanitized_search_term = sanitize_for_path(search_term)
    batch_obs_target_path = os.path.join(
        config_obj.pre_misc_batch_path.replace("misc", sanitized_search_term),
        str(current_pat_client_id_code) + ".csv",
    )
    existence_check = exist_check(batch_obs_target_path, config_obj)

    should_fetch = False
    if config_obj.storage_backend == "database":
        should_fetch = True
    elif (
        config_obj.store_pat_batch_observations
        and not existence_check
        or existence_check is False
    ):
        should_fetch = True

    try:
        if should_fetch:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode	obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f'obscatalogmasteritem_displayname:("{search_term}") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            if (
                config_obj.store_pat_batch_docs
                or config_obj.overwrite_stored_pat_observations
            ):

                if config_obj.storage_backend == "database":
                    safe_search_term = "".join(
                        e for e in search_term if e.isalnum() or e == "_"
                    ).lower()
                    table_name = f"raw_obs_{safe_search_term}"
                    save_raw_patient_batch(
                        batch_target,
                        current_pat_client_id_code,
                        table_name,
                        config_obj,
                    )
                else:
                    directory_path = config_obj.pre_misc_batch_path.replace(
                        "misc", sanitized_search_term
                    )

                    if not os.path.exists(directory_path):
                        os.makedirs(directory_path)
                    batch_target.to_csv(batch_obs_target_path)
        else:
            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        logging.error(f"Error retrieving batch {search_term}: {e}")
        return pd.DataFrame()
