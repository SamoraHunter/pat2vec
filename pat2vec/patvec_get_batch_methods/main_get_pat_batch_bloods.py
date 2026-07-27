from pat2vec.util.filter_methods import (
    apply_bloods_data_type_filter,
    filter_dataframe_by_fuzzy_terms,
)
from pat2vec.util.helper_functions import (
    get_df_from_db,
    get_df_from_db_with_temporal_filter,
)
from pat2vec.util.methods_get import exist_check


import pandas as pd
from sqlalchemy import text


import logging
import os
from typing import Any


def get_pat_batch_bloods(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of blood test observations for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of blood test observations.
    """

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations

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

    bloods_time_field = config_obj.bloods_time_field

    batch_target = pd.DataFrame()

    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_bloods"
            schema_name = "raw_data"

            # Determine date range for filtering
            start_date = None
            end_date = None

            # Check patient_dict first (for IPW with anchor dates)
            pat_dates = None
            if (
                hasattr(config_obj, "patient_dict")
                and config_obj.patient_dict is not None
            ):
                pat_dates = config_obj.patient_dict.get(current_pat_client_id_code)
            if pat_dates and len(pat_dates) == 2:
                start_date, end_date = pat_dates
            elif config_obj.individual_patient_window:
                # Use global dates from config
                try:
                    start_date_str = f"{config_obj.global_start_year}-{config_obj.global_start_month.zfill(2)}-{config_obj.global_start_day.zfill(2)}"
                    end_date_str = f"{config_obj.global_end_year}-{config_obj.global_end_month.zfill(2)}-{config_obj.global_end_day.zfill(2)}"
                    start_date = pd.to_datetime(start_date_str)
                    end_date = pd.to_datetime(end_date_str)
                except (ValueError, TypeError) as e:
                    logging.warning(
                        f"Could not parse dates for IPW temporal filter: {e}"
                    )

            if not overwrite_stored_pat_observations:
                # Fetch with temporal filtering if in IPW mode
                if start_date is not None and end_date is not None:
                    df = get_df_from_db_with_temporal_filter(
                        config_obj,
                        schema_name,
                        table_name,
                        patient_ids=[current_pat_client_id_code],
                        time_column=bloods_time_field,
                        start_date=start_date,
                        end_date=end_date,
                    )
                else:
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
                f"Error with database backend for bloods for patient {current_pat_client_id_code}: {e}"
            )
            return pd.DataFrame()

    batch_obs_target_path = os.path.join(
        config_obj.pre_bloods_batch_path, str(current_pat_client_id_code) + ".csv"
    )

    existence_check = exist_check(batch_obs_target_path, config_obj)

    should_fetch = False
    if config_obj.storage_backend == "database":
        should_fetch = True
    elif (
        store_pat_batch_observations and not existence_check or existence_check is False
    ):
        should_fetch = True

    try:
        if should_fetch:

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "basicobs_itemname_analysed",
                    "basicobs_value_numeric",
                    "basicobs_entered",
                    "clientvisit_serviceguid",
                    "updatetime",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f"basicobs_value_numeric:* AND "
                f"{bloods_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            if config_obj.data_type_filter_dict is not None:
                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists").get(
                        "bloods"
                    )
                    is not None
                ):

                    if config_obj.verbosity >= 1:
                        logging.info(
                            "applying doc type filter to bloods",
                            config_obj.data_type_filter_dict,
                        )

                        filter_term_list = config_obj.data_type_filter_dict.get(
                            "filter_term_lists"
                        ).get("bloods")

                        batch_target = filter_dataframe_by_fuzzy_terms(
                            batch_target,
                            filter_term_list,
                            column_name="basicobs_itemname_analysed",
                            verbose=config_obj.verbosity,
                        )

            batch_target = apply_bloods_data_type_filter(config_obj, batch_target)

            if (
                config_obj.store_pat_batch_observations
                or overwrite_stored_pat_observations
            ):
                if config_obj.storage_backend == "database" and not batch_target.empty:
                    # Drop Elasticsearch metadata columns before saving to database

                    cols_to_drop = ["_id", "_index", "_score"]

                    for col in cols_to_drop:

                        if col in batch_target.columns:

                            batch_target.drop(columns=col, inplace=True)

                    try:
                        engine = config_obj.db_engine
                        if engine:
                            with engine.begin() as connection:
                                table_name = "raw_bloods"
                                schema_name = "raw_data"
                                db_table = (
                                    f"{schema_name}_{table_name}"
                                    if engine.name == "sqlite"
                                    else table_name
                                )
                                db_schema = (
                                    None if engine.name == "sqlite" else schema_name
                                )
                                if overwrite_stored_pat_observations:
                                    del_query = text(
                                        f"DELETE FROM {db_table if engine.name == 'sqlite' else f'{schema_name}.{table_name}'} WHERE client_idcode = :pat_id"
                                    )
                                    connection.execute(
                                        del_query,
                                        {"pat_id": current_pat_client_id_code},
                                    )
                                batch_target.to_sql(
                                    name=db_table,
                                    con=connection,
                                    schema=db_schema,
                                    if_exists="append",
                                    index=False,
                                )
                    except Exception as e:
                        logging.error(f"Failed to save bloods batch to DB: {e}")
                elif not batch_target.empty:
                    os.makedirs(os.path.dirname(batch_obs_target_path), exist_ok=True)
                    batch_target.to_csv(batch_obs_target_path)

        else:
            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch blood test-related observations: {e}")
        return pd.DataFrame()
