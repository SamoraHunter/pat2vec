import logging
import os
from typing import Any

_logger = logging.getLogger(__name__)


import pandas as pd
from sqlalchemy import inspect, text

from pat2vec.util.elasticsearch_index_config import DRUG_FIELDS
from pat2vec.util.helper_functions import get_df_from_db
from pat2vec.util.methods_get import exist_check


def get_pat_batch_drugs(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of medication orders for a patient.

    Args:
    ----
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
    -------
        A DataFrame containing the batch of medication orders.

    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        msg = "Invalid or missing configuration object."
        raise ValueError(msg)

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    drug_time_field = config_obj.drug_time_field

    batch_target = pd.DataFrame()

    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_drugs"
            schema_name = "raw_data"

            if not config_obj.overwrite_stored_pat_observations:
                df = get_df_from_db(
                    config_obj,
                    schema_name,
                    table_name,
                    patient_ids=[current_pat_client_id_code],
                    warn_on_missing=False,
                )
                if not df.empty:
                    return df
        except Exception as e:
            msg = f"Critical failure fetching data from database for patient {current_pat_client_id_code}: {e}"
            _logger.error(msg)
            raise RuntimeError(msg)

    drug_time_field = config_obj.drug_time_field

    batch_obs_target_path = os.path.join(
        config_obj.pre_drugs_batch_path,
        str(current_pat_client_id_code) + ".csv",
    )
    existence_check = exist_check(batch_obs_target_path, config_obj)

    should_fetch = False
    if config_obj.storage_backend == "database" or (
        (config_obj.store_pat_batch_observations and not existence_check)
        or existence_check is False
    ):
        should_fetch = True

    try:
        if should_fetch:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="order",
                fields_list=DRUG_FIELDS,
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f'order_typecode:"medication" AND '
                f"{drug_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or config_obj.overwrite_stored_pat_observations
            ):
                if config_obj.storage_backend == "database" and not batch_target.empty:
                    # Drop Elasticsearch metadata columns before saving to database

                    cols_to_drop = ["_id", "_index", "_score"]

                    for col in cols_to_drop:
                        if col in batch_target.columns:
                            batch_target = batch_target.drop(columns=col)

                    try:
                        engine = config_obj.db_engine
                        if engine:
                            with engine.begin() as connection:
                                table_name = "raw_drugs"
                                schema_name = "raw_data"
                                db_table = (
                                    f"{schema_name}_{table_name}"
                                    if engine.name == "sqlite"
                                    else table_name
                                )
                                db_schema = (
                                    None if engine.name == "sqlite" else schema_name
                                )
                                if config_obj.overwrite_stored_pat_observations:
                                    inspector = inspect(connection)
                                    if inspector.has_table(db_table, schema=db_schema):
                                        del_query = text(
                                            f"DELETE FROM {db_table if engine.name == 'sqlite' else f'{schema_name}.{table_name}'} WHERE client_idcode = :pat_id",
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
                        msg = f"Failed to save data batch for patient {current_pat_client_id_code} to database: {e}"
                        _logger.error(msg)
                        raise RuntimeError(msg)
                elif not batch_target.empty:
                    os.makedirs(os.path.dirname(batch_obs_target_path), exist_ok=True)
                    batch_target.to_csv(batch_obs_target_path)

        else:
            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        msg = f"Critical failure retrieving batch for patient {current_pat_client_id_code}: {e}"
        _logger.error(msg)
        raise RuntimeError(msg)
