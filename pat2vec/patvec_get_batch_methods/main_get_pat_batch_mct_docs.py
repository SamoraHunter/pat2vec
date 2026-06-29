from pat2vec.util.clinical_note_splitter import split_and_append_chunks
from pat2vec.util.filter_methods import apply_data_type_mct_docs_filters
from pat2vec.util.helper_functions import get_df_from_db
from pat2vec.util.methods_get import exist_check


import json
import pandas as pd
from sqlalchemy import text


import logging
import os
from typing import Any


def get_pat_batch_mct_docs(
    current_pat_client_id_code: str,
    search_term: str,  # noqa
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of MCT (MRC clinical notes) documents for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of MCT documents.
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
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs

    split_clinical_notes_bool = config_obj.split_clinical_notes

    batch_epr_target_path_mct = os.path.join(
        config_obj.pre_document_batch_path_mct, str(current_pat_client_id_code) + ".csv"
    )

    batch_target = pd.DataFrame()

    try:
        if config_obj.storage_backend == "database":
            table_name = "raw_mct_docs"
            schema_name = "raw_data"

            if not overwrite_stored_pat_docs:
                df = get_df_from_db(
                    config_obj,
                    schema_name,
                    table_name,
                    patient_ids=[current_pat_client_id_code],
                )
                if not df.empty:
                    return df

        existence_check = exist_check(batch_epr_target_path_mct, config_obj)

        should_fetch = False
        if config_obj.storage_backend == "database":
            should_fetch = True
        elif not existence_check or overwrite_stored_pat_docs:
            should_fetch = True

        if should_fetch:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                    observation_valuetext_analysed observationdocument_recordeddtm
                                    clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f'obscatalogmasteritem_displayname:("AoMRC_ClinicalSummary_FT") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            batch_target = apply_data_type_mct_docs_filters(config_obj, batch_target)

            if config_obj.store_pat_batch_docs or overwrite_stored_pat_docs:
                if config_obj.verbosity >= 3:
                    logging.debug("get_epr_mct_docs_predropna: %d", len(batch_target))
                col_list_drop_nan = [
                    "observation_valuetext_analysed",
                    "observationdocument_recordeddtm",
                    "client_idcode",
                ]
                # Ensure columns exist before dropna to avoid KeyError
                valid_cols = [c for c in col_list_drop_nan if c in batch_target.columns]
                if not batch_target.empty and valid_cols:
                    batch_target = batch_target.dropna(subset=valid_cols).copy()

                if config_obj.verbosity >= 3:
                    logging.debug("get_epr_mct_docs_postdropna: %d", len(batch_target))

                if split_clinical_notes_bool:
                    batch_target = split_and_append_chunks(
                        batch_target, epr=False, mct=True
                    )

                if config_obj.storage_backend == "database" and not batch_target.empty:
                    try:
                        engine = config_obj.db_engine
                        if engine:
                            # Drop Elasticsearch/MongoDB metadata columns and index column that conflict with SQLite
                            cols_to_drop = [
                                "_id",
                                "_index",
                                "_score",
                                "search_term",
                                "index",
                            ]
                            for col in cols_to_drop:
                                batch_target.drop(
                                    columns=col, inplace=True, errors="ignore"
                                )

                            # Fix problematic backslashes in text columns that cause SQLite parameter binding issues
                            text_cols = [
                                "observation_valuetext_analysed",
                                "obscatalogmasteritem_displayname",
                            ]
                            for col in text_cols:
                                if col in batch_target.columns:
                                    batch_target[col] = (
                                        batch_target[col]
                                        .astype(str)
                                        .str.replace("\\", "", regex=False)
                                    )

                            # Convert Timestamp columns to strings for SQLite compatibility
                            timestamp_cols = [
                                "updatetime",
                                "observationdocument_recordeddtm",
                                "basicobs_entered",
                                "observationdocument_createdwhen",
                                "document_CreatedWhen",
                            ]
                            for col in timestamp_cols:
                                if col in batch_target.columns:
                                    batch_target[col] = pd.to_datetime(
                                        batch_target[col], errors="coerce"
                                    ).dt.strftime("%Y-%m-%d %H:%M:%S")

                            # Convert any list/dict/tuple columns to JSON strings for database compatibility
                            for col in batch_target.columns:
                                if batch_target[col].dtype == "object":
                                    if (
                                        batch_target[col]
                                        .apply(
                                            lambda x: isinstance(x, (list, dict, tuple))
                                        )
                                        .any()
                                    ):
                                        batch_target[col] = batch_target[col].apply(
                                            lambda x: (
                                                json.dumps(x)
                                                if isinstance(x, (list, dict, tuple))
                                                else x
                                            )
                                        )

                            with engine.begin() as connection:
                                table_name = "raw_mct_docs"
                                schema_name = "raw_data"
                                db_table = (
                                    f"{schema_name}_{table_name}"
                                    if engine.name == "sqlite"
                                    else table_name
                                )
                                db_schema = (
                                    None if engine.name == "sqlite" else schema_name
                                )
                                if overwrite_stored_pat_docs:
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
                        logging.error(f"Failed to save MCT docs batch to DB: {e}")
                elif not batch_target.empty:
                    os.makedirs(
                        os.path.dirname(batch_epr_target_path_mct), exist_ok=True
                    )
                    batch_target.to_csv(batch_epr_target_path_mct, index=False)
        else:
            batch_target = pd.read_csv(batch_epr_target_path_mct)
        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch MCT documents: {e}")
        return pd.DataFrame()
