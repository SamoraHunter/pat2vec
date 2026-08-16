import logging
import os
from typing import Any

import pandas as pd

from pat2vec.util.helper_functions import (
    get_df_from_db,
    save_annotations_to_db,
    save_raw_patient_batch,
)
from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
    get_pat_document_annotation_batch_epic_medical_history,
)
from pat2vec.util.methods_get import exist_check, update_pbar


def _fetch_epic_medical_history_from_elasticsearch(
    current_pat_client_id_code: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
    t=None,
) -> pd.DataFrame:
    """Fetches Epic medical history data from Elasticsearch.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The configuration object with search settings.
        cohort_searcher_with_terms_and_search: Search function to use for ES queries.
            If None, attempts to use config_obj.cohort_searcher_with_terms_and_search.
        t: tqdm progress bar instance.

    Returns:
        A DataFrame containing the raw Epic medical history for the patient.

    """
    try:
        start_time = config_obj.start_time

        update_pbar(
            current_pat_client_id_code="",
            start_time=start_time,
            stage_int=0,
            stage_str="epic_medical_history_batch_fetch",
            t=t,
            config_obj=config_obj,
        )

        start_year = config_obj.global_start_year
        start_month = config_obj.global_start_month
        start_day = config_obj.global_start_day
        end_year = config_obj.global_end_year
        end_month = config_obj.global_end_month
        end_day = config_obj.global_end_day

        # Use the provided search function or fall back to config_obj
        search_func = (
            cohort_searcher_with_terms_and_search
            if cohort_searcher_with_terms_and_search is not None
            else getattr(config_obj, "cohort_searcher_with_terms_and_search", None)
        )

        results = search_func(
            index_name="epic_medical_history",
            fields_list=None,
            term_name="document_PatientDurableKey",
            entered_list=[current_pat_client_id_code],
            search_string=f"document_CreatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
        )
        if results is not None and not results.empty:
            if "document_PatientDurableKey" in results.columns:
                results.rename(
                    columns={"document_PatientDurableKey": "client_idcode"},
                    inplace=True,
                )
            if "document_CreatedWhen" in results.columns:
                results.rename(
                    columns={"document_CreatedWhen": "updatetime"},
                    inplace=True,
                )
            if "document_Comment" in results.columns:
                results.rename(
                    columns={"document_Comment": "body_analysed"},
                    inplace=True,
                )
            # Handle id -> document_guid rename, with fallback for medical history index
            if "id" in results.columns:
                results.rename(columns={"id": "document_guid"}, inplace=True)
            elif "document_SourceId" in results.columns:
                results.rename(
                    columns={"document_SourceId": "document_guid"},
                    inplace=True,
                )
            if "document_Name" in results.columns:
                results.rename(
                    columns={"document_Name": "document_description"},
                    inplace=True,
                )
        return results if results is not None else pd.DataFrame()
    except Exception as e:
        logging.error(
            f"Error fetching epic medical history from ES for {current_pat_client_id_code}: {e}",
        )
        return pd.DataFrame()


def get_pat_batch_epic_medical_history_annotations(
    current_pat_client_id_code: str,
    config_obj: Any,
    cat: Any,
    t: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
) -> pd.DataFrame | None:
    """Retrieves or creates annotations for a patient's Epic medical history batch.

    This function checks if an annotation file for the patient's Epic medical history
    already exists. If so, it reads it. If not, it reads the raw document
    batch, generates annotations using the provided MedCAT model,
    and saves the result.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The main configuration object.
        cat: The loaded MedCAT `CAT` object.
        t: The tqdm progress bar instance.
        cohort_searcher_with_terms_and_search: Optional search function to fetch
            data from Elasticsearch. If provided and DB returns empty, ES will be used.

    Returns:
        A DataFrame containing the annotations for the patient's Epic medical history.

    """
    if config_obj.storage_backend == "database":
        table_name = "ann_epic_medical_history"
        schema_name = "annotations"

        if not config_obj.overwrite_stored_pat_docs:
            df = get_df_from_db(
                config_obj,
                schema_name,
                table_name,
                patient_ids=[current_pat_client_id_code],
            )
            if not df.empty:
                return df

    batch_epic_medical_history_path = os.path.join(
        config_obj.pre_epic_medical_history_batch_path,
        str(current_pat_client_id_code) + ".csv",
    )

    pre_document_annotation_batch_path = (
        config_obj.pre_epic_medical_history_annotation_batch_path
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path,
        current_pat_client_id_code + ".csv",
    )

    if exist_check(current_pat_document_annotation_batch_path, config_obj=config_obj):
        batch_target = pd.read_csv(current_pat_document_annotation_batch_path)
    else:
        pat_batch = pd.DataFrame()

        # First try to get from database
        if config_obj.storage_backend == "database":
            pat_batch = get_df_from_db(
                config_obj,
                "raw_data",
                "raw_epic_medical_history",
                patient_ids=[current_pat_client_id_code],
            )

        # If not in DB, try from file
        if pat_batch.empty:
            try:
                pat_batch = pd.read_csv(batch_epic_medical_history_path)
            except (FileNotFoundError, pd.errors.EmptyDataError):
                pat_batch = pd.DataFrame()

        # If still empty and ES search function exists, fetch from Elasticsearch and save to DB
        if pat_batch.empty and cohort_searcher_with_terms_and_search is not None:
            pat_batch = _fetch_epic_medical_history_from_elasticsearch(
                current_pat_client_id_code,
                config_obj,
                cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
                t=t,
            )

            # Save raw batch to database after fetching from ES
            if not pat_batch.empty and config_obj.storage_backend == "database":
                try:
                    save_raw_patient_batch(
                        pat_batch,
                        current_pat_client_id_code,
                        "raw_epic_medical_history",
                        config_obj,
                    )
                except Exception as e:
                    logging.error(
                        f"Failed to save raw epic medical history batch for {current_pat_client_id_code}: {e}",
                    )

        if config_obj.verbosity >= 6:
            print(
                f"DEBUG: Got {len(pat_batch)} rows from raw epic_medical_history source",
            )

        if pat_batch.empty:
            logging.info(
                f"No raw medical history found for patient {current_pat_client_id_code}, ensuring annotation table exists",
            )
            if (
                config_obj.storage_backend == "database"
                and config_obj.store_pat_batch_docs
            ):
                from pat2vec.util.post_processing_annotations import EMPTY_ANNOT_COLS

                empty_df = pd.DataFrame(columns=EMPTY_ANNOT_COLS)
                empty_df["client_idcode"] = current_pat_client_id_code
                try:
                    save_annotations_to_db(
                        empty_df,
                        current_pat_client_id_code,
                        "ann_epic_medical_history",
                        config_obj,
                        id_column="client_idcode",
                    )
                except Exception as e:
                    logging.warning(
                        f"Could not create annotation table for epic_medical_history: {e}",
                    )

            if getattr(config_obj, "testing", False) and getattr(
                config_obj,
                "dummy_medcat_model",
                False,
            ):
                pat_batch = pd.DataFrame(
                    {
                        "client_idcode": [current_pat_client_id_code],
                        "body_analysed": ["Patient medical history"],
                        "updatetime": [config_obj.start_time],
                        "document_guid": ["dummy_doc_" + current_pat_client_id_code],
                    },
                )
            else:
                from pat2vec.util.post_processing_annotations import EMPTY_ANNOT_COLS

                return pd.DataFrame(columns=EMPTY_ANNOT_COLS)

        batch_target = get_pat_document_annotation_batch_epic_medical_history(
            current_pat_client_idcode=current_pat_client_id_code,
            pat_batch=pat_batch,
            cat=cat,
            config_obj=config_obj,
            t=t,
        )

    should_store = (
        config_obj.store_pat_batch_docs or config_obj.overwrite_stored_pat_docs
    )
    if (
        should_store
        and config_obj.storage_backend == "database"
        and isinstance(batch_target, pd.DataFrame)
        and not batch_target.empty
    ):
        try:
            engine = config_obj.db_engine
            if not engine:
                logging.error(
                    "Database engine not initialized in config_obj for epic medical history annotations.",
                )
                return batch_target

            with engine.begin() as connection:
                table_name = "ann_epic_medical_history"
                schema_name = "annotations"
                db_table = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                cols_to_drop = ["_id", "_index", "_score"]

                for col in cols_to_drop:
                    if col in batch_target.columns:
                        batch_target.drop(columns=col, inplace=True)

                batch_to_save = batch_target.copy()
                for col in batch_to_save.columns:
                    if batch_to_save[col].dtype == "object":
                        if (
                            batch_to_save[col]
                            .apply(lambda x: isinstance(x, (list, dict)))
                            .any()
                        ):
                            batch_to_save[col] = batch_to_save[col].apply(
                                lambda x: str(x) if isinstance(x, (list, dict)) else x,
                            )

                if config_obj.overwrite_stored_pat_docs:
                    connection.execute(
                        f'DELETE FROM "{db_table}" WHERE client_idcode = :pat_id',
                        {"pat_id": current_pat_client_id_code},
                    )
                batch_to_save.to_sql(
                    name=db_table,
                    con=connection,
                    schema=db_schema,
                    if_exists="append",
                    index=False,
                )
        except Exception as e:
            logging.error(
                f"Could not write epic medical history annotations to DB for patient {current_pat_client_id_code}: {e}",
            )
    return batch_target
