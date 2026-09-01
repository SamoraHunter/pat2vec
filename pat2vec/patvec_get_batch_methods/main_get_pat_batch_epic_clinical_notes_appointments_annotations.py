import json
import logging
import os
from typing import Any

_logger = logging.getLogger(__name__)


import pandas as pd
from sqlalchemy import inspect, text

from pat2vec.util.helper_functions import (
    get_df_from_db,
    save_annotations_to_db,
    save_raw_patient_batch,
)
from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
    get_pat_document_annotation_batch_epic_clinical_notes_appointments,
)
from pat2vec.util.methods_get import exist_check, update_pbar


def _fetch_epic_clinical_notes_from_elasticsearch(
    current_pat_client_id_code: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
    t=None,
) -> pd.DataFrame:
    """Fetches Epic clinical notes data from Elasticsearch.

    Args:
    ----
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The configuration object with search settings.
        cohort_searcher_with_terms_and_search: Search function to use for ES queries.
            If None, attempts to use config_obj.cohort_searcher_with_terms_and_search.
        t: tqdm progress bar instance.

    Returns:
    -------
        A DataFrame containing the raw Epic clinical notes for the patient.

    """
    try:
        start_time = config_obj.start_time

        update_pbar(
            current_pat_client_id_code="",
            start_time=start_time,
            stage_int=0,
            stage_str="epic_clinical_notes_batch_fetch",
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
            index_name="epic_clinical_notes_appointments",
            fields_list=None,
            term_name="document_PatientDurableKey",
            entered_list=[current_pat_client_id_code],
            search_string=f"document_UpdatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
        )
        if results is not None and not results.empty:
            if "document_PatientDurableKey" in results.columns:
                results = results.rename(
                    columns={"document_PatientDurableKey": "client_idcode"},
                )
            # Handle time column - rename either UpdatedWhen or CreatedWhen to updatetime
            if "document_UpdatedWhen" in results.columns:
                results = results.rename(
                    columns={"document_UpdatedWhen": "updatetime"},
                )
                if "document_CreatedWhen" in results.columns:
                    results = results.drop(columns=["document_CreatedWhen"])
            elif "document_CreatedWhen" in results.columns:
                results = results.rename(
                    columns={"document_CreatedWhen": "updatetime"},
                )
            if "document_Content" in results.columns:
                results = results.rename(
                    columns={"document_Content": "body_analysed"},
                )
            # Handle id -> document_guid rename, with fallback for appointments index
            if "id" in results.columns:
                results = results.rename(columns={"id": "document_guid"})
            elif "document_SourceId" in results.columns:
                results = results.rename(
                    columns={"document_SourceId": "document_guid"},
                )
            if "document_Name" in results.columns:
                results = results.rename(
                    columns={"document_Name": "document_description"},
                )
        return results if results is not None else pd.DataFrame()
    except Exception as e:
        msg = f"Critical failure fetching data from ES for patient {current_pat_client_id_code}: {e}"
        _logger.error(msg)
        raise RuntimeError(msg)


def get_pat_batch_epic_clinical_notes_appointments_annotations(
    current_pat_client_id_code: str,
    config_obj: Any,
    cat: Any,
    t: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
) -> pd.DataFrame | None:
    """Retrieves or creates annotations for a patient's Epic clinical notes appointments batch.

    This function checks if an annotation file for the patient's epic clinical notes
    appointments documents already exists. If so, it reads it. If not, it retrieves
    the raw document batch from database, generates annotations using the provided
    MedCAT model, and saves the result.

    Args:
    ----
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: Optional search function to fetch
            data from Elasticsearch. If provided and DB returns empty, ES will be used.
        cat: The loaded MedCAT `CAT` object.
        t: The tqdm progress bar instance.

    Returns:
    -------
        A DataFrame containing the annotations for the patient's epic clinical notes
        appointments documents, or None if no data is available.

    """
    if config_obj.storage_backend == "database":
        table_name = "ann_epic_clinical_notes_appointments"
        schema_name = "annotations"

        if not config_obj.overwrite_stored_pat_docs:
            df = get_df_from_db(
                config_obj,
                schema_name,
                table_name,
                patient_ids=[current_pat_client_id_code],
                warn_on_missing=False,
            )
            if not df.empty:
                return df

    batch_target_path = os.path.join(
        config_obj.pre_document_batch_path,
        str(current_pat_client_id_code) + ".csv",
    )

    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path,
        current_pat_client_id_code + ".csv",
    )

    if exist_check(current_pat_document_annotation_batch_path, config_obj=config_obj):
        batch_target = pd.read_csv(current_pat_document_annotation_batch_path)
    else:
        if config_obj.storage_backend == "database":
            pat_batch = get_df_from_db(
                config_obj,
                "raw_data",
                "raw_epic_clinical_notes_appointments",
                patient_ids=[current_pat_client_id_code],
            )

            # If not in DB, try from file
            if pat_batch.empty:
                try:
                    pat_batch = pd.read_csv(batch_target_path)
                except (FileNotFoundError, pd.errors.EmptyDataError):
                    pat_batch = pd.DataFrame()
        else:
            pat_batch = pd.read_csv(batch_target_path)

        # If still empty and ES search function exists, fetch from Elasticsearch and save to DB
        if pat_batch.empty and cohort_searcher_with_terms_and_search is not None:
            # Use the clinical notes fetcher for appointments too - they use similar index
            pat_batch = _fetch_epic_clinical_notes_from_elasticsearch(
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
                        "raw_epic_clinical_notes_appointments",
                        config_obj,
                    )
                except Exception as e:
                    _logger.error(
                        f"Failed to save raw epic clinical notes appointments batch for {current_pat_client_id_code}: {e}",
                    )
                    raise

        # When no raw data is found, handle testing mode with dummy MedCAT
        if pat_batch.empty:
            _logger.info(
                f"No clinical notes appointments found for patient {current_pat_client_id_code}, ensuring annotation table exists",
            )

            # Create annotation table even with no data (for DB schema)
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
                        "ann_epic_clinical_notes_appointments",
                        config_obj,
                        id_column="client_idcode",
                    )
                except Exception as e:
                    msg = f"Failed to save annotations for patient {current_pat_client_id_code}: {e}"
                    _logger.error(msg)
                    raise RuntimeError(msg)

            # If testing with dummy MedCAT, generate dummy annotations even without raw data
            if getattr(config_obj, "testing", False) and getattr(
                config_obj,
                "dummy_medcat_model",
                False,
            ):
                _logger.info(
                    f"Testing mode with dummy MedCAT: generating annotations for patient {current_pat_client_id_code}",
                )
                pat_batch = pd.DataFrame(
                    {
                        "client_idcode": [current_pat_client_id_code],
                        "body_analysed": ["Patient clinical notes appointments"],
                        "updatetime": [config_obj.start_time],
                        "document_guid": ["dummy_doc_" + current_pat_client_id_code],
                    },
                )
            else:
                return None

        batch_target = (
            get_pat_document_annotation_batch_epic_clinical_notes_appointments(
                current_pat_client_idcode=current_pat_client_id_code,
                pat_batch=pat_batch,
                cat=cat,
                config_obj=config_obj,
                t=t,
            )
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
                error_msg = (
                    "Database engine not initialized in config_obj for appointments annotations. "
                    "Annotations output is enabled but database storage cannot proceed without a valid database engine."
                )
                raise RuntimeError(error_msg)

            with engine.begin() as connection:
                table_name = "ann_epic_clinical_notes_appointments"
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
                        batch_target = batch_target.drop(columns=col)

                batch_to_save = batch_target.copy()
                for col in batch_to_save.columns:
                    if batch_to_save[col].dtype == "object":
                        if (
                            batch_to_save[col]
                            .apply(lambda x: isinstance(x, (list, dict)))
                            .any()
                        ):
                            batch_to_save[col] = batch_to_save[col].apply(
                                lambda x: (
                                    json.dumps(x) if isinstance(x, (list, dict)) else x
                                ),
                            )

                if config_obj.overwrite_stored_pat_docs:
                    inspector = inspect(connection)
                    if inspector.has_table(db_table, schema=db_schema):
                        del_query = text(
                            f'DELETE FROM "{db_table if engine.name == "sqlite" else f"{schema_name}.{table_name}"}" WHERE client_idcode = :pat_id',
                        )
                        connection.execute(
                            del_query,
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
            _logger.error(
                f"Could not write appointments annotations to DB for patient {current_pat_client_id_code}: {e}",
            )
            raise

    return batch_target
