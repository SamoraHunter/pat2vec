import logging
import os
from typing import Any

_logger = logging.getLogger(__name__)


import pandas as pd

from pat2vec.util.helper_functions import (
    get_df_from_db,
    save_annotations_to_db,
    save_raw_patient_batch,
)
from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
    get_pat_document_annotation_batch_epic_imaging_reports,
)
from pat2vec.util.methods_get import exist_check, update_pbar


def _fetch_epic_imaging_reports_from_elasticsearch(
    current_pat_client_id_code: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
    t=None,
) -> pd.DataFrame:
    """Fetches Epic imaging reports data from Elasticsearch.

    Args:
    ----
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The configuration object with search settings.
        cohort_searcher_with_terms_and_search: Search function to use for ES queries.
            If None, attempts to use config_obj.cohort_searcher_with_terms_and_search.
        t: tqdm progress bar instance.

    Returns:
    -------
        A DataFrame containing the raw Epic imaging reports for the patient.

    """
    try:
        start_time = config_obj.start_time

        update_pbar(
            current_pat_client_id_code="",
            start_time=start_time,
            stage_int=0,
            stage_str="epic_imaging_reports_batch_fetch",
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
            index_name="epic_imaging_reports",
            fields_list=None,
            term_name="document_PatientDurableKey",
            entered_list=[current_pat_client_id_code],
            search_string=f"document_CreatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
        )
        if results is None:
            _logger.error("ES fetch returned None for epic_imaging_reports")
        elif results.empty:
            _logger.warning(
                f"ES fetch returned empty DataFrame for Epic Imaging Reports. Patient: {current_pat_client_id_code}",
            )
        else:
            _logger.info(
                f"ES fetch got {len(results)} rows for Epic Imaging Reports. Patient: {current_pat_client_id_code}, columns: {list(results.columns)[:5]}",
            )

        # Debug: check what search_func returns by calling it directly and logging
        if results is not None:
            _logger.debug(
                f"Results shape: {results.shape}, columns: {list(results.columns)}",
            )
            _logger.debug(
                f"First row sample: {results.iloc[0].to_dict() if not results.empty else 'empty'}",
            )

        if results is not None and not results.empty:
            if "document_PatientDurableKey" in results.columns:
                results = results.rename(
                    columns={"document_PatientDurableKey": "client_idcode"},
                )
            if "document_CreatedWhen" in results.columns:
                pass  # Not renamed - matches MAPPINGS schema
            if "document_Content" in results.columns:
                results = results.rename(
                    columns={"document_Content": "body_analysed"},
                )
            # Handle id -> document_guid rename, with fallback for imaging reports index
            if "id" in results.columns:
                results = results.rename(columns={"id": "document_guid"})
            elif "document_SourceId" in results.columns:
                results = results.rename(
                    columns={"document_SourceId": "document_guid"},
                )
            if "document_Name" in results.columns:
                results.rename(
                    # Note: document_Name removed from ES field_map to avoid schema mismatch
                )
        return results if results is not None else pd.DataFrame()
    except Exception as e:
        _logger.error(
            f"Error fetching epic imaging reports from ES for {current_pat_client_id_code}: {e}",
        )
        return pd.DataFrame()


def get_pat_batch_epic_imaging_reports_annotations(
    current_pat_client_id_code: str,
    config_obj: Any,
    cat: Any,
    t: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
) -> pd.DataFrame | None:
    """Retrieves or creates annotations for a patient's Epic imaging reports batch.

    This function checks if an annotation file for the patient's Epic imaging reports
    already exists. If so, it reads it. If not, it reads the raw document
    batch, generates annotations using the provided MedCAT model,
    and saves the result.

    Args:
    ----
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The main configuration object.
        cat: The loaded MedCAP `CAT` object.
        t: The tqdm progress bar instance.
        cohort_searcher_with_terms_and_search: Optional search function to fetch
            data from Elasticsearch. If provided and DB returns empty, ES will be used.

    Returns:
    -------
        A DataFrame containing the annotations for the patient's Epic imaging reports.

    """
    if config_obj.storage_backend == "database":
        table_name = "ann_epic_imaging_reports"
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

    batch_epic_imaging_reports_path = os.path.join(
        config_obj.pre_epic_imaging_reports_batch_path,
        str(current_pat_client_id_code) + ".csv",
    )

    pre_document_annotation_batch_path = (
        config_obj.pre_epic_imaging_reports_annotation_batch_path
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
                "raw_epic_imaging_reports",
                patient_ids=[current_pat_client_id_code],
            )

        # If not in DB, try from file
        if pat_batch.empty:
            try:
                pat_batch = pd.read_csv(batch_epic_imaging_reports_path)
            except (FileNotFoundError, pd.errors.EmptyDataError):
                pat_batch = pd.DataFrame()

        # If still empty and ES search function exists, fetch from Elasticsearch and save to DB
        if pat_batch.empty and cohort_searcher_with_terms_and_search is not None:
            _logger.info(
                f"Fetching epic_imaging_reports from ES for patient {current_pat_client_id_code}",
            )
            pat_batch = _fetch_epic_imaging_reports_from_elasticsearch(
                current_pat_client_id_code,
                config_obj,
                cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
                t=t,
            )
            if not pat_batch.empty:
                _logger.info(
                    f"Got {len(pat_batch)} rows from ES for epic_imaging_reports",
                )

            # Save raw batch to database after fetching from ES
            if not pat_batch.empty and config_obj.storage_backend == "database":
                try:
                    save_raw_patient_batch(
                        pat_batch,
                        current_pat_client_id_code,
                        "raw_epic_imaging_reports",
                        config_obj,
                    )
                except Exception as e:
                    _logger.error(
                        f"Failed to save raw epic imaging reports batch for {current_pat_client_id_code}: {e}",
                    )

        if config_obj.verbosity >= 6:
            print(
                f"DEBUG: Got {len(pat_batch)} rows from raw epic_imaging_reports source",
            )

        if pat_batch.empty:
            _logger.info(
                f"No raw imaging reports found for patient {current_pat_client_id_code}, ensuring annotation table exists",
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
                        "ann_epic_imaging_reports",
                        config_obj,
                        id_column="client_idcode",
                    )
                except Exception as e:
                    _logger.warning(
                        f"Could not create annotation table for epic_imaging_reports: {e}",
                    )

            if getattr(config_obj, "testing", False) and getattr(
                config_obj,
                "dummy_medcat_model",
                False,
            ):
                pat_batch = pd.DataFrame(
                    {
                        "client_idcode": [current_pat_client_id_code],
                        "body_analysed": ["Patient imaging report"],
                        "updatetime": [config_obj.start_time],
                        "document_guid": ["dummy_doc_" + current_pat_client_id_code],
                        "document_PatientDurableKey": [current_pat_client_id_code],
                        "document_CreatedWhen": [config_obj.start_time],
                        "id": [
                            "dummy_id_" + current_pat_client_id_code,
                        ],  # Added for database storage
                    },
                )
                # Save raw data to DB when ES fetch fails in testing mode
                if config_obj.storage_backend == "database":
                    try:
                        save_raw_patient_batch(
                            pat_batch,
                            current_pat_client_id_code,
                            "raw_epic_imaging_reports",
                            config_obj,
                        )
                    except Exception as e:
                        _logger.error(
                            f"Failed to save raw epic imaging reports batch for {current_pat_client_id_code}: {e}",
                        )
            else:
                from pat2vec.util.post_processing_annotations import EMPTY_ANNOT_COLS

                return pd.DataFrame(columns=EMPTY_ANNOT_COLS)

        batch_target = get_pat_document_annotation_batch_epic_imaging_reports(
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
                _logger.error(
                    "Database engine not initialized in config_obj for epic imaging reports annotations.",
                )
                return batch_target

            with engine.begin() as connection:
                table_name = "ann_epic_imaging_reports"
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
            _logger.error(
                f"Could not write epic imaging reports annotations to DB for patient {current_pat_client_id_code}: {e}",
            )
    return batch_target
