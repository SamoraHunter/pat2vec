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
    get_pat_document_annotation_batch_epic_clinical_notes,
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

        # Try with .keyword suffix first (standard for exact match), then without
        term_names_to_try = [
            "document_PatientDurableKey.keyword",
            "document_PatientDurableKey",
        ]

        results = pd.DataFrame()
        for term_name in term_names_to_try:
            _logger.debug(
                f"Fetching from ES for patient {current_pat_client_id_code} with field '{term_name}', date range: {start_year}-{start_month}-{start_day} to {end_year}-{end_month}-{end_day}",
            )
            results = search_func(
                index_name="epic_clinical_notes",
                fields_list=None,
                term_name=term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f"document_UpdatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
            )
            _logger.debug(
                f"After ES fetch with '{term_name}' (with date filter): results={type(results)}, empty={results.empty if results is not None else 'N/A'}",
            )
            if results is not None and not results.empty:
                break

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
            # Handle id -> document_guid rename, with fallback for clinical notes index
            if "id" in results.columns:
                results = results.rename(columns={"id": "document_guid"})
            elif "document_SourceId" in results.columns:
                results = results.rename(
                    columns={"document_SourceId": "document_guid"},
                )
            # Note: document_Name removed from ES field_map to avoid schema mismatch
        return results if results is not None else pd.DataFrame()
    except Exception as e:
        _logger.error(
            f"Error fetching epic clinical notes from ES for {current_pat_client_id_code}: {e}",
        )
        return pd.DataFrame()


def get_pat_batch_epic_clinical_notes_annotations(
    current_pat_client_id_code: str,
    config_obj: Any,
    cat: Any,
    t: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
) -> pd.DataFrame | None:
    """Retrieves or creates annotations for a patient's Epic clinical notes batch.

    This function checks if an annotation file for the patient's Epic clinical notes
    already exists. If so, it reads it. If not, it reads the raw document
    batch, generates annotations using the provided MedCAT model,
    and saves the result.

    Args:
    ----
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The main configuration object.
        cat: The loaded MedCAT `CAT` object.
        t: The tqdm progress bar instance.
        cohort_searcher_with_terms_and_search: Optional search function to fetch
            data from Elasticsearch. If provided and DB returns empty, ES will be used.

    Returns:
    -------
        A DataFrame containing the annotations for the patient's Epic clinical notes.

    """
    print("\n=== DEBUG get_pat_batch_epic_clinical_notes_annotations START ===")
    print(f"Patient: {current_pat_client_id_code}")
    print(f"Storage backend: {config_obj.storage_backend if config_obj else 'None'}")

    if cat is None:
        print("WARNING: cat (MedCAT) is None! Annotations cannot be generated.")
    else:
        print(f"MedCAT cat object available: {type(cat)}")

    if config_obj.storage_backend == "database":
        table_name = "ann_epic_clinical_notes"
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

    batch_epic_clinical_notes_path = os.path.join(
        config_obj.pre_epic_clinical_notes_batch_path,
        str(current_pat_client_id_code) + ".csv",
    )

    pre_document_annotation_batch_path = (
        config_obj.pre_epic_clinical_notes_annotation_batch_path
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
                "raw_epic_clinical_notes",
                patient_ids=[current_pat_client_id_code],
            )

        # If not in DB, try from file
        if pat_batch.empty:
            try:
                pat_batch = pd.read_csv(batch_epic_clinical_notes_path)
            except (FileNotFoundError, pd.errors.EmptyDataError):
                pat_batch = pd.DataFrame()

        # If still empty, fetch from Elasticsearch using provided search function
        _logger.debug(
            f"ES fetch check: pat_batch.empty={pat_batch.empty}, cohort_searcher_available={cohort_searcher_with_terms_and_search is not None}",
        )
        if pat_batch.empty and cohort_searcher_with_terms_and_search is not None:
            _logger.info(
                f"Fetching from Elasticsearch for patient {current_pat_client_id_code}",
            )
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
                        "raw_epic_clinical_notes",
                        config_obj,
                    )
                except Exception as e:
                    _logger.error(
                        f"Failed to save raw epic clinical notes batch for {current_pat_client_id_code}: {e}",
                    )
                    raise

        _logger.debug(
            f"After DB/file fetch, pat_batch.empty={pat_batch.empty}, shape={pat_batch.shape if not pat_batch.empty else 'N/A'}",
        )

        if config_obj.verbosity >= 6:
            print(
                f"DEBUG: Got {len(pat_batch)} rows from raw epic_clinical_notes source",
            )

        # When no raw data is found, create annotation table and handle testing mode
        if pat_batch.empty:
            _logger.info(
                f"No raw clinical notes found for patient {current_pat_client_id_code}, ensuring annotation table exists",
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
                        "ann_epic_clinical_notes",
                        config_obj,
                        id_column="client_idcode",
                    )
                except Exception as e:
                    _logger.warning(
                        f"Could not create annotation table for epic_clinical_notes: {e}",
                    )

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
                        "body_analysed": ["Patient clinical notes"],
                        "updatetime": [config_obj.start_time],
                        "document_guid": ["dummy_doc_" + current_pat_client_id_code],
                        "document_PatientDurableKey": [current_pat_client_id_code],
                        "document_CreatedWhen": [config_obj.start_time],
                        "id": ["dummy_id_" + current_pat_client_id_code],
                    },
                )
                # Save raw data to DB when ES fetch fails in testing mode
                if config_obj.storage_backend == "database":
                    try:
                        save_raw_patient_batch(
                            pat_batch,
                            current_pat_client_id_code,
                            "raw_epic_clinical_notes",
                            config_obj,
                        )
                    except Exception as e:
                        _logger.error(
                            f"Failed to save raw epic clinical notes batch for {current_pat_client_id_code}: {e}",
                        )
                        raise
            else:
                from pat2vec.util.post_processing_annotations import EMPTY_ANNOT_COLS

                return pd.DataFrame(columns=EMPTY_ANNOT_COLS)

        batch_target = get_pat_document_annotation_batch_epic_clinical_notes(
            current_pat_client_idcode=current_pat_client_id_code,
            pat_batch=pat_batch,
            cat=cat,
            config_obj=config_obj,
            t=t,
        )

    print(
        f"\nDEBUG: After annotation generation, batch_target shape: {batch_target.shape if batch_target is not None else 'None'}",
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
                    "Database engine not initialized in config_obj for epic clinical notes annotations.",
                )
                return batch_target

            with engine.begin() as connection:
                table_name = "ann_epic_clinical_notes"
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
            print(
                f"DEBUG: Successfully wrote epic clinical notes annotations to DB for patient {current_pat_client_id_code}",
            )
        except Exception as e:
            _logger.error(
                f"Could not write epic clinical notes annotations to DB for patient {current_pat_client_id_code}: {e}",
            )
            raise
    else:
        print("DEBUG: Skipping database storage (should_store=False or empty batch)")

    print("\n=== DEBUG get_pat_batch_epic_clinical_notes_annotations END ===\n")

    return batch_target
