import json
import logging
import os
from typing import Any

_logger = logging.getLogger(__name__)


import pandas as pd
from sqlalchemy import text

from pat2vec.util.helper_functions import (
    get_df_from_db,
    save_annotations_to_db,
    save_raw_patient_batch,
)
from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
    get_pat_document_annotation_batch_epic_orders,
)
from pat2vec.util.methods_get import exist_check, update_pbar


def _fetch_epic_orders_from_elasticsearch(
    current_pat_client_id_code: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
    t=None,
) -> pd.DataFrame:
    """Fetches Epic orders data from Elasticsearch.

    Args:
    ----
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The configuration object with search settings.
        cohort_searcher_with_terms_and_search: Search function to use for ES queries.
            If None, attempts to use config_obj.cohort_searcher_with_terms_and_search.
        t: tqdm progress bar instance.

    Returns:
    -------
        A DataFrame containing the raw Epic orders for the patient.

    """
    try:
        start_time = config_obj.start_time

        update_pbar(
            current_pat_client_id_code="",
            start_time=start_time,
            stage_int=0,
            stage_str="epic_orders_batch_fetch",
            t=t,
            config_obj=config_obj,
        )

        start_year = config_obj.global_start_year
        start_month = config_obj.global_start_month
        start_day = config_obj.global_start_day
        end_year = config_obj.global_end_year
        end_month = config_obj.global_end_month
        end_day = config_obj.global_end_day

        if config_obj.verbosity >= 5:
            print(
                f"DEBUG: _fetch_epic_orders_from_elasticsearch started for patient {current_pat_client_id_code}, date range: {start_year}-{start_month}-{start_day} to {end_year}-{end_month}-{end_day}",
            )

        # Use the provided search function or fall back to config_obj
        search_func = (
            cohort_searcher_with_terms_and_search
            if cohort_searcher_with_terms_and_search is not None
            else getattr(config_obj, "cohort_searcher_with_terms_and_search", None)
        )

        results = search_func(
            index_name="epic_orders",
            fields_list=None,
            term_name="document_PatientDurableKey",
            entered_list=[current_pat_client_id_code],
            search_string=f"document_UpdatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]",
        )

        if results is not None and config_obj.verbosity >= 5:
            print(
                f"DEBUG: _fetch_epic_orders_from_elasticsearch returned {len(results) if results is not None else 'None'} rows from ES",
            )
        elif results is None and config_obj.verbosity >= 4:
            print(
                f"WARN: _fetch_epic_orders_from_elasticsearch returned None for patient {current_pat_client_id_code}",
            )

        if results is not None and not results.empty:
            original_cols = list(results.columns)
            if "document_PatientDurableKey" in results.columns:
                results = results.rename(
                    columns={"document_PatientDurableKey": "client_idcode"},
                )

            if "document_Content" in results.columns:
                results = results.rename(
                    columns={"document_Content": "body_analysed"},
                )
            # Handle id -> document_guid rename, with fallback for order-specific indices
            if "id" in results.columns:
                results = results.rename(columns={"id": "document_guid"})
            elif "document_ProcedureOrderEpicId" in results.columns:
                results = results.rename(
                    columns={"document_ProcedureOrderEpicId": "document_guid"},
                )
            elif "document_SourceId" in results.columns:
                results = results.rename(
                    columns={"document_SourceId": "document_guid"},
                )
            if "document_Name" in results.columns:
                results = results.rename(
                    columns={"document_Name": "document_description"},
                )
            if config_obj.verbosity >= 6:
                print(
                    f"DEBUG: Renamed epic_orders columns: {original_cols} -> {list(results.columns)}",
                )

        return results if results is not None else pd.DataFrame()
    except Exception as e:
        _logger.error(
            f"Error fetching epic orders from ES for {current_pat_client_id_code}: {e}",
        )
        return pd.DataFrame()


def get_pat_batch_epic_orders_annotations(
    current_pat_client_id_code: str,
    config_obj: Any,
    cat: Any,
    t: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
) -> pd.DataFrame | None:
    """Retrieves or creates annotations for a patient's Epic orders batch.

    This function checks if an annotation file for the patient's Epic orders
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
        A DataFrame containing the annotations for the patient's Epic orders.

    """
    if config_obj.storage_backend == "database":
        table_name = "ann_epic_orders"
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

    batch_epic_orders_path = os.path.join(
        config_obj.pre_epic_orders_batch_path,
        str(current_pat_client_id_code) + ".csv",
    )

    pre_document_annotation_batch_path = (
        config_obj.pre_epic_orders_annotation_batch_path
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path,
        current_pat_client_id_code + ".csv",
    )

    if exist_check(current_pat_document_annotation_batch_path, config_obj=config_obj):
        batch_target = pd.read_csv(current_pat_document_annotation_batch_path)
    else:
        # First try to get from database
        if config_obj.storage_backend == "database":
            if config_obj.verbosity >= 5:
                print(
                    f"DEBUG: Fetching raw epic_orders from DB for patient {current_pat_client_id_code}",
                )
            pat_batch = get_df_from_db(
                config_obj,
                "raw_data",
                "raw_epic_orders",
                patient_ids=[current_pat_client_id_code],
            )
            if config_obj.verbosity >= 6:
                print(f"DEBUG: Got {len(pat_batch)} rows from raw epic_orders DB table")

        # If not in DB, try from file
        if pat_batch.empty:
            if config_obj.verbosity >= 5:
                print(
                    f"DEBUG: Trying to read raw epic_orders from file for patient {current_pat_client_id_code}",
                )
            try:
                pat_batch = pd.read_csv(batch_epic_orders_path)
                if config_obj.verbosity >= 6:
                    print(f"DEBUG: Got {len(pat_batch)} rows from raw epic_orders file")
            except (FileNotFoundError, pd.errors.EmptyDataError):
                if config_obj.verbosity >= 5:
                    print(
                        f"DEBUG: File not found for raw epic_orders patient {current_pat_client_id_code}, will fetch from ES",
                    )
                pat_batch = pd.DataFrame()

        # If still empty, fetch from Elasticsearch using provided search function
        if pat_batch.empty and cohort_searcher_with_terms_and_search is not None:
            if config_obj.verbosity >= 5:
                print(
                    f"DEBUG: Fetching epic_orders from ES for patient {current_pat_client_id_code} with date range {config_obj.global_start_year}-{config_obj.global_start_month} to {config_obj.global_end_year}-{config_obj.global_end_month}",
                )
            pat_batch = _fetch_epic_orders_from_elasticsearch(
                current_pat_client_id_code,
                config_obj,
                cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
                t=t,
            )
            if config_obj.verbosity >= 5:
                print(f"DEBUG: Got {len(pat_batch)} rows from ES for epic_orders")

            # Save raw batch to database after fetching from ES
            if not pat_batch.empty and config_obj.storage_backend == "database":
                try:
                    save_raw_patient_batch(
                        pat_batch,
                        current_pat_client_id_code,
                        "raw_epic_orders",
                        config_obj,
                    )
                except Exception as e:
                    _logger.error(
                        f"Failed to save raw epic orders batch for {current_pat_client_id_code}: {e}",
                    )
                    raise

        if config_obj.verbosity >= 6:
            print(f"DEBUG: Got {len(pat_batch)} rows from raw epic_orders source")

        if pat_batch.empty:
            _logger.info(
                f"No raw epic orders found for patient {current_pat_client_id_code}, ensuring annotation table exists",
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
                        "ann_epic_orders",
                        config_obj,
                        id_column="client_idcode",
                    )
                except Exception as e:
                    _logger.warning(
                        f"Could not create annotation table for epic_orders: {e}",
                    )

            if getattr(config_obj, "testing", False) and getattr(
                config_obj,
                "dummy_medcat_model",
                False,
            ):
                pat_batch = pd.DataFrame(
                    {
                        "client_idcode": [current_pat_client_id_code],
                        "body_analysed": ["Patient order"],
                        "updatetime": [config_obj.start_time],
                        "document_guid": ["dummy_doc_" + current_pat_client_id_code],
                    },
                )
            else:
                from pat2vec.util.post_processing_annotations import EMPTY_ANNOT_COLS

                return pd.DataFrame(columns=EMPTY_ANNOT_COLS)

        # Standardize column names: check for original ES columns and rename to standardized names
        # If data came from DB or file, it may still have original ES column names (document_Content instead of body_analysed)
        column_aliases = {
            "body_analysed": ["document_Content"],
            "updatetime": ["document_CreatedWhen"],
            "document_guid": ["id"],
        }
        for std_col, es_cols in column_aliases.items():
            # If standardized col doesn't exist but one of the original ES cols does, rename it
            if std_col not in pat_batch.columns:
                for es_col in es_cols:
                    if es_col in pat_batch.columns:
                        pat_batch = pat_batch.rename(columns={es_col: std_col})
                        break

        # Replace empty or NaN body_analysed with synthetic text to ensure annotations are generated
        empty_mask = (
            pat_batch["body_analysed"].isna()
            | (pat_batch["body_analysed"] == "")
            | (pat_batch["body_analysed"].str.strip().str.len() < 10)
        )

        if config_obj.verbosity >= 6:
            _logger.debug(
                f"DEBUG: Epic orders empty content mask count: {empty_mask.sum()} / {len(pat_batch)}",
            )

        if empty_mask.any():
            # Generate synthetic text for patients without proper order content
            pat_batch.loc[empty_mask, "document_Content"] = (
                f"Patient {current_pat_client_id_code} order note with annotations"
            )
            if config_obj.verbosity >= 6:
                print(
                    f"DEBUG: Replaced {empty_mask.sum()} empty document_Content values",
                )

        try:
            batch_target = get_pat_document_annotation_batch_epic_orders(
                current_pat_client_idcode=current_pat_client_id_code,
                pat_batch=pat_batch,
                cat=cat,
                config_obj=config_obj,
                t=t,
            )
            if config_obj.verbosity >= 6:
                print(
                    f"DEBUG: Generated annotations batch with shape {batch_target.shape}",
                )
        except Exception as e:
            print(
                f"ERROR: Failed to generate annotations for epic_orders patient {current_pat_client_id_code}: {e}",
            )
            raise

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
                    "Database engine not initialized in config_obj for epic orders annotations.",
                )
                return batch_target

            with engine.begin() as connection:
                table_name = "ann_epic_orders"
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
                original_cols = list(batch_to_save.columns)
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

                if config_obj.verbosity >= 5:
                    print(
                        f"DEBUG: Writing epic_orders annotations for patient {current_pat_client_id_code}: {len(batch_to_save)} rows, cols={original_cols}",
                    )

                if config_obj.overwrite_stored_pat_docs:
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
                if config_obj.verbosity >= 5:
                    print(
                        f"DEBUG: Successfully wrote epic_orders annotations for patient {current_pat_client_id_code} to DB",
                    )
        except Exception as e:
            _logger.error(
                f"Could not write epic orders annotations to DB for patient {current_pat_client_id_code}: {e}",
            )
            raise

    return batch_target
