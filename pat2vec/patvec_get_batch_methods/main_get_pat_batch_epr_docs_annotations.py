# return []


from pat2vec.util.helper_functions import get_df_from_db
from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
    get_pat_document_annotation_batch,
)
from pat2vec.util.methods_get import exist_check


import pandas as pd
from sqlalchemy import text


import logging
import json
import os
from typing import Any, Optional


def get_pat_batch_epr_docs_annotations(
    current_pat_client_id_code: str, config_obj: Any, cat: Any, t: Any
) -> Optional[pd.DataFrame]:
    """Retrieves or creates annotations for a patient's EPR document batch.

    This function checks if an annotation file for the patient's EPR documents
    already exists. If so, it reads it. If not, it reads the raw document
    batch, generates annotations using the provided MedCAT model, and saves
    the result.

    Args:
    current_pat_client_id_code: The patient's unique identifier.
    config_obj: The main configuration object.
    cat: The loaded MedCAT `CAT` object.
    t: The tqdm progress bar instance.

    Returns:
    A DataFrame containing the annotations for the patient's EPR documents.
    """
    if config_obj.storage_backend == "database":
        table_name = "ann_epr_docs"
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

    batch_epr_target_path = os.path.join(
        config_obj.pre_document_batch_path, str(current_pat_client_id_code) + ".csv"
    )

    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_id_code + ".csv"
    )

    if exist_check(current_pat_document_annotation_batch_path, config_obj=config_obj):
        batch_target = pd.read_csv(current_pat_document_annotation_batch_path)
    else:
        if config_obj.storage_backend == "database":
            # Use the helper function which handles table existence checks
            pat_batch = get_df_from_db(
                config_obj,
                "raw_data",
                "raw_epr_docs",
                patient_ids=[current_pat_client_id_code],
            )
        else:
            pat_batch = pd.read_csv(batch_epr_target_path)

        if pat_batch.empty:
            return None

        pat_batch.dropna(subset=["body_analysed"], axis=0, inplace=True)

        batch_target = get_pat_document_annotation_batch(
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
                    "Database engine not initialized in config_obj for reports annotations."
                )
                return batch_target

            with engine.begin() as connection:
                table_name = "ann_epr_docs"
                schema_name = "annotations"
                db_table = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                # Create a copy and serialize lists to strings for SQL compatibility
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
                                )
                            )

                if config_obj.overwrite_stored_pat_docs:
                    del_query = text(
                        f'DELETE FROM "{db_table if engine.name == "sqlite" else f"{schema_name}.{table_name}"}" WHERE client_idcode = :pat_id'
                    )
                    connection.execute(
                        del_query, {"pat_id": current_pat_client_id_code}
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
                f"Could not write EPR annotations to DB for patient {current_pat_client_id_code}: {e}"
            )
    return batch_target
