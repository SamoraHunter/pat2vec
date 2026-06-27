import os
import logging
import pandas as pd
from typing import Any, Dict, List, Optional
from sqlalchemy import text
from pat2vec.util.helper_functions import save_annotations_to_db
from pat2vec.util.methods_annotation_json_to_dataframe import (
    json_to_dataframe,
)  # Keep for now, might move later
from pat2vec.util.methods_get import exist_check, update_pbar
from pat2vec.util.post_processing_annotations import (
    join_icd10_codes_to_annot,
    join_icd10_OPC4S_codes_to_annot,
    EMPTY_ANNOT_COLS,
)

logger = logging.getLogger(__name__)


def check_pat_document_annotation_complete(
    current_pat_client_id_code: str, config_obj: Any = None
) -> bool:
    """Checks if a patient's document annotation data already exists (file or database).

    Args:
        current_pat_client_id_code: The patient's ID code.
        config_obj: The configuration object containing file paths.

      Returns:
        bool: True if the annotation data exists, False otherwise.

    Raises:
        Exception: Propagates exceptions from database operations.
    """
    if getattr(config_obj, "storage_backend", "file") == "database":
        try:
            engine = config_obj.db_engine
            if not engine:
                return False

            # Check ann_epr_docs for existence
            with engine.connect() as connection:
                if engine.name == "sqlite":
                    query = text(
                        'SELECT 1 FROM "annotations_ann_epr_docs" WHERE "client_idcode" = :pat_id LIMIT 1'
                    )
                else:
                    query = text(
                        'SELECT 1 FROM "annotations"."ann_epr_docs" WHERE "client_idcode" = :pat_id LIMIT 1'
                    )

                result = connection.execute(
                    query, {"pat_id": current_pat_client_id_code}
                ).scalar()
                return result is not None
        except Exception:
            return False

    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path

    current_pat_batch_annot_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_id_code + ".csv"
    )

    bool1 = exist_check(current_pat_batch_annot_path, config_obj=config_obj)

    return bool1


def annot_pat_batch_docs(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
    text_column: str = "body_analysed",
) -> List[Dict[str, Any]]:
    """Annotates a batch of patient documents using a MedCAT model.

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame containing the documents to be annotated.
        cat: The loaded MedCAT `CAT` object.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the column in `pat_batch` containing the
            text to annotate.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary contains
        the MedCAT annotation entities for a document.

    Raises:
        Exception: Propagates exceptions from MedCAT annotation operations.
    """
    start_time = config_obj.start_time

    n_docs_to_annotate = len(pat_batch)

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "annot_pat_batch_docs_get_entities_multi_texts",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    # Fill missing values with empty strings to ensure the output list of annotations
    # is aligned (same length) with the input document batch.
    texts = pat_batch[text_column].fillna("").tolist()

    # Call MedCAT on the aligned list
    multi_annots = cat.get_entities_multi_texts(texts)

    return multi_annots


def multi_annots_to_df_textual_obs(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "textualObs",
    time_column: str = "basicobs_entered",
    include_text_sample: bool = False,
    guid_column: str = "basicobs_guid",
) -> pd.DataFrame:
    """Converts MedCAT annotations for textual observations to a DataFrame and saves it (file or DB).

    This function processes a list of annotations, converts them to a structured
    DataFrame, optionally joins ICD-10/OPCS-4 codes, and saves the result
    to a patient-specific CSV file or database table.

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        guid_column: The name of the document identifier column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)

    start_time = config_obj.start_time

    pre_document_annotation_batch_path = (
        config_obj.pre_textual_obs_annotation_batch_path
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_textual_obs",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []

    for i in range(0, len(pat_batch)):

        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )

        # drop nan rows
        # Check for NaN values in any column of the specified list
        col_list_drop_nan = [
            "client_idcode",
            time_column,
        ]

        if config_obj.verbosity >= 14:
            logger.debug(f"multi_annots_to_df_textualObs: {len(doc_to_annot_df)}")
        rows_with_nan = doc_to_annot_df[
            doc_to_annot_df[col_list_drop_nan].isna().any(axis=1)
        ]

        # Drop rows with NaN values
        doc_to_annot_df = doc_to_annot_df.drop(rows_with_nan.index).copy()
        if config_obj.verbosity >= 14:
            logger.debug(f"multi_annots_to_df_textualObs: {len(doc_to_annot_df)}")

        all_annot_dfs.append(doc_to_annot_df)

    if all_annot_dfs:
        final_df = pd.concat(all_annot_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=EMPTY_ANNOT_COLS)

    # Apply mappings sequentially to allow multiple sources to contribute codes.
    # Clashing is handled within the join functions by dropping placeholders.
    if config_obj.add_icd10:
        final_df = join_icd10_codes_to_annot(df=final_df, inner=False)
    if config_obj.add_opc4s:
        final_df = join_icd10_OPC4S_codes_to_annot(df=final_df, inner=False)

    if getattr(config_obj, "storage_backend", "file") == "file":
        final_df.to_csv(current_pat_document_annotation_batch_path, index=False)

    save_annotations_to_db(
        final_df, current_pat_client_idcode, "ann_textual_obs", config_obj
    )

    return final_df


def multi_annots_to_df_epr_docs(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "body_analysed",
    time_column: str = "updatetime",
    include_text_sample: bool = False,
    guid_column: str = "document_guid",
) -> pd.DataFrame:
    """Converts MedCAT annotations for EPR documents to a DataFrame and saves it (file or DB).

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        guid_column: The name of the document identifier column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)

    start_time = config_obj.start_time

    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_epr_docs",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []

    for i in range(0, len(pat_batch)):
        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )

        col_list_drop_nan = ["client_idcode", time_column]
        doc_to_annot_df = doc_to_annot_df.dropna(subset=col_list_drop_nan).copy()
        all_annot_dfs.append(doc_to_annot_df)

    if all_annot_dfs:
        final_df = pd.concat(all_annot_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=EMPTY_ANNOT_COLS)

    # Apply mappings sequentially to allow multiple sources to contribute codes.
    if config_obj.add_icd10:
        final_df = join_icd10_codes_to_annot(df=final_df, inner=False)
    if config_obj.add_opc4s:
        final_df = join_icd10_OPC4S_codes_to_annot(df=final_df, inner=False)

    if getattr(config_obj, "storage_backend", "file") == "file":
        os.makedirs(
            os.path.dirname(current_pat_document_annotation_batch_path), exist_ok=True
        )
        final_df.to_csv(current_pat_document_annotation_batch_path, index=False)

    save_annotations_to_db(
        final_df, current_pat_client_idcode, "ann_epr_docs", config_obj
    )

    return final_df


def multi_annots_to_df_reports(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "body_analysed",
    time_column: str = "updatetime",
    include_text_sample: bool = False,
    guid_column: str = "basicobs_guid",
) -> pd.DataFrame:
    """Converts MedCAT annotations for reports to a DataFrame and saves it (file or DB).

    This function processes a list of annotations from reports, converts them
    to a structured DataFrame, optionally joins ICD-10/OPCS-4 codes, and saves
    the result to a patient-specific CSV file or database table.

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original report documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        guid_column: The name of the document identifier column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)

    start_time = config_obj.start_time

    pre_document_annotation_batch_path = (
        config_obj.pre_document_annotation_batch_path_reports
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_reports",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []

    for i in range(0, len(pat_batch)):

        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )

        # drop nan rows
        # Check for NaN values in any column of the specified list
        col_list_drop_nan = [
            "client_idcode",
            time_column,
        ]

        if config_obj.verbosity >= 14:
            logger.debug(f"multi_annots_to_df_reports: {len(doc_to_annot_df)}")
        rows_with_nan = doc_to_annot_df[
            doc_to_annot_df[col_list_drop_nan].isna().any(axis=1)
        ]

        # Drop rows with NaN values
        doc_to_annot_df = doc_to_annot_df.drop(rows_with_nan.index).copy()
        if config_obj.verbosity >= 14:
            logger.debug(f"multi_annots_to_df_reports: {len(doc_to_annot_df)}")

        all_annot_dfs.append(doc_to_annot_df)

    if all_annot_dfs:
        final_df = pd.concat(all_annot_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=EMPTY_ANNOT_COLS)

    # Apply mappings sequentially to allow multiple sources to contribute codes.
    if config_obj.add_icd10:
        final_df = join_icd10_codes_to_annot(df=final_df, inner=False)
    if config_obj.add_opc4s:
        final_df = join_icd10_OPC4S_codes_to_annot(df=final_df, inner=False)

    if getattr(config_obj, "storage_backend", "file") == "file":
        final_df.to_csv(current_pat_document_annotation_batch_path, index=False)

    save_annotations_to_db(
        final_df, current_pat_client_idcode, "ann_reports", config_obj
    )

    return final_df


def multi_annots_to_df_epic_lab_results(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "document_LabComponentValue",
    time_column: str = "document_CreatedWhen",
    include_text_sample: bool = False,
    guid_column: str = "id",
) -> pd.DataFrame:
    """Converts MedCAT annotations for Epic lab results to a DataFrame and saves it.

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.
        guid_column: The name of the document identifier column in `pat_batch`.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)
    start_time = config_obj.start_time

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_epic_lab_results",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []
    for i in range(0, len(pat_batch)):
        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )
        all_annot_dfs.append(
            doc_to_annot_df.dropna(subset=["client_idcode", time_column])
        )

    final_df = (
        pd.concat(all_annot_dfs, ignore_index=True)
        if all_annot_dfs
        else pd.DataFrame(columns=EMPTY_ANNOT_COLS)
    )
    save_annotations_to_db(
        final_df, current_pat_client_idcode, "ann_epic_lab_results", config_obj
    )
    return final_df


def multi_annots_to_df_epic_orders(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "document_Content",
    time_column: str = "document_CreatedWhen",
    include_text_sample: bool = False,
    guid_column: str = "id",
) -> pd.DataFrame:
    """Converts MedCAT annotations for Epic orders to a DataFrame and saves it (file or DB).

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.
        guid_column: The name of the document identifier column in `pat_batch`.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)
    start_time = config_obj.start_time

    pre_document_annotation_batch_path = (
        config_obj.pre_epic_orders_annotation_batch_path
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_epic_orders",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []

    for i in range(0, len(pat_batch)):
        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )
        all_annot_dfs.append(
            doc_to_annot_df.dropna(subset=["client_idcode", time_column])
        )

    final_df = (
        pd.concat(all_annot_dfs, ignore_index=True)
        if all_annot_dfs
        else pd.DataFrame(columns=EMPTY_ANNOT_COLS)
    )

    if getattr(config_obj, "storage_backend", "file") == "file":
        os.makedirs(
            os.path.dirname(current_pat_document_annotation_batch_path), exist_ok=True
        )
        final_df.to_csv(current_pat_document_annotation_batch_path, index=False)

    save_annotations_to_db(
        final_df, current_pat_client_idcode, "ann_epic_orders", config_obj
    )
    return final_df


def multi_annots_to_df_epic_clinical_notes(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "document_Content",
    time_column: str = "document_CreatedWhen",
    include_text_sample: bool = False,
    guid_column: str = "id",
) -> pd.DataFrame:
    """Converts MedCAT annotations for Epic clinical notes to a DataFrame and saves it (file or DB).

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.
        guid_column: The name of the document identifier column in `pat_batch`.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)
    start_time = config_obj.start_time

    pre_document_annotation_batch_path = (
        config_obj.pre_epic_clinical_notes_annotation_batch_path
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_epic_clinical_notes",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []

    for i in range(0, len(pat_batch)):
        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )

        col_list_drop_nan = ["client_idcode", time_column]
        doc_to_annot_df = doc_to_annot_df.dropna(subset=col_list_drop_nan).copy()
        all_annot_dfs.append(doc_to_annot_df)

    if all_annot_dfs:
        final_df = pd.concat(all_annot_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=EMPTY_ANNOT_COLS)

    # Apply mappings sequentially to allow multiple sources to contribute codes.
    if config_obj.add_icd10:
        final_df = join_icd10_codes_to_annot(df=final_df, inner=False)
    if config_obj.add_opc4s:
        final_df = join_icd10_OPC4S_codes_to_annot(df=final_df, inner=False)

    if getattr(config_obj, "storage_backend", "file") == "file":
        os.makedirs(
            os.path.dirname(current_pat_document_annotation_batch_path), exist_ok=True
        )
        final_df.to_csv(current_pat_document_annotation_batch_path, index=False)

    save_annotations_to_db(
        final_df, current_pat_client_idcode, "ann_epic_clinical_notes", config_obj
    )

    return final_df


def multi_annots_to_df_epic_clinical_notes_appointments(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "document_Content",
    time_column: str = "document_CreatedWhen",
    include_text_sample: bool = False,
    guid_column: str = "id",
) -> pd.DataFrame:
    """Converts MedCAT annotations for Epic clinical notes appointments to a DataFrame and saves it.

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.
        guid_column: The name of the document identifier column in `pat_batch`.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)
    start_time = config_obj.start_time

    pre_document_annotation_batch_path = (
        config_obj.pre_epic_clinical_notes_appointments_annotation_batch_path
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_epic_clinical_notes_appointments",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []

    for i in range(0, len(pat_batch)):
        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )

        col_list_drop_nan = ["client_idcode", time_column]
        doc_to_annot_df = doc_to_annot_df.dropna(subset=col_list_drop_nan).copy()
        all_annot_dfs.append(doc_to_annot_df)

    if all_annot_dfs:
        final_df = pd.concat(all_annot_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=EMPTY_ANNOT_COLS)

    # Apply mappings sequentially to allow multiple sources to contribute codes.
    if config_obj.add_icd10:
        final_df = join_icd10_codes_to_annot(df=final_df, inner=False)
    if config_obj.add_opc4s:
        final_df = join_icd10_OPC4S_codes_to_annot(df=final_df, inner=False)

    if getattr(config_obj, "storage_backend", "file") == "file":
        os.makedirs(
            os.path.dirname(current_pat_document_annotation_batch_path), exist_ok=True
        )
        final_df.to_csv(current_pat_document_annotation_batch_path, index=False)

    save_annotations_to_db(
        final_df,
        current_pat_client_idcode,
        "ann_epic_clinical_notes_appointments",
        config_obj,
    )

    return final_df


def multi_annots_to_df_epic_imaging_reports(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "document_Content",
    time_column: str = "document_CreatedWhen",
    include_text_sample: bool = False,
    guid_column: str = "id",
) -> pd.DataFrame:
    """Converts MedCAT annotations for Epic imaging reports to a DataFrame and saves it.

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.
        guid_column: The name of the document identifier column in `pat_batch`.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)
    start_time = config_obj.start_time

    pre_document_annotation_batch_path = (
        config_obj.pre_epic_imaging_reports_annotation_batch_path
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_epic_imaging_reports",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []
    for i in range(0, len(pat_batch)):
        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )
        all_annot_dfs.append(
            doc_to_annot_df.dropna(subset=["client_idcode", time_column])
        )

    final_df = (
        pd.concat(all_annot_dfs, ignore_index=True)
        if all_annot_dfs
        else pd.DataFrame(columns=EMPTY_ANNOT_COLS)
    )

    if getattr(config_obj, "storage_backend", "file") == "file":
        os.makedirs(
            os.path.dirname(current_pat_document_annotation_batch_path), exist_ok=True
        )
        final_df.to_csv(current_pat_document_annotation_batch_path, index=False)

    save_annotations_to_db(
        final_df, current_pat_client_idcode, "ann_epic_imaging_reports", config_obj
    )
    return final_df


def multi_annots_to_df_epic_medical_history(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "document_Comment",
    time_column: str = "document_CreatedWhen",
    include_text_sample: bool = False,
    guid_column: str = "id",
) -> pd.DataFrame:
    """Converts MedCAT annotations for Epic medical history to a DataFrame and saves it (file or DB).

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.
        guid_column: The name of the document identifier column in `pat_batch`.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)
    start_time = config_obj.start_time

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_epic_medical_history",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []

    for i in range(0, len(pat_batch)):
        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )
        all_annot_dfs.append(
            doc_to_annot_df.dropna(subset=["client_idcode", time_column])
        )

    final_df = (
        pd.concat(all_annot_dfs, ignore_index=True)
        if all_annot_dfs
        else pd.DataFrame(columns=EMPTY_ANNOT_COLS)
    )

    save_annotations_to_db(
        final_df, current_pat_client_idcode, "ann_epic_medical_history", config_obj
    )

    return final_df


def multi_annots_to_df_mct(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "observation_valuetext_analysed",
    time_column: str = "observationdocument_recordeddtm",
    include_text_sample: bool = False,
    guid_column: str = "observation_guid",
) -> pd.DataFrame:
    """Converts MedCAT annotations for MCT documents to a DataFrame and saves it (file or DB).

    This function processes a list of annotations from MCT documents, converts
    them to a structured DataFrame, optionally joins ICD-10/OPCS-4 codes, and
    saves the result to a patient-specific CSV file or database table.

    Args:
        current_pat_client_idcode: The patient identifier (ID code).
        pat_batch: DataFrame of the original MCT documents that were annotated.
        multi_annots: The list of annotation dictionaries from MedCAT.
        config_obj: The configuration object.
        t: The tqdm progress bar instance to update.
        text_column: The name of the text column in `pat_batch`.
        time_column: The name of the timestamp column in `pat_batch`.
        guid_column: The name of the document identifier column in `pat_batch`.
        include_text_sample: If True, includes a text sample around the annotation.

    Returns:
        pd.DataFrame: The annotated dataframe.

    Raises:
        Exception: Propagates exceptions from database operations or file I/O.
    """
    n_docs_to_annotate = len(pat_batch)

    start_time = config_obj.start_time

    pre_document_annotation_batch_path = (
        config_obj.pre_document_annotation_batch_path_mct
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    update_pbar(
        current_pat_client_idcode,
        start_time,
        5,
        "multi_annots_to_df_mct",
        n_docs_to_annotate=n_docs_to_annotate,
        t=t,
        config_obj=config_obj,
    )

    all_annot_dfs = []

    for i in range(0, len(pat_batch)):

        doc_to_annot_df = json_to_dataframe(
            json_data=multi_annots[i],
            doc=pat_batch.iloc[i],
            current_pat_client_id_code=current_pat_client_idcode,
            text_column=text_column,
            time_column=time_column,
            guid_column=guid_column,
            include_text_sample=include_text_sample,
        )

        # drop nan rows
        # Check for NaN values in any column of the specified list
        col_list_drop_nan = [
            "client_idcode",
            time_column,
        ]

        if config_obj.verbosity >= 3:
            logger.debug(f"multi_annots_to_df: {len(doc_to_annot_df)}")
        rows_with_nan = doc_to_annot_df[
            doc_to_annot_df[col_list_drop_nan].isna().any(axis=1)
        ]

        # Drop rows with NaN values
        doc_to_annot_df = doc_to_annot_df.drop(rows_with_nan.index).copy()
        if config_obj.verbosity >= 3:
            logger.debug(f"multi_annots_to_df: {len(doc_to_annot_df)}")

        all_annot_dfs.append(doc_to_annot_df)

    if all_annot_dfs:
        final_df = pd.concat(all_annot_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=EMPTY_ANNOT_COLS)

    # Apply mappings sequentially to allow multiple sources to contribute codes.
    if config_obj.add_icd10:
        final_df = join_icd10_codes_to_annot(df=final_df, inner=False)
    if config_obj.add_opc4s:
        final_df = join_icd10_OPC4S_codes_to_annot(df=final_df, inner=False)

    if getattr(config_obj, "storage_backend", "file") == "file":
        final_df.to_csv(current_pat_document_annotation_batch_path, index=False)

    save_annotations_to_db(
        final_df, current_pat_client_idcode, "ann_mct_docs", config_obj
    )

    return final_df


def calculate_pretty_name_count_features(
    df_copy: pd.DataFrame, suffix: str = "epr"
) -> Optional[pd.DataFrame]:
    """Calculates count-based features from the 'pretty_name' column.

    This function groups a DataFrame by 'pretty_name' and calculates the count
    for each name, returning the result as a single-row DataFrame (vector).

    Args:
        df_copy: The input DataFrame, expected to have a 'pretty_name' column.
        suffix: A suffix to append to the feature name.

    Returns:
        Optional[pd.DataFrame]: A single-row DataFrame with counts for each
            pretty_name, or None if the input DataFrame is empty.

    Raises:
        Exception: Propagates exceptions from groupby operations.
    """
    if len(df_copy) > 0:
        # Group by 'pretty_name' and calculate counts
        counts = df_copy.groupby("pretty_name").size()

        # Convert to a single-row DataFrame (vector)
        result_vector = counts.to_frame().T

        # Format feature names as 'pretty_name_count_{suffix}_{pretty_name}'
        # This matches the expectation in integration tests.
        result_vector.columns = [
            f"pretty_name_count_{suffix}_{col}" for col in result_vector.columns
        ]

        result_vector.reset_index(drop=True, inplace=True)

        # Convert all values to float
        result_vector = result_vector.astype(float)
    else:
        result_vector = None

    return result_vector
