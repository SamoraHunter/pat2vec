import os
import gc
from pathlib import Path
import logging
from typing import Any, List, Optional, Callable, Tuple, Union
import pandas as pd
from tqdm import tqdm

from pat2vec.util.post_processing import retrieve_pat_annots_mct_epr, EMPTY_ANNOT_COLS  # type: ignore
from pat2vec.util.helper_functions import get_df_from_db, get_ram_usage

logger = logging.getLogger(__name__)


def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Downcasts numeric columns to save memory."""
    for col in df.columns:
        if pd.api.types.is_float_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast="float")
        elif pd.api.types.is_integer_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast="integer")
    return df


def _generic_merged_builder(
    all_pat_list: List[str],
    config_obj: Any,
    output_filename: str,
    standard_cols: List[str],
    db_sources: List[Tuple[str, str, str]],  # (schema, table, source_name_or_none)
    file_retriever: Callable[[str, Any], pd.DataFrame],
    overwrite: bool = False,
    patient_id_col: str = "client_idcode",
    post_chunk_processor: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    float_format: Optional[str] = None,
    chunk_size: int = 500,
    fetch_cols: Optional[List[str]] = None,
) -> str:
    """Internal helper to build merged dataframes in chunks to prevent RAM overloading."""
    # Aggressively clear memory from previous builds (like docs) before starting
    gc.collect()

    # Project path consistency: root / [proj_name] / merged_batches
    # config_obj.root_path is already expected to be the project-specific root (e.g., .../HFE_13_2026/new_project/)
    directory_path = os.path.join(config_obj.root_path, "merged_batches")
    output_file_path = os.path.join(directory_path, output_filename)
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    if not overwrite and os.path.isfile(output_file_path):
        logger.info(f"File already exists at {output_file_path}. Skipping.")
        return output_file_path

    if not all_pat_list:
        logger.warning(
            f"No patients provided for {output_filename}. Creating empty file."
        )
        pd.DataFrame(columns=standard_cols).to_csv(output_file_path, index=False)
        return output_file_path

    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    first_write = True
    any_data_found = False

    logger.info(
        f"GENERIC BUILDER START: {output_filename}. "
        f"Processing {len(all_pat_list)} patient(s) with chunk_size={chunk_size}."
    )

    def get_data_stream():
        nonlocal any_data_found

        for i in range(0, len(all_pat_list), chunk_size):
            chunk_pats = all_pat_list[i : i + chunk_size]

            if config_obj.storage_backend == "database" and db_sources:
                # KEY FIX: one query per (table, chunk) instead of per (table, patient)
                for schema, table, source_name in db_sources:
                    df = get_df_from_db(
                        config_obj,
                        schema,
                        table,
                        patient_ids=chunk_pats,  # <-- whole chunk at once
                        patient_id_column=patient_id_col,
                        columns=fetch_cols,
                    )

                    if df.empty:
                        continue

                    any_data_found = True

                    source_col = None
                    if source_name:
                        source_col = (
                            "annotation_batch_source"
                            if "ann_" in table
                            else "document_batch_source"
                        )
                        df[source_col] = source_name

                    if standard_cols:
                        df = df[
                            [
                                c
                                for c in df.columns
                                if c in standard_cols or c == source_col
                            ]
                        ]

                    if post_chunk_processor:
                        df = post_chunk_processor(df)

                    df = optimize_dtypes(df)

                    if standard_cols:
                        # Ensure consistency for append mode
                        df = df.reindex(columns=standard_cols)

                    yield df
                    del df
                    # Aggressive cleanup after each table in the chunk to mitigate RAM doubling
                    gc.collect()

                if get_ram_usage() > 4.0:  # type: ignore
                    gc.collect()

            else:
                # File-based path: aggregate DataFrames per chunk to significantly reduce disk I/O overhead
                chunk_dfs = []
                for pat_id in chunk_pats:
                    df = file_retriever(pat_id, config_obj)
                    if df.empty:
                        continue

                    any_data_found = True

                    if "Unnamed: 0" in df.columns:
                        df = df.drop("Unnamed: 0", axis=1)

                    if post_chunk_processor:
                        df = post_chunk_processor(df)

                    chunk_dfs.append(df)

                if chunk_dfs:
                    combined_df = pd.concat(chunk_dfs, ignore_index=True)
                    combined_df = optimize_dtypes(combined_df)
                    if standard_cols:
                        # Ensure consistency for append mode
                        combined_df = combined_df.reindex(columns=standard_cols)

                    yield combined_df
                    del combined_df
                    del chunk_dfs
                    gc.collect()

    # --- Write loop ---
    n_chunks = (len(all_pat_list) + chunk_size - 1) // chunk_size
    pbar = tqdm(total=n_chunks, desc=f"Merging {output_filename}", unit="chunk")

    yields_per_chunk = (
        len(db_sources)
        if (config_obj.storage_backend == "database" and db_sources)
        else 1
    )
    yield_count = 0

    for df_chunk in get_data_stream():
        if not df_chunk.empty:
            df_chunk.to_csv(
                output_file_path,
                index=False,
                mode="a",
                header=first_write,
                float_format=float_format,
            )
            first_write = False

        yield_count += 1
        if yield_count % yields_per_chunk == 0:
            pbar.update(1)

        del df_chunk
        gc.collect()

    if pbar.n < n_chunks:
        pbar.update(n_chunks - pbar.n)
    pbar.close()

    if not any_data_found:
        logger.warning(
            f"No data found for {output_filename}. Creating empty file with headers."
        )
        pd.DataFrame(columns=standard_cols).to_csv(output_file_path, index=False)

    return output_file_path


# Example usage:
# Assuming df and annot_filter_arguments are defined with appropriate values
# filtered_df = filter_annot_dataframe(df, annot_filter_arguments)


def build_merged_epr_mct_annot_df(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> Optional[str]:
    """Builds a merged DataFrame of annotations from EPR and MCT sources (file or DB)."""

    # Sanitise all_pat_list using dict.fromkeys (faster and more memory efficient than set for large lists)
    all_pat_list = list(dict.fromkeys(str(p) for p in all_pat_list if p is not None))
    gc.collect()

    def annot_processor(df: pd.DataFrame) -> pd.DataFrame:
        mappings = {
            "updatetime": [
                "observationannotation_recordeddtm",
                "basicobs_entered",
                "observationdocument_recordeddtm",
            ],
            "document_guid": ["basicobs_guid", "observation_guid"],
            "annotation_description": ["obscatalogmasteritem_displayname"],
        }
        for target, sources in mappings.items():
            if target not in df.columns:
                df[target] = pd.NA
            for src in sources:
                if src not in df.columns:
                    continue
                mask = df[target].isna()
                if not mask.any():
                    break
                df.loc[mask, target] = df.loc[mask, src].values

        if (
            "observationannotation_recordeddtm" in df.columns
            and "updatetime" in df.columns
        ):
            mask = df["observationannotation_recordeddtm"].isna()
            if mask.any():
                df.loc[mask, "observationannotation_recordeddtm"] = df.loc[
                    mask, "updatetime"
                ].values

        # Aggressive RAM safety: Drop huge text blobs if they accidentally leaked in
        text_cols = [
            "body_analysed",
            "textualObs",
            "observation_valuetext_analysed",
            "text_sample",
        ]
        # Preserve text_sample if explicitly requested by configuration
        if config_obj.include_text_sample_in_annots:
            text_cols.remove("text_sample")

        found_text = [c for c in text_cols if c in df.columns]
        if found_text:
            df.drop(columns=found_text, inplace=True)

        # Drop source columns not in final output schema to save memory
        allowed_cols = EMPTY_ANNOT_COLS + (
            ["text_sample"] if config_obj.include_text_sample_in_annots else []
        )
        cols_to_drop = [c for c in df.columns if c not in allowed_cols]
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)
        df = optimize_dtypes(df)
        return df

    # Determine columns to fetch from DB: standard cols + any mapping sources
    # We explicitly exclude the massive text blobs (body_analysed, textualObs, etc.)
    source_metadata_cols = [
        "observationannotation_recordeddtm",
        "basicobs_entered",
        "observationdocument_recordeddtm",
        "basicobs_guid",
        "observation_guid",
        "obscatalogmasteritem_displayname",
    ]

    # Conditionally include 'text_sample' based on config_obj
    conditional_cols = (
        ["text_sample"] if config_obj.include_text_sample_in_annots else []
    )
    fetch_list = list(set(EMPTY_ANNOT_COLS + source_metadata_cols + conditional_cols))
    output_schema = EMPTY_ANNOT_COLS + conditional_cols

    merged_path = _generic_merged_builder(
        all_pat_list=all_pat_list,
        config_obj=config_obj,
        output_filename="annots_mct_epr.csv",
        standard_cols=output_schema,
        db_sources=[
            ("annotations", "ann_epr_docs", "epr"),
            ("annotations", "ann_mct_docs", "mct"),
            ("annotations", "ann_textual_obs", "textual_obs"),
            ("annotations", "ann_reports", "report"),
        ],
        file_retriever=lambda p, c: retrieve_pat_annots_mct_epr(  # type: ignore
            p,
            c,
            # Ensure we don't accidentally load text blobs even in file mode
            columns_epr=[
                c
                for c in fetch_list
                if c
                not in ["body_analysed", "textualObs", "observation_valuetext_analysed"]
            ],
            columns_mct=[
                c
                for c in fetch_list
                if c
                not in ["body_analysed", "textualObs", "observation_valuetext_analysed"]
            ],
            columns_to=[
                c
                for c in fetch_list
                if c
                not in ["body_analysed", "textualObs", "observation_valuetext_analysed"]
            ],
            columns_report=fetch_list,
            merge_columns=False,  # Let annot_processor handle merging for the whole chunk
        ),
        overwrite=overwrite,
        post_chunk_processor=annot_processor,
        chunk_size=500,
        fetch_cols=fetch_list,
    )
    gc.collect()
    return merged_path


def load_merged_epr_mct_annots(
    config_obj: Any, all_pat_list: List[str], nrows: Optional[int] = None
) -> Union[str, pd.DataFrame]:
    """Loads merged EPR and MCT annotations.

    If nrows is specified, returns a DataFrame containing that many rows for inspection.
    Otherwise, returns the path to the CSV file.

    DO NOT load the entire resulting file into a single DataFrame if it is large.
    """
    directory_path = os.path.join(config_obj.root_path, "merged_batches")
    output_file_path = os.path.join(directory_path, "annots_mct_epr.csv")

    # If the user only wants a preview and the file isn't built yet, don't build it.
    # Just fetch from storage directly for a few patients.
    if nrows is not None and not os.path.exists(output_file_path):
        sample_dfs = []
        current_count = 0
        # Safely convert to string list to handle potential numeric ID lists
        all_pat_list = [str(p) for p in all_pat_list]

        for pat_id in all_pat_list:
            df = retrieve_pat_annots_mct_epr(str(pat_id), config_obj)
            if not df.empty:
                sample_dfs.append(df)
                current_count += len(df)
            if current_count >= nrows:
                break
        if not sample_dfs:
            return pd.DataFrame()
        return pd.concat(sample_dfs, ignore_index=True).head(nrows)

    merged_path = build_merged_epr_mct_annot_df(all_pat_list, config_obj)
    if merged_path and os.path.isfile(merged_path):
        if nrows is not None:
            return pd.read_csv(merged_path, nrows=nrows)
        return merged_path
    return pd.DataFrame() if nrows is not None else ""


def build_merged_bloods(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Builds a merged CSV file of bloods data from patient batch files or database."""
    return _generic_merged_builder(
        all_pat_list=all_pat_list,
        config_obj=config_obj,
        output_filename="bloods_batches.csv",
        standard_cols=[
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_numeric",
            "basicobs_entered",
            "clientvisit_serviceguid",
            "updatetime",
        ],
        db_sources=[("raw_data", "raw_bloods", None)],
        file_retriever=retrieve_pat_bloods,
        overwrite=overwrite,
        float_format="%.6f",
    )


def build_merged_epr_mct_doc_df(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Builds a merged CSV of documents from EPR and MCT sources (file or DB)."""

    def doc_processor(df: pd.DataFrame) -> pd.DataFrame:
        for col1, col2 in [
            ("updatetime", "observationdocument_recordeddtm"),
            ("updatetime", "basicobs_entered"),
            ("observationdocument_recordeddtm", "updatetime"),
            ("document_guid", "observation_guid"),
            ("document_guid", "basicobs_guid"),
            ("document_description", "obscatalogmasteritem_displayname"),
            ("document_description", "basicobs_itemname_analysed"),
            ("body_analysed", "observation_valuetext_analysed"),
            ("body_analysed", "textualObs"),
        ]:
            if col1 in df.columns and col2 in df.columns:
                df[col1] = df[col1].fillna(df[col2])
        df = optimize_dtypes(df)
        return df

    DOC_STANDARD_COLS = [
        "client_idcode",
        "document_guid",
        "document_description",
        "body_analysed",
        "updatetime",
        "clientvisit_visitidcode",
        "document_batch_source",
        "observationdocument_recordeddtm",
        "obscatalogmasteritem_displayname",
        "observation_valuetext_analysed",
        "basicobs_entered",
        "basicobs_itemname_analysed",
        "textualObs",
        "basicobs_guid",
    ]

    merged_path = _generic_merged_builder(
        all_pat_list=all_pat_list,
        config_obj=config_obj,
        output_filename="docs_mct_epr.csv",
        standard_cols=DOC_STANDARD_COLS,
        db_sources=[
            ("raw_data", "raw_epr_docs", "epr"),
            ("raw_data", "raw_mct_docs", "mct"),
            ("raw_data", "raw_textual_obs", "textual_obs"),
            ("raw_data", "raw_reports", "report"),
        ],
        file_retriever=lambda p, c: retrieve_pat_docs_mct_epr(
            p,
            c,
            columns_epr=DOC_STANDARD_COLS,
            columns_mct=DOC_STANDARD_COLS,
            columns_to=DOC_STANDARD_COLS,
            columns_report=DOC_STANDARD_COLS,
            merge_columns=False,  # Let doc_processor handle merging for the whole chunk
        ),
        overwrite=overwrite,
        post_chunk_processor=doc_processor,
        chunk_size=1,  # Documents contain large text; process one patient at a time
        fetch_cols=DOC_STANDARD_COLS,
    )
    gc.collect()
    return merged_path


def retrieve_pat_bloods(client_idcode: str, config_obj: Any) -> pd.DataFrame:
    """Retrieve bloods data for the given client_idcode (from file or DB).

    Args:
        client_idcode: Unique identifier for the patient.
        config_obj: Configuration object containing storage backend settings.

    Returns:
        Bloods data for the given client_idcode, or an empty DataFrame if not found.
    """
    if config_obj.storage_backend == "database":
        return get_df_from_db(
            config_obj, "raw_data", "raw_bloods", patient_ids=[client_idcode]
        )

    pre_bloods_batch_path = config_obj.pre_bloods_batch_path

    pat_bloods_path = f"{pre_bloods_batch_path}/{client_idcode}.csv"

    try:
        pat_bloods = pd.read_csv(pat_bloods_path)
    except Exception as e:
        logger.error(e)
        pat_bloods = pd.DataFrame()

    return pat_bloods


def retrieve_pat_epr_docs(client_idcode: str, config_obj: Any) -> pd.DataFrame:
    """Retrieve EPR documents data for the given client_idcode (from file or DB).

    Args:
        client_idcode: Unique identifier for the patient.
        config_obj: Configuration object containing storage backend settings.

    Returns:
        EPR documents data for the given client_idcode, or an empty DataFrame if not found.
    """
    if config_obj.storage_backend == "database":
        return get_df_from_db(
            config_obj, "raw_data", "raw_epr_docs", patient_ids=[client_idcode]
        )

    pre_document_batch_path = config_obj.pre_document_batch_path
    pat_docs_path = f"{pre_document_batch_path}/{client_idcode}.csv"

    try:
        pat_docs = pd.read_csv(pat_docs_path)
    except Exception as e:
        logger.error(e)
        pat_docs = pd.DataFrame()

    return pat_docs


def retrieve_pat_docs_mct_epr(
    client_idcode: str,
    config_obj: Any,
    columns_epr: Optional[List[str]] = None,
    columns_mct: Optional[List[str]] = None,
    columns_to: Optional[List[str]] = None,
    columns_report: Optional[List[str]] = None,
    merge_columns: bool = True,
) -> pd.DataFrame:
    """Retrieves and merges document data for a patient from multiple sources (file or DB).

    This function reads document data for a specified patient from four potential
    sources: EPR documents, MCT documents, textual observations, and reports.
    It loads the corresponding data, optionally selecting specific columns,
    and concatenates them into a single DataFrame. It can also merge related

    columns (like timestamps and content) to create a more unified dataset.

    Args:
        client_idcode: The unique identifier for the patient.
        config_obj: A configuration object containing paths to document batches.
        columns_epr: A list of columns to load from the EPR documents CSV.
        columns_mct: A list of columns to load from the MCT documents CSV.
        columns_to: A list of columns to load from the textual observations CSV.
        columns_report: A list of columns to load from the reports CSV.
        merge_columns: If True, attempts to merge corresponding columns
            (e.g., timestamps, content) from the different sources into a
            unified set of columns.

    Returns:
        A DataFrame containing the concatenated and optionally
                      merged document data for the patient. Returns an empty
                      DataFrame if no data is found for the patient in any
                      of the sources.
    """
    if config_obj.storage_backend == "database":
        dfs = []

        sources = {
            "raw_epr_docs": ("epr", columns_epr),
            "raw_mct_docs": ("mct", columns_mct),
            "raw_textual_obs": ("textual_obs", columns_to),
            "raw_reports": ("report", columns_report),
        }

        for table, (source_name, cols) in sources.items():
            try:
                df_temp = get_df_from_db(
                    config_obj,
                    schema="raw_data",
                    table=table,
                    patient_ids=[client_idcode],
                    columns=cols,
                )
                if not df_temp.empty:
                    # Ensure the columns we want to use exist before assignment
                    if cols:
                        existing_cols = [c for c in cols if c in df_temp.columns]
                        df_temp = df_temp[existing_cols]
                    df_temp["document_batch_source"] = source_name
                    dfs.append(df_temp)
            except Exception as e:
                # get_df_from_db already logs errors, but we can add context
                logger.warning(
                    f"Could not retrieve {source_name} for {client_idcode}: {e}"
                )

        if not dfs:
            return pd.DataFrame()

        all_docs = pd.concat(dfs, ignore_index=True)
    else:
        pre_document_batch_path = config_obj.pre_document_batch_path
        pre_document_batch_path_mct = config_obj.pre_document_batch_path_mct
        pre_textual_obs_document_batch_path = (
            config_obj.pre_textual_obs_document_batch_path
        )
        pre_document_batch_path_reports = config_obj.pre_document_batch_path_reports

        epr_file_path = f"{pre_document_batch_path}/{client_idcode}.csv"
        mct_file_path = f"{pre_document_batch_path_mct}/{client_idcode}.csv"
        textual_obs_files_path = (
            f"{pre_textual_obs_document_batch_path}/{client_idcode}.csv"
        )
        report_file_path = f"{pre_document_batch_path_reports}/{client_idcode}.csv"

        dfs = []
        if os.path.exists(epr_file_path):
            dfa = pd.read_csv(epr_file_path, usecols=columns_epr)
            dfa["document_batch_source"] = "epr"
            dfs.append(dfa)

        if os.path.exists(mct_file_path):
            dfa_mct = pd.read_csv(mct_file_path, usecols=columns_mct)
            dfa_mct["document_batch_source"] = "mct"
            dfs.append(dfa_mct)

        if os.path.exists(textual_obs_files_path):
            dfa_to = pd.read_csv(textual_obs_files_path, usecols=columns_to)
            dfa_to["document_batch_source"] = "textual_obs"
            dfs.append(dfa_to)

        if os.path.exists(report_file_path):
            dfr = pd.read_csv(report_file_path, usecols=columns_report)
            dfr["document_batch_source"] = "report"
            dfs.append(dfr)

        if not dfs:
            return pd.DataFrame()

        all_docs = pd.concat(dfs, ignore_index=True)

    if merge_columns and not all_docs.empty:

        for col1, col2 in [
            ("updatetime", "observationdocument_recordeddtm"),
            ("observationdocument_recordeddtm", "updatetime"),
            ("document_guid", "observation_guid"),
            ("document_description", "obscatalogmasteritem_displayname"),
            ("body_analysed", "observation_valuetext_analysed"),
        ]:
            if col1 in all_docs.columns and col2 in all_docs.columns:
                all_docs[col1] = all_docs[col1].fillna(all_docs[col2])

    return all_docs.reset_index(drop=True)


def join_docs_to_annots(
    annots_df: pd.DataFrame, docs_temp: pd.DataFrame, drop_duplicates: bool = True
) -> pd.DataFrame:
    """Merge two DataFrames based on the 'document_guid' column.

    Args:
        annots_df: The DataFrame containing annotations.
        docs_temp: The DataFrame containing documents.
        drop_duplicates: If True, drops duplicated columns from `docs_temp`
            before merging.

    Returns:
        A merged DataFrame.
    """

    if drop_duplicates:
        # Get the sets of column names
        annots_columns_set = set(annots_df.columns)
        docs_columns_set = set(docs_temp.columns)

        # Identify duplicated column names
        duplicated_columns = annots_columns_set.intersection(docs_columns_set)
        # Assuming 'document_guid' is a unique identifier
        duplicated_columns.remove("document_guid")
        if duplicated_columns:
            logger.info(f"Duplicated columns found: {duplicated_columns}")
            # Drop duplicated columns from docs_temp
            docs_temp_dropped = docs_temp.drop(columns=duplicated_columns)
        else:
            docs_temp_dropped = docs_temp

    # Merge the DataFrames on 'document_guid' column
    merged_df = pd.merge(annots_df, docs_temp_dropped, on="document_guid", how="left")

    return merged_df


def get_annots_joined_to_docs(
    config_obj: Any, pat2vec_obj: Any, nrows: Optional[int] = None
) -> Union[str, pd.DataFrame]:
    """Builds and merges document and annotation dataframes, then joins them.

    Returns the path to the joined CSV file, or a sampled DataFrame if nrows is set.
    This function processes data in small patient-level batches to avoid RAM spikes.
    """

    pre_path = os.path.join(config_obj.root_path, config_obj.proj_name)
    filename = "annots_joined_docs_full.csv"
    output_path = os.path.join(pre_path, "merged_batches", filename)

    logger.info("Building joined annotations and documents incrementally...")

    # Internal retriever that performs the join per patient
    def joined_retriever(pat_id: str, cfg: Any) -> pd.DataFrame:
        # Restrict columns during join to avoid loading unused text into RAM
        annots = retrieve_pat_annots_mct_epr(
            pat_id,
            cfg,
            columns_epr=EMPTY_ANNOT_COLS,
            columns_mct=EMPTY_ANNOT_COLS,
            columns_to=EMPTY_ANNOT_COLS,
            columns_report=EMPTY_ANNOT_COLS,
        )
        if annots.empty:
            return pd.DataFrame()

        docs = retrieve_pat_docs_mct_epr(
            pat_id,
            cfg,
            columns_epr=DOC_STANDARD_COLS,
            columns_mct=DOC_STANDARD_COLS,
            columns_to=DOC_STANDARD_COLS,
            columns_report=DOC_STANDARD_COLS,
        )
        if docs.empty:
            return annots

        return join_docs_to_annots(annots, docs, drop_duplicates=True)

    # Union of standard columns for the final joined output
    DOC_STANDARD_COLS = [
        "client_idcode",
        "document_guid",
        "document_description",
        "body_analysed",
        "updatetime",
        "clientvisit_visitidcode",
        "document_batch_source",
        "observationdocument_recordeddtm",
        "obscatalogmasteritem_displayname",
        "observation_valuetext_analysed",
        "basicobs_entered",
        "basicobs_itemname_analysed",
        "textualObs",
        "basicobs_guid",
    ]
    JOINED_COLS = list(dict.fromkeys(EMPTY_ANNOT_COLS + DOC_STANDARD_COLS))

    # We use _generic_merged_builder which handles the chunked appending to disk.
    # No DB sources needed here because retrieve_pat_* methods handle DB logic internally.
    final_path = _generic_merged_builder(
        all_pat_list=pat2vec_obj.all_patient_list,
        config_obj=config_obj,
        output_filename=filename,
        standard_cols=JOINED_COLS,
        db_sources=[],
        file_retriever=joined_retriever,
        overwrite=True,
        chunk_size=1,  # One patient at a time to keep RAM flat
    )

    logger.info(f"Join complete. Data saved to {final_path}")
    if nrows is not None:
        return pd.read_csv(final_path, nrows=nrows)

    return final_path


def merge_demographics_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all demographics data (files or DB) that match the patient list."""
    return _generic_merged_builder(
        all_pat_list=all_pat_list,
        config_obj=config_obj,
        output_filename="merged_demographics.csv",
        standard_cols=[
            "client_idcode",
            "client_firstname",
            "client_lastname",
            "client_dob",
            "client_gendercode",
            "client_racecode",
            "client_deceaseddtm",
            "updatetime",
        ],
        db_sources=[("raw_data", "raw_demographics", None)],
        file_retriever=lambda p, c: (
            pd.read_csv(os.path.join(c.pre_demo_batch_path, f"{p}.csv"))
            if os.path.isfile(os.path.join(c.pre_demo_batch_path, f"{p}.csv"))
            else pd.DataFrame()
        ),
        overwrite=overwrite,
    )


def merge_bmi_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all BMI data (files or DB) that match the patient list."""
    return _generic_merged_builder(
        all_pat_list=all_pat_list,
        config_obj=config_obj,
        output_filename="merged_bmi.csv",
        standard_cols=[
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
        ],
        db_sources=[("raw_data", "raw_bmi", None)],
        file_retriever=lambda p, c: (
            pd.read_csv(os.path.join(c.pre_bmi_batch_path, f"{p}.csv"))
            if os.path.isfile(os.path.join(c.pre_bmi_batch_path, f"{p}.csv"))
            else pd.DataFrame()
        ),
        overwrite=overwrite,
    )


def merge_news_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all NEWS data (files or DB) that match the patient list."""
    return _generic_merged_builder(
        all_pat_list=all_pat_list,
        config_obj=config_obj,
        output_filename="merged_news.csv",
        standard_cols=[
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
        ],
        db_sources=[("raw_data", "raw_news", None)],
        file_retriever=lambda p, c: (
            pd.read_csv(os.path.join(c.pre_news_batch_path, f"{p}.csv"))
            if os.path.isfile(os.path.join(c.pre_news_batch_path, f"{p}.csv"))
            else pd.DataFrame()
        ),
        overwrite=overwrite,
    )


def merge_diagnostics_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all diagnostics data (files or DB) that match the patient list."""
    return _generic_merged_builder(
        all_pat_list=all_pat_list,
        config_obj=config_obj,
        output_filename="merged_diagnostics.csv",
        standard_cols=[
            "client_idcode",
            "order_guid",
            "order_name",
            "order_summaryline",
            "order_holdreasontext",
            "order_entered",
            "clientvisit_visitidcode",
            "order_performeddtm",
            "order_createdwhen",
        ],
        db_sources=[("raw_data", "raw_diagnostics", None)],
        file_retriever=lambda p, c: (
            pd.read_csv(os.path.join(c.pre_diagnostics_batch_path, f"{p}.csv"))
            if os.path.isfile(os.path.join(c.pre_diagnostics_batch_path, f"{p}.csv"))
            else pd.DataFrame()
        ),
        overwrite=overwrite,
    )


def merge_drugs_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all drugs data (files or DB) that match the patient list."""
    return _generic_merged_builder(
        all_pat_list=all_pat_list,
        config_obj=config_obj,
        output_filename="merged_drugs.csv",
        standard_cols=[
            "client_idcode",
            "order_guid",
            "order_name",
            "order_summaryline",
            "order_holdreasontext",
            "order_entered",
            "clientvisit_visitidcode",
            "order_performeddtm",
            "order_createdwhen",
        ],
        db_sources=[("raw_data", "raw_drugs", None)],
        file_retriever=lambda p, c: (
            pd.read_csv(os.path.join(c.pre_drugs_batch_path, f"{p}.csv"))
            if os.path.isfile(os.path.join(c.pre_drugs_batch_path, f"{p}.csv"))
            else pd.DataFrame()
        ),
        overwrite=overwrite,
    )


def merge_appointments_csv(
    all_pat_list: List[str], config_obj: Any, overwrite: bool = False
) -> str:
    """Merge all appointments data (files or DB) that match the patient list."""
    return _generic_merged_builder(
        all_pat_list=all_pat_list,
        config_obj=config_obj,
        output_filename="merged_appointments.csv",
        standard_cols=[],  # Appointments doesn't have a hardcoded empty header list in existing code
        db_sources=[("raw_data", "raw_appointments", None)],
        file_retriever=lambda p, c: (
            pd.read_csv(os.path.join(c.pre_appointments_batch_path, f"{p}.csv"))
            if os.path.isfile(os.path.join(c.pre_appointments_batch_path, f"{p}.csv"))
            else pd.DataFrame()
        ),
        overwrite=overwrite,
        patient_id_col="HospitalID",
    )
