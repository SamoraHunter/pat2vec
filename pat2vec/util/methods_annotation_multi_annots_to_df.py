from pat2vec.util.methods_annotation_json_to_dataframe import json_to_dataframe
from pat2vec.util.methods_get import update_pbar
from pat2vec.util.post_processing import (
    join_icd10_OPC4S_codes_to_annot,
    join_icd10_codes_to_annot,
)

import pandas as pd
import os
import shutil
import tempfile
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List
from IPython.display import display


@contextmanager
def temporary_file(suffix: str = ".csv", delete: bool = True) -> Iterator[str]:
    """Context manager for creating and cleaning up temporary files.

    Args:
        suffix: The file suffix for the temporary file.
        delete: If True, the file is deleted upon exiting the context.

    Yields:
        The path to the temporary file.
    """
    temp_file = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    temp_file.close()
    try:
        yield temp_file.name
    finally:
        if delete and os.path.exists(temp_file.name):
            os.remove(temp_file.name)


def multi_annots_to_df(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    multi_annots: List[Dict[str, Any]],
    config_obj: Any,
    t: Any,
    text_column: str = "body_analysed",
    time_column: str = "updatetime",
    guid_column: str = "document_guid",
) -> pd.DataFrame:
    """Processes MedCAT annotations for a batch of documents, creating and saving a DataFrame.

    This function takes a list of MedCAT annotation results, corresponding to a
    batch of documents for a single patient. It iterates through each document's
    annotations, converts them from JSON-like dictionary format into a structured
    pandas DataFrame using `json_to_dataframe`, and concatenates them into a
    single master DataFrame for the patient.

    The function can optionally enrich the annotation data by joining it with
    ICD-10 and OPCS-4 codes based on settings in the configuration object.

    Finally, the resulting DataFrame is saved as a CSV file in the patient's
    designated annotation directory.

    Args:
        current_pat_client_idcode: The unique identifier for the patient.
        pat_batch: A DataFrame where each row represents a document
            in the patient's batch.
        multi_annots: A list of dictionaries, where each dictionary contains
            the MedCAT annotation entities for a corresponding document in `pat_batch`.
        config_obj: A configuration object containing settings
            such as file paths (`pre_document_annotation_batch_path`), verbosity
            level, and flags for `add_icd10` and `add_opc4s`. Defaults to None.
        t: A tqdm progress bar object for providing real-time feedback.
        text_column: The name of the column in `pat_batch` that
            contains the document text to be annotated. Defaults to 'body_analysed'.
        time_column: The name of the column in `pat_batch` that
            holds the timestamp for each document. Defaults to 'updatetime'.
        guid_column: The name of the column in `pat_batch` that
            contains the unique identifier for each document. Defaults to 'document_guid'.

    Returns:
        A consolidated DataFrame containing all annotations for the
        patient's document batch. An empty DataFrame is returned if no valid
        annotations are processed.

    Raises:
        ValueError: If `config_obj` is not provided.
    """
    if config_obj is None:
        raise ValueError("config_obj is required")

    processed_dfs = []
    for i in range(len(pat_batch)):
        try:
            doc_to_annot_df = json_to_dataframe(
                json_data=multi_annots[i],
                doc=pat_batch.iloc[i],
                current_pat_client_id_code=current_pat_client_idcode,
                text_column=text_column,
                time_column=time_column,
                guid_column=guid_column,
            )

            if not doc_to_annot_df.empty:
                doc_to_annot_df.dropna(
                    subset=["client_idcode", time_column], inplace=True
                )
                if not doc_to_annot_df.empty:
                    processed_dfs.append(doc_to_annot_df)
        except Exception as e:
            if config_obj.verbosity >= 1:
                print(f"Error processing document {i}: {str(e)}")
            continue

    if processed_dfs:
        final_df = pd.concat(processed_dfs, ignore_index=True)
    else:
        # If no data, create an empty DataFrame with the correct columns
        col_list = [
            "client_idcode",
            time_column,
            "pretty_name",
            "cui",
            "type_ids",
            "types",
            "source_value",
            "detected_name",
            "acc",
            "context_similarity",
            "start",
            "end",
            "icd10",
            "ontologies",
            "snomed",
            "id",
            "Time_Value",
            "Time_Confidence",
            "Presence_Value",
            "Presence_Confidence",
            "Subject_Value",
            "Subject_Confidence",
            "text_sample",
            "full_doc",
            guid_column,
        ]
        final_df = pd.DataFrame(columns=col_list)

    # Handle ICD10 and OPC4S code joining
    try:
        if not final_df.empty:  # Only join if there is data
            if config_obj.add_icd10 and config_obj.add_opc4s:
                final_df = join_icd10_OPC4S_codes_to_annot(df=final_df, inner=False)
            elif config_obj.add_icd10:
                final_df = join_icd10_codes_to_annot(df=final_df, inner=False)
    except Exception as e:
        if config_obj.verbosity >= 1:
            print(f"Error joining ICD10/OPC4S codes: {str(e)}")

    # Write the final DataFrame to CSV. This will now run every time.
    destination_path = os.path.join(
        config_obj.pre_document_annotation_batch_path,
        current_pat_client_idcode + ".csv",
    )

    final_df.to_csv(destination_path, index=False)

    return final_df
