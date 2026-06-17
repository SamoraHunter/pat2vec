import numpy as np
import pandas as pd
import logging
from typing import Any, Dict


def json_to_dataframe(
    json_data: Dict[str, Any],
    doc: pd.Series,
    current_pat_client_id_code: str,
    full_doc: bool = False,
    window: int = 300,
    text_column: str = "body_analysed",
    time_column: str = "updatetime",
    guid_column: str = "document_guid",
    include_text_sample: bool = False,
) -> pd.DataFrame:
    """Converts a MedCAT JSON entity dictionary to a pandas DataFrame.

    This function takes the 'entities' dictionary from a MedCAT output for a
    single document and transforms it into a structured DataFrame. Each row in
    the resulting DataFrame represents a single annotation (entity). It also
    extracts a text sample around the annotation and includes document-level
    metadata.

    Args:
        json_data: The 'entities' dictionary from MedCAT's output.
        doc: The pandas Series representing the original document, containing
            metadata like text, timestamp, and GUID.
        current_pat_client_id_code: The patient's unique identifier.
        full_doc: If True, includes the full document text in the first
            annotation row. Defaults to False.
        window: The number of characters to include on either side of the
            annotation for the 'text_sample'. Defaults to 300.
        text_column: The name of the column in `doc` containing the text.
        time_column: The name of the column in `doc` containing the timestamp.
        guid_column: The name of the column in `doc` containing the document GUID.
        include_text_sample: If True, includes a text sample around the annotation.

    Returns:
        A pandas DataFrame where each row is a single annotation, or an empty
        DataFrame if no entities are present in the input.
    """
    logger = logging.getLogger(__name__)

    if any(json_data.values()):
        done = False

        # Standardize document identifier column name to avoid conflict with MedCAT entity 'id'
        # This is common in Epic tables where the primary key is 'id'.
        target_guid_column = "document_guid" if guid_column == "id" else guid_column

        df_parts = []

        keys = list(json_data["entities"].keys())

        columns = [
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
            target_guid_column,
        ]

        for i in range(0, len(keys)):

            entities_data = json_data["entities"][keys[i]]
            pretty_name = entities_data.get("pretty_name")
            cui = entities_data.get("cui")
            type_ids = entities_data.get("type_ids", [])
            types = entities_data.get("types", [])
            source_value = entities_data.get("source_value")
            detected_name = entities_data.get("detected_name")
            acc = entities_data.get("acc", 1.0)
            context_similarity = entities_data.get("context_similarity", 1.0)
            start = entities_data.get("start", 0)
            end = entities_data.get("end", 0)
            icd10 = entities_data.get("icd10", [])
            ontologies = entities_data.get("ontologies", [])
            snomed = entities_data.get("snomed", [])
            id = entities_data.get("id")
            meta_anns = entities_data.get("meta_anns", {})

            # Parse meta annotations
            parsed_meta_anns = parse_meta_anns(meta_anns)

            mapped_annot_doc_entity = (
                str(doc[text_column]) if pd.notna(doc[text_column]) else ""
            )

            document_len = len(mapped_annot_doc_entity)

            virtual_start = max(0, start - window)

            virtual_end = min(document_len, end + window)

            text_sample_value = np.nan
            if include_text_sample:
                text_sample_value = mapped_annot_doc_entity[virtual_start:virtual_end]

            updatetime_value = doc[time_column]

            document_guid_value = doc[guid_column]

            full_doc_value = np.nan

            if full_doc and not done:
                full_doc_value = mapped_annot_doc_entity
                done = True
            else:
                full_doc_value = np.nan

            # Define DataFrame columns and create the DataFrame

            data = [
                [
                    current_pat_client_id_code,
                    updatetime_value,
                    pretty_name,
                    cui,
                    type_ids,
                    types,
                    source_value,
                    detected_name,
                    acc,
                    context_similarity,
                    start,
                    end,
                    icd10,
                    ontologies,
                    snomed,
                    id,
                    parsed_meta_anns["Time_Value"],
                    parsed_meta_anns["Time_Confidence"],
                    parsed_meta_anns["Presence_Value"],
                    parsed_meta_anns["Presence_Confidence"],
                    parsed_meta_anns["Subject_Value"],
                    parsed_meta_anns["Subject_Confidence"],
                    text_sample_value,
                    full_doc_value,
                    document_guid_value,
                ]
            ]

            df = pd.DataFrame(data, columns=columns)

            df_parts.append(df)

        try:

            super_df = pd.concat(df_parts)
            super_df.reset_index(drop=True, inplace=True)
            return super_df

        except Exception as e:
            logger.error(e)
            logger.error(f"json_date: {json_data}")
            logger.error(f"type(json_data): {type(json_data)}")
            logger.error(f"len(json_data): {len(json_data)}")
            raise e

    else:
        # Standardize document identifier column name for empty DataFrames
        target_guid_column = "document_guid" if guid_column == "id" else guid_column

        columns = [
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
            target_guid_column,
        ]

        empty_df = pd.DataFrame(data=None, columns=columns)
        return empty_df


def parse_meta_anns(meta_anns: Dict[str, Any]) -> Dict[str, Any]:
    """Parses meta-annotations from a MedCAT entity dictionary.

    This function extracts the value and confidence for 'Time', 'Presence',
    and 'Subject/Experiencer' meta-annotations. It includes a fallback to
    check for 'Subject' if 'Subject/Experiencer' is not found.

    Args:
        meta_anns: The meta_anns dictionary from a MedCAT entity.

    Returns:
        A dictionary containing the parsed meta-annotation values and confidences.
    """
    time_value = meta_anns.get("Time", {}).get("value")
    time_confidence = meta_anns.get("Time", {}).get("confidence")

    presence_value = meta_anns.get("Presence", {}).get("value")
    presence_confidence = meta_anns.get("Presence", {}).get("confidence")

    subject_value = meta_anns.get("Subject/Experiencer", {}).get("value")
    subject_confidence = meta_anns.get("Subject/Experiencer", {}).get("confidence")

    # If 'Subject/Experiencer' is not found, fallback to 'Subject'
    if subject_value is None:
        subject_value = meta_anns.get("Subject", {}).get("value")
        subject_confidence = meta_anns.get("Subject", {}).get("confidence")

    return {
        "Time_Value": time_value,
        "Time_Confidence": time_confidence,
        "Presence_Value": presence_value,
        "Presence_Confidence": presence_confidence,
        "Subject_Value": subject_value,
        "Subject_Confidence": subject_confidence,
    }
