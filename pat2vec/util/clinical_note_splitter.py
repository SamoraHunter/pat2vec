from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import regex
from pandas import Timestamp
import logging

logger = logging.getLogger(__name__)


def find_date(
    txt: str,
    original_update_time_value: Optional[Timestamp] = None,
    reg: str = r"Entered on -",
    window: int = 50,
    verbosity: int = 0,
) -> List[Dict[str, Any]]:
    """Finds and extracts date-stamped text chunks from a larger text body.

    This function scans through a given text for a specific regular expression
    pattern that indicates a date entry (e.g., "Entered on -"). For each match,
    it attempts to parse a timestamp from the subsequent text. It then splits
    the original text into chunks, with each chunk ending at a newly found
    timestamp.

    Args:
        txt: The input text to search for date entries.
        original_update_time_value: A fallback timestamp to use if a date cannot be parsed from a chunk.
        reg: The regular expression pattern to identify the start of a date entry.
        window: The character window size after the `reg` match to search for a timestamp.
        verbosity: The level of logging for messages.

    Returns:
        A list of dictionaries, where each dictionary represents a text chunk
        and contains the text, the parsed date, and metadata about the match.
    """

    m = regex.finditer(reg, txt)
    chunks: List[Dict[str, Any]] = []

    # Store all found date entry points and their parsed dates
    date_entries: List[Tuple[int, int, pd.Timestamp]] = []

    for match_reg in m:
        # The actual date string is expected right after the 'reg' match
        date_window_start_idx = match_reg.span()[1]
        date_window_end_idx = date_window_start_idx + window
        date_text_window = txt[date_window_start_idx:date_window_end_idx].strip()

        # Regex to capture DD-Mon-YYYY HH:MM or YYYY-MM-DD HH:MM:SS
        ts_match = regex.search(
            r"(\d{1,2}-[A-Za-z]{3}-\d{4}|\d{4}-\d{2}-\d{2})\s*\d{1,2}:\d{2}(?::\d{2})?",
            date_text_window,
        )

        if ts_match:
            date_str = ts_match.group(0)
            try:
                parsed_date = pd.to_datetime(date_str)
                end_of_date_string_in_text = date_window_start_idx + ts_match.end()
                date_entries.append(
                    (match_reg.span()[0], end_of_date_string_in_text, parsed_date)
                )
            except Exception as e:
                if verbosity > 1:
                    logger.debug(f"Could not parse date '{date_str}': {e}")
        elif verbosity > 1:
            logger.debug(f"No timestamp found in '{date_text_window}'.")

    if not date_entries:
        return [
            {
                "text": txt,
                "date": original_update_time_value,
                "date_found": False,
                "text_start": 0,
                "text_end": len(txt),
            }
        ]

    # Handle text before the first date entry
    if date_entries[0][0] > 0:
        chunks.append(
            {
                "text": txt[0 : date_entries[0][0]],
                "date": original_update_time_value,
                "date_found": False,
                "text_start": 0,
                "text_end": date_entries[0][0],
            }
        )

    for i in range(len(date_entries)):
        # Start index is the beginning of the marker (e.g., "Entered on -")
        start_idx = date_entries[i][0]
        # End index is the beginning of the next entry, or end of string
        end_idx = date_entries[i + 1][0] if i < len(date_entries) - 1 else len(txt)

        chunks.append(
            {
                "text": txt[start_idx:end_idx],
                "date": date_entries[i][2],
                "date_found": True,
                "text_start": start_idx,
                "text_end": end_idx,
            }
        )

    return chunks


def split_clinical_notes(
    clin_note: pd.DataFrame, verbosity_val: int = 0
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Splits clinical notes from an EPR schema DataFrame into date-stamped chunks.

    This function iterates through a DataFrame of clinical notes (assuming an
    EPR-like schema with 'body_analysed' and 'updatetime' columns). It uses
    the `find_date` function to break down each note's text into smaller
    documents based on embedded timestamps.

    Args:
        clin_note: A DataFrame containing the clinical notes to be split.
        verbosity_val: The verbosity level passed to the `find_date` function.

    Returns:
        A tuple containing two DataFrames:
        - pd.DataFrame: The processed notes, split into smaller chunks.
        - pd.DataFrame: The original rows of notes that could not be split.
    """

    extracted = []
    none_found = []
    document_description_list = []
    id_list = []
    document_guid_list = []
    clientvisit_visitidcode_list = []
    index_list = []
    none_rows = []

    for index, row in clin_note.iterrows():
        d = row["body_analysed"]
        ch = []
        try:
            if d:  # Only try to find dates if there's text
                ch = find_date(
                    d,
                    original_update_time_value=row["updatetime"],
                    verbosity=verbosity_val,
                )
                row_id = row.get("id", row.get("_id", "unknown"))
                extracted.append(
                    {"id": row_id, "client_idcode": row["client_idcode"], "chunks": ch}
                )

                document_description_list.append(
                    row.get("document_description", "Unknown")
                )
                id_list.append(row_id)
                document_guid_list.append(row.get("document_guid", "Unknown"))
                clientvisit_visitidcode_list.append(
                    row.get("clientvisit_visitidcode", "Unknown")
                )
                index_list.append(row.get("_index", "Unknown"))
        except Exception:
            ch = []

        if len(ch) == 0:
            none_found.append(d)
            none_rows.append(row)

    new_docs = []
    counter_1 = 0
    for ex in extracted:
        counter = 0
        for ch in ex["chunks"]:
            nd = {
                "client_idcode": ex["client_idcode"],
                "body_analysed": ch["text"],
                "updatetime": ch["date"],
            }
            nd["document_description"] = (
                f"{document_description_list[counter_1]}_clinical note chunk_{counter}"
            )
            nd["_id"] = id_list[counter_1]
            nd["document_guid"] = document_guid_list[counter_1]
            nd["clientvisit_visitidcode"] = clientvisit_visitidcode_list[counter_1]
            nd["_index"] = index_list[counter_1]

            nd["source_file"] = ex["id"]
            new_docs.append(nd)
            counter += 1
        counter_1 += 1
    processed = (
        pd.DataFrame(new_docs).assign(
            updatetime=lambda x: pd.to_datetime(x["updatetime"])
        )
        if new_docs
        else pd.DataFrame()
    )
    none_rows = pd.DataFrame(none_rows)
    return processed, none_rows


def split_clinical_notes_mct(
    clin_note: pd.DataFrame, verbosity_val: int = 0
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Splits clinical notes from an MCT schema DataFrame into date-stamped chunks.

    This function is similar to `split_clinical_notes` but is tailored for an
    MCT/observations schema (with 'observation_valuetext_analysed' and
    'observationdocument_recordeddtm' columns). It breaks down each note's
    text into smaller documents based on embedded timestamps.

    Args:
        clin_note: A DataFrame containing the clinical notes to be split.
        verbosity_val: The verbosity level passed to the `find_date` function.

    Returns:
        A tuple containing two DataFrames:
        - pd.DataFrame: The processed notes, split into smaller chunks.
        - pd.DataFrame: The original rows of notes that could not be split.
    """

    # n.b possibly redundant, no split ever needed?

    extracted = []
    none_found = []
    document_description_list = []
    id_list = []
    document_guid_list = []
    clientvisit_visitidcode_list = []
    index_list = []
    none_rows = []

    for index, row in clin_note.iterrows():
        d = row["observation_valuetext_analysed"]
        ch = []
        try:
            ch = find_date(
                d, row["observationdocument_recordeddtm"], verbosity=verbosity_val
            )
            row_id = row.get("id", row.get("_id", "unknown"))
            extracted.append(
                {"id": row_id, "client_idcode": row["client_idcode"], "chunks": ch}
            )

            document_description_list.append(
                row.get("obscatalogmasteritem_displayname", "Unknown")
            )
            id_list.append(row_id)
            document_guid_list.append(row.get("observation_guid", "Unknown"))
            clientvisit_visitidcode_list.append(
                row.get("clientvisit_visitidcode", "Unknown")
            )
            index_list.append(row.get("_index", "Unknown"))

        except Exception:
            ch = []

        if len(ch) == 0:
            none_found.append(d)
            none_rows.append(row)

    new_docs = []
    counter_1 = 0
    for ex in extracted:
        counter = 0
        for ch in ex["chunks"]:
            nd = {
                "client_idcode": ex["client_idcode"],
                "observation_valuetext_analysed": ch["text"],
                "observationdocument_recordeddtm": ch["date"],
                "updatetime": ch["date"],
            }
            nd["obscatalogmasteritem_displayname"] = (
                f"{document_description_list[counter_1]}_clinical note chunk_{counter}"
            )
            nd["_id"] = id_list[counter_1]
            nd["observation_guid"] = document_guid_list[counter_1]
            nd["clientvisit_visitidcode"] = clientvisit_visitidcode_list[counter_1]
            nd["_index"] = index_list[counter_1]

            nd["source_file"] = ex["id"]
            new_docs.append(nd)
            counter += 1
        counter_1 += 1
    if new_docs:
        processed = pd.DataFrame(new_docs).assign(
            updatetime=lambda x: pd.to_datetime(x["updatetime"])
        )
        # Explicitly convert updatetime to avoid FutureWarning in pandas
        processed["updatetime"] = pd.to_datetime(processed["updatetime"])
    else:
        processed = pd.DataFrame(
            columns=[
                "client_idcode",
                "body_analysed",
                "updatetime",
                "document_description",
                "_id",
                "document_guid",
                "clientvisit_visitidcode",
                "_index",
                "source_file",
            ]
        )

    none_rows = pd.DataFrame(none_rows)
    return processed, none_rows


def split_epic_clinical_notes(
    clin_note: pd.DataFrame, verbosity_val: int = 0
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Splits clinical notes from Epic schema DataFrame into date-stamped chunks.

    This function iterates through a DataFrame of Epic clinical notes (assuming an
    Epic-like schema with 'document_Content' and 'document_CreatedWhen' columns). It uses
    the `find_date` function to break down each note's text into smaller
    documents based on embedded timestamps.

    Args:
        clin_note: A DataFrame containing the clinical notes to be split.
        verbosity_val: The verbosity level passed to the `find_date` function.

    Returns:
        A tuple containing two DataFrames:
        - pd.DataFrame: The processed notes, split into smaller chunks.
        - pd.DataFrame: The original rows of notes that could not be split.
    """
    extracted = []
    none_found = []
    document_name_list = []
    id_list = []
    document_guid_list = []  # Assuming 'id' in Epic is like a GUID
    encounter_epic_csn_list = []
    encounter_key_list = []
    index_list = []
    none_rows = []

    for index, row in clin_note.iterrows():
        d = row["document_Content"]
        ch = []
        try:
            ch = find_date(
                d,
                original_update_time_value=row["document_CreatedWhen"],
                verbosity=verbosity_val,
            )
            row_id = row.get("id", row.get("_id", "unknown"))
            extracted.append(
                {
                    "id": row_id,
                    "client_idcode": row["document_PatientDurableKey"],
                    "chunks": ch,
                }
            )

            document_name_list.append(row.get("document_Name", "Unknown"))
            id_list.append(row_id)
            document_guid_list.append(
                row_id
            )  # Using 'id' as document_guid for Epic notes
            encounter_epic_csn_list.append(row.get("document_EncounterEpicCsn"))
            encounter_key_list.append(row.get("document_EncounterKey"))
            index_list.append(row.get("_index", "Unknown"))
        except Exception:
            ch = []

        if len(ch) == 0:
            none_found.append(d)
            none_rows.append(row)

    new_docs = []
    counter_1 = 0
    for ex in extracted:
        counter = 0
        for ch in ex["chunks"]:
            nd = {
                "document_PatientDurableKey": ex["client_idcode"],
                "document_Content": ch["text"],
                "document_CreatedWhen": ch["date"],
                "document_Name": f"{document_name_list[counter_1]}_clinical note chunk_{counter}",
                "id": id_list[counter_1],
                "_index": index_list[counter_1],
                "document_EncounterEpicCsn": encounter_epic_csn_list[counter_1],
                "document_EncounterKey": encounter_key_list[counter_1],
                "source_file": ex["id"],
            }
            new_docs.append(nd)
            counter += 1
        counter_1 += 1
    processed = (
        pd.DataFrame(new_docs).assign(
            document_CreatedWhen=lambda x: pd.to_datetime(x["document_CreatedWhen"])
        )
        if new_docs
        else pd.DataFrame()
    )
    none_rows = pd.DataFrame(none_rows)
    return processed, none_rows


def split_and_append_chunks(
    docs: pd.DataFrame, epr: bool = True, mct: bool = False, verbosity: int = 0
) -> pd.DataFrame:
    """Filters, splits, and re-appends clinical notes within a DataFrame.

    This function acts as a wrapper to orchestrate the clinical note splitting
    process. It identifies clinical notes within a larger document DataFrame,
    sends them to the appropriate splitting function (`split_clinical_notes` or
    `split_clinical_notes_mct`), and then concatenates the resulting smaller
    chunks back with the original non-clinical documents.

    Args:
        docs: The input DataFrame containing various document types.
        epr: If True, assumes an EPR schema for splitting.
        mct: If True, assumes an MCT/observations schema for splitting.
        verbosity: The verbosity level for logging and splitting.

    Returns:
        A new DataFrame containing the original non-clinical notes plus the
        newly created smaller chunks from the split clinical notes.
    """

    # Filter clinical and non-clinical notes
    clinical_notes = pd.DataFrame()
    non_clinical_notes = docs.copy()
    split_function = None

    # Determine the type of clinical notes and the appropriate splitting function
    if "document_description" in docs.columns and epr:
        clinical_notes = docs[docs["document_description"] == "Clinical Note"].copy()
        non_clinical_notes = docs[docs["document_description"] != "Clinical Note"]
        split_function = split_clinical_notes
        if verbosity > 1:
            logger.debug("Identified EPR clinical notes for splitting.")
    elif "obscatalogmasteritem_displayname" in docs.columns and mct:
        clinical_notes = docs[
            docs["obscatalogmasteritem_displayname"] == "AoMRC_ClinicalSummary_FT"
        ].copy()
        non_clinical_notes = docs[
            docs["obscatalogmasteritem_displayname"] != "AoMRC_ClinicalSummary_FT"
        ]
        split_function = split_clinical_notes_mct
        if verbosity > 1:
            logger.debug("Identified MCT clinical notes for splitting.")
    elif "document_Name" in docs.columns and "document_Content" in docs.columns:
        # Assuming Epic clinical notes are identified by having both document_Name and document_Content
        # and potentially a specific pattern in document_Name if needed.
        # For now, let's assume any document with content and a name could be split.
        # A more robust check might involve a list of known Epic clinical note names.
        is_epic_clinical_note = docs["document_Name"].str.contains(
            "note", case=False, na=False
        ) | docs["document_Name"].str.contains("summary", case=False, na=False)

        clinical_notes = docs[is_epic_clinical_note].copy()
        non_clinical_notes = docs[~is_epic_clinical_note]
        split_function = split_epic_clinical_notes
        if verbosity > 1:
            logger.debug("Identified Epic clinical notes for splitting.")
    else:
        if verbosity > 1:
            logger.debug(
                "No identifiable clinical notes for splitting based on known patterns."
            )
        # If no clinical notes are identified, return the original DataFrame
        return docs

    # Check verbosity and print sizes if needed
    if verbosity > 1:
        logger.debug(f"Size of clinical_notes dataframe: {len(clinical_notes)}")
        logger.debug(f"Size of non_clinical_notes dataframe: {len(non_clinical_notes)}")

    if clinical_notes.empty:
        return docs  # No clinical notes to split, return original docs

    # Rename the '_id' column to 'id' if it exists and is not already 'id'
    if "_id" in clinical_notes.columns and "id" not in clinical_notes.columns:
        clinical_notes.rename(columns={"_id": "id"}, inplace=True)

    if split_function:
        split_clinical_notes_result, none_found = split_function(
            clinical_notes, verbosity_val=verbosity
        )
    else:
        split_clinical_notes_result = pd.DataFrame()
        none_found = (
            clinical_notes  # If no split function, all clinical notes are "none_found"
        )

    # Rename id back to _id in none_found to match existing schema and avoid "no column named id" errors
    if not none_found.empty and "id" in none_found.columns:
        none_found.rename(columns={"id": "_id"}, inplace=True)

    # Standardize Epic patient ID column to client_idcode for concatenation
    if "document_PatientDurableKey" in split_clinical_notes_result.columns:
        split_clinical_notes_result.rename(
            columns={"document_PatientDurableKey": "client_idcode"}, inplace=True
        )
    if "document_PatientDurableKey" in none_found.columns:
        none_found.rename(
            columns={"document_PatientDurableKey": "client_idcode"}, inplace=True
        )
    if "document_PatientDurableKey" in non_clinical_notes.columns:
        non_clinical_notes.rename(
            columns={"document_PatientDurableKey": "client_idcode"}, inplace=True
        )

    # Concatenate non-clinical and split clinical notes
    concatenated_notes = pd.concat([non_clinical_notes, split_clinical_notes_result])

    concatenated_notes = pd.concat([concatenated_notes, none_found], ignore_index=True)

    # Ensure unique columns before returning to prevent ValueError in subsequent operations
    concatenated_notes = concatenated_notes.loc[
        :, ~concatenated_notes.columns.duplicated()
    ]

    # Reset index
    concatenated_notes.reset_index(inplace=True)

    return concatenated_notes
