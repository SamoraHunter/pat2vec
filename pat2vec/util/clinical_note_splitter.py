from typing import Any, Dict, List, Optional

import pandas as pd
import regex


def find_date(
    txt: str,
    original_update_time_value=None,
    reg: str = "Entered on -",
    window: int = 20,
    verbosity: int = 0,
) -> List[Dict[str, Any]]:
    """
    Find and extract chunks of text with associated dates from the input text.

    Parameters:
    - txt (str): Input text to search for date entries.
    - reg (str, optional): Regular expression pattern to identify date entries (default is "Entered on -").
    - window (int, optional): Size of the window to extract text around the identified date entries (default is 20).

    Returns:
    List[Dict[str, Any]]: List of dictionaries containing extracted chunks with associated dates.

    Example:
    >>> chunks_list = find_date("Sample text with date entries.")
    """

    reg = "Entered on -"
    window = 20
    m = regex.finditer(reg, txt)
    text_start = 0
    chunks = []
    for match in m:
        # print("end",match.span()[1])
        date_window_start = match.span()[1]
        date_window_end = date_window_start + window
        dw = txt[date_window_start:date_window_end]
        dw = dw.strip()
        # print(dw)

        ts = regex.findall(r"[\d]{2}-\w{3}-[\d]{4} [\d]{2}:[\d]{2}", dw)
        date_l = 0  # used to find start of next section
        date_found = False
        if len(ts) == 1:
            # timestamp found ok
            date_found = True
            date = pd.to_datetime(ts[0])
            date_l = len(ts[0])
        else:
            if len(ts) == 0:
                # no timestamp found
                if verbosity > 1:
                    print("no timestamp found in ", dw)
                else:
                    pass

            else:
                if verbosity > 1:
                    # multiple matches
                    print("too many timestamps found in ", dw)
                else:
                    pass

            # date = None
            # return original date to avoid None
            date = original_update_time_value

        # +1 as all seem to be 1 char short
        text_end = match.span()[1] + date_l + 1
        chunk_t = txt[text_start:text_end]
        chunks.append(
            {
                "text": chunk_t,
                "date_text": dw,
                "date": date,
                "date_found": date_found,
                "text_start": text_start,
                "text_end": text_end,
            }
        )

        # next window starts at end of this one, try
        if date_found:
            text_start = match.span()[1] + date_l + 1
        else:
            text_start = match.span()[0]

    return chunks


def split_clinical_notes(clin_note: pd.DataFrame, verbosity_val=0) -> pd.DataFrame:
    """
    Split clinical notes into chunks based on extracted dates.

    Parameters:
    - clin_note (pd.DataFrame): DataFrame containing clinical notes to be split.

    Returns:
    pd.DataFrame: DataFrame containing the split clinical notes.

    Example:
    >>> result_df = split_clinical_notes(clinical_notes_df)
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
            ch = find_date(
                d, original_update_time_value=row["updatetime"], verbosity=verbosity_val
            )
            extracted.append(
                {"id": row["id"], "client_idcode": row["client_idcode"], "chunks": ch}
            )

            document_description_list.append(row["document_description"])
            id_list.append(row["id"])
            document_guid_list.append(row["document_guid"])
            clientvisit_visitidcode_list.append(row["clientvisit_visitidcode"])
            index_list.append(row["_index"])
        except Exception as e:
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
    processed = pd.DataFrame(new_docs)
    none_rows = pd.DataFrame(none_rows)
    return processed, none_rows


def split_clinical_notes_mct(clin_note: pd.DataFrame, verbosity_val=0) -> pd.DataFrame:
    """
    Split clinical notes into chunks based on extracted dates. MCT schema from observations index.

    Parameters:
    - clin_note (pd.DataFrame): DataFrame containing clinical notes to be split.

    Returns:
    pd.DataFrame: DataFrame containing the split clinical notes.

    Example:
    >>> result_df = split_clinical_notes(clinical_notes_df)
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
            extracted.append(
                {"id": row["id"], "client_idcode": row["client_idcode"], "chunks": ch}
            )

            document_description_list.append(row["obscatalogmasteritem_displayname"])
            id_list.append(row["id"])
            document_guid_list.append(row["observation_guid"])
            clientvisit_visitidcode_list.append(row["clientvisit_visitidcode"])
            index_list.append(row["_index"])

        except Exception as e:
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
    processed = pd.DataFrame(new_docs)
    none_rows = pd.DataFrame(none_rows)
    return processed, none_rows


def split_and_append_chunks(
    docs: pd.DataFrame, epr=True, mct=False, verbosity: int = 0
) -> pd.DataFrame:
    """
    Split and append clinical notes in a DataFrame.

    Parameters:
    - docs (pd.DataFrame): Input DataFrame containing documents with a 'document_description' column.
    - verbosity (int, optional): Verbosity level for logging (default is 0).

    Returns:
    pd.DataFrame: Concatenated DataFrame containing both non-clinical and split clinical notes.

    Example:
    >>> result_df = split_and_append_chunks(input_df, verbosity=1)
    """

    # Filter clinical and non-clinical notes
    column_name = "document_description"
    column_name_mct = "obscatalogmasteritem_displayname"

    if column_name in docs.columns:
        clinical_notes = docs[docs[column_name] == "Clinical Note"]
        non_clinical_notes = docs[docs[column_name] != "Clinical Note"]
        if verbosity > 1:
            print(f"Found column '{column_name}' in DataFrame.")
    elif column_name_mct in docs.columns:
        clinical_notes = docs[docs[column_name_mct] == "AoMRC_ClinicalSummary_FT"]
        non_clinical_notes = docs[docs[column_name_mct] != "AoMRC_ClinicalSummary_FT"]
        if verbosity > 1:
            print(f"Found column '{column_name_mct}' in DataFrame.")
    else:
        raise ValueError(
            f"Neither {column_name} nor {column_name_mct} found in DataFrame columns."
        )

    # Check verbosity and print sizes if needed
    if verbosity > 1:
        print(f"Size of clinical_notes dataframe: {len(clinical_notes)}")
        print(f"Size of non_clinical_notes dataframe: {len(non_clinical_notes)}")

    # Rename the '_id' column to 'id'
    clinical_notes.rename(columns={"_id": "id"}, inplace=True)

    if epr:
        # Split clinical notes according to epr schema
        split_clinical_notes_result, none_found = split_clinical_notes(
            clinical_notes, verbosity_val=verbosity
        )
    if mct:
        # Split clinical notes according to mct schema
        split_clinical_notes_result, none_found = split_clinical_notes_mct(
            clinical_notes, verbosity_val=verbosity
        )

    # Concatenate non-clinical and split clinical notes
    concatenated_notes = pd.concat([non_clinical_notes, split_clinical_notes_result])

    concatenated_notes = pd.concat([concatenated_notes, none_found], ignore_index=True)

    # Reset index
    concatenated_notes.reset_index(inplace=True)

    return concatenated_notes
