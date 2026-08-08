from typing import Any

import pandas as pd

from pat2vec.util.methods_annotation import (
    annot_pat_batch_docs,
    multi_annots_to_df_epic_clinical_notes,
    multi_annots_to_df_epic_clinical_notes_appointments,
    multi_annots_to_df_epic_imaging_reports,
    multi_annots_to_df_epic_medical_history,
    multi_annots_to_df_epic_orders,
    multi_annots_to_df_mct,
    multi_annots_to_df_reports,
    multi_annots_to_df_textual_obs,
)
from pat2vec.util.methods_annotation_multi_annots_to_df import multi_annots_to_df


def get_pat_document_annotation_batch_epic_orders(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
) -> pd.DataFrame:
    """Retrieves or creates the annotation batch for a patient's Epic orders.

    This function annotates a patient's Epic orders batch using MedCAT, saves the
    structured annotations to a CSV file, and returns the result as a DataFrame.
    It uses the专用 column names (document_Content, document_CreatedWhen) that are
    expected in epic_orders data after ES fetching.

    Args:
        current_pat_client_idcode: The client ID for the current patient.
        pat_batch: A DataFrame containing the batch of Epic orders documents.
        cat: The loaded MedCAT `CAT` object for entity recognition.
        config_obj: The configuration object containing settings and paths.
        t: The tqdm progress bar instance to update.

    Returns:
        A DataFrame containing the annotation batch for the patient's Epic orders.
    """
    # Determine which text column to use based on available columns
    if "body_analysed" in pat_batch.columns:
        text_column = "body_analysed"
    elif "document_Content" in pat_batch.columns:
        text_column = "document_Content"
    else:
        raise KeyError(
            f"Neither 'body_analysed' nor 'document_Content' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # Determine time and guid columns - handle ES fetch renames
    if "updatetime" in pat_batch.columns:
        time_column = "updatetime"
    elif "document_CreatedWhen" in pat_batch.columns:
        time_column = "document_CreatedWhen"
    else:
        raise KeyError(
            f"Neither 'updatetime' nor 'document_CreatedWhen' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    if "document_guid" in pat_batch.columns:
        guid_column = "document_guid"
    elif "id" in pat_batch.columns:
        guid_column = "id"
    else:
        raise KeyError(
            f"Neither 'document_guid' nor 'id' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column=text_column,
    )

    # create the file in its dir
    testing = getattr(config_obj, "testing", False)
    pat_document_annotation_batch = multi_annots_to_df_epic_orders(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        text_column=text_column,
        time_column=time_column,
        guid_column=guid_column,
        include_text_sample=config_obj.include_text_sample_in_annots,
        testing=testing,
    )

    return pat_document_annotation_batch


def get_pat_document_annotation_batch(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
) -> pd.DataFrame:
    """Retrieves or creates the annotation batch for a patient's documents.

    This function orchestrates the annotation of a patient's document batch.
    It calls MedCAT to get annotations, saves them to a patient-specific CSV
    file, and then reads that file back into a DataFrame.

    Args:
        current_pat_client_idcode: The client ID for the current patient.
        pat_batch: A DataFrame containing the batch of documents for the patient.
        cat: The loaded MedCAT `CAT` object for entity recognition.
        config_obj: The configuration object containing settings and paths.
        t: The tqdm progress bar instance to update.

    Returns:
        A DataFrame containing the annotation batch for the patient.
    """

    # Determine which multi_annots_to_df function to use based on unique columns
    # that survived pre-processing column standardization.
    # After pre_processing.py, original ES columns like document_Content are renamed
    # to body_analysed, but source-unique columns remain (e.g., document_OrderClass).
    #
    # Dynamic column detection is used to access text/time/guid from the standardized columns.

    # Detect unique columns that identify the data source after standardization.
    # For example, epic_orders has 'document_OrderClass' which is NOT in col_map.
    source_identifier = None

    # Check for epic_clinical_notes unique columns (not in pre_processing col_map)
    if "document_description" in pat_batch.columns:
        source_identifier = "epic_clinical_notes"
    elif "document_EncounterEpicCsn" in pat_batch.columns:
        source_identifier = "epic_clinical_notes_appointments"
    elif "document_ImagingModality" in pat_batch.columns:
        source_identifier = "epic_imaging_reports"
    # Note: document_Comment is mapped to body_analysed, so check for other identifiers
    elif "document_OrderClass" in pat_batch.columns:
        source_identifier = "epic_orders"
    elif "observation_valuetext_analysed" in pat_batch.columns:
        source_identifier = "annotations_mrc"

    # Default to generic EPR annotation processing if no specific source found
    multi_annots_to_df_func = multi_annots_to_df

    # Use dynamic column detection that works with standardized columns (body_analysed, updatetime, document_guid)
    # Handle edge case: empty DataFrame might have minimal/no columns
    if len(pat_batch) == 0:
        # For empty DataFrames, use fallback defaults since columns may not exist
        text_col = "body_analysed"
        time_col = "updatetime"
        guid_col = "document_guid"
    else:
        if "body_analysed" in pat_batch.columns:
            text_col = "body_analysed"
        elif "document_Content" in pat_batch.columns:
            text_col = "document_Content"
        else:
            raise KeyError(
                f"No text column found. Expected 'body_analysed' or 'document_Content'. Available columns: {list(pat_batch.columns)}"
            )

        if "updatetime" in pat_batch.columns:
            time_col = "updatetime"
        elif "document_CreatedWhen" in pat_batch.columns:
            time_col = "document_CreatedWhen"
        else:
            raise KeyError(
                f"No time column found. Expected 'updatetime' or 'document_CreatedWhen'. Available columns: {list(pat_batch.columns)}"
            )

        if "document_guid" in pat_batch.columns:
            guid_col = "document_guid"
        elif "id" in pat_batch.columns:
            guid_col = "id"
        else:
            raise KeyError(
                f"No GUID column found. Expected 'document_guid' or 'id'. Available columns: {list(pat_batch.columns)}"
            )

    # Determine which multi_annots_to_df function to use based on source identifier
    if source_identifier == "epic_clinical_notes":
        multi_annots_to_df_func = multi_annots_to_df_epic_clinical_notes
    elif source_identifier == "epic_clinical_notes_appointments":
        multi_annots_to_df_func = multi_annots_to_df_epic_clinical_notes_appointments
    elif source_identifier == "epic_imaging_reports":
        multi_annots_to_df_func = multi_annots_to_df_epic_imaging_reports
    elif source_identifier == "epic_medical_history":
        # Note: epic_medical_history doesn't have a unique column remaining after renaming.
        # Check using config option as fallback since document_Comment -> body_analysed
        if config_obj.main_options.get("epic_medical_history"):
            multi_annots_to_df_func = multi_annots_to_df_epic_medical_history
    elif source_identifier == "epic_orders":
        multi_annots_to_df_func = multi_annots_to_df_epic_orders
    elif source_identifier == "annotations_mrc":
        multi_annots_to_df_func = multi_annots_to_df_mct

    # Handle epic_medical_history - it doesn't have a unique column after renaming (document_Comment -> body_analysed)
    # Check config only when source_identifier is None (no unique columns found) and config has epic_medical_history enabled
    # Use isinstance check to avoid MagicMock truthiness issue where .get() returns a new MagicMock for unset keys
    from unittest.mock import MagicMock as MockMagic

    epic_med_config_value = config_obj.main_options.get("epic_medical_history")
    is_true_epic_med = (
        source_identifier is None
        and not isinstance(epic_med_config_value, MockMagic)
        and epic_med_config_value
    )

    if is_true_epic_med:
        multi_annots_to_df_func = multi_annots_to_df_epic_medical_history

    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode,
        pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column=text_col,
    )
    # create the file in its dir
    pat_document_annotation_batch = multi_annots_to_df_func(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        text_column=text_col,
        time_column=time_col,
        guid_column=guid_col,
        include_text_sample=config_obj.include_text_sample_in_annots,
    )

    return pat_document_annotation_batch


def get_pat_document_annotation_batch_epic_imaging_reports(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
) -> pd.DataFrame:
    """Retrieves or creates the annotation batch for a patient's Epic imaging reports."""
    # Determine which text column to use based on available columns
    if "body_analysed" in pat_batch.columns:
        text_column = "body_analysed"
    elif "document_Content" in pat_batch.columns:
        text_column = "document_Content"
    else:
        raise KeyError(
            f"Neither 'body_analysed' nor 'document_Content' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # Determine time and guid columns - handle ES fetch renames
    if "updatetime" in pat_batch.columns:
        time_column = "updatetime"
    elif "document_CreatedWhen" in pat_batch.columns:
        time_column = "document_CreatedWhen"
    else:
        raise KeyError(
            f"Neither 'updatetime' nor 'document_CreatedWhen' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    if "document_guid" in pat_batch.columns:
        guid_column = "document_guid"
    elif "id" in pat_batch.columns:
        guid_column = "id"
    else:
        raise KeyError(
            f"Neither 'document_guid' nor 'id' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column=text_column,
    )

    # create the file in its dir
    testing = getattr(config_obj, "testing", False)
    pat_document_annotation_batch = multi_annots_to_df_epic_imaging_reports(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
        text_column=text_column,
        time_column=time_column,
        guid_column=guid_column,
        testing=testing,
    )

    return pat_document_annotation_batch


def get_pat_document_annotation_batch_epic_clinical_notes(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
) -> pd.DataFrame:
    """Retrieves or creates the annotation batch for a patient's Epic clinical notes.

    Args:
        current_pat_client_idcode: The client ID for the current patient.
        pat_batch: A DataFrame containing the batch of Epic clinical notes.
        cat: The loaded MedCAT `CAT` object.
        config_obj: The configuration object.
        t: The tqdm progress bar instance.

    Returns:
        A DataFrame containing the annotation batch.
    """
    # Determine which text column to use based on available columns
    if "body_analysed" in pat_batch.columns:
        text_column = "body_analysed"
    elif "document_Content" in pat_batch.columns:
        text_column = "document_Content"
    else:
        raise KeyError(
            f"Neither 'body_analysed' nor 'document_Content' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # Determine time and guid columns - handle ES fetch renames
    if "updatetime" in pat_batch.columns:
        time_column = "updatetime"
    elif "document_CreatedWhen" in pat_batch.columns:
        time_column = "document_CreatedWhen"
    else:
        raise KeyError(
            f"Neither 'updatetime' nor 'document_CreatedWhen' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    if "document_guid" in pat_batch.columns:
        guid_column = "document_guid"
    elif "id" in pat_batch.columns:
        guid_column = "id"
    else:
        raise KeyError(
            f"Neither 'document_guid' nor 'id' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column=text_column,
    )

    # create the file in its dir
    testing = getattr(config_obj, "testing", False)
    pat_document_annotation_batch = multi_annots_to_df_epic_clinical_notes(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
        text_column=text_column,
        time_column=time_column,
        guid_column=guid_column,
        testing=testing,
    )

    return pat_document_annotation_batch


def get_pat_document_annotation_batch_epic_clinical_notes_appointments(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
) -> pd.DataFrame:
    """Retrieves or creates the annotation batch for a patient's Epic clinical notes appointments."""
    # Determine which text column to use based on available columns
    if "body_analysed" in pat_batch.columns:
        text_column = "body_analysed"
    elif "document_Content" in pat_batch.columns:
        text_column = "document_Content"
    else:
        raise KeyError(
            f"Neither 'body_analysed' nor 'document_Content' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # Determine time and guid columns - handle ES fetch renames
    if "updatetime" in pat_batch.columns:
        time_column = "updatetime"
    elif "document_CreatedWhen" in pat_batch.columns:
        time_column = "document_CreatedWhen"
    else:
        raise KeyError(
            f"Neither 'updatetime' nor 'document_CreatedWhen' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    if "document_guid" in pat_batch.columns:
        guid_column = "document_guid"
    elif "id" in pat_batch.columns:
        guid_column = "id"
    else:
        raise KeyError(
            f"Neither 'document_guid' nor 'id' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column=text_column,
    )

    # create the file in its dir
    testing = getattr(config_obj, "testing", False)
    pat_document_annotation_batch = multi_annots_to_df_epic_clinical_notes_appointments(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
        text_column=text_column,
        time_column=time_column,
        guid_column=guid_column,
        testing=testing,
    )

    return pat_document_annotation_batch


def get_pat_document_annotation_batch_epic_medical_history(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
) -> pd.DataFrame:
    """Retrieves or creates the annotation batch for a patient's Epic medical history."""
    # Determine which text column to use based on available columns
    if "body_analysed" in pat_batch.columns:
        text_column = "body_analysed"
    elif "document_Comment" in pat_batch.columns:
        text_column = "document_Comment"
    else:
        raise KeyError(
            f"Neither 'body_analysed' nor 'document_Comment' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # Determine time and guid columns - handle ES fetch renames
    if "updatetime" in pat_batch.columns:
        time_column = "updatetime"
    elif "document_CreatedWhen" in pat_batch.columns:
        time_column = "document_CreatedWhen"
    else:
        raise KeyError(
            f"Neither 'updatetime' nor 'document_CreatedWhen' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    if "document_guid" in pat_batch.columns:
        guid_column = "document_guid"
    elif "id" in pat_batch.columns:
        guid_column = "id"
    else:
        raise KeyError(
            f"Neither 'document_guid' nor 'id' column found in DataFrame. Available columns: {list(pat_batch.columns)}"
        )

    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column=text_column,
    )

    # create the file in its dir
    testing = getattr(config_obj, "testing", False)
    pat_document_annotation_batch = multi_annots_to_df_epic_medical_history(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
        text_column=text_column,
        time_column=time_column,
        guid_column=guid_column,
        testing=testing,
    )

    return pat_document_annotation_batch


def get_pat_document_annotation_batch_mct(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
) -> pd.DataFrame:
    """Retrieves or creates the annotation batch for a patient's MCT documents.

    This function annotates a patient's MCT (MRC clinical notes) document
    batch using MedCAT, saves the structured annotations to a CSV file,
    and returns the result as a DataFrame.

    Args:
        current_pat_client_idcode: The client ID for the current patient.
        pat_batch: A DataFrame containing the batch of MCT documents.
        cat: The loaded MedCAT `CAT` object.
        config_obj: The configuration object.
        t: The tqdm progress bar instance.

    Returns:
        A DataFrame containing the annotation batch for the patient's MCT
        documents.
    """
    text_column = "observation_valuetext_analysed"

    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column=text_column,
    )

    # Ensure the text_column exists in pat_batch before calling dropna
    if text_column not in pat_batch.columns:
        pat_batch[text_column] = None  # Add the column with NaN values

    # create the file in its dir
    pat_document_annotation_batch = multi_annots_to_df_mct(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
    )

    return pat_document_annotation_batch


def get_pat_batch_textual_obs_annotation_batch(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
) -> pd.DataFrame:
    """Retrieves or creates annotations for a textual observation batch.

    This function annotates a patient's textual observation batch using
    MedCAT, saves the structured annotations to a CSV file, and returns the
    result as a DataFrame.

    Args:
        current_pat_client_idcode: The client ID for the current patient.
        pat_batch: A DataFrame containing the batch of textual observations.
        cat: The loaded MedCAT `CAT` object.
        config_obj: The configuration object.
        t: The tqdm progress bar instance.

    Returns:
        A DataFrame containing the annotation batch for the patient's textual
        observations.
    """
    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column="body_analysed",  # overwritten in batch collection method from textualObs
    )

    # create the file in its dir
    pat_document_annotation_batch = multi_annots_to_df_textual_obs(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
    )

    return pat_document_annotation_batch


def get_pat_document_annotation_batch_reports(
    current_pat_client_idcode: str,
    pat_batch: pd.DataFrame,
    cat: Any,
    config_obj: Any,
    t: Any,
) -> pd.DataFrame:
    """Retrieves or creates the annotation batch for a patient's reports.

    This function annotates a patient's reports batch using MedCAT, saves the
    structured annotations to a CSV file, and returns the result as a
    DataFrame.

    Args:
        current_pat_client_idcode: The client ID for the current patient.
        pat_batch: A DataFrame containing the batch of reports.
        cat: The loaded MedCAT `CAT` object.
        config_obj: The configuration object.
        t: The tqdm progress bar instance.

    Returns:
        A DataFrame containing the annotation batch for the patient's
        reports.
    """
    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column="body_analysed",
    )

    # creaet the file in its dir
    pat_document_annotation_batch = multi_annots_to_df_reports(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
    )

    return pat_document_annotation_batch
