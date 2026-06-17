from pat2vec.util.methods_annotation import (
    annot_pat_batch_docs,
    multi_annots_to_df_mct,
    multi_annots_to_df_epic_clinical_notes,
    multi_annots_to_df_epic_clinical_notes_appointments,
    multi_annots_to_df_epic_medical_history,
    multi_annots_to_df_reports,
    multi_annots_to_df_epic_imaging_reports,
    multi_annots_to_df_textual_obs,
    multi_annots_to_df_epic_orders,
)
from pat2vec.util.methods_annotation_multi_annots_to_df import multi_annots_to_df
import pandas as pd
from typing import Any


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

    # Determine which multi_annots_to_df function to use based on config_obj.main_options
    # and the expected source of the pat_batch.
    # This assumes pat_batch will have specific columns that identify its source.
    # For a more robust solution, the caller might explicitly pass the source type.
    # For now, we'll infer based on common Epic column names.

    # Default to generic EPR annotation processing
    multi_annots_to_df_func = multi_annots_to_df
    text_col = "body_analysed"
    time_col = "updatetime"
    guid_col = "document_guid"

    if (
        config_obj.main_options.get("epic_clinical_notes")
        and "document_Content" in pat_batch.columns
        and "document_Name" in pat_batch.columns
    ):
        multi_annots_to_df_func = multi_annots_to_df_epic_clinical_notes
        text_col = "document_Content"
        time_col = "document_CreatedWhen"
        guid_col = "id"
    elif (
        config_obj.main_options.get("epic_clinical_notes_appointments")
        and "document_Content" in pat_batch.columns
        and "document_EncounterEpicCsn" in pat_batch.columns
    ):
        multi_annots_to_df_func = multi_annots_to_df_epic_clinical_notes_appointments
        text_col = "document_Content"
        time_col = "document_CreatedWhen"
        guid_col = "id"
    elif (
        config_obj.main_options.get("epic_imaging_reports")
        and "document_Content" in pat_batch.columns
        and "document_ImagingModality" in pat_batch.columns
    ):
        multi_annots_to_df_func = multi_annots_to_df_epic_imaging_reports
        text_col = "document_Content"
        time_col = "document_CreatedWhen"
        guid_col = "id"
    elif (
        config_obj.main_options.get("epic_medical_history")
        and "document_Comment" in pat_batch.columns
    ):
        multi_annots_to_df_func = multi_annots_to_df_epic_medical_history
        text_col = "document_Comment"
        time_col = "document_CreatedWhen"
        guid_col = "id"
    elif (
        config_obj.main_options.get("epic_orders_annotations")
        and "document_Content" in pat_batch.columns
        and "document_OrderClass" in pat_batch.columns
    ):
        multi_annots_to_df_func = multi_annots_to_df_epic_orders
        text_col = "document_Content"
        time_col = "document_CreatedWhen"
        guid_col = "id"
    elif (
        config_obj.main_options.get("annotations_mrc")
        and "observation_valuetext_analysed" in pat_batch.columns
    ):
        multi_annots_to_df_func = multi_annots_to_df_mct
        text_col = "observation_valuetext_analysed"
        time_col = "observationdocument_recordeddtm"
        guid_col = "observation_guid"
    # Add other specific annotation types here if needed

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
    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column="document_Content",
    )

    # create the file in its dir
    pat_document_annotation_batch = multi_annots_to_df_epic_imaging_reports(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
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
    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column="document_Content",
    )

    # create the file in its dir
    pat_document_annotation_batch = multi_annots_to_df_epic_clinical_notes(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
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
    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column="document_Content",
    )

    # create the file in its dir
    pat_document_annotation_batch = multi_annots_to_df_epic_clinical_notes_appointments(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
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
    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column="document_Comment",
    )

    # create the file in its dir
    pat_document_annotation_batch = multi_annots_to_df_epic_medical_history(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        include_text_sample=config_obj.include_text_sample_in_annots,
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
