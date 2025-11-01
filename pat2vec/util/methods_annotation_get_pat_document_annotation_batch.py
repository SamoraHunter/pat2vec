from pat2vec.util.methods_annotation import (
    annot_pat_batch_docs,
    multi_annots_to_df_mct,
    multi_annots_to_df_reports,
    multi_annots_to_df_textual_obs,
)
from pat2vec.util.methods_annotation_multi_annots_to_df import multi_annots_to_df
import pandas as pd
import os
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

    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode, pat_batch, cat=cat, config_obj=config_obj, t=t
    )
    # create the file in its dir
    multi_annots_to_df(
        current_pat_client_idcode, pat_batch, multi_annots, config_obj=config_obj, t=t
    )

    # read_newly created file:
    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    pat_document_annotation_batch = pd.read_csv(
        current_pat_document_annotation_batch_path
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
    # get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(
        current_pat_client_idcode=current_pat_client_idcode,
        pat_batch=pat_batch,
        cat=cat,
        config_obj=config_obj,
        t=t,
        text_column="observation_valuetext_analysed",
    )

    # create the file in its dir
    multi_annots_to_df_mct(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        text_column="observation_valuetext_analysed",
        time_column="observationdocument_recordeddtm",
        guid_column="observation_guid",
    )

    # read_newly created file:
    pre_document_annotation_batch_path_mct = (
        config_obj.pre_document_annotation_batch_path_mct
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path_mct, current_pat_client_idcode + ".csv"
    )

    pat_document_annotation_batch = pd.read_csv(
        current_pat_document_annotation_batch_path
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
    multi_annots_to_df_textual_obs(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        text_column="body_analysed",  # overwritten in batch collection method from textualObs
        time_column="basicobs_entered",
        guid_column="basicobs_guid",
    )

    # read_newly created file:
    pre_textual_obs_annotation_batch_path = (
        config_obj.pre_textual_obs_annotation_batch_path
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_textual_obs_annotation_batch_path, current_pat_client_idcode + ".csv"
    )

    pat_document_annotation_batch = pd.read_csv(
        current_pat_document_annotation_batch_path
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
    multi_annots_to_df_reports(
        current_pat_client_idcode,
        pat_batch,
        multi_annots,
        config_obj=config_obj,
        t=t,
        text_column="body_analysed",
        time_column="updatetime",
        guid_column="basicobs_guid",
    )

    # read_newly created file:
    pre_document_annotation_batch_path_reports = (
        config_obj.pre_document_annotation_batch_path_reports
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path_reports, current_pat_client_idcode + ".csv"
    )

    pat_document_annotation_batch = pd.read_csv(
        current_pat_document_annotation_batch_path
    )

    return pat_document_annotation_batch
