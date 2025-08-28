from pat2vec.util.methods_annotation import (
    annot_pat_batch_docs,
    multi_annots_to_df_mct,
    multi_annots_to_df_reports,
    multi_annots_to_df_textual_obs,
)
from pat2vec.util.methods_annotation_multi_annots_to_df import multi_annots_to_df
import pandas as pd
import os
from IPython.display import display


def get_pat_document_annotation_batch(
    current_pat_client_idcode, pat_batch, cat=None, config_obj=None, t=None
):
    """
    Retrieves the annotation batch for the given patient.

    This function annotates the given patient's batch of documents and stores the
    annotations in a CSV file. It then reads and returns the annotation batch.

    Parameters
    ----------
    current_pat_client_idcode : str
        The client ID code for the current patient.
    pat_batch : pandas.DataFrame
        A DataFrame containing the batch of documents for the patient.
    cat : object, optional
        An object for entity recognition.
    config_obj : object, optional
        An object containing configuration settings.
    t : object, optional
        A progress tracker object.

    Returns
    -------
    pandas.DataFrame
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
    current_pat_client_idcode, pat_batch, cat=None, config_obj=None, t=None
):

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
    current_pat_client_idcode, pat_batch, cat=None, config_obj=None, t=None
):
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
    current_pat_client_idcode, pat_batch, cat=None, config_obj=None, t=None
):

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
