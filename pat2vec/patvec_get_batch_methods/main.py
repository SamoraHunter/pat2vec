import os
import logging
import pandas as pd
from typing import Any

from IPython.display import display

from pat2vec.util.clinical_note_splitter import split_and_append_chunks
from pat2vec.util.filter_methods import filter_dataframe_by_fuzzy_terms
from pat2vec.util.filter_methods import (
    apply_bloods_data_type_filter,
    apply_data_type_mct_docs_filters,
)
from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
    get_pat_document_annotation_batch_reports,
)
from pat2vec.util.methods_annotation_get_pat_document_annotation_batch import (
    get_pat_batch_textual_obs_annotation_batch,
    get_pat_document_annotation_batch,
    get_pat_document_annotation_batch_mct,
)
from pat2vec.util.methods_annotation_regex import append_regex_term_counts
from pat2vec.util.methods_get import exist_check
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp


def get_pat_batch_obs(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of specific observations for a patient.

    This function fetches observation data for a single patient, filtering by a
    specific `search_term` (e.g., 'CORE_SmokingStatus') within the globally
    defined time window. It includes logic to read from a cached file if it
    exists or query the data source otherwise.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The specific observation term to search for.
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of specified observations.
    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    batch_obs_target_path = os.path.join(
        config_obj.pre_misc_batch_path.replace("misc", search_term),
        str(current_pat_client_id_code) + ".csv",
    )
    existence_check = exist_check(batch_obs_target_path, config_obj)

    try:
        if (
            config_obj.store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode	obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f'obscatalogmasteritem_displayname:("{search_term}") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            if (
                config_obj.store_pat_batch_docs
                or config_obj.overwrite_stored_pat_observations
            ):

                directory_path = config_obj.pre_misc_batch_path.replace(
                    "misc", search_term
                )

                if not os.path.exists(directory_path):
                    os.makedirs(directory_path)
                batch_target.to_csv(batch_obs_target_path)
        else:
            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:

        logging.error(f"Error retrieving batch observations: {e}")
        return pd.DataFrame()


def get_pat_batch_news(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of NEWS score observations for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of NEWS observations.
    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    batch_obs_target_path = os.path.join(
        config_obj.pre_news_batch_path, str(current_pat_client_id_code) + ".csv"
    )
    existence_check = exist_check(batch_obs_target_path, config_obj)

    try:
        if (
            config_obj.store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f'obscatalogmasteritem_displayname:("NEWS" OR "NEWS2") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            if (
                config_obj.store_pat_batch_docs
                or config_obj.overwrite_stored_pat_observations
            ):

                batch_target.to_csv(batch_obs_target_path)
        else:
            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:

        logging.error(f"Error retrieving batch NEWS observations: {e}")
        return pd.DataFrame()


def get_pat_batch_bmi(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of BMI-related observations for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of BMI-related observations.
    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    batch_obs_target_path = os.path.join(
        config_obj.pre_bmi_batch_path, str(current_pat_client_id_code) + ".csv"
    )
    existence_check = exist_check(batch_obs_target_path, config_obj)

    try:
        if (
            config_obj.store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f'obscatalogmasteritem_displayname:("OBS BMI" OR "OBS Weight" OR "OBS height") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            if (
                config_obj.store_pat_batch_docs
                or config_obj.overwrite_stored_pat_observations
            ):

                batch_target.to_csv(batch_obs_target_path)
        else:
            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch BMI-related observations: {e}")
        return pd.DataFrame()


def get_pat_batch_bloods(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of blood test observations for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of blood test observations.
    """

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations

    batch_obs_target_path = os.path.join(
        config_obj.pre_bloods_batch_path, str(current_pat_client_id_code) + ".csv"
    )

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    existence_check = exist_check(batch_obs_target_path, config_obj)

    bloods_time_field = config_obj.bloods_time_field

    try:
        if (
            store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "basicobs_itemname_analysed",
                    "basicobs_value_numeric",
                    "basicobs_entered",
                    "clientvisit_serviceguid",
                    "updatetime",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f"basicobs_value_numeric:* AND "
                f"{bloods_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            if config_obj.data_type_filter_dict is not None:
                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists").get(
                        "bloods"
                    )
                    is not None
                ):

                    if config_obj.verbosity >= 1:
                        logging.info(
                            "applying doc type filter to bloods",
                            config_obj.data_type_filter_dict,
                        )

                        filter_term_list = config_obj.data_type_filter_dict.get(
                            "filter_term_lists"
                        ).get("bloods")

                        batch_target = filter_dataframe_by_fuzzy_terms(
                            batch_target,
                            filter_term_list,
                            column_name="basicobs_itemname_analysed",
                            verbose=config_obj.verbosity,
                        )

            batch_target = apply_bloods_data_type_filter(config_obj, batch_target)

            if config_obj.store_pat_batch_docs or overwrite_stored_pat_observations:
                batch_target.to_csv(batch_obs_target_path)

        else:
            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch blood test-related observations: {e}")
        return pd.DataFrame()


def get_pat_batch_drugs(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of medication orders for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of medication orders.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    drug_time_field = config_obj.drug_time_field

    batch_obs_target_path = os.path.join(
        config_obj.pre_drugs_batch_path, str(current_pat_client_id_code) + ".csv"
    )
    existence_check = exist_check(batch_obs_target_path, config_obj)

    try:

        if (
            config_obj.store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="order",
                fields_list="""client_idcode order_guid order_name order_summaryline order_holdreasontext order_entered clientvisit_visitidcode order_performeddtm order_createdwhen""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f'order_typecode:"medication" AND '
                f"{drug_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_docs
                or config_obj.overwrite_stored_pat_observations
            ):
                batch_target.to_csv(batch_obs_target_path)

        else:
            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch medication orders: {e}")
        return pd.DataFrame()


def get_pat_batch_diagnostics(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of diagnostic orders for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of diagnostic orders.
    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    diagnosic_time_field = config_obj.diagnostic_time_field

    batch_obs_target_path = os.path.join(
        config_obj.pre_diagnostics_batch_path, str(current_pat_client_id_code) + ".csv"
    )
    existence_check = exist_check(batch_obs_target_path, config_obj)

    try:
        if (
            config_obj.store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="order",
                fields_list="""client_idcode order_guid order_name order_summaryline order_holdreasontext order_entered clientvisit_visitidcode order_performeddtm order_createdwhen""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f'order_typecode:"diagnostic" AND '
                f"{diagnosic_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            if (
                config_obj.store_pat_batch_docs
                or config_obj.overwrite_stored_pat_observations
            ):

                batch_target.to_csv(batch_obs_target_path)
        else:
            batch_target = pd.read_csv(batch_obs_target_path)
        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch diagnostic orders: {e}")
        return pd.DataFrame()


def get_pat_batch_epr_docs(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of EPR documents for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of EPR documents.
    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs

    split_clinical_notes_bool = config_obj.split_clinical_notes

    batch_epr_target_path = os.path.join(
        config_obj.pre_document_batch_path, str(current_pat_client_id_code) + ".csv"
    )

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    global_start_year = str(global_start_year).zfill(4)
    global_start_month = str(global_start_month).zfill(2)
    global_end_year = str(global_end_year).zfill(4)
    global_end_month = str(global_end_month).zfill(2)

    global_start_day = str(global_start_day).zfill(2)
    global_end_day = str(global_end_day).zfill(2)

    if config_obj.verbosity >= 6:
        logging.debug("batch_epr_target_path: %s", batch_epr_target_path)
        logging.debug("global_start_year: %s", global_start_year)
        logging.debug("global_start_month: %s", global_start_month)
        logging.debug("global_end_year: %s", global_end_year)
        logging.debug("global_end_month: %s", global_end_month)
        logging.debug("global_start_day: %s", global_start_day)
        logging.debug("global_end_day: %s", global_end_day)

    existence_check = exist_check(batch_epr_target_path, config_obj)

    try:

        if (
            overwrite_stored_pat_docs
            and not existence_check
            or existence_check is False
        ):

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="epr_documents",
                fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if config_obj.data_type_filter_dict is not None and not batch_target.empty:
                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists").get(
                        "epr_docs"
                    )
                    is not None
                ):

                    if config_obj.verbosity >= 1:
                        logging.info(
                            "applying doc type filter to EPR docs",
                            config_obj.data_type_filter_dict,
                        )

                        filter_term_list = config_obj.data_type_filter_dict.get(
                            "filter_term_lists"
                        ).get("epr_docs")

                        batch_target = filter_dataframe_by_fuzzy_terms(
                            batch_target,
                            filter_term_list,
                            column_name="document_description",
                            verbose=config_obj.verbosity,
                        )

                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists").get(
                        "epr_docs_term_regex"
                    )
                    is not None
                ):
                    if config_obj.verbosity > 1:
                        logging.debug("append_regex_term_counts...")
                        display(batch_target)
                    batch_target = append_regex_term_counts(
                        df=batch_target,
                        terms=config_obj.data_type_filter_dict.get(
                            "filter_term_lists"
                        ).get("epr_docs_term_regex"),
                        text_column="body_analysed",
                        debug=config_obj.verbosity > 5,
                    )
            # display(batch_target)

            if config_obj.store_pat_batch_docs or overwrite_stored_pat_docs:
                # batch_target.dropna(subset='body_analysed', inplace=True)

                if config_obj.verbosity >= 3:
                    logging.debug("get_epr_docs_predropna: %d", len(batch_target))

                col_list_drop_nan = ["body_analysed", "updatetime", "client_idcode"]

                rows_with_nan = batch_target[
                    batch_target[col_list_drop_nan].isna().any(axis=1)
                ]

                # Drop rows with NaN values
                batch_target = batch_target.drop(rows_with_nan.index).copy()

                if config_obj.verbosity >= 3:
                    logging.debug("get_epr_docs_postdropna: %d", len(batch_target))

                if split_clinical_notes_bool:

                    batch_target = split_and_append_chunks(batch_target, epr=True)

                    # if drop out of range notes, filter batch_target by global date. before writing.

                    if config_obj.filter_split_notes:

                        pre_filter_split_notes_len = len(batch_target)

                        # reuse dataframe filter
                        batch_target = filter_dataframe_by_timestamp(
                            df=batch_target,
                            start_year=int(global_start_year),
                            start_month=int(global_start_month),
                            end_year=int(global_end_year),
                            end_month=int(global_end_month),
                            start_day=int(global_start_day),
                            end_day=int(global_end_day),
                            timestamp_string="updatetime",
                            dropna=False,
                        )
                        if config_obj.verbosity > 2:
                            logging.debug(
                                f"pre_filter_split_notes_len: {pre_filter_split_notes_len}"
                            )
                            logging.debug(
                                f"post_filter_split_notes_len: {len(batch_target)}"
                            )

                batch_target.to_csv(batch_epr_target_path)

        else:
            batch_target = pd.read_csv(batch_epr_target_path)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch EPR documents: {e}")
        raise UnboundLocalError("Error retrieving batch EPR documents.")
        # return []


def get_pat_batch_epr_docs_annotations(
    current_pat_client_id_code: str, config_obj: Any, cat: Any, t: Any
) -> pd.DataFrame:
    """Retrieves or creates annotations for a patient's EPR document batch.

    This function checks if an annotation file for the patient's EPR documents
    already exists. If so, it reads it. If not, it reads the raw document
    batch, generates annotations using the provided MedCAT model, and saves
    the result.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The main configuration object.
        cat: The loaded MedCAT `CAT` object.
        t: The tqdm progress bar instance.

    Returns:
        A DataFrame containing the annotations for the patient's EPR documents.
    """
    batch_epr_target_path = os.path.join(
        config_obj.pre_document_batch_path, str(current_pat_client_id_code) + ".csv"
    )

    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_id_code + ".csv"
    )

    if exist_check(current_pat_document_annotation_batch_path, config_obj=config_obj):

        batch_target = pd.read_csv(current_pat_document_annotation_batch_path)

    else:

        pat_batch = pd.read_csv(batch_epr_target_path)

        pat_batch.dropna(subset=["body_analysed"], axis=0, inplace=True)

        batch_target = get_pat_document_annotation_batch(
            current_pat_client_idcode=current_pat_client_id_code,
            pat_batch=pat_batch,
            cat=cat,
            config_obj=config_obj,
            t=t,
        )

    return batch_target


def get_pat_batch_mct_docs_annotations(
    current_pat_client_id_code: str, config_obj: Any, cat: Any, t: Any
) -> pd.DataFrame:
    """Retrieves or creates annotations for a patient's MCT document batch.

    This function checks if an annotation file for the patient's MCT documents
    already exists. If so, it reads it. If not, it reads the raw document
    batch, generates annotations using the provided MedCAT model, and saves
    the result.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The main configuration object.
        cat: The loaded MedCAT `CAT` object.
        t: The tqdm progress bar instance.

    Returns:
        A DataFrame containing the annotations for the patient's MCT documents.
    """
    batch_epr_target_path_mct = os.path.join(
        config_obj.pre_document_batch_path_mct, str(current_pat_client_id_code) + ".csv"
    )

    pre_document_annotation_batch_path_mct = (
        config_obj.pre_document_annotation_batch_path_mct
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path_mct, current_pat_client_id_code + ".csv"
    )

    if exist_check(current_pat_document_annotation_batch_path, config_obj=config_obj):

        # if annotation batch already created, read it

        batch_target = pd.read_csv(current_pat_document_annotation_batch_path)

    else:

        pat_batch = pd.read_csv(batch_epr_target_path_mct)

        pat_batch.dropna(
            subset=["observation_valuetext_analysed"], axis=0, inplace=True
        )

        batch_target = get_pat_document_annotation_batch_mct(
            current_pat_client_idcode=current_pat_client_id_code,
            pat_batch=pat_batch,
            cat=cat,
            config_obj=config_obj,
            t=t,
        )

    return batch_target


def get_pat_batch_textual_obs_annotations(
    current_pat_client_id_code: str, config_obj: Any, cat: Any, t: Any
) -> pd.DataFrame:
    """Retrieves or creates annotations for a patient's textual observation batch.

    This function checks if an annotation file for the patient's textual
    observations already exists. If so, it reads it. If not, it reads the raw
    observation batch, generates annotations using the provided MedCAT model,
    and saves the result.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The main configuration object.
        cat: The loaded MedCAT `CAT` object.
        t: The tqdm progress bar instance.

    Returns:
        A DataFrame containing the annotations for the patient's textual
        observations.
    """

    batch_textual_obs_document_path = os.path.join(
        config_obj.pre_textual_obs_document_batch_path,
        str(current_pat_client_id_code) + ".csv",
    )

    pre_textual_obs_annotation_batch_path = (
        config_obj.pre_textual_obs_annotation_batch_path
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_textual_obs_annotation_batch_path, current_pat_client_id_code + ".csv"
    )

    if exist_check(current_pat_document_annotation_batch_path, config_obj=config_obj):

        # if annotation batch already created, read it

        batch_target = pd.read_csv(current_pat_document_annotation_batch_path)

    else:

        pat_batch = pd.read_csv(batch_textual_obs_document_path)

        pat_batch.dropna(subset=["textualObs"], axis=0, inplace=True)

        pat_batch = pat_batch[pat_batch["textualObs"] != ""]

        batch_target = get_pat_batch_textual_obs_annotation_batch(
            current_pat_client_idcode=current_pat_client_id_code,
            pat_batch=pat_batch,
            cat=cat,
            config_obj=config_obj,
            t=t,
        )

    return batch_target


def get_pat_batch_reports_docs_annotations(
    current_pat_client_id_code: str, config_obj: Any, cat: Any, t: Any
) -> pd.DataFrame:
    """Retrieves or creates annotations for a patient's reports batch.

    This function checks if an annotation file for the patient's reports
    already exists. If so, it reads it. If not, it reads the raw reports
    batch, generates annotations using the provided MedCAT model, and saves
    the result.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        config_obj: The main configuration object.
        cat: The loaded MedCAT `CAT` object.
        t: The tqdm progress bar instance.

    Returns:
        A DataFrame containing the annotations for the patient's reports.
    """
    batch_reports_target_path_report = os.path.join(
        config_obj.pre_document_batch_path_reports,
        str(current_pat_client_id_code) + ".csv",
    )

    pre_document_annotation_batch_path_reports = (
        config_obj.pre_document_annotation_batch_path_reports
    )

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path_reports, current_pat_client_id_code + ".csv"
    )

    if exist_check(current_pat_document_annotation_batch_path, config_obj=config_obj):
        batch_target = pd.read_csv(current_pat_document_annotation_batch_path)
    else:
        pat_batch = pd.read_csv(batch_reports_target_path_report)
        pat_batch.dropna(
            subset=["body_analysed"], axis=0, inplace=True
        )  # composite of textual obs and value analysed concat
        batch_target = get_pat_document_annotation_batch_reports(
            current_pat_client_idcode=current_pat_client_id_code,
            pat_batch=pat_batch,
            cat=cat,
            config_obj=config_obj,
            t=t,
        )

    return batch_target


def get_pat_batch_mct_docs(
    current_pat_client_id_code: str,
    search_term: str,  # noqa
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of MCT (MRC clinical notes) documents for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of MCT documents.
    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs

    split_clinical_notes_bool = config_obj.split_clinical_notes

    batch_epr_target_path_mct = os.path.join(
        config_obj.pre_document_batch_path_mct, str(current_pat_client_id_code) + ".csv"
    )

    existence_check = exist_check(batch_epr_target_path_mct, config_obj)

    try:
        if overwrite_stored_pat_docs or existence_check is False:

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f'obscatalogmasteritem_displayname:("AoMRC_ClinicalSummary_FT") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            batch_target = apply_data_type_mct_docs_filters(config_obj, batch_target)

            if config_obj.store_pat_batch_docs or overwrite_stored_pat_docs:

                if config_obj.verbosity >= 3:
                    logging.debug("get_epr_mct_docs_predropna: %d", len(batch_target))
                col_list_drop_nan = [
                    "observation_valuetext_analysed",
                    "observationdocument_recordeddtm",
                    "client_idcode",
                ]

                rows_with_nan = batch_target[
                    batch_target[col_list_drop_nan].isna().any(axis=1)
                ]

                # Drop rows with NaN values
                batch_target = batch_target.drop(rows_with_nan.index).copy()

                if config_obj.verbosity >= 3:
                    logging.debug("get_epr_mct_docs_postdropna: %d", len(batch_target))

                if split_clinical_notes_bool:

                    batch_target = split_and_append_chunks(
                        batch_target, epr=False, mct=True
                    )

                batch_target.to_csv(batch_epr_target_path_mct)
        else:
            batch_target = pd.read_csv(batch_epr_target_path_mct)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch MCT documents: {e}")
        return pd.DataFrame()


def get_pat_batch_demo(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of demographic information for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of demographic information.
    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    batch_obs_target_path = os.path.join(
        config_obj.pre_demo_batch_path, str(current_pat_client_id_code) + ".csv"
    )
    existence_check = exist_check(batch_obs_target_path, config_obj)

    try:
        if (
            config_obj.store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="epr_documents",
                fields_list=[
                    "client_idcode",
                    "client_firstname",
                    "client_lastname",
                    "client_dob",
                    "client_gendercode",
                    "client_racecode",
                    "client_deceaseddtm",
                    "updatetime",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )
            if (
                config_obj.store_pat_batch_docs
                or config_obj.overwrite_stored_pat_observations
            ):

                batch_target.to_csv(batch_obs_target_path)
        else:
            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch demographic information: {e}")
        return pd.DataFrame()


def get_pat_batch_reports(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of reports for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The specific report type to search for.
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of reports.
    """

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations

    search_term = "report"

    batch_obs_target_path = os.path.join(
        config_obj.pre_document_batch_path_reports,
        str(current_pat_client_id_code) + ".csv",
    )

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    existence_check = exist_check(batch_obs_target_path, config_obj)

    try:
        if (
            store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "updatetime",
                    "textualObs",
                    "basicobs_guid",
                    "basicobs_value_analysed",
                    "basicobs_itemname_analysed",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f"basicobs_itemname_analysed:{search_term} AND "
                f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            batch_target["body_analysed"] = (
                batch_target["textualObs"].astype(str)
                + "\n"
                + batch_target["basicobs_value_analysed"].astype(str)
            )

            if config_obj.store_pat_batch_docs or overwrite_stored_pat_observations:
                batch_target.to_csv(batch_obs_target_path)

        else:

            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch reports: {e}")
        return pd.DataFrame()


def get_pat_batch_textual_obs_docs(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of textual observation documents for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of textual observation documents.
    """

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations

    bloods_time_field = config_obj.bloods_time_field

    batch_obs_target_path = os.path.join(
        config_obj.pre_textual_obs_document_batch_path,
        str(current_pat_client_id_code) + ".csv",
    )

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    existence_check = exist_check(batch_obs_target_path, config_obj)

    try:
        if (
            store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "basicobs_itemname_analysed",
                    "basicobs_value_numeric",
                    "basicobs_value_analysed",
                    "basicobs_entered",
                    "clientvisit_serviceguid",
                    "basicobs_guid",
                    "updatetime",
                    "textualObs",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=""
                + f"{bloods_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            # Drop rows with no textualObs
            batch_target = batch_target.dropna(subset=["textualObs"])
            # Drop rows with empty string in textualObs
            batch_target = batch_target[batch_target["textualObs"] != ""]

            batch_target["body_analysed"] = batch_target["textualObs"].astype(str)

            if config_obj.store_pat_batch_docs or overwrite_stored_pat_observations:
                batch_target.to_csv(batch_obs_target_path)

        else:

            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch textualObs: {e}")
        return pd.DataFrame()


def get_pat_batch_appointments(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of appointments for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of appointments.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    appointments_time_field = config_obj.appointments_time_field

    appointments_target_path = os.path.join(
        config_obj.pre_appointments_batch_path, str(current_pat_client_id_code) + ".csv"
    )
    existence_check = exist_check(appointments_target_path, config_obj)

    try:
        if (
            config_obj.store_pat_batch_observations
            and not existence_check
            or existence_check is False
        ):
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="pims_apps*",
                fields_list=[
                    "Popular",
                    "AppointmentType",
                    "AttendanceReference",
                    "ClinicCode",
                    "ClinicDesc",
                    "Consultant",
                    "DateModified",
                    "DNA",
                    "HospitalID",
                    "PatNHSNo",
                    "Specialty",
                    "AppointmentDateTime",
                    "Attended",
                    "CancDesc",
                    "CancRefNo",
                    "ConsultantCode",
                    "DateCreated",
                    "Ethnicity",
                    "Gender",
                    "NHSNoStatusCode",
                    "NotSpec",
                    "PatDateOfBirth",
                    "PatForename",
                    "PatPostCode",
                    "PatSurname",
                    "PiMsPatRefNo",
                    "Primarykeyfieldname",
                    "Primarykeyfieldvalue",
                    "SessionCode",
                    "SpecialtyCode",
                ],
                term_name="HospitalID.keyword",  # alt HospitalID.keyword #warn non case
                entered_list=[current_pat_client_id_code],
                search_string=f"{appointments_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_docs
                or config_obj.overwrite_stored_pat_observations
            ):

                batch_target.to_csv(appointments_target_path)
        else:
            batch_target = pd.read_csv(appointments_target_path)
        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch appointments orders: {e}")
        return pd.DataFrame()
