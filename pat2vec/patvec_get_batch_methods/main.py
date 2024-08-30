import os

import pandas as pd
from clinical_note_splitter.clinical_notes_splitter import split_and_append_chunks
from IPython.display import display

from pat2vec.util.filter_methods import filter_dataframe_by_fuzzy_terms
from pat2vec.util.filter_methods import (
    apply_bloods_data_type_filter,
    apply_data_type_epr_docs_filters,
    apply_data_type_mct_docs_filters,
    filter_dataframe_by_fuzzy_terms,
)
from pat2vec.util.methods_annotation import (
    get_pat_batch_textual_obs_annotation_batch,
    get_pat_document_annotation_batch,
    get_pat_document_annotation_batch_mct,
    get_pat_document_annotation_batch_reports,
)
from pat2vec.util.methods_annotation_regex import append_regex_term_counts
from pat2vec.util.methods_get import exist_check
from pat2vec.util.methods_get import filter_dataframe_by_timestamp


def get_pat_batch_obs(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch observations for a patient based on the given parameters.

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching observations.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of observations.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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
        if config_obj.store_pat_batch_observations or existence_check is False:
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

        print(f"Error retrieving batch observations: {e}")
        return []


def get_pat_batch_news(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch observations for a patient based on the given parameters, specifically for NEWS observations.

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching NEWS observations.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of NEWS observations.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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
        if config_obj.store_pat_batch_observations or existence_check is False:
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

        print(f"Error retrieving batch NEWS observations: {e}")
        return []


def get_pat_batch_bmi(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch observations for a patient based on the given parameters, specifically for BMI-related observations.

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching BMI-related observations.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of BMI-related observations.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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
        if config_obj.store_pat_batch_observations or existence_check is False:
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
        print(f"Error retrieving batch BMI-related observations: {e}")
        return []


def get_pat_batch_bloods(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch basic observations for a patient based on the given parameters, specifically for blood tests.

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching blood test-related observations.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of blood test-related observations.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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
        if store_pat_batch_observations or existence_check is False:

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
                        print(
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
        print(f"Error retrieving batch blood test-related observations: {e}")
        return []


def get_pat_batch_drugs(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch medication orders for a patient based on the given parameters.

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching medication orders.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of medication orders.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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

        if config_obj.store_pat_batch_observations or existence_check is False:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="order",
                fields_list="""client_idcode order_guid order_name order_summaryline order_holdreasontext order_entered clientvisit_visitidcode order_performeddtm""".split(),
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
        print(f"Error retrieving batch medication orders: {e}")
        return []


def get_pat_batch_diagnostics(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch diagnostic orders for a patient based on the given parameters.

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching diagnostic orders.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of diagnostic orders.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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
        if config_obj.store_pat_batch_observations or existence_check is False:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="order",
                fields_list="""client_idcode order_guid order_name order_summaryline order_holdreasontext order_entered clientvisit_visitidcode order_performeddtm""".split(),
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
        print(f"Error retrieving batch diagnostic orders: {e}")
        return []


def get_pat_batch_epr_docs(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch EPR documents for a patient based on the given parameters.

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching EPR documents.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of EPR documents.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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
    store_pat_batch_docs = config_obj.store_pat_batch_docs

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
        print("batch_epr_target_path:", batch_epr_target_path)
        print("global_start_year:", global_start_year)
        print("global_start_month:", global_start_month)
        print("global_end_year:", global_end_year)
        print("global_end_month:", global_end_month)
        print("global_start_day:", global_start_day)
        print("global_end_day:", global_end_day)

    existence_check = exist_check(batch_epr_target_path, config_obj)

    try:

        if overwrite_stored_pat_docs or existence_check is False:

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
                        print(
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
                        print("append_regex_term_counts...")
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
                    print("get_epr_docs_predropna", len(batch_target))

                col_list_drop_nan = ["body_analysed", "updatetime", "client_idcode"]

                rows_with_nan = batch_target[
                    batch_target[col_list_drop_nan].isna().any(axis=1)
                ]

                # Drop rows with NaN values
                batch_target = batch_target.drop(rows_with_nan.index).copy()

                if config_obj.verbosity >= 3:
                    print("get_epr_docs_postdropna", len(batch_target))

                # #handle non datetime obs recorded
                # batch_target['updatetime'] = pd.to_datetime(batch_target['updatetime'], errors='coerce')
                # batch_target.dropna(subset=['updatetime'], inplace=True)
                # if(config_obj.verbosity >= 3):
                #     print('get_epr_mct_docs_postdropna on dt col', len(batch_target))
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
                            print(
                                f"pre_filter_split_notes_len: {pre_filter_split_notes_len}"
                            )
                            print(f"post_filter_split_notes_len: {len(batch_target)}")

                batch_target.to_csv(batch_epr_target_path)

        else:
            batch_target = pd.read_csv(batch_epr_target_path)

        return batch_target
    except Exception as e:
        """"""
        print(f"Error retrieving batch EPR documents: {e}")
        raise UnboundLocalError("Error retrieving batch EPR documents.")
        # return []


def get_pat_batch_epr_docs_annotations(
    current_pat_client_id_code, config_obj=None, cat=None, t=None
):

    batch_epr_target_path = os.path.join(
        config_obj.pre_document_batch_path, str(current_pat_client_id_code) + ".csv"
    )

    # print(batch_epr_target_path)
    # cat = config_obj.cat

    # t = config_obj.t

    # add read existing if exist here..
    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path

    current_pat_document_annotation_batch_path = os.path.join(
        pre_document_annotation_batch_path, current_pat_client_id_code + ".csv"
    )

    if exist_check(current_pat_document_annotation_batch_path, config_obj=config_obj):

        # if annotation batch already created, read it

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
    current_pat_client_id_code, config_obj=None, cat=None, t=None
):

    batch_epr_target_path_mct = os.path.join(
        config_obj.pre_document_batch_path_mct, str(current_pat_client_id_code) + ".csv"
    )

    # cat = config_obj.cat

    # t = config_obj.t

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
    current_pat_client_id_code, config_obj=None, cat=None, t=None
):

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
    current_pat_client_id_code, config_obj=None, cat=None, t=None
):
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
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch MCT documents for a patient based on the given parameters.

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching MCT documents.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of MCT documents.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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
    store_pat_batch_docs = config_obj.store_pat_batch_docs

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
                # batch_target.dropna(subset='observation_valuetext_analysed', inplace=True)
                if config_obj.verbosity >= 3:
                    print("get_epr_mct_docs_predropna", len(batch_target))
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
                    print("get_epr_mct_docs_postdropna", len(batch_target))

                # #handle non datetime obs recorded
                # batch_target['observationdocument_recordeddtm'] = pd.to_datetime(batch_target['observationdocument_recordeddtm'], errors='coerce')
                # batch_target.dropna(subset=['observationdocument_recordeddtm'], inplace=True)
                # if(config_obj.verbosity >= 3):
                #     print('get_epr_mct_docs_postdropna on dt col', len(batch_target))

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
        print(f"Error retrieving batch MCT documents: {e}")
        return []


def get_pat_batch_demo(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch demographic information for a patient based on the given parameters.

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching demographic information.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of demographic information.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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
        if config_obj.store_pat_batch_observations or existence_check is False:
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
        print(f"Error retrieving batch demographic information: {e}")
        return []


def get_pat_batch_reports(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch reports for a patient based on the given parameters.
    Filter reports with doc type filter arg set in config: KHMDC >>>> retreives KHMDC reports

    Args:
        current_pat_client_id_code (str): The client ID code for the current patient.
        search_term (str): The term used for searching report-related observations.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        list: Batch of report-related observations.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
    """

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations

    search_term = "report"

    batch_obs_target_path = os.path.join(
        config_obj.pre_document_batch_path_reports,
        str(current_pat_client_id_code) + ".csv",
    )

    # os.makedirs(batch_obs_target_path, exist_ok=True)

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
        if store_pat_batch_observations or existence_check is False:

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

            # batch_target = batch_target.rename(columns={'textualObs': 'body_analysed'})
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
        print(f"Error retrieving batch reports: {e}")
        return []


def get_pat_batch_textual_obs_docs(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch textual observations for a patient based on the given parameters.

    Parameters:
        current_pat_client_id_code (str): The client ID code for the current patient.

        config_obj (ConfigObject): An object containing global start and end year/month. (Default: None)
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms. (Default: None)

    Returns:
        list: Batch of textualObs observations.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
    """

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations

    # search_term = "report"

    bloods_time_field = config_obj.bloods_time_field

    batch_obs_target_path = os.path.join(
        config_obj.pre_textual_obs_document_batch_path,
        str(current_pat_client_id_code) + ".csv",
    )
    # os.makedirs(batch_obs_target_path, exist_ok=True)

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
        if store_pat_batch_observations or existence_check is False:

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
                search_string=f""
                + f"{bloods_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            # Drop rows with no textualObs
            batch_target = batch_target.dropna(subset=["textualObs"])
            # Drop rows with empty string in textualObs
            batch_target = batch_target[batch_target["textualObs"] != ""]

            # batch_target = batch_target.rename(columns={'textualObs': 'body_analysed'})
            # batch_target["body_analysed"] = (
            #     batch_target["textualObs"].astype(str)
            #     + "\n"
            #     + batch_target["basicobs_value_analysed"].astype(str)
            # )
            batch_target["body_analysed"] = batch_target["textualObs"].astype(str)

            if config_obj.store_pat_batch_docs or overwrite_stored_pat_observations:
                batch_target.to_csv(batch_obs_target_path)

        else:

            batch_target = pd.read_csv(batch_obs_target_path)

        return batch_target
    except Exception as e:
        """"""
        print(f"Error retrieving batch textualObs: {e}")
        return []


def get_pat_batch_appointments(
    current_pat_client_id_code,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """ """
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
        if config_obj.store_pat_batch_observations or existence_check is False:
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
        print(f"Error retrieving batch appointments orders: {e}")
        return []
