import os

import pandas as pd
from util.methods_annotation import get_pat_document_annotation_batch, get_pat_document_annotation_batch_mct

from util.methods_get import exist_check


def get_pat_batch_obs(current_pat_client_id_code, search_term, config_obj=None, cohort_searcher_with_terms_and_search=None):
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
    if config_obj is None or not all(hasattr(config_obj, attr) for attr in ['global_start_year', 'global_start_month', 'global_end_year', 'global_end_month']):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month

    try:
        batch_target = cohort_searcher_with_terms_and_search(
            index_name="observations",
            fields_list="""observation_guid client_idcode	obscatalogmasteritem_displayname
                            observation_valuetext_analysed observationdocument_recordeddtm 
                            clientvisit_visitidcode""".split(),
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f"obscatalogmasteritem_displayname:(\"{search_term}\") AND "
                          f'observationdocument_recordeddtm:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
        )
        return batch_target
    except Exception as e:
        
        print(f"Error retrieving batch observations: {e}")
        return []




def get_pat_batch_news(current_pat_client_id_code, search_term, config_obj=None, cohort_searcher_with_terms_and_search=None):
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
    if config_obj is None or not all(hasattr(config_obj, attr) for attr in ['global_start_year', 'global_start_month', 'global_end_year', 'global_end_month']):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month

    try:
        batch_target = cohort_searcher_with_terms_and_search(
            index_name="observations",
            fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                            observation_valuetext_analysed observationdocument_recordeddtm 
                            clientvisit_visitidcode""".split(),
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f'obscatalogmasteritem_displayname:("NEWS" OR "NEWS2") AND '
                          f'observationdocument_recordeddtm:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
        )
        return batch_target
    except Exception as e:
        
        print(f"Error retrieving batch NEWS observations: {e}")
        return []




def get_pat_batch_bmi(current_pat_client_id_code, search_term, config_obj=None, cohort_searcher_with_terms_and_search=None):
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
    if config_obj is None or not all(hasattr(config_obj, attr) for attr in ['global_start_year', 'global_start_month', 'global_end_year', 'global_end_month']):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month

    try:
        batch_target = cohort_searcher_with_terms_and_search(
            index_name="observations",
            fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                            observation_valuetext_analysed observationdocument_recordeddtm 
                            clientvisit_visitidcode""".split(),
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f'obscatalogmasteritem_displayname:("OBS BMI" OR "OBS Weight" OR "OBS height") AND '
                          f'observationdocument_recordeddtm:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
        )
        return batch_target
    except Exception as e:
        ""
        print(f"Error retrieving batch BMI-related observations: {e}")
        return []



def get_pat_batch_bloods(current_pat_client_id_code, search_term, config_obj=None, cohort_searcher_with_terms_and_search=None):
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
    if config_obj is None or not all(hasattr(config_obj, attr) for attr in ['global_start_year', 'global_start_month', 'global_end_year', 'global_end_month']):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month

    try:
        batch_target = cohort_searcher_with_terms_and_search(
            index_name="basic_observations",
            fields_list=["client_idcode", "basicobs_itemname_analysed", "basicobs_value_numeric", "basicobs_entered", "clientvisit_serviceguid"],
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f'basicobs_value_numeric:* AND '
                          f'updatetime:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
        )
        return batch_target
    except Exception as e:
        ""
        print(f"Error retrieving batch blood test-related observations: {e}")
        return []



def get_pat_batch_drugs(current_pat_client_id_code, search_term, config_obj=None, cohort_searcher_with_terms_and_search=None):
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
    if config_obj is None or not all(hasattr(config_obj, attr) for attr in ['global_start_year', 'global_start_month', 'global_end_year', 'global_end_month']):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month

    try:
        batch_target = cohort_searcher_with_terms_and_search(
            index_name="order",
            fields_list="""client_idcode order_guid order_name order_summaryline order_holdreasontext order_entered clientvisit_visitidcode""".split(),
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f'order_typecode:"medication" AND '
                          f'updatetime:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
        )
        return batch_target
    except Exception as e:
        ""
        print(f"Error retrieving batch medication orders: {e}")
        return []



def get_pat_batch_diagnostics(current_pat_client_id_code, search_term, config_obj=None, cohort_searcher_with_terms_and_search=None):
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
    if config_obj is None or not all(hasattr(config_obj, attr) for attr in ['global_start_year', 'global_start_month', 'global_end_year', 'global_end_month']):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month

    try:
        batch_target = cohort_searcher_with_terms_and_search(
            index_name="order",
            fields_list="""client_idcode order_guid order_name order_summaryline order_holdreasontext order_entered clientvisit_visitidcode""".split(),
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f'order_typecode:"diagnostic" AND '
                          f'updatetime:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
        )
        return batch_target
    except Exception as e:
        ""
        print(f"Error retrieving batch diagnostic orders: {e}")
        return []



def get_pat_batch_epr_docs(current_pat_client_id_code, search_term, config_obj=None, cohort_searcher_with_terms_and_search=None):
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
    if config_obj is None or not all(hasattr(config_obj, attr) for attr in ['global_start_year', 'global_start_month', 'global_end_year', 'global_end_month']):
        raise ValueError("Invalid or missing configuration object.")


    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs
    store_pat_batch_docs = config_obj.store_pat_batch_docs
    
    
    batch_epr_target_path = os.path.join(config_obj.pre_document_batch_path, str(current_pat_client_id_code) + ".csv")

    if(config_obj.verbosity >=6):
        print("batch_epr_target_path", batch_epr_target_path)

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month



    try:
        
        if(overwrite_stored_pat_docs or exist_check(batch_epr_target_path, config_obj) is False):
        
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="epr_documents",
                fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                term_name="client_idcode.keyword",
                entered_list=[current_pat_client_id_code],
                search_string=f'updatetime:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
            )
            
            
            
            
            if(config_obj.store_pat_batch_docs or overwrite_stored_pat_docs):
                batch_target.to_csv(batch_epr_target_path)
            
        
        
        return batch_target
    except Exception as e:
        ""
        print(f"Error retrieving batch EPR documents: {e}")
        raise UnboundLocalError("Error retrieving batch EPR documents.")
        #return []


def get_pat_batch_epr_docs_annotations(current_pat_client_id_code, config_obj = None, cat=None, t=None):
    
    batch_epr_target_path = os.path.join(config_obj.pre_document_batch_path, str(current_pat_client_id_code) + ".csv")
    
    #cat = config_obj.cat
    
    #t = config_obj.t
    
    pat_batch = pd.read_csv(batch_epr_target_path)
    
    batch_target = get_pat_document_annotation_batch(current_pat_client_idcode = current_pat_client_id_code, pat_batch=pat_batch, cat=cat, config_obj=config_obj, t=t)

    return batch_target



def get_pat_batch_mct_docs_annotations(current_pat_client_id_code, config_obj = None, cat=None, t=None):
    
    batch_epr_target_path_mct = os.path.join(config_obj.pre_document_batch_path_mct, str(current_pat_client_id_code) + ".csv")
    
    #cat = config_obj.cat
    
    #t = config_obj.t
    
    pat_batch = pd.read_csv(batch_epr_target_path_mct)
    
    batch_target = get_pat_document_annotation_batch_mct(current_pat_client_idcode = current_pat_client_id_code, pat_batch=pat_batch, cat=cat, config_obj=config_obj, t=t)

    return batch_target

def get_pat_batch_mct_docs(current_pat_client_id_code, search_term, config_obj=None, cohort_searcher_with_terms_and_search=None):
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
    if config_obj is None or not all(hasattr(config_obj, attr) for attr in ['global_start_year', 'global_start_month', 'global_end_year', 'global_end_month']):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    
    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs
    store_pat_batch_docs = config_obj.store_pat_batch_docs
    
    batch_epr_target_path_mct = os.path.join(config_obj.pre_document_batch_path_mct, str(current_pat_client_id_code) + ".csv")

    try:
        if(overwrite_stored_pat_docs or exist_check(batch_epr_target_path_mct, config_obj) is False):
        
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm 
                                clientvisit_visitidcode""".split(),
                term_name="client_idcode.keyword",
                entered_list=[current_pat_client_id_code],
                search_string=f'obscatalogmasteritem_displayname:("AoMRC_ClinicalSummary_FT") AND '
                            f'observationdocument_recordeddtm:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
            )
            
            if(config_obj.store_pat_batch_docs or overwrite_stored_pat_docs):
                batch_target.to_csv(batch_epr_target_path_mct)
            
            
        return batch_target
    except Exception as e:
        ""
        print(f"Error retrieving batch MCT documents: {e}")
        return []


def get_pat_batch_demo(current_pat_client_id_code, search_term, config_obj=None, cohort_searcher_with_terms_and_search=None):
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
    if config_obj is None or not all(hasattr(config_obj, attr) for attr in ['global_start_year', 'global_start_month', 'global_end_year', 'global_end_month']):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month

    try:
        batch_target = cohort_searcher_with_terms_and_search(
            index_name="epr_documents",
            fields_list=["client_idcode", "client_firstname", "client_lastname", "client_dob", "client_gendercode", "client_racecode", "client_deceaseddtm", "updatetime"],
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f'updatetime:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
        )
        return batch_target
    except Exception as e:
        ""
        print(f"Error retrieving batch demographic information: {e}")
        return []






