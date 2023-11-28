
import time
from IPython.display import display
from IPython.utils import io

from util.methods_get import (dump_results, exist_check,
                              filter_dataframe_by_timestamp,
                              get_start_end_year_month, update_pbar)


def get_current_pat_annotations_to_file(current_pat_client_id_code, target_date_range, config_obj = None, cohort_searcher_with_terms_and_search=None, cat=None, t=None ):
    
    if config_obj is None:
        raise ValueError("config_obj cannot be None. Please provide a valid configuration. get_current_pat_annotations_to_file")

    
    pre_annotation_path = config_obj.pre_annotation_path
    
    store_annot = config_obj.store_annot
    
    
    start_time = time.time()
    
    file_exists = exist_check(pre_annotation_path + current_pat_client_id_code+"_"+str(target_date_range), config_obj = config_obj)
    
    if(file_exists == False):
    
        start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)

        current_pat_docs = cohort_searcher_with_terms_and_search(index_name="epr_documents", 
                                                                  fields_list = """client_idcode document_guid	document_description	body_analysed updatetime clientvisit_visitidcode""".split(),
                                                                  term_name = "client_idcode.keyword", 
                                                                  entered_list = [current_pat_client_id_code],
                                                                search_string = f'updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}] ')



        n_docs_to_annotate = len(current_pat_docs)
        update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations', n_docs_to_annotate = n_docs_to_annotate,
                    t=t, config_obj=config_obj)

    annotation_map = {'True':1,
                     'Presence':1 ,
                     'Recent': 1,
                     'Past':0,
                     'Subject/Experiencer':1,
                     'Other':0,
                     'Hypothetical':0,
                     'Patient': 1}

    #remove filter from cdb?
    #print("getting annotations")
    
#     file_exists = exists(pre_annotation_path + current_pat_client_id_code)
    
    
    if(file_exists==False):
        with io.capture_output() as captured:
            pats_anno_annotations = cat.get_entities_multi_texts(current_pat_docs['body_analysed'].dropna());#, n_process=1
        if(store_annot):
            dump_results(pats_anno_annotations, pre_annotation_path + current_pat_client_id_code+"_"+str(target_date_range), config_obj =config_obj)
    
    


def get_current_pat_annotations_mct_batch_to_file(current_pat_client_id_code, target_date_range, pat_doc_batch, skip_check=False, config_obj =None, cat=None, t=None):
    
    store_annot = config_obj.store_annot
    
    pre_annotation_path_mrc = config_obj.pre_annotation_path_mrc
    
    start_time = time.time()
    
    current_annot_file_path = pre_annotation_path_mrc + current_pat_client_id_code + "/" + current_pat_client_id_code+"_"+str(target_date_range)
    
    if(skip_check):
        file_exists = False
    else:
        file_exists = exist_check(current_annot_file_path, config_obj = config_obj)

    if(file_exists == False):
    
        #start_year, start_month, end_year, end_month = get_start_end_year_month(target_date_range)
        
        start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)

        current_pat_docs = filter_dataframe_by_timestamp(pat_doc_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'observationdocument_recordeddtm')


        n_docs_to_annotate = len(current_pat_docs)
        update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations_mct', n_docs_to_annotate = n_docs_to_annotate,
                    t=t, config_obj=config_obj)

    
    if(file_exists==False):
        with io.capture_output() as captured:
            pats_anno_annotations = cat.get_entities_multi_texts(current_pat_docs['observation_valuetext_analysed'].dropna());#, n_process=1
        if(store_annot):
            dump_results(pats_anno_annotations, current_annot_file_path, config_obj=config_obj)



def get_current_pat_annotations_batch_to_file(current_pat_client_id_code, target_date_range, pat_doc_batch, sftp_obj=None, skip_check=False, config_obj=None, cat=None, t=None):
    
    store_annot = config_obj.store_annot
    
    pre_annotation_path = config_obj.pre_annotation_path
    
    start_time = time.time()
    
    
    current_annotation_file_path = pre_annotation_path + current_pat_client_id_code + "/" + current_pat_client_id_code+"_"+str(target_date_range)
    
    if(skip_check):
        file_exists = False
    else:
        file_exists = exist_check(current_annotation_file_path, config_obj = config_obj)
    
    
    
    if(file_exists == False):
    
        start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)

        current_pat_docs = filter_dataframe_by_timestamp(pat_doc_batch, start_year, start_month, end_year, end_month,start_day, end_day, 'updatetime')


        n_docs_to_annotate = len(current_pat_docs)
        update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations', n_docs_to_annotate = n_docs_to_annotate,
                    t=t, config_obj=config_obj)

    annotation_map = {'True':1,
                     'Presence':1 ,
                     'Recent': 1,
                     'Past':0,
                     'Subject/Experiencer':1,
                     'Other':0,
                     'Hypothetical':0,
                     'Patient': 1}

    #remove filter from cdb?
    #print("getting annotations")
    
#     file_exists = exists(pre_annotation_path + current_pat_client_id_code)
    
    
    if(file_exists==False):
        with io.capture_output() as captured:
            pats_anno_annotations = cat.get_entities_multi_texts(current_pat_docs['body_analysed'].dropna());#, n_process=1
        if(store_annot):
            dump_results(pats_anno_annotations, current_annotation_file_path,  config_obj=config_obj)
    
    
    

