


import time
from IPython.display import display
from IPython.utils import io

from pat2vec.util.methods_get import (dump_results, exist_check,
                              filter_dataframe_by_timestamp,
                              get_start_end_year_month, update_pbar)


def get_current_pat_annotations_batch_to_file(current_pat_client_id_code, target_date_range, pat_doc_batch,t=None,  skip_check=False, config_obj = None, cat=None):
    
    pre_annotation_path = config_obj.pre_annotation_path
    
    store_annot = config_obj.store_annot
    
    sftp_obj = config_obj.sftp_obj
    
    start_time = time.time()
    
    if config_obj is None:
        raise ValueError("config_obj cannot be None. Please provide a valid configuration. get_current_pat_annotations_batch_to_file")

    
    current_annotation_file_path = pre_annotation_path + current_pat_client_id_code + "/" + current_pat_client_id_code+"_"+str(target_date_range)
    
    if(skip_check):
        file_exists = False
    else:
        file_exists = exist_check(current_annotation_file_path, config_obj = config_obj)
    
    
    
    if(file_exists == False):
    
        start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range, config_obj = config_obj)

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
    