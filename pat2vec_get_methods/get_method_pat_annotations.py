
import os
import pickle

import numpy as np
import pandas as pd
import paramiko
from IPython.display import display
from IPython.utils import io
from util.methods_annotation import check_pat_document_annotation_complete, get_pat_document_annotation_batch

from util.methods_get import (dump_results, exist_check,
                              filter_dataframe_by_timestamp,
                              get_start_end_year_month, update_pbar)





def get_current_pat_annotations(current_pat_client_id_code, target_date_range, pat_batch, config_obj = None, t=None, cohort_searcher_with_terms_and_search =None, cat=None):
    
    if config_obj is None:
        raise ValueError("config_obj cannot be None. Please provide a valid configuration. (get_current_pat_annotations)")

    
    start_time = config_obj.start_time
    
    pre_annotation_path = config_obj.pre_annotation_path
    
    batch_mode = config_obj.batch_mode
    
    remote_dump = config_obj.remote_dump
    
    hostname = config_obj.hostname
    
    username = config_obj.username
    
    password = config_obj.password
    
    share_sftp = config_obj.share_sftp
    
    negated_presence_annotations = config_obj.negated_presence_annotations
    
    store_annot = config_obj.store_annot
    
    sftp_obj = config_obj.sftp_obj
    
    #pre_document_annotation_day_path = config_obj.pre_document_annotation_day_path
    
    pre_document_batch_path = config_obj.pre_document_batch_path
    
    
    current_annotation_file_path = pre_annotation_path + current_pat_client_id_code + "/" +  current_pat_client_id_code+"_"+str(target_date_range)
    
    current_document_annotation_batch_file_path = pre_document_batch_path + current_pat_client_id_code + '/' 
    
    
    current_document_file_path = pre_document_batch_path + current_pat_client_id_code + '/'
    
    
    #file_exists = exist_check(current_annotation_file_path, config_obj = config_obj)
    
    pat_annotations_complete = check_pat_document_annotation_complete(current_pat_client_id_code, config_obj=config_obj) #check_pat_document_annotation_complete(current_document_file_path, current_document_annotation_file_path)
    
    current_pat_batch_path = os.path.join(pre_document_batch_path, current_pat_client_id_code)
    
    
    if(pat_annotations_complete == False):
        
        #current_pat_batch_docs = pat_batch
        
        get_pat_document_annotation_batch(current_pat_client_idcode = current_pat_client_id_code, pat_batch=pat_batch, cat=cat, config_obj=config_obj, t=t)
        #enumerate_pat_documents(current_pat_client_id_code, pat_batch, target_date_range, config_obj, t)
    
        #annotate_pat_batch_documents(current_pat_client_id_code, target_date_range, pat_batch, config_obj=config_obj, t=None, cat=cat)

        pass
        
    else:
        
        n_docs_to_annotate = "Reading preannotated..."
        
        
    

    
    df_pat_target = None
    
    
    if config_obj.verbosity >= 6: display(df_pat_target)
            
    return df_pat_target