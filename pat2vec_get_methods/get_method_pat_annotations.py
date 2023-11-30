
import os
import pickle
import time

import numpy as np
import pandas as pd
import paramiko
from IPython.display import display
from IPython.utils import io
from util.methods_annotation import calculate_pretty_name_count_features, check_pat_document_annotation_complete, filter_annot_dataframe, get_pat_document_annotation_batch

from util.methods_get import (dump_results, exist_check,
                              filter_dataframe_by_timestamp,
                              get_start_end_year_month, update_pbar)


def get_current_pat_annotations(current_pat_client_id_code, target_date_range, batch_epr_docs_annotations, config_obj = None, t=None, cohort_searcher_with_terms_and_search =None, cat=None):
    
    if config_obj is None:
        raise ValueError("config_obj cannot be None. Please provide a valid configuration. (get_current_pat_annotations)")

    
    start_time = config_obj.start_time

    
    p_bar_entry='annotations_epr'
    
    update_pbar(current_pat_client_id_code, start_time, 0, p_bar_entry, t, config_obj, config_obj.skipped_counter)

    
    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)
    
    if(batch_epr_docs_annotations is not None):
    
        filtered_batch_epr_docs_annotations = filter_dataframe_by_timestamp(batch_epr_docs_annotations, 
                                                                            start_year,
                                                                            start_month,
                                                                            end_year, 
                                                                            end_month,
                                                                            start_day, end_day, 'updatetime')
    
        if(len(filtered_batch_epr_docs_annotations)>0):
        
            df_pat_target = calculate_pretty_name_count_features(filtered_batch_epr_docs_annotations)
        
    else:
        df_pat_target = pd.DataFrame(data = [current_pat_client_id_code], columns=['client_idcode'])
  
    
    if config_obj.verbosity >= 6: display(df_pat_target)
            
    return df_pat_target