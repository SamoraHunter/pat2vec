 
 
import time
import traceback

import pandas as pd

from pat2vec.pat2vec_get_methods.get_method_bed import get_bed
from pat2vec.pat2vec_get_methods.get_method_bloods import \
    get_current_pat_bloods
from pat2vec.pat2vec_get_methods.get_method_bmi import get_bmi_features
from pat2vec.pat2vec_get_methods.get_method_core02 import get_core_02
from pat2vec.pat2vec_get_methods.get_method_core_resus import get_core_resus
from pat2vec.pat2vec_get_methods.get_method_current_pat_annotations_mrc_cs import \
    get_current_pat_annotations_mrc_cs
from pat2vec.pat2vec_get_methods.get_method_demographics import get_demo
#from pat2vec_get_methods.get_method_demo import get_demographics3
from pat2vec.pat2vec_get_methods.get_method_diagnostics import \
    get_current_pat_diagnostics
from pat2vec.pat2vec_get_methods.get_method_drugs import get_current_pat_drugs
from pat2vec.pat2vec_get_methods.get_method_hosp_site import get_hosp_site
from pat2vec.pat2vec_get_methods.get_method_news import get_news
from pat2vec.pat2vec_get_methods.get_method_pat_annotations import \
    get_current_pat_annotations
from pat2vec.pat2vec_get_methods.get_method_smoking import get_smoking
from pat2vec.pat2vec_get_methods.get_method_vte_status import get_vte_status
from pat2vec.util.methods_get import (enum_target_date_vector,
                                      list_dir_wrapper, update_pbar,
                                      write_remote)


def main_batch(current_pat_client_id_code,
               target_date_range,
               batch_demo = None,
               batch_smoking = None,
               batch_core_02 = None,
               batch_bednumber = None,
               batch_vte = None,
               batch_hospsite = None,
               batch_resus = None,
               batch_news = None,
               batch_bmi = None,
               batch_diagnostics = None,
               batch_epr = None,
               batch_mct = None,
               batch_bloods = None,
               batch_drugs = None,
               batch_epr_docs_annotations = None,
               batch_epr_docs_annotations_mct = None,

              config_obj=None,
              stripped_list_start = None,
              t = None,
              cohort_searcher_with_terms_and_search=None,
              cat = None
              ):
    
    
    #global skipped_counter
    #global start_time
    if config_obj is None:
        raise ValueError("config_obj cannot be None. Please provide a valid configuration. (main_batch)")
    
    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None. Please provide a valid configuration. (main_batch)")

    if t is None:
        raise ValueError("t cannot be None. Please provide a valid configuration. (main_batch)")
    
    if cat is None:
        raise ValueError("cat cannot be None. Please provide a valid configuration. (main_batch)")
    
    # if type(current_pat_client_id_code) is not str and type(current_pat_client_id_code) is not int:
    #     raise ValueError("current_pat_client_id_code cannot be other than str. Please provide a valid configuration. (main_batch)")
    
    current_pat_client_id_code = str(current_pat_client_id_code)
    
    
    start_time = time.time()
    
    
    skipped_counter = config_obj.skipped_counter
    #stripped_list_start = config_obj.stripped_list_start
    n_pat_lines = config_obj.n_pat_lines
    skip_additional_listdir = config_obj.skip_additional_listdir
    current_pat_line_path = config_obj.current_pat_line_path
    current_pat_lines_path = config_obj.current_pat_lines_path
    #write_remote = config_obj.write_remote
    remote_dump = config_obj.remote_dump
    sftp_client = config_obj.sftp_client
    sftp_obj = config_obj.sftp_obj
    multi_process = config_obj.multi_process
    main_options = config_obj.main_options
    
    start_time = config_obj.start_time
    
    already_done = False
    
    done_list = []
    if(current_pat_client_id_code not in stripped_list_start):
        
        
        if(skip_additional_listdir):
            stripped_list = stripped_list_start
        else:
            
            if(len(list_dir_wrapper(current_pat_lines_path + str(current_pat_client_id_code), config_obj)) >= n_pat_lines):
                already_done = True
                stripped_list_start.append(current_pat_client_id_code)
            stripped_list = stripped_list_start.copy()
            
                
            #stripped_list = []
#             stripped_list = [x for x in list_dir_wrapper(current_pat_lines_path)]

        if(current_pat_client_id_code not in stripped_list and already_done==False):
        
            try:
                patient_vector = []
                
                p_bar_entry = current_pat_client_id_code + "_" + str(target_date_range)

                

                if main_options.get('demo'):
                    update_pbar(p_bar_entry, start_time, 0, 'demo', t, config_obj)
                    current_pat_demo = get_demo(current_pat_client_id_code, target_date_range, batch_demo, config_obj=config_obj)
                    patient_vector.append(current_pat_demo)

                

                if main_options.get('bmi'):
                    update_pbar(p_bar_entry, start_time, 1, 'bmi', t, config_obj)
                    bmi_features = get_bmi_features(current_pat_client_id_code, target_date_range, batch_bmi, config_obj=config_obj)
                    patient_vector.append(bmi_features)

                

                if main_options.get('bloods'):
                    update_pbar(p_bar_entry, start_time, 2, 'bloods', t, config_obj)
                    current_pat_bloods = get_current_pat_bloods(current_pat_client_id_code, target_date_range, batch_bloods,config_obj=config_obj)
                    patient_vector.append(current_pat_bloods)

                

                if main_options.get('drugs'):
                    update_pbar(p_bar_entry, start_time, 3, 'drugs', t, config_obj)
                    current_pat_drugs = get_current_pat_drugs(current_pat_client_id_code, target_date_range, batch_drugs, config_obj=config_obj)
                    patient_vector.append(current_pat_drugs)

                

                if main_options.get('diagnostics'):
                    update_pbar(p_bar_entry, start_time, 4, 'diagnostics', t, config_obj)
                    current_pat_diagnostics = get_current_pat_diagnostics(current_pat_client_id_code, target_date_range, batch_diagnostics, config_obj=config_obj)
                    patient_vector.append(current_pat_diagnostics)
                    
                if main_options.get('annotations'):
                    update_pbar(p_bar_entry, start_time, 4, 'annotations_epr', t, config_obj)
                    df_pat_target = get_current_pat_annotations(current_pat_client_id_code,
                                                                target_date_range,
                                                                #batch_epr,
                                                                batch_epr_docs_annotations =batch_epr_docs_annotations,
                                                                config_obj=config_obj,
                                                                t=t,
                                                                cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search,
                                                                cat = cat,
                                                                
                                                                )
                    
                    patient_vector.append(df_pat_target)

                if main_options.get('annotations_mrc'):
                    update_pbar(p_bar_entry, start_time, 4, 'annotations_mrc', t, config_obj)
                    df_pat_target = get_current_pat_annotations_mrc_cs(current_pat_client_id_code, target_date_range,
                                                                       #batch_mct,
                                                                       batch_epr_docs_annotations = batch_epr_docs_annotations_mct,
                                                                       config_obj = config_obj,
                                                                       t=t,
                                                                       cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search,
                                                                    cat = cat,
                                                                       )
                    patient_vector.append(df_pat_target)

                

                if main_options.get('core_02'):
                    update_pbar(p_bar_entry, start_time, 1, 'core_02', t, config_obj)
                    df_pat_target = get_core_02(current_pat_client_id_code, target_date_range, batch_core_02, config_obj=config_obj,
                                                                       cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search)
                    patient_vector.append(df_pat_target)

                

                if main_options.get('bed'):
                    update_pbar(p_bar_entry, start_time, 2, 'bed', t, config_obj)
                    df_pat_target = get_bed(current_pat_client_id_code, target_date_range, batch_bednumber, config_obj=config_obj,
                                                                       cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search)
                    patient_vector.append(df_pat_target)

                

                if main_options.get('vte_status'):
                    update_pbar(p_bar_entry, start_time, 3, 'vte_status', t, config_obj)
                    df_pat_target = get_vte_status(current_pat_client_id_code, target_date_range, batch_vte, config_obj=config_obj,
                                                                       cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search)
                    patient_vector.append(df_pat_target)

                

                if main_options.get('hosp_site'):
                    update_pbar(p_bar_entry, start_time, 4, 'hosp_site', t, config_obj)
                    df_pat_target = get_hosp_site(current_pat_client_id_code, target_date_range, batch_hospsite, config_obj = config_obj,
                                                                       cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search)
                    patient_vector.append(df_pat_target)

                

                if main_options.get('core_resus'):
                    update_pbar(p_bar_entry, start_time, 1, 'core_resus', t, config_obj)
                    df_pat_target = get_core_resus(current_pat_client_id_code, target_date_range, batch_resus, config_obj=config_obj,
                                                                       cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search)
                    patient_vector.append(df_pat_target)

                

                if main_options.get('news'):
                    update_pbar(p_bar_entry, start_time, 2, 'news', t, config_obj)
                    df_pat_target = get_news(current_pat_client_id_code, target_date_range, batch_news, config_obj=config_obj,
                                                                       cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search)
                    patient_vector.append(df_pat_target)
                
                

                if main_options.get('smoking'):
                    update_pbar(p_bar_entry, start_time, 2, 'smoking', t, config_obj)
                    df_pat_target = get_smoking(current_pat_client_id_code, target_date_range, batch_smoking, config_obj=config_obj,
                                                                       cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search)
                    patient_vector.append(df_pat_target)
                    

                update_pbar(p_bar_entry, start_time, 2, 'concatenating', t, config_obj)

                target_date_vector = enum_target_date_vector(target_date_range, current_pat_client_id_code, config_obj = config_obj)
                                             
                patient_vector.append(target_date_vector)
                

                pat_concatted = pd.concat(patient_vector, axis=1)

                pat_concatted.drop('client_idcode', axis=1, inplace=True)

                pat_concatted.insert(0, 'client_idcode', current_pat_client_id_code)
                                            
                update_pbar(p_bar_entry, start_time, 2, 'saving...', t , config_obj)
                
                output_path = current_pat_line_path  + current_pat_client_id_code + "/" +str(current_pat_client_id_code) + "_" + str(target_date_range)+".csv"
                
                
                if(remote_dump==False):
                
                    pat_concatted.to_csv(output_path)   
                else:
                    
                    if(multi_process == True):
                        
                        write_remote(output_path, pat_concatted, config_obj=config_obj)
                    else:
                        with sftp_client.open(output_path, 'w') as file:
                            pat_concatted.to_csv(file)
                        
                        
          
                try:
                    update_pbar(p_bar_entry, start_time, 2, f'Done {len(pat_concatted.columns)} cols in {int(time.time() - start_time)}s, {int((len(pat_concatted.columns)+1)/int(time.time() - start_time)+1)} p/s', t , config_obj)
                except:
                    update_pbar(p_bar_entry, start_time, 2, f'Columns n={len(pat_concatted.columns)}', t , config_obj)
                    pass
                
 
                if(config_obj.verbosity >=9):
                    print("Reached end main batch")
            except RuntimeError as RuntimeError_exception:
                print("Caught runtime error... is torch?")
                print(RuntimeError)
                print("sleeping 1h")
                time.sleep(3600)

            except Exception as e:
                print(e)
                print(traceback.format_exc())
                print(f"Reproduce on {current_pat_client_id_code, target_date_range}")
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(e).__name__, e.args)
                print(message)
                raise
    
        else:
            if(multi_process == False):
                skipped_counter = skipped_counter + 1
            else:
                with skipped_counter.get_lock():
                    skipped_counter.value += 1
            pass
            #print(f"{current_pat_client_id_code} done already")
            #pat_concatted