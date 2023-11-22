 
 
import time

import traceback

from util.methods_get import enum_target_date_vector, list_dir_wrapper, update_pbar

from patvec_get_batch_methods.main import (get_pat_batch_bloods,
                                           get_pat_batch_bmi,
                                           get_pat_batch_demo,
                                           get_pat_batch_diagnostics,
                                           get_pat_batch_drugs,
                                           get_pat_batch_epr_docs,
                                           get_pat_batch_mct_docs,
                                           get_pat_batch_news,
                                           get_pat_batch_obs)
 
 
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

              config_obj=None,
              stripped_list_start = None,
              t = None,
              ):
    
    
    #global skipped_counter
    #global start_time
    
    
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
            
            if(len(list_dir_wrapper(current_pat_lines_path + current_pat_client_id_code, config_obj)) >=n_pat_lines):
                already_done = True
                stripped_list_start.append(current_pat_client_id_code)
            stripped_list = stripped_list_start.copy()
            
                
            #stripped_list = []
#             stripped_list = [x for x in list_dir_wrapper(current_pat_lines_path)]

        if(current_pat_client_id_code not in stripped_list and already_done==False):
        
            try:
                patient_vector = []
                
                p_bar_entry = current_pat_client_id_code + "_" + str(target_date_range)

                update_pbar(p_bar_entry, start_time, 0, 'demo', t, config_obj)

                if main_options.get('demo'):
                    current_pat_demo = get_demo(current_pat_client_id_code, target_date_range, batch_demo)
                    patient_vector.append(current_pat_demo)

                update_pbar(p_bar_entry, start_time, 1, 'bmi', t, config_obj)

                if main_options.get('bmi'):
                    bmi_features = get_bmi_features(current_pat_client_id_code, target_date_range, batch_bmi)
                    patient_vector.append(bmi_features)

                update_pbar(p_bar_entry, start_time, 2, 'bloods', t, config_obj)

                if main_options.get('bloods'):
                    current_pat_bloods = get_current_pat_bloods(current_pat_client_id_code, target_date_range, batch_bloods)
                    patient_vector.append(current_pat_bloods)

                update_pbar(p_bar_entry, start_time, 3, 'drugs', t, config_obj)

                if main_options.get('drugs'):
                    current_pat_drugs = get_current_pat_drugs(current_pat_client_id_code, target_date_range, batch_drugs)
                    patient_vector.append(current_pat_drugs)

                update_pbar(p_bar_entry, start_time, 4, 'diagnostics', t, config_obj)

                if main_options.get('diagnostics'):
                    current_pat_diagnostics = get_current_pat_diagnostics(current_pat_client_id_code, target_date_range, batch_diagnostics)
                    patient_vector.append(current_pat_diagnostics)

                if main_options.get('annotations'):
                    df_pat_target = get_current_pat_annotations(current_pat_client_id_code, target_date_range, batch_epr, sftp_obj)
                    patient_vector.append(df_pat_target)

                if main_options.get('annotations_mrc'):
                    df_pat_target = get_current_pat_annotations_mrc_cs(current_pat_client_id_code, target_date_range, batch_mct, sftp_obj)
                    patient_vector.append(df_pat_target)

                update_pbar(p_bar_entry, start_time, 1, 'core_02', t, config_obj)

                if main_options.get('core_02'):
                    df_pat_target = get_core_02(current_pat_client_id_code, target_date_range, batch_core_02)
                    patient_vector.append(df_pat_target)

                update_pbar(p_bar_entry, start_time, 2, 'bed', t, config_obj)

                if main_options.get('bed'):
                    df_pat_target = get_bed(current_pat_client_id_code, target_date_range, batch_bednumber)
                    patient_vector.append(df_pat_target)

                update_pbar(p_bar_entry, start_time, 3, 'vte_status', t, config_obj)

                if main_options.get('vte_status'):
                    df_pat_target = get_vte_status(current_pat_client_id_code, target_date_range, batch_vte)
                    patient_vector.append(df_pat_target)

                update_pbar(p_bar_entry, start_time, 4, 'hosp_site', t, config_obj)

                if main_options.get('hosp_site'):
                    df_pat_target = get_hosp_site(current_pat_client_id_code, target_date_range, batch_hospsite)
                    patient_vector.append(df_pat_target)

                update_pbar(p_bar_entry, start_time, 1, 'core_resus', t, config_obj)

                if main_options.get('core_resus'):
                    df_pat_target = get_core_resus(current_pat_client_id_code, target_date_range, batch_resus)
                    patient_vector.append(df_pat_target)

                update_pbar(p_bar_entry, start_time, 2, 'news', t, config_obj)

                if main_options.get('news'):
                    df_pat_target = get_news(current_pat_client_id_code, target_date_range, batch_news)
                    patient_vector.append(df_pat_target)

                update_pbar(p_bar_entry, start_time, 2, 'concatenating', t, config_obj)

                target_date_vector = enum_target_date_vector(target_date_range, current_pat_client_id_code)
                
                
                
                    
                patient_vector.append(target_date_vector)
                

                pat_concatted = pd.concat(patient_vector, axis=1)

                pat_concatted.drop('client_idcode', axis=1, inplace=True)

                pat_concatted.insert(0, 'client_idcode', current_pat_client_id_code)
                
                
                                
                update_pbar(p_bar_entry, start_time, 2, 'saving...')
                
                output_path = current_pat_line_path  + current_pat_client_id_code + "/" +str(current_pat_client_id_code) + "_" + str(target_date_range)+".csv"
                
                
                if(remote_dump==False):
                
                    pat_concatted.to_csv(output_path)   
                else:
                    
                    if(multi_process == True):
                        
                        write_remote(output_path, pat_concatted, sftp_obj)
                    else:
                        with sftp_client.open(output_path, 'w') as file:
                            pat_concatted.to_csv(file)
                        
                        
                #display(type(pat_concatted))
                try:
                    update_pbar(p_bar_entry, start_time, 2, f'Done {len(pat_concatted.columns)} cols in {int(time.time() - start_time)}s, {int((len(pat_concatted.columns)+1)/int(time.time() - start_time)+1)} p/s')
                except:
                    update_pbar(p_bar_entry, start_time, 2, f'Columns n={len(pat_concatted.columns)}')
                    pass
                
                #display(pat_concatted)
                #print(time.time() - start_time, current_pat_client_id_code)
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
    
        else:
            if(multi_process == False):
                skipped_counter = skipped_counter + 1
            else:
                with skipped_counter.get_lock():
                    skipped_counter.value += 1
            pass
            #print(f"{current_pat_client_id_code} done already")
            #pat_concatted