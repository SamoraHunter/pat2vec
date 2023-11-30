
import pickle

import numpy as np
import paramiko
from IPython.utils import io
import pandas as pd
from IPython.display import display

from util.methods_get import (dump_results, exist_check,
                              filter_dataframe_by_timestamp,
                              get_start_end_year_month, update_pbar)





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


def get_current_pat_annotations_mrc_cs(current_pat_client_id_code, target_date_range, batch_epr_docs_annotations, config_obj = None, t=None, cohort_searcher_with_terms_and_search =None, cat=None):
    
    if config_obj is None:
        raise ValueError("config_obj cannot be None. Please provide a valid configuration. (get_current_pat_annotations_mrc_cs)")

    
    start_time = config_obj.start_time

    
    p_bar_entry='annotations_mrc_cs'
    
    update_pbar(current_pat_client_id_code, start_time, 0, p_bar_entry, t, config_obj, config_obj.skipped_counter)

    
    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)
    
    if(batch_epr_docs_annotations is not None):
    
        filtered_batch_epr_docs_annotations = filter_dataframe_by_timestamp(batch_epr_docs_annotations, 
                                                                            start_year,
                                                                            start_month,
                                                                            end_year, 
                                                                            end_month,
                                                                            start_day, end_day, 'observationdocument_recordeddtm')
    
        if(len(filtered_batch_epr_docs_annotations)>0):
        
            df_pat_target = calculate_pretty_name_count_features(filtered_batch_epr_docs_annotations)
        
    else:
        df_pat_target = pd.DataFrame(data = [current_pat_client_id_code], columns=['client_idcode'])
  
    
    if config_obj.verbosity >= 6: display(df_pat_target)
            
    return df_pat_target



# def get_current_pat_annotations_mrc_cs(current_pat_client_id_code, target_date_range, pat_batch, sftp_obj=None, config_obj = None, t=None, cohort_searcher_with_terms_and_search =None, cat=None):
    
    
#     start_time = config_obj.start_time
    
#     pre_annotation_path_mrc = config_obj.pre_annotation_path_mrc
    
#     batch_mode = config_obj.batch_mode
    
#     remote_dump = config_obj.remote_dump
    
#     hostname = config_obj.hostname
    
#     username = config_obj.username
    
#     password = config_obj.password
    
#     share_sftp = config_obj.share_sftp
    
#     negated_presence_annotations = config_obj.negated_presence_annotations
    
#     current_annot_file_path = pre_annotation_path_mrc  + current_pat_client_id_code + "/" + current_pat_client_id_code  +"_"+str(target_date_range)
    
    
#     file_exists = exist_check(current_annot_file_path, config_obj = config_obj)
    
#     if(file_exists==False):
    
#         start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)

        
#         if(batch_mode):
#             current_pat_docs = filter_dataframe_by_timestamp(pat_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'observationdocument_recordeddtm')

#         else:
        
#             current_pat_docs = cohort_searcher_with_terms_and_search(index_name="observations", 
#                                                                        fields_list="""observation_guid client_idcode	obscatalogmasteritem_displayname	observation_valuetext_analysed	observationdocument_recordeddtm clientvisit_visitidcode""".split(),
#                                                                        term_name="client_idcode.keyword", 
#                                                                        entered_list=[current_pat_client_id_code], 
#                                                                        search_string="obscatalogmasteritem_displayname:(\"AoMRC_ClinicalSummary_FT\") AND " + f'observationdocument_recordeddtm:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]')


#         n_docs_to_annotate = len(current_pat_docs)
#         update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations_mrc', n_docs_to_annotate = n_docs_to_annotate, t=t, config_obj = config_obj, skipped_counter = None)
#     else:
#         n_docs_to_annotate = "Reading preannotated mrc..."
        
#     annotation_map = {'True':1,
#                      'Presence':1 ,
#                      'Recent': 1,
#                      'Past':0,
#                      'Subject/Experiencer':1,
#                      'Other':0,
#                      'Hypothetical':0,
#                      'Patient': 1}

#     #remove filter from cdb?
#     #print("getting annotations")
    
# #     file_exists = exists(pre_annotation_path_mrc + current_pat_client_id_code)
    
    
#     if(file_exists==False):
#         with io.capture_output() as captured:
#             pats_anno_annotations = cat.get_entities_multi_texts(current_pat_docs['observation_valuetext_analysed'].dropna());#, n_process=1
            
#             dump_results(pats_anno_annotations, current_annot_file_path, config_obj=config_obj)
                
# #                 with open(pre_annotation_path_mrc + current_pat_client_id_code, 'wb') as f:
# #                     pickle.dump(pats_anno_annotations, f)
                    
#     else:
#         if(remote_dump):
#             if(share_sftp == False):
#                 ssh_client = paramiko.SSHClient()
#                 ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#                 ssh_client.connect(hostname=hostname, username=username, password=password)

#                 sftp_client = ssh_client.open_sftp()
#                 sftp_obj = sftp_client
            
            
#             with sftp_obj.open(current_annot_file_path, 'r') as file:
    
#                 pats_anno_annotations = pickle.load(file)
        
#             if(share_sftp == False):
#                 sftp_obj.close()
#                 sftp_obj.close()
            
        
#         else:
#             with open(current_annot_file_path, 'rb') as f:
#                 pats_anno_annotations = pickle.load(f)                
                    
#     n_docs_to_annotate = len(pats_anno_annotations)
    
    
#     #print(f"Annotated {current_pat_client_id_code}")
#     #length of chars in documents summed
#     #average number of documents as a divisor for mention counts
#     #we want to keep the fact that lots of documents is a bad sign... 
#     #Lots of mentions of something could indicate severity etc

#     #pats_anno_annotations = cat.get_entities(current_pat_docs['body_analysed'])
#     update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations_mrc', n_docs_to_annotate = n_docs_to_annotate, t=t, config_obj=config_obj)


#     sum_count = 0
#     for i in range(0, len(pats_anno_annotations)):
#         sum_count = sum_count + len(list(pats_anno_annotations[i]['entities'].keys()))

#     sum_count_index_list = [x for x in range(0, sum_count)]
#     all_doc_entities = {'entities': dict.fromkeys(sum_count_index_list, {})}
#     sum_count_index = 0
#     for i in range(0, len(pats_anno_annotations)):

#         key_list = list(pats_anno_annotations[i]['entities'].keys())
#         for j in range(0, len(key_list)):

#             all_doc_entities['entities'][sum_count_index] = pats_anno_annotations[i]['entities'].get(key_list[j])
#             sum_count_index = sum_count_index + 1


#     pats_anno_annotations = all_doc_entities

#     all_cui_list = []

#     all_meta_anno = False
#     confidence_threshold_presence = 0.8
#     confidence_threshold_subject = 0.8
#     confidence_threshold_concept_accuracy = 0.8

#     cui_list_pretty_names = []
#     doc_keys = list(pats_anno_annotations.keys())
#     for i in range(0, len(doc_keys)):
#         current_pats_entry = pats_anno_annotations.get('entities')
#         current_pats_entry_keys = list(pats_anno_annotations.get('entities').keys())
#         for j in range(0, len(current_pats_entry_keys)):
#             all_cui_list.append(current_pats_entry.get(current_pats_entry_keys[j])['cui'])
#             cui_list_pretty_names.append(current_pats_entry.get(current_pats_entry_keys[j])['pretty_name'])
#     #print("len(all_cui_list)", len(all_cui_list))      
#     #print("len(set(all_cui_list))", len(set(all_cui_list)))

#     cui_list = all_cui_list

# #     cui_list_pretty_names = []
# #     for i in range(0, len(cui_list)):
# #         cui_list_pretty_names.append(cat.cdb.cui2preferred_name.get(cui_list[i]))

#     #print("len(set(cui_list_pretty_names))", len(set(cui_list_pretty_names)))

#     cui_list_pretty_names = list(set(cui_list_pretty_names))

#     #cui_list_pretty_names.remove(None)



#     cui_list_pretty_names_meta_list = []
#     if(all_meta_anno):
#         cui_list_pretty_names_meta = [x + '_meta' for x in cui_list_pretty_names]

#         cui_list_pretty_names_meta_list = []

#         meta_key_list = ['Time', 'Presence', 'Subject/Experiencer']

#         meta_sub_key_list = ['value'] #,'confidence', 'name']

#         for i in range(0, len(cui_list_pretty_names)):
#             for j in range(0, len(meta_key_list)):
#                 for k in range(0, len(meta_sub_key_list)):
#                     cui_list_pretty_names_meta_list.append(cui_list_pretty_names[i] + "_"+meta_key_list[j]+"_"+meta_sub_key_list[k])

#         print("len(set(cui_list_pretty_names_meta_list))", len(set(cui_list_pretty_names_meta_list)))    

#     cui_list_pretty_names_count_list = []
#     for i in range(0, len(cui_list_pretty_names)):
#             cui_list_pretty_names_count_list.append(cui_list_pretty_names[i] +"_count_mrc_cs")
#             cui_list_pretty_names_count_list.append(cui_list_pretty_names[i] +"_count_subject_present_mrc_cs")
            
#             if(negated_presence_annotations):
#                 cui_list_pretty_names_count_list.append(cui_list_pretty_names[i] +"_count_subject_not_present_mrc_cs")
#                 cui_list_pretty_names_count_list.append(cui_list_pretty_names[i] +"_count_relative_not_present_mrc_cs")


#     all_columns_to_append =   cui_list_pretty_names_count_list
#     if(all_meta_anno):
#         all_columns_to_append.append(cui_list_pretty_names_meta_list)

#     dummy_data = np.empty((1,len(all_columns_to_append)));
#     dummy_data[:] = np.nan
#     df_pat_entry = pd.DataFrame(data = dummy_data,columns=all_columns_to_append)

#     #df = pd.read_csv(file_name) #call outside
#     df_pat = df_pat_entry.copy() 
#     df_pat['client_idcode'] = current_pat_client_id_code 
#     df_pat['n_docs'] = n_docs_to_annotate

#     df_pat.reset_index(inplace=True)
#     #df_pat['n'] = [i for i in range(0, len(df_pat))]
#     df_pat = df_pat[['client_idcode']].copy()
#     df_pat_target = df_pat.copy(deep=True)
#     #print("Reindexing df_pat_target")
#     df_pat_target = df_pat_target.reindex(list(df_pat_target.columns) + all_columns_to_append, axis=1)



#     a = list(df_pat_target.columns)
#     b = cui_list_pretty_names_meta_list



#     #print("filling df pat target with nans")
#     df_pat_target[[x for x in a if (x not in b)]] = df_pat_target[[x for x in a if (x not in b)]].fillna(0)

#     #break
#     df_pat_target['client_idcode'].iloc[0] = current_pat_client_id_code

#     df_pat_target = df_pat_target.copy()

#     list_targ=[x for x in range(0, len(df_pat_target))]

#     columns = list(cui_list_pretty_names_meta_list)


#     df_pat_target = df_pat_target.copy()#[0:1]



#     entry_counter = 0
#     meta_counter = 0
#     i = 0
#     #print("Starting annotation frame builder...")
#     #for i in tqdm(range(0, len(df_pat_target))):
#         #ci = df_pat_target['client_idcode'].iloc[i]

#     #annotations = cat.get_entities(current_pat_docs['body_analysed']) #docs.get(ci)
#     annotations = pats_anno_annotations


#     if annotations is not None:
#         annotation_keys = list(annotations['entities'].keys())

#         for j in range(0, len(annotation_keys)):

#             cui = annotations['entities'][annotation_keys[j]].get("cui")
#             if(cui in cui_list): 
#                 current_col_name = annotations['entities'][annotation_keys[j]].get('pretty_name')
#                 current_col_meta = annotations['entities'][annotation_keys[j]].get('meta_anns')

#                 df_pat_target.at[i, current_col_name+"_count_mrc_cs"] = df_pat_target.loc[i][current_col_name+"_count_mrc_cs"] + 1

#                 if(current_col_meta is not None):
                    
#                     if(current_col_meta['Presence']['value']=='True' and 
#                        current_col_meta['Subject/Experiencer']['value']=='Patient' and 
#                        current_col_meta['Presence']['confidence']> confidence_threshold_presence and
#                        current_col_meta['Subject/Experiencer']['confidence']> confidence_threshold_presence and
#                        annotations['entities'][annotation_keys[j]]['acc'] >confidence_threshold_concept_accuracy):

#                             df_pat_target.at[i, current_col_name+"_count_subject_present_mrc_cs"] = df_pat_target.loc[i][current_col_name+"_count_subject_present_mrc_cs"] + 1
                    
#                     elif(current_col_meta['Presence']['value']=='True' and 
#                        current_col_meta['Subject/Experiencer']['value']=='Relative' and 
#                        current_col_meta['Presence']['confidence']> confidence_threshold_presence and
#                        current_col_meta['Subject/Experiencer']['confidence']> confidence_threshold_presence and
#                        annotations['entities'][annotation_keys[j]]['acc'] >confidence_threshold_concept_accuracy):
#                             if(current_col_name+"_count_relative_present_mrc_cs" in df_pat_target.columns):
                                
#                                 df_pat_target.at[i, current_col_name+"_count_relative_present_mrc_cs"] = df_pat_target.loc[i][current_col_name+"_count_relative_present_mrc_cs"] + 1
                    
#                             else:
#                                 df_pat_target[current_col_name+"_count_relative_present_mrc_cs"] = 1
                                
#                     if(negated_presence_annotations):
                        
#                         if(current_col_meta is not None):
                    
#                             if(current_col_meta['Presence']['value']=='False' and 
#                                current_col_meta['Subject/Experiencer']['value']=='Patient' and 
#                                current_col_meta['Presence']['confidence']> confidence_threshold_presence and
#                                current_col_meta['Subject/Experiencer']['confidence']> confidence_threshold_presence and
#                                annotations['entities'][annotation_keys[j]]['acc'] >confidence_threshold_concept_accuracy):

#                                     df_pat_target.at[i, current_col_name+"_count_subject_not_present_mrc_cs"] = df_pat_target.loc[i][current_col_name+"_count_subject_not_present_mrc_cs"] + 1


#                             elif(current_col_meta['Presence']['value']=='True' and 
#                                current_col_meta['Subject/Experiencer']['value']=='Relative' and 
#                                current_col_meta['Presence']['confidence']> confidence_threshold_presence and
#                                current_col_meta['Subject/Experiencer']['confidence']> confidence_threshold_presence and
#                                annotations['entities'][annotation_keys[j]]['acc'] >confidence_threshold_concept_accuracy):
#                                     if(current_col_name+"_count_relative_not_present_mrc_cs" in df_pat_target.columns):

#                                         df_pat_target.at[i, current_col_name+"_count_relative_not_present_mrc_cs"] = df_pat_target.loc[i][current_col_name+"_count_relative_not_present_mrc_cs"] + 1

#                                     else:
#                                         df_pat_target[current_col_name+"_count_relative_not_present_mrc_cs"] = 1
                                
                                
#                     else:
#                         #OLD: set to nan instead of zero for impute and medcat precision persistence
#                         #Dont set anything here as this will overwrite existing 1 entries?
#                         pass
#                         #df_pat_target.at[i, current_col_name+"_count_subject_present"] = np.nan


#                 if(all_meta_anno):
#                     if(len(list(current_col_meta.keys()))>0):

#                         for key in current_col_meta.keys():

#                             sub_anot = current_col_meta.get(key)
#                             if(len(list(sub_anot.keys()))>0):
#                                 for sub_key in sub_anot.keys():
#                                     if(sub_key in meta_sub_key_list):
#                                         try:

#                                             key_result = sub_anot.get(sub_key)
#                                             if(type(key_result) is str):
#                                                 key_result = annotation_map.get(key_result)

#                                             #print(current_col_name+'_'+str(key)+'_'+str(sub_key), key_result, sub_anot.get(sub_key))

#                                             df_pat_target.at[i, current_col_name+'_'+str(key)+'_'+str(sub_key)] = key_result


#                                             meta_counter = meta_counter+1
#                                         except Exception as e:
#                                             print(e)
#                                             pass



#                 meta_counter  = meta_counter  + 1
#                 entry_counter = entry_counter + 1

#             else:
#                 pass
#     update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations_mrc', n_docs_to_annotate = n_docs_to_annotate, t=t, config_obj=config_obj)
#     #df_pat_target.drop("n", axis=1, inplace=True)

#     #df_pat_target.to_csv(entry_file_name)
#     #print(f"Made {entry_counter} entry_counter  entries")
#     #print(f"Made {meta_counter} meta_counter entries")
#             #print("done")
#     if config_obj.verbosity >= 6: display(df_pat_target)
            
#     return df_pat_target