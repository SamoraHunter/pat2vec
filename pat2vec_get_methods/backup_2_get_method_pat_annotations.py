

import os
import pickle

import numpy as np
import pandas as pd
import paramiko
from IPython.display import display
from IPython.utils import io

from util.methods_get import (dump_results, exist_check,
                              filter_dataframe_by_timestamp,
                              get_start_end_year_month, update_pbar)

#function to put docs into folders with day space and guid as name
#additionally to annot docs in folders and write to same structure for annot folder with guid as name
#now get current pat annotations will parse this structure to build windowed vectors for the target date



# def get_current_pat_docs(current_pat_client_id_code, target_date_range, pat_batch, config_obj = None, t=None, cohort_searcher_with_terms_and_search =None, cat=None):

#     batch_mode = config_obj.batch_mode
    
#     start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)

#     if(batch_mode):
        
#         current_pat_docs = filter_dataframe_by_timestamp(pat_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'updatetime')

#     else:

#         current_pat_docs = cohort_searcher_with_terms_and_search(index_name="epr_documents", 
#                                                                 fields_list = """client_idcode document_guid	document_description	body_analysed updatetime clientvisit_visitidcode""".split(),
#                                                                 term_name = "client_idcode.keyword", 
#                                                                 entered_list = [current_pat_client_id_code],
#                                                             search_string = f'updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}] ')
        
    
#     return current_pat_docs


def get_current_pat_docs(current_pat_client_id_code, target_date_range, pat_batch, config_obj=None, cohort_searcher_with_terms_and_search =None):
    """
    Retrieve current patient documents based on the provided parameters.

    Args:
        current_pat_client_id_code (str): The client ID code of the current patient.
        target_date_range (str): The target date range for document retrieval.
        pat_batch (pd.DataFrame): DataFrame containing patient documents.
        config_obj (Config, optional): Configuration object. Defaults to None.

    Returns:
        pd.DataFrame: Current patient documents based on the specified criteria.
    """
    
    batch_mode = config_obj.batch_mode if config_obj else False
    
    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)

    if batch_mode:
        current_pat_docs = filter_dataframe_by_timestamp(pat_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'updatetime')
    else:
        current_pat_docs = cohort_searcher_with_terms_and_search(
            index_name="epr_documents",
            fields_list=["client_idcode", "document_guid", "document_description", "body_analysed", "updatetime", "clientvisit_visitidcode"],
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=f'updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]'
        )
    
    return current_pat_docs



def filter_and_save_documents(pat_batch, target_date_range, client_idcode, config_obj = None):
    
    if(config_obj.verbosity>6):
        print(filter_and_save_documents)
        display(pat_batch)
    
    pre_document_day_path = config_obj.pre_document_day_path
    
    # Convert target_date_range to a datetime object
    target_date = pd.Timestamp(*target_date_range)
    
    # Filter the dataframe based on the target_date
    filtered_dataframe = pat_batch[pat_batch['updatetime'].dt.date == target_date.date()]
    
    # Create the directory path
    output_directory = os.path.join(pre_document_day_path, f"{client_idcode}/", f"{target_date.year}_{target_date.month}_{target_date.day}/")
    
    # Create the directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    
    # Iterate over rows and save each row to a separate CSV file
    for index, row in filtered_dataframe.iterrows():
        document_guid = row['document_guid']
        filename = os.path.join(output_directory, f"{document_guid}.csv")
        row.to_csv(filename, index=False)


# filter_and_save_data(df, (2023, 1, 1), '/path/to/output/', 'client123')



def get_pat_batch_documents(current_pat_client_id_code,pat_batch,target_date_range, config_obj=None, cohort_searcher_with_terms_and_search = None):
    
    if(config_obj.verbosity > 6):
        print(get_pat_batch_documents)
        
    
    pre_document_batch_path = config_obj.pre_document_batch_path

    current_pat_batch_path = os.path.join(pre_document_batch_path, current_pat_client_id_code + ".csv")
    
    current_pat_batch_path_exists = exist_check(current_pat_batch_path, config_obj)
    
    if(config_obj.verbosity >= 6):
        print(pre_document_batch_path, "pre_document_batch_path")
        print(current_pat_batch_path, "current_pat_batch_path")
        print(current_pat_batch_path_exists, "current_pat_batch_path_exists")
        
    
    if(current_pat_batch_path_exists):
            
            current_pat_docs = pd.read_csv(current_pat_batch_path)
            
    else:
        
        current_pat_docs = get_current_pat_docs(current_pat_client_id_code, target_date_range, pat_batch, config_obj=config_obj, cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search)

        current_pat_docs.to_csv(current_pat_batch_path)
        
    return current_pat_docs
        
    
def enumerate_pat_documents(current_pat_client_id_code, pat_batch, target_date_range, config_obj, t):
    
    start_time = config_obj.start_time
    
    t=t
    
    n_docs_to_annotate = len(pat_batch)
    
    update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations_enumerate_pat_documents', n_docs_to_annotate = n_docs_to_annotate,
                t=t, config_obj=config_obj)
    
    current_pat_docs = get_pat_batch_documents(current_pat_client_id_code = current_pat_client_id_code,
                                                   pat_batch=pat_batch,
                                                   target_date_range = target_date_range, 
                                                   config_obj = config_obj)

    n_docs_to_annotate = len(current_pat_docs)
    
    update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations_filter_and_save_documents', n_docs_to_annotate = n_docs_to_annotate,
                t=t, config_obj=config_obj)
    
    filter_and_save_documents(pat_batch = pat_batch, target_date_range = target_date_range, client_idcode = current_pat_client_id_code, config_obj = config_obj)

    

def annotate_pat_batch_documents(current_pat_client_id_code, target_date_range, pat_batch, config_obj=None, t=None, cat=None):
    """
    Annotate PAT batch documents with specified client ID, date range, and configuration.

    Parameters:
    - current_pat_client_id_code (str): The client ID code for the current PAT batch.
    - target_date_range (tuple): A tuple representing the target date range for annotation.
    - pat_batch: (type not specified): The PAT batch object to be annotated.
    - config_obj (optional): Configuration object containing paths and verbosity settings.
    - t (optional): Parameter t (type not specified).
    - cat (optional): Parameter cat (type not specified).
    """
    
    # Check if config_obj is provided; if not, use a default empty configuration
    config_obj = config_obj 

    # Get paths from the configuration object
    pre_document_annotation_day_path = config_obj.pre_document_annotation_day_path
    pre_document_batch_path = config_obj.pre_document_batch_path

    # Construct current document annotation file path
    current_document_annotation_file_path = os.path.join(pre_document_annotation_day_path, current_pat_client_id_code)

    # Construct current PAT batch path
    current_pat_batch_path = os.path.join(pre_document_batch_path, current_pat_client_id_code)

    # List files in the current PAT batch path
    #current_pat_batch_path_files = os.listdir(current_pat_batch_path)
    # Check verbosity level and print paths if greater than 5
    if config_obj.verbosity >= 5:
        print("annotate_pat_batch_documents Paths:")
        print(f"- Document Annotation File Path: {current_document_annotation_file_path}")
        print(f"- PAT Batch Path: {current_pat_batch_path}")
    
    
    # Mirror folder structure of the current PAT batch path to the document annotation file path
    mirror_folder_structure(current_pat_batch_path, current_document_annotation_file_path)

    

    
    


import os
from shutil import copyfile

def mirror_folder_structure(source_root, dest_root):
    """
    Mirror the folder structure from source to destination recursively.

    Parameters:
    - source_root (str): The root directory of the source folder structure.
    - dest_root (str): The root directory of the destination folder structure.
    """
    for client_id in os.listdir(source_root):
        client_id_path = os.path.join(source_root, client_id)
        dest_client_id_path = os.path.join(dest_root, client_id)

        if os.path.isdir(client_id_path):
            mirror_client_folder(client_id_path, dest_client_id_path)


def mirror_client_folder(source_client_path, dest_client_path):
    """
    Recursively mirror the client folder structure.

    Parameters:
    - source_client_path (str): The source client folder path.
    - dest_client_path (str): The destination client folder path.
    """
    if not os.path.exists(dest_client_path):
        os.makedirs(dest_client_path)

    for item in os.listdir(source_client_path):
        source_item_path = os.path.join(source_client_path, item)
        dest_item_path = os.path.join(dest_client_path, item)

        if os.path.isdir(source_item_path):
            mirror_client_folder(source_item_path, dest_item_path)
        else:
            # Call function to produce files with unique names
            produce_files(source_item_path, dest_item_path)


def produce_files(source_file_path, dest_file_path):
    """
    Produce files with unique names in the destination folder.

    Parameters:
    - source_file_path (str): The source file path.
    - dest_file_path (str): The destination file path.
    """
    # In this example, we'll just copy the files from the source to the destination
    
    json_to_dataframe(source_file_path, dest_file_path)
    copyfile(source_file_path, dest_file_path)



# def handle_annotation():
    
#     annotation_map = {'True':1,
#                      'Presence':1 ,
#                      'Recent': 1,
#                      'Past':0,
#                      'Subject/Experiencer':1,
#                      'Other':0,
#                      'Hypothetical':0,
#                      'Patient': 1}
    
#     if(file_exists==False):
#         with io.capture_output() as captured:
#             pats_anno_annotations = cat.get_entities_multi_texts(current_pat_docs['body_analysed'].dropna());#, n_process=1
#         if(store_annot):
#             dump_results(pats_anno_annotations, current_annotation_file_path, config_obj=config_obj)
    
    
#     else:
#         if(remote_dump):
#             if(share_sftp == False):
#                 ssh_client = paramiko.SSHClient()
#                 ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#                 ssh_client.connect(hostname=hostname, username=username, password=password)

#                 sftp_client = ssh_client.open_sftp()
#                 sftp_obj = sftp_client
            

#             with sftp_obj.open(current_annotation_file_path, 'r') as file:

#                 pats_anno_annotations = pickle.load(file)
            
#             if(share_sftp == False):
#                 sftp_obj.close()
#                 sftp_obj.close()
            
#         else:

#             with open(current_annotation_file_path, 'rb') as f:
#                 pats_anno_annotations = pickle.load(f)
   
#     n_docs_to_annotate = len(pats_anno_annotations)
    
#     #print(f"Annotated {current_pat_client_id_code}")
#     #length of chars in documents summed
#     #average number of documents as a divisor for mention counts
#     #we want to keep the fact that lots of documents is a bad sign... 
#     #Lots of mentions of something could indicate severity etc

#     #pats_anno_annotations = cat.get_entities(current_pat_docs['body_analysed'])
#     update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations', n_docs_to_annotate = n_docs_to_annotate,
#                 t=t, config_obj=config_obj)


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
#             cui_list_pretty_names_count_list.append(cui_list_pretty_names[i] +"_count")
#             cui_list_pretty_names_count_list.append(cui_list_pretty_names[i] +"_count_subject_present")
            
#             if(negated_presence_annotations):
#                 cui_list_pretty_names_count_list.append(cui_list_pretty_names[i] +"_count_subject_not_present")
#                 cui_list_pretty_names_count_list.append(cui_list_pretty_names[i] +"_count_relative_not_present")


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
#     df_pat_target = df_pat_target.reindex(list(df_pat_target.columns) +all_columns_to_append, axis=1)



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

#                 df_pat_target.at[i, current_col_name+"_count"] = df_pat_target.loc[i][current_col_name+"_count"] + 1

#                 if(current_col_meta is not None):
                    
#                     if(current_col_meta['Presence']['value']=='True' and 
#                        current_col_meta['Subject/Experiencer']['value']=='Patient' and 
#                        current_col_meta['Presence']['confidence']> confidence_threshold_presence and
#                        current_col_meta['Subject/Experiencer']['confidence']> confidence_threshold_presence and
#                        annotations['entities'][annotation_keys[j]]['acc'] >confidence_threshold_concept_accuracy):
                     
#                             df_pat_target.at[i, current_col_name+"_count_subject_present"] = df_pat_target.loc[i][current_col_name+"_count_subject_present"] + 1
                    
                    
                    
                
                
                
#                     elif(current_col_meta['Presence']['value']=='True' and 
#                        current_col_meta['Subject/Experiencer']['value']=='Relative' and 
#                        current_col_meta['Presence']['confidence']> confidence_threshold_presence and
#                        current_col_meta['Subject/Experiencer']['confidence']> confidence_threshold_presence and
#                        annotations['entities'][annotation_keys[j]]['acc'] >confidence_threshold_concept_accuracy):
#                             if(current_col_name+"_count_relative_present" in df_pat_target.columns):
                                
#                                 df_pat_target.at[i, current_col_name+"_count_relative_present"] = df_pat_target.loc[i][current_col_name+"_count_relative_present"] + 1
                    
#                             else:
#                                 df_pat_target[current_col_name+"_count_relative_present"] = 1
                                
#                     if(negated_presence_annotations):
                        
#                         if(current_col_meta is not None):
                    
#                             if(current_col_meta['Presence']['value']=='False' and 
#                                current_col_meta['Subject/Experiencer']['value']=='Patient' and 
#                                current_col_meta['Presence']['confidence']> confidence_threshold_presence and
#                                current_col_meta['Subject/Experiencer']['confidence']> confidence_threshold_presence and
#                                annotations['entities'][annotation_keys[j]]['acc'] >confidence_threshold_concept_accuracy):

#                                     df_pat_target.at[i, current_col_name+"_count_subject_not_present"] = df_pat_target.loc[i][current_col_name+"_count_subject_not_present"] + 1


#                             elif(current_col_meta['Presence']['value']=='True' and 
#                                current_col_meta['Subject/Experiencer']['value']=='Relative' and 
#                                current_col_meta['Presence']['confidence']> confidence_threshold_presence and
#                                current_col_meta['Subject/Experiencer']['confidence']> confidence_threshold_presence and
#                                annotations['entities'][annotation_keys[j]]['acc'] >confidence_threshold_concept_accuracy):
#                                     if(current_col_name+"_count_relative_not_present" in df_pat_target.columns):

#                                         df_pat_target.at[i, current_col_name+"_count_relative_not_present"] = df_pat_target.loc[i][current_col_name+"_count_relative_not_present"] + 1

#                                     else:
#                                         df_pat_target[current_col_name+"_count_relative_not_present"] = 1
                        
                                
                                
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
#     update_pbar(current_pat_client_id_code+"_"+str(target_date_range), start_time, 5, 'annotations', n_docs_to_annotate = n_docs_to_annotate,
#                 t=t, config_obj=config_obj)
#     #df_pat_target.drop("n", axis=1, inplace=True)

#     #df_pat_target.to_csv(entry_file_name)
#     #print(f"Made {entry_counter} entry_counter  entries")
#     #print(f"Made {meta_counter} meta_counter entries")
#             #print("done")




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
    
    pre_document_annotation_day_path = config_obj.pre_document_annotation_day_path
    
    pre_document_batch_path = config_obj.pre_document_batch_path
    
    
    current_annotation_file_path = pre_annotation_path + current_pat_client_id_code + "/" +  current_pat_client_id_code+"_"+str(target_date_range)
    
    current_document_annotation_file_path = pre_document_annotation_day_path + current_pat_client_id_code + '/' 
    
    current_document_file_path = pre_document_batch_path + current_pat_client_id_code + '/'
    
    
    #file_exists = exist_check(current_annotation_file_path, config_obj = config_obj)
    
    pat_annotations_complete = False #check_pat_document_annotation_complete(current_document_file_path, current_document_annotation_file_path)
    
    current_pat_batch_path = os.path.join(pre_document_batch_path, current_pat_client_id_code)
    
    
    if(pat_annotations_complete == False):
        
        enumerate_pat_documents(current_pat_client_id_code, pat_batch, target_date_range, config_obj, t)
    
    
        annotate_pat_batch_documents(current_pat_client_id_code, target_date_range, pat_batch, config_obj=config_obj, t=None, cat=cat)

        
    else:
        
        n_docs_to_annotate = "Reading preannotated..."
        
        
    

    
    df_pat_target = None
    
    
    if config_obj.verbosity >= 6: display(df_pat_target)
            
    return df_pat_target