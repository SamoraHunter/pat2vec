
import os
import shutil

import numpy as np
import pandas as pd
from IPython.display import display

from util.methods_get import exist_check, update_pbar
from util.post_processing import join_icd10_codes_to_annot


def check_pat_document_annotation_complete(current_pat_client_id_code, config_obj=None):
    
    pre_document_batch_path = config_obj.pre_document_batch_path
    
    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path
    
    current_pat_batch_path = os.path.join(pre_document_batch_path, current_pat_client_id_code)
    
    current_pat_batch_annot_path = os.path.join(pre_document_annotation_batch_path, current_pat_client_id_code + ".csv")
    
    bool1 = exist_check(current_pat_batch_annot_path ,config_obj=config_obj)

    
    return bool1

def parse_meta_anns(meta_anns):
    time_value = meta_anns.get('Time', {}).get('value')
    time_confidence = meta_anns.get('Time', {}).get('confidence')

    presence_value = meta_anns.get('Presence', {}).get('value')
    presence_confidence = meta_anns.get('Presence', {}).get('confidence')

    subject_value = meta_anns.get('Subject/Experiencer', {}).get('value')
    subject_confidence = meta_anns.get('Subject/Experiencer', {}).get('confidence')

    return {
        'Time_Value': time_value,
        'Time_Confidence': time_confidence,
        'Presence_Value': presence_value,
        'Presence_Confidence': presence_confidence,
        'Subject_Value': subject_value,
        'Subject_Confidence': subject_confidence
    }

def get_pat_document_annotation_batch(current_pat_client_idcode, pat_batch, cat=None, config_obj=None, t=None):
    
    
    multi_annots = annot_pat_batch_docs(current_pat_client_idcode, pat_batch, cat=cat, config_obj=config_obj, t=t)
    
    #create the file in its dir
    multi_annots_to_df(current_pat_client_idcode, pat_batch, multi_annots, config_obj = config_obj, t=t)
    
    #read_newly created file:
    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path
    
    
    current_pat_document_annotation_batch_path = os.path.join(pre_document_annotation_batch_path, current_pat_client_idcode + ".csv")
    
    pat_document_annotation_batch = pd.read_csv(current_pat_document_annotation_batch_path)
    
    return pat_document_annotation_batch
    
    
def get_pat_document_annotation_batch_mct(current_pat_client_idcode, pat_batch, cat=None, config_obj=None, t=None):
    
    
    #get the annotations for the pat documents
    multi_annots = annot_pat_batch_docs(current_pat_client_idcode=current_pat_client_idcode, pat_batch=pat_batch, cat=cat, config_obj=config_obj, t=t, text_column='observation_valuetext_analysed')
    
 
    
    #creaet the file in its dir
    multi_annots_to_df_mct(current_pat_client_idcode, pat_batch, multi_annots, config_obj = config_obj, t=t, text_column='observation_valuetext_analysed', time_column='observationdocument_recordeddtm', guid_column='observation_guid')
    
    #read_newly created file:
    pre_document_annotation_batch_path_mct = config_obj.pre_document_annotation_batch_path_mct
    
    
    current_pat_document_annotation_batch_path = os.path.join(pre_document_annotation_batch_path_mct, current_pat_client_idcode + ".csv")
    
    pat_document_annotation_batch = pd.read_csv(current_pat_document_annotation_batch_path)
    
    return pat_document_annotation_batch
    
    
    
    

def annot_pat_batch_docs(current_pat_client_idcode, pat_batch, cat=None, config_obj=None, t=None, text_column='body_analysed'):
    
    start_time = config_obj.start_time
    
    
    
    n_docs_to_annotate = len(pat_batch)
    
    update_pbar(current_pat_client_idcode, start_time, 5, 'annot_pat_batch_docs_get_entities_multi_texts',
                n_docs_to_annotate = n_docs_to_annotate,
                t=t, config_obj=config_obj)
    
    
    
    multi_annots = cat.get_entities_multi_texts(pat_batch[text_column].dropna())
    
    return multi_annots
    
    
def multi_annots_to_df(current_pat_client_idcode, 
                       pat_batch, multi_annots, 
                       config_obj = None, t=None,
                       text_column='body_analysed',
                       time_column='updatetime', 
                       guid_column='document_guid'):
    
    
    n_docs_to_annotate = len(pat_batch)
    
    start_time = config_obj.start_time
    
    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path
    
    
    
    current_pat_document_annotation_batch_path = os.path.join(pre_document_annotation_batch_path, current_pat_client_idcode + ".csv")
    
    update_pbar(current_pat_client_idcode, start_time, 5, 'multi_annots_to_df',
                n_docs_to_annotate = n_docs_to_annotate,
                t=t, config_obj=config_obj)
    
    temp_file_path = 'temp_annot_file.csv'
    
    col_list = [ 'client_idcode', time_column, 'pretty_name', 'cui',
       'type_ids', 'types', 'source_value', 'detected_name', 'acc',
       'context_similarity', 'start', 'end', 'icd10', 'ontologies', 'snomed',
       'id', 'Time_Value', 'Time_Confidence', 'Presence_Value',
       'Presence_Confidence', 'Subject_Value', 'Subject_Confidence',
       'text_sample', 'full_doc', guid_column]
    
    pd.DataFrame(None, columns=col_list).to_csv(temp_file_path)
    
    for i in range(0, len(pat_batch)):

        #current_pat_client_id_code = docs.iloc[0]['client_idcode']

        doc_to_annot_df = json_to_dataframe(json_data = multi_annots[i],
                                                            doc = pat_batch.iloc[i],
                                                            current_pat_client_id_code = current_pat_client_idcode,
                                                            text_column = text_column,
                                                            time_column = time_column,
                                                            guid_column = guid_column)

        #drop nan rows
        #Check for NaN values in any column of the specified list
        col_list_drop_nan = ['client_idcode', time_column, ]
        
        if(config_obj.verbosity >= 3):
            print('multi_annots_to_df', len(doc_to_annot_df))
        rows_with_nan = doc_to_annot_df[doc_to_annot_df[col_list_drop_nan].isna().any(axis=1)]

        # Drop rows with NaN values
        doc_to_annot_df = doc_to_annot_df.drop(rows_with_nan.index).copy()
        if(config_obj.verbosity >= 3):
            print('multi_annots_to_df', len(doc_to_annot_df))


        doc_to_annot_df.to_csv(temp_file_path, mode='a', header=False, index=False)

    
    
    
    shutil.copy(temp_file_path, current_pat_document_annotation_batch_path)
    
    if config_obj.add_icd10:
        
        temp_df = pd.read_csv(current_pat_document_annotation_batch_path)
        temp_result = join_icd10_codes_to_annot(df = temp_df, inner=False)
        
        temp_result.to_csv(current_pat_document_annotation_batch_path)
        
        
        
    
    
    
def multi_annots_to_df_mct(current_pat_client_idcode,
                           pat_batch, multi_annots,
                           config_obj = None,
                           t=None,
                           text_column='observation_valuetext_analysed',
                           time_column='observationdocument_recordeddtm',
                           guid_column='observation_guid'):
    
    
    
    n_docs_to_annotate = len(pat_batch)
    
    start_time = config_obj.start_time
    
    pre_document_annotation_batch_path = config_obj.pre_document_annotation_batch_path_mct
    
    
    
    current_pat_document_annotation_batch_path = os.path.join(pre_document_annotation_batch_path, current_pat_client_idcode + ".csv")
    
    update_pbar(current_pat_client_idcode, start_time, 5, 'multi_annots_to_df',
                n_docs_to_annotate = n_docs_to_annotate,
                t=t, config_obj=config_obj)
    
    temp_file_path = 'temp_annot_file.csv'
    
    col_list = ['client_idcode', time_column, 'pretty_name', 'cui',
       'type_ids', 'types', 'source_value', 'detected_name', 'acc',
       'context_similarity', 'start', 'end', 'icd10', 'ontologies', 'snomed',
       'id', 'Time_Value', 'Time_Confidence', 'Presence_Value',
       'Presence_Confidence', 'Subject_Value', 'Subject_Confidence',
       'text_sample', 'full_doc', guid_column]
    
    pd.DataFrame(None, columns=col_list).to_csv(temp_file_path)
    
    for i in range(0, len(pat_batch)):

        #current_pat_client_id_code = docs.iloc[0]['client_idcode']

        doc_to_annot_df = json_to_dataframe(json_data = multi_annots[i],
                                                            doc = pat_batch.iloc[i],
                                                            current_pat_client_id_code = current_pat_client_idcode,
                                                            text_column=text_column,
                                                            time_column=time_column,
                                                            guid_column = guid_column)

        #drop nan rows
        # Check for NaN values in any column of the specified list
        col_list_drop_nan = ['client_idcode', time_column, ]
        
        if(config_obj.verbosity >= 3):
            print('multi_annots_to_df', len(doc_to_annot_df))
        rows_with_nan = doc_to_annot_df[doc_to_annot_df[col_list_drop_nan].isna().any(axis=1)]

        # Drop rows with NaN values
        doc_to_annot_df = doc_to_annot_df.drop(rows_with_nan.index).copy()
        if(config_obj.verbosity >= 3):
            print('multi_annots_to_df', len(doc_to_annot_df))


        doc_to_annot_df.to_csv(temp_file_path, mode='a', header=False, index=False)

    
    
    
    shutil.copy(temp_file_path, current_pat_document_annotation_batch_path)

    if config_obj.add_icd10:
        
        temp_df = pd.read_csv(current_pat_document_annotation_batch_path)
        temp_result = join_icd10_codes_to_annot(df = temp_df, inner=False)
        
        temp_result.to_csv(current_pat_document_annotation_batch_path)


def json_to_dataframe(json_data, doc,current_pat_client_id_code, full_doc=False, window=300, text_column='body_analysed', time_column='updatetime', guid_column = 'document_guid'):
    # Extract data from the JSON
    #doc to be passed as pandas series
    #observation_guid
    if any(json_data.values()):
        done=False
        
        df_parts = []

        keys = list(json_data['entities'].keys())
        
        columns = ['client_idcode',time_column,'pretty_name', 'cui', 'type_ids', 'types', 'source_value', 'detected_name', 'acc', 'context_similarity',
                    'start', 'end', 'icd10', 'ontologies', 'snomed', 'id',
                    'Time_Value', 'Time_Confidence', 'Presence_Value', 'Presence_Confidence',
                    'Subject_Value', 'Subject_Confidence', 'text_sample', 'full_doc', guid_column]
        
        empty_df = pd.DataFrame(data=None, columns=columns)

        for i in range(0, len(keys)):
        
            entities_data = json_data['entities'][keys[i]]
            pretty_name = entities_data['pretty_name']
            cui = entities_data['cui']
            type_ids = entities_data['type_ids']
            types = entities_data['types']
            source_value = entities_data['source_value']
            detected_name = entities_data['detected_name']
            acc = entities_data['acc']
            context_similarity = entities_data['context_similarity']
            start = entities_data['start']
            end = entities_data['end']
            icd10 = entities_data['icd10']
            ontologies = entities_data['ontologies']
            snomed = entities_data['snomed']
            id = entities_data['id']
            meta_anns = entities_data['meta_anns']
            
            # Parse meta annotations
            parsed_meta_anns = parse_meta_anns(meta_anns)
            
            mapped_annot_doc_entity = doc[text_column]
            
            document_len = len(mapped_annot_doc_entity)
            
            document_len = len(mapped_annot_doc_entity)
            
            virtual_start = max(0, start-window)
                    
            virtual_end = min(document_len, end+window)
            
            text_sample = mapped_annot_doc_entity[virtual_start:virtual_end]
            
            updatetime = doc[time_column]
            
            document_guid_value = doc[guid_column]
            
            full_doc_value = np.nan
            
            if(full_doc and not done):
                full_doc_value = mapped_annot_doc_entity
                done = True
            else:
                full_doc_value = np.nan
        

            # Define DataFrame columns and create the DataFrame
            

            data = [[current_pat_client_id_code, updatetime,
                    pretty_name, cui, type_ids, types, source_value, detected_name, acc, context_similarity, start, end,
                    icd10, ontologies, snomed, id,
                    parsed_meta_anns['Time_Value'], parsed_meta_anns['Time_Confidence'],
                    parsed_meta_anns['Presence_Value'], parsed_meta_anns['Presence_Confidence'],
                    parsed_meta_anns['Subject_Value'], parsed_meta_anns['Subject_Confidence'],
                    text_sample, full_doc_value, document_guid_value]]

            df = pd.DataFrame(data, columns=columns)


        
            
            
            df_parts.append(df)

     
        try:
            
            super_df = pd.concat(df_parts)
            super_df.reset_index(inplace=True)
            return super_df
        
        
        except Exception as e:
            print(e)
            print("json_date", json_data)
            print(type(json_data))
            print(len(json_data))
            raise e
        
    else:
        columns = ['client_idcode', time_column,'pretty_name', 'cui', 'type_ids', 'types', 'source_value', 'detected_name', 'acc', 'context_similarity',
                    'start', 'end', 'icd10', 'ontologies', 'snomed', 'id',
                    'Time_Value', 'Time_Confidence', 'Presence_Value', 'Presence_Confidence',
                    'Subject_Value', 'Subject_Confidence', 'text_sample', 'full_doc', guid_column]
        
        empty_df = pd.DataFrame(data=None, columns=columns)
        return empty_df
    
    
    


def filter_annot_dataframe(dataframe, filter_args):
    """
    Filter a DataFrame based on specified filter arguments.

    Parameters:
    - dataframe: pandas DataFrame
    - filter_args: dict
        A dictionary containing filter arguments.

    Returns:
    - pandas DataFrame
        The filtered DataFrame.
    """
    # Initialize a boolean mask with True values for all rows
    mask = pd.Series(True, index=dataframe.index)

    # Apply filters based on the provided arguments
    for column, value in filter_args.items():
        if column in dataframe.columns:
            # Special case for 'types' column
            if column == 'types':
                mask &= dataframe[column].apply(lambda x: any(item.lower() in x for item in value))
            elif column in ['Time_Value', 'Presence_Value', 'Subject_Value']:
                # Include rows where the column is in the specified list of values
                mask &= dataframe[column].isin(value) if isinstance(value, list) else (dataframe[column] == value)
            elif column in ['Time_Confidence', 'Presence_Confidence', 'Subject_Confidence']:
                # Include rows where the column is greater than or equal to the specified confidence threshold
                mask &= dataframe[column] >= value
            elif column in ['acc']:
                # Include rows where the column is greater than or equal to the specified confidence threshold
                mask &= dataframe[column] >= value
                
            else:
                mask &= dataframe[column] >= value

    # Return the filtered DataFrame
    return dataframe[mask]


def calculate_pretty_name_count_features(df_copy, suffix = "epr"):
    
    if(len(df_copy)>0):
    
        additional_features = {
        'count': ('pretty_name', 'count'),
        # Add more features as needed
        }
        
        # Group by 'pretty_name' and apply the additional features
        result_vector = df_copy.groupby('pretty_name').size().reset_index(name='count')

        # Create a one-dimensional vector (single-row DataFrame)
        result_vector = result_vector.set_index('pretty_name').T.rename(columns={'count': f'pretty_name_count_{suffix}'})

        # Add additional features
        for feature_name, (column, function) in additional_features.items():
            result_vector[feature_name] = df_copy.groupby('pretty_name')[column].agg(function)
        
        result_vector.reset_index(drop=True, inplace=True)
        # Remove the 'pretty_name' column from the result
        #result_vector = result_vector.drop('pretty_name', axis=1, errors='ignore')
        result_vector = result_vector.drop('count', axis=1, errors='ignore')

        # Convert all values to float
        result_vector = result_vector.astype(float)
        
        col_names = result_vector.columns#[1:]
        
        result_vector = pd.DataFrame(result_vector.values, columns=col_names)
    else:
        result_vector = None

    return result_vector



