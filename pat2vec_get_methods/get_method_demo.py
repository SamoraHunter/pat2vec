import pandas as pd

from util.methods_get import filter_dataframe_by_timestamp, get_start_end_year_month  # replace 'your_module' with the actual module name containing get_start_end_year_month
import sys

sys.path.insert(0,'/home/aliencat/samora/gloabl_files')
sys.path.insert(0,'/data/AS/Samora/gloabl_files')
sys.path.insert(0,'/home/jovyan/work/gloabl_files')
sys.path.insert(0, '/home/cogstack/samora/_data/gloabl_files')

from COGStats import EthnicityAbstractor

from COGStats import append_age_at_record_series

import numpy as np

#cogstack object pass?

def get_demographics3(patlist, target_date_range, cohort_searcher_with_terms_and_search):
    """
    Get demographics information for a list of patients within a specified date range.

    Parameters:
    - patlist (list): List of patient IDs.
    - target_date_range (str): Date range in the format "YYYY-MM-DD to YYYY-MM-DD".

    Returns:
    - pd.DataFrame: Demographics information for the specified patients.
    """
    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)

    demo = cohort_searcher_with_terms_and_search(
        index_name="epr_documents",
        fields_list=["client_idcode", "client_firstname", "client_lastname", "client_dob", "client_gendercode", "client_racecode", "client_deceaseddtm", "updatetime"],
        term_name="client_idcode.keyword",
        entered_list=patlist,
        search_string=f'updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]'
    )

    demo["updatetime"] = pd.to_datetime(demo["updatetime"], utc=True)
    demo = demo.sort_values(["client_idcode", "updatetime"])

    if len(demo) > 1:
        try:
            return demo.iloc[-1].to_frame()
        except Exception as e:
            print(e)
    elif len(demo) == 1:
        return demo
    else:
        demo = pd.DataFrame(data=None, columns=None)
        demo['client_idcode'] = patlist
        return demo

# # Example use:
# patlist_example = ["patient_id1", "patient_id2"]
# date_range_example = "2023-01-01 to 2023-12-31"
# result = get_demographics3(patlist_example, date_range_example)
# print(result)



def get_demographics3_batch(patlist, target_date_range, pat_batch, config_obj=None, cohort_searcher_with_terms_and_search=None):
    
    
    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)
    
    batch_mode = config_obj.batch_mode
    
    if(batch_mode):
        
        demo = filter_dataframe_by_timestamp(pat_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'updatetime')

        
        
    else:
        demo = cohort_searcher_with_terms_and_search(index_name="epr_documents", 
                                             fields_list=["client_idcode", "client_firstname", "client_lastname", "client_dob", "client_gendercode", "client_racecode", "client_deceaseddtm", "updatetime"], 
                                             term_name="client_idcode.keyword", 
                                             entered_list=patlist,
                                            search_string= f'updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}] '
                                                )
    
    
    demo["updatetime"] = pd.to_datetime(demo["updatetime"], utc=True)
    demo = demo.sort_values(["client_idcode", "updatetime"]) #.drop_duplicates(subset = ["client_idcode"], keep = "last", inplace = True)
    
    #if more than one in the range return the nearest the end of the period
    if(len(demo)> 1):
        try:
            #print("case1")
            return demo.tail(1)
            #return demo.iloc[-1].to_frame()
        except Exception as e:
            print(e)
            
    #if only one return it        
    elif len(demo)==1:
        return demo
    
    #otherwise return only the client id
    else:
        demo = pd.DataFrame(data=None, columns=None)
        demo['client_idcode'] = patlist
        return demo
        
        
        
def get_demo(current_pat_client_id_code, target_date_range, pat_batch, config_obj=None,cohort_searcher_with_terms_and_search= None):

    
    
    current_pat_demo = get_demographics3_batch([current_pat_client_id_code], target_date_range, pat_batch, config_obj, cohort_searcher_with_terms_and_search)
    
    #display(current_pat_demo)
    
    #print(len(current_pat_demo.columns))

    if(len(current_pat_demo.columns)>1):

        current_pat_demo = append_age_at_record_series(current_pat_demo)

        #demo_dataframe = pd.DataFrame(current_pat_demo).T

        #demo_dataframe.reset_index(inplace=True)
        
        demo_dataframe = current_pat_demo.copy()

        current_pat_demo = EthnicityAbstractor.abstractEthnicity(demo_dataframe, outputNameString = '_census', ethnicityColumnString='client_racecode')

        dummied_dummy_ethnicity_dataframe = pd.DataFrame(data = [(np.zeros(5))], columns = ['census_white', 
                                                                                            'census_asian_or_asian_british',
                                                                                            'census_black_african_caribbean_or_black_british',
                                                                                            'census_mixed_or_multiple_ethnic_groups',
                                                                                            'census_other_ethnic_group'])

        cen_res = pd.get_dummies(current_pat_demo['census'], prefix = 'census')

        dummied_dummy_ethnicity_dataframe[cen_res.columns[0]] = cen_res[cen_res.columns[0]]

        abstrated_ethnicity_dummied = dummied_dummy_ethnicity_dataframe

        #abstrated_ethnicity_dummied = pd.concat([dummied_dummy_ethnicity_dataframe, pd.get_dummies(current_pat_demo['census'], prefix = 'census')], axis=1)

        current_pat_demo = pd.concat([current_pat_demo.reset_index(), abstrated_ethnicity_dummied], axis=1)

        current_pat_demo.reset_index( inplace=True)

        #current_pat_demo

        sex_map = {'Male': 1,
                   'Female': 0,
                   'male': 1,
                   'female':0}


        #         def is_dead(line):
        #             try:
        #                 if(type(line)==str or type(line)==float):
        #                     return np.isnan(line)
        #                 else:
        #                     return line.isnull
        #             except Exception as e:
        #                 print(e)
        #                 print(type(line))
        #                 print(line)
        #         def is_dead(line):
        #             return line.isna()


        current_pat_demo['male'] = current_pat_demo['client_gendercode'].map(sex_map)

        #current_pat_demo['dead'] = current_pat_demo['client_deceaseddtm'].apply(is_dead)

        #current_pat_demo['dead'] = int(int((np.isnan(current_pat_demo['client_deceaseddtm'].iloc[0])))!=1)

        current_pat_demo['dead'] = int(type(current_pat_demo['client_deceaseddtm'])==str)

        current_pat_demo = current_pat_demo[['client_idcode',
                                             'male',
                                             'age',
                                             'dead',
                                             'census_white',
                                             'census_asian_or_asian_british',
                                             'census_black_african_caribbean_or_black_british',
                                             'census_mixed_or_multiple_ethnic_groups',
                                             'census_other_ethnic_group']].copy()

        current_pat_demo['dead'] = current_pat_demo['dead'].astype(int)

        current_pat_demo['age'] = current_pat_demo['age'].astype(int)

        current_pat_demo['dead'] = current_pat_demo['dead'].astype(float)

        current_pat_demo['age'] = current_pat_demo['age'].astype(float)

        current_pat_demo['male'] = current_pat_demo['male'].astype(float)



        return current_pat_demo