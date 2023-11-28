import os
import sys

import numpy as np
import pandas as pd
from IPython.display import display

sys.path.insert(0,'/home/aliencat/samora/gloabl_files')
sys.path.insert(0,'/data/AS/Samora/gloabl_files')
sys.path.insert(0,'/home/jovyan/work/gloabl_files')
sys.path.insert(0,'/home/cogstack/samora/gloabl_files')
sys.path.insert(0,'/home/cogstack/samora/_data/gloabl_files')
from cogstack_v8_lite import append_age_at_record_series
from COGStats import *
from COGStats import EthnicityAbstractor

from util.methods_get import get_demographics3_batch


def get_demo(current_pat_client_id_code, target_date_range, pat_batch, config_obj=None):

    
    
    current_pat_demo = get_demographics3_batch([current_pat_client_id_code], target_date_range, pat_batch, config_obj=config_obj)
    
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

        if config_obj.verbosity >= 6: display(current_pat_demo)

        return current_pat_demo