import os
import sys

import numpy as np
import pandas as pd
from cogstack_search_methods.cogstack_v8_lite import append_age_at_record_series
from IPython.display import display

# from COGStats import EthnicityAbstractor
# from COGStats import *
from pat2vec.util.ethnicity_abstractor import EthnicityAbstractor
from pat2vec.util.methods_get import get_demographics3_batch

def get_demo(current_pat_client_id_code, target_date_range, pat_batch, config_obj=None):
    """
    Retrieve demographic information for a patient based on the provided parameters.

    Parameters:
    - current_pat_client_id_code (str): The client ID code for the current patient.
    - target_date_range (tuple): A tuple representing the date range for which demographic information is required.
    - pat_batch (dataframe): The demo batch dataframe for the patient.
    - config_obj (Config,): Contains config options.

    Returns:
    - pd.DataFrame: A DataFrame containing the demographic information for the specified patient, including columns such as:
      - 'client_idcode': The client ID code for the patient.
      - 'male': Binary representation of gender (1 for male, 0 for female).
      - 'age': Age of the patient.
      - 'dead': Binary representation of whether the patient is deceased (1 for deceased, 0 for alive).
      - 'census_white': Binary representation of white ethnicity.
      - 'census_asian_or_asian_british': Binary representation of Asian or Asian British ethnicity.
      - 'census_black_african_caribbean_or_black_british': Binary representation of Black African, Caribbean, or Black British ethnicity.
      - 'census_mixed_or_multiple_ethnic_groups': Binary representation of mixed or multiple ethnic groups.
      - 'census_other_ethnic_group': Binary representation of other ethnic groups.

    Note:
    - The function utilizes various helper functions, such as get_demographics3_batch, append_age_at_record_series,
      and EthnicityAbstractor.abstractEthnicity.
    - The 'config_obj' parameter allows for customization of the function's behavior, with verbosity control.
    """

    current_pat_demo = get_demographics3_batch(
        [current_pat_client_id_code], target_date_range, pat_batch, config_obj=config_obj)

    # display(current_pat_demo)

    # print(len(current_pat_demo.columns))

    if (len(current_pat_demo.columns) > 1):

        current_pat_demo = append_age_at_record_series(current_pat_demo)

        # demo_dataframe = pd.DataFrame(current_pat_demo).T

        # demo_dataframe.reset_index(inplace=True)

        demo_dataframe = current_pat_demo.copy()

        current_pat_demo = EthnicityAbstractor.abstractEthnicity(
            demo_dataframe, outputNameString='_census', ethnicityColumnString='client_racecode')

        dummied_dummy_ethnicity_dataframe = pd.DataFrame(data=[(np.zeros(5))], columns=['census_white',
                                                                                        'census_asian_or_asian_british',
                                                                                        'census_black_african_caribbean_or_black_british',
                                                                                        'census_mixed_or_multiple_ethnic_groups',
                                                                                        'census_other_ethnic_group'])

        cen_res = pd.get_dummies(current_pat_demo['census'], prefix='census')

        dummied_dummy_ethnicity_dataframe[cen_res.columns[0]
                                          ] = cen_res[cen_res.columns[0]]

        abstrated_ethnicity_dummied = dummied_dummy_ethnicity_dataframe

        # abstrated_ethnicity_dummied = pd.concat([dummied_dummy_ethnicity_dataframe, pd.get_dummies(current_pat_demo['census'], prefix = 'census')], axis=1)

        current_pat_demo = pd.concat(
            [current_pat_demo.reset_index(), abstrated_ethnicity_dummied], axis=1)

        current_pat_demo.reset_index(inplace=True)

        # current_pat_demo

        sex_map = {'Male': 1,
                   'Female': 0,
                   'male': 1,
                   'female': 0}

        current_pat_demo['male'] = current_pat_demo['client_gendercode'].map(
            sex_map)

        # current_pat_demo['dead'] = current_pat_demo['client_deceaseddtm'].apply(is_dead)

        # current_pat_demo['dead'] = int(int((np.isnan(current_pat_demo['client_deceaseddtm'].iloc[0])))!=1)

        current_pat_demo['dead'] = int(
            type(current_pat_demo['client_deceaseddtm']) == str)

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

        if config_obj.verbosity >= 6:
            display(current_pat_demo)

        return current_pat_demo
