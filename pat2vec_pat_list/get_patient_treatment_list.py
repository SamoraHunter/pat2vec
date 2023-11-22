import pandas as pd
import random
import pickle
from typing import List

def extract_treatment_id_list_from_docs(config_obj):
    """
    Retrieves a list of unique client IDs from a treatment document specified in the configuration.

    Parameters:
    - config_obj (object): An object containing configuration parameters.
        - treatment_doc_filename (str): The filename of the treatment document (CSV format).

    Returns:
    - list: A list of unique client IDs from the treatment document.
    """
    
    
    
    # Extract the treatment document filename from the configuration object
    treatment_doc_filename = config_obj.treatment_doc_filename
    
    
    
    # Read the treatment document into a pandas DataFrame
    docs = pd.read_csv(treatment_doc_filename)
    
    # Extract the unique client IDs from the document
    treatment_client_id_list = list(docs['client_idcode'].unique())
    
    return treatment_client_id_list



def generate_control_list(treatment_client_id_list: List[str], treatment_control_ratio_n: int, control_list_path: str, verbosity: int = 0) -> None:
    """
    Generate a list of control patients for a given list of treatment patients.

    Args:
        treatment_client_id_list (List[str]): A list of client IDs for the treatment group.
        treatment_control_ratio_n (int): The ratio of control patients to treatment patients.
        control_list_path (str): The path to save the control list.
        verbosity (int, optional): The level of verbosity. Defaults to 0.

    Returns:
        all_patient_list_control
    """
    random.seed(42)

    # Get control docs default 1:1
    all_idcodes = pd.read_csv('all_client_idcodes_epr_unique.csv')['client_idcode']

    full_control_client_id_list = list(set(all_idcodes) - set(treatment_client_id_list))
    full_control_client_id_list.sort()  # ensure sort for repeatability

    n_treatments = len(treatment_client_id_list) * treatment_control_ratio_n
    if verbosity > 0:
        print(f"{n_treatments} selected as controls")  # Soft control selection, many treatments will be false positives

    treatment_control_sample = pd.DataFrame(full_control_client_id_list).sample(n_treatments, random_state=42)['client_idcode']
    all_patient_list_control = list(treatment_control_sample.values)

    with open(control_list_path, 'wb') as f:
        pickle.dump(all_patient_list_control, f)

    if verbosity > 0:
        print(all_patient_list_control[0:10])
        
    return all_patient_list_control


def get_all_patients_list(config_obj):
    """
    Extracts a list of all patient IDs from the given configuration object.
    
    Args:
        config_obj: A configuration object containing the necessary parameters.
        
    Returns:
        A list of all patient IDs.
    """
    patient_ids = extract_treatment_id_list_from_docs(config_obj)
    
    all_patient_list = patient_ids.copy()
    
    if config_obj.use_controls:
        
        control_ids = generate_control_list(treatment_client_id_list=patient_ids,
                                             treatment_control_ratio_n=config_obj.treatment_control_ratio_n,
                                             control_list_path=config_obj.control_list_path,
                                             verbosity=config_obj.verbosity)
        all_patient_list.extend(control_ids)
        
        
    # Propensity score matching here or in super function?
        
    return all_patient_list


