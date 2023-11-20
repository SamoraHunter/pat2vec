

import csv
import multiprocessing
import os
#import tqdm
import re
import sys
import warnings
from csv import writer
from multiprocessing import Pool
from os.path import exists

import numpy as np
import pandas as pd
#from tqdm import trange
from colorama import Back, Fore, Style
from IPython.utils import io
from tqdm import trange
from pat2vec_get_methods.current_pat_annotations_to_file import get_current_pat_annotations_batch_to_file, get_current_pat_annotations_mct_batch_to_file
from patvec_get_batch_methods.main import get_pat_batch_bloods, get_pat_batch_bmi, get_pat_batch_demo, get_pat_batch_diagnostics, get_pat_batch_drugs, get_pat_batch_epr_docs, get_pat_batch_mct_docs, get_pat_batch_news, get_pat_batch_obs

from util.methods_get import list_dir_wrapper, update_pbar

color_bars = [Fore.RED,
    Fore.GREEN,
    Fore.BLUE,
    Fore.MAGENTA,
    Fore.YELLOW,
    Fore.CYAN,
    Fore.WHITE]

#nb_full_path = os.path.join(os.getcwd(), nb_name)
import datetime as dt
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from os.path import exists
from pathlib import Path

import config_pat2vec
import paramiko
from cogstack_v8_lite import *  # wrap with option and put behind boolean check, no wildcard in function.
from credentials import *
from medcat.cat import CAT

#stuff paths for portability
sys.path.insert(0,'/home/aliencat/samora/gloabl_files')
sys.path.insert(0,'/data/AS/Samora/gloabl_files')
sys.path.insert(0,'/home/jovyan/work/gloabl_files')
sys.path.insert(0, '/home/cogstack/samora/_data/gloabl_files')


import pickle
import traceback
from datetime import datetime
from pathlib import Path

from COGStats import *
from scipy import stats


def convert_date(date_string):
    date_string = date_string.split("T")[0]
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    return date_object

import os
import subprocess
import time
from datetime import datetime
from io import StringIO

from dateutil.relativedelta import relativedelta


class main:
    def __init__(self, parameter1, parameter2, aliencat=False, dgx=False, dhcap=False, dhcap02=True,
             batch_mode=True, remote_dump=False, negated_presence_annotations=False,
             store_annot=True, share_sftp=True, multi_process=True, annot_first=False,
             strip_list=True, cogstack=True, verbosity = 0, config = None, use_filter=False,
             json_filter_path = None, random_seed_val=42, treatment_client_id_list = None,
             hostname =None):


        # Additional parameters
        self.aliencat = aliencat
        self.dgx = dgx
        self.dhcap = dhcap
        self.dhcap02 = dhcap02
        self.batch_mode = batch_mode
        self.remote_dump = remote_dump
        self.negated_presence_annotations = negated_presence_annotations
        self.store_annot = store_annot
        self.share_sftp = share_sftp
        self.multi_process = multi_process
        self.annot_first = annot_first
        self.strip_list = strip_list
        self.verbosity = verbosity
        self.random_seed_val = random_seed_val
        self.treatment_client_id_list = treatment_client_id_list
        self.hostname = hostname
        
        if(config==None):
            config = config_pat2vec.config_class()
            
        
        
    
        #config parameters
        self.suffix = config.suffix
        self.treatment_doc_filename = config.treatment_doc_filename
        self.treatment_control_ratio_n = config.treatment_control_ratio_n
        self.pre_annotation_path = config.pre_annotation_path
        self.pre_annotation_path_mrc = config.pre_annotation_path_mrc
        self.proj_name = config.proj_name

        

        
        # Create a folder for logs if it doesn't exist
        log_folder = "logs"
        os.makedirs(log_folder, exist_ok=True)

        # Create a logger
        self.logger = logging.getLogger(__name__)

        # Create a handler that writes log messages to a file with a timestamp
        log_file = f"{log_folder}/logfile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)

        # Create a formatter to include timestamp in the log messages
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Optionally set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        self.logger.setLevel(logging.DEBUG)

        # Add the handlers to the logger
        self.logger.addHandler(file_handler)

        # Optionally, add a StreamHandler to print log messages to the console as well
        stdout_handler = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(stdout_handler)

        # Now you can use the logger to log messages within the class
        self.logger.info("Initialized pat2vec.main")
        


        
        
        if self.verbosity > 0:
            print(self.pre_annotation_path)
            print(self.pre_annotation_path_mrc)
            
            
        self.use_filter = use_filter
        
        if(self.use_filter):
            self.json_filter_path = json_filter_path
            import json

            with open(self.json_filter_path, 'r') as f:
                json_data = json.load(f)
                
            len(json_data['projects'][0])
            json_cuis = json_data['projects'][0]['cuis'].split(",")
            self.cat.cdb.filter_by_cui(json_cuis)
    
        if not(self.dhcap) and not (self.dhcap02):

            gpu_index,free_mem  = self.get_free_gpu()

        if(int(free_mem)>4000):
            os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_index)
            print(f"Setting gpu with {free_mem} free")
        else:
            print(f"Setting NO gpu, most free memory: {free_mem} !")
            os.environ["CUDA_VISIBLE_DEVICES"] = "-1"



    
    


        random.seed(self.random_seed_val)
        use_controls = False
        if(use_controls):
            # Get control docs default 1:1

            all_idcodes = pd.read_csv('all_client_idcodes_epr_unique.csv')['client_idcode']

            
            print(len(all_idcodes), len(self.treatment_client_id_list))

            full_control_client_id_list = list(set(all_idcodes) - set(self.treatment_client_id_list))
            
            full_control_client_id_list.sort() # ensure sort for repeatability

            len(full_control_client_id_list) - len(all_idcodes)

            n_treatments = len(self.treatment_client_id_list) * self.treatment_control_ratio_n
            print(f"{n_treatments} selected as controls") #Soft control selection, many treatments will be false positives
            treatment_control_sample = pd.Series(full_control_client_id_list).sample(n_treatments, random_state=42)

            treatment_control_sample

            self.all_patient_list_control = list(treatment_control_sample.values)
            
            with open('control_list.pkl', 'wb') as f:
                pickle.dump(self.all_patient_list_control, f)
                
            print(self.all_patient_list_control[0:10])
            
            

        self.all_patient_list = list(self.treatment_client_id_list)



        random.shuffle(self.all_patient_list)
        

        print(f"remote_dump {self.remote_dump}")
        print(self.pre_annotation_path)
        print(self.pre_annotation_path_mrc)
        print(self.current_pat_line_path)

        if(self.remote_dump):


            pre_path = f'/mnt/hdd1/samora/{self.proj_name}/'

            # Set the hostname, username, and password for the remote machine
            
            if(not self.aliencat or self.dgx):
                hostname = '%HOSTIPADDRESS%'
                
            if(self.aliencat and not self.dgx):
                hostname = 'localhost'
            
            username = '%USERNAME%'
            password = '%PASSWORD%'

            # Create an SSH client and connect to the remote machine
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()

            if(self.remote_dump):
                try:
                    sftp_client.chdir(pre_path)  # Test if remote_path exists
                except IOError:
                    sftp_client.mkdir(pre_path)  # Create remote_path



            pre_annotation_path = f"{pre_path}{self.pre_annotation_path}"
            pre_annotation_path_mrc = f"{pre_path}{self.pre_annotation_path_mrc}"
            current_pat_line_path = f"{pre_path}{self.current_pat_line_path}"
            current_pat_lines_path = current_pat_line_path
            
            
            if(self.remote_dump==False):
                Path(self.current_pat_annot_path).mkdir(parents=True, exist_ok=True)
                Path(pre_annotation_path_mrc).mkdir(parents=True, exist_ok=True)

            else:
                try:
                    sftp_client.chdir(pre_annotation_path)  # Test if remote_path exists
                except IOError:
                    sftp_client.mkdir(pre_annotation_path)  # Create remote_path

                try:
                    sftp_client.chdir(pre_annotation_path_mrc)  # Test if remote_path exists
                except IOError:
                    sftp_client.mkdir(pre_annotation_path_mrc)  # Create remote_path
                    
                try:
                    sftp_client.chdir(current_pat_line_path)  # Test if remote_path exists
                except IOError:
                    sftp_client.mkdir(current_pat_line_path)  # Create remote_path
        else:
            sftp_client = None
            
            
            
        
        self.stripped_list_start = [x.replace(".csv","") for x in list_dir_wrapper(self.current_pat_lines_path, self.sftp_client)]
        
        
        print(len(self.stripped_list_start))

                
                
        self.stripped_list = [x.replace(".csv","") for x in list_dir_wrapper(self.current_pat_lines_path, self.sftp_client)]
        


        random.seed()
        random.shuffle(self.all_patient_list)

        skipped_counter = 0
        self.t = trange(len(self.all_patient_list), desc='Bar desc', leave=True, colour="GREEN", position=0, total=len(self.all_patient_list))
        
        
        
        
        
    #------------------------------------begin main----------------------------------       
    
            

        
        
    
            

    def pat_maker(self, i):
        #global skipped_counter
        #global stripped_list
        
        skipped_counter = self.config_obj.skipped_counter
        stripped_list = self.config_obj.stripped_list
        all_patient_list = self.config_obj.all_patient_list
        skipped_counter = self.config_obj.skipped_counter
        
        remote_dump = self.config_obj.remote_dump
        hostname = self.config_obj.hostname
        username = self.config_obj.username
        password = self.config_obj.password
        annot_first = self.config_obj.annot_first
        
        stripped_list_start = self.stripped_list_start
        
        combinations = self.combinations
        
        
        
        
        
        
        current_pat_client_id_code = all_patient_list[i]
        
        p_bar_entry = current_pat_client_id_code
        
        start_time = time.time()
        
        update_pbar(p_bar_entry, start_time, 0, f'Pat_maker called on {i}...')
        
        #time.sleep(random.randint(1, 50))
        #i, sftp_obj = i[0], i[1]
        if(remote_dump):
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password, timeout=60)

            sftp_obj = ssh_client.open_sftp()
        else:
            sftp_obj = None
            
        
        
        
        

        
        #get_pat batches
        
        stripped_list = stripped_list_start.copy()
        
        
        if(current_pat_client_id_code not in stripped_list_start):
            
            update_pbar(p_bar_entry, start_time, 0, 'Getting batches...')
        
        
            search_term = None # inside function
            batch_epr = get_pat_batch_epr_docs(current_pat_client_id_code, search_term)


            search_term = None # inside function
            batch_mct = get_pat_batch_mct_docs(current_pat_client_id_code, search_term)

            if(annot_first == False):

                search_term = 'CORE_SmokingStatus'

                batch_smoking = get_pat_batch_obs(current_pat_client_id_code, search_term)


                search_term = 'CORE_SpO2'

                batch_core_02 = get_pat_batch_obs(current_pat_client_id_code, search_term)


                search_term = 'CORE_BedNumber3'

                batch_bednumber = get_pat_batch_obs(current_pat_client_id_code, search_term)


                search_term = 'CORE_VTE_STATUS'

                batch_vte = get_pat_batch_obs(current_pat_client_id_code, search_term)


                search_term = 'CORE_HospitalSite'

                batch_hospsite = get_pat_batch_obs(current_pat_client_id_code, search_term)


                search_term = 'CORE_RESUS_STATUS'

                batch_resus = get_pat_batch_obs(current_pat_client_id_code, search_term)


                search_term = None # inside function
                batch_news = get_pat_batch_news(current_pat_client_id_code, search_term)


                search_term = None # inside function
                batch_bmi = get_pat_batch_bmi(current_pat_client_id_code, search_term)


                search_term = None # inside function
                batch_diagnostics = get_pat_batch_diagnostics(current_pat_client_id_code, search_term)

                search_term = None # inside function
                batch_drugs = get_pat_batch_drugs(current_pat_client_id_code, search_term)




                search_term = None # inside function
                batch_demo = get_pat_batch_demo(current_pat_client_id_code, search_term)

                search_term = None # inside function
                batch_bloods =  get_pat_batch_bloods(current_pat_client_id_code, search_term)

            update_pbar(p_bar_entry, start_time, 0, f'Done batches in {time.time()-start_time}')


            run_on_pat = False

            only_check_last = True

            last_check = all_patient_list[i] not in stripped_list

            skip_check = last_check

            for j in range(0, len(combinations)):
                try:
                    if(only_check_last):
                        run_on_pat = last_check
                    else:
                        run_on_pat = all_patient_list[i]  not in stripped_list


                    if(run_on_pat):   
                        if(annot_first):

                            get_current_pat_annotations_batch_to_file(all_patient_list[i], combinations[j], batch_epr, sftp_obj, skip_check=skip_check)

                            get_current_pat_annotations_mct_batch_to_file(all_patient_list[i], combinations[j], batch_mct, sftp_obj, skip_check=skip_check)

                        else:
                            main_batch(all_patient_list[i],
                            combinations[j],
                            batch_demo = batch_demo,
                            batch_smoking = batch_smoking,
                            batch_core_02 = batch_core_02,
                            batch_bednumber = batch_bednumber,
                            batch_vte = batch_vte,
                            batch_hospsite = batch_hospsite,
                            batch_resus = batch_resus,
                            batch_news = batch_news,
                            batch_bmi = batch_bmi,
                            batch_diagnostics = batch_diagnostics,
                            batch_epr = batch_epr,
                            batch_mct = batch_mct,
                            batch_bloods = batch_bloods,
                            batch_drugs = batch_drugs,
                            sftp_obj = sftp_obj

                            )

                except Exception as e:
                    print(e)
                    print(f"Exception in patmaker on {all_patient_list[i], combinations[j]}")
                    print(traceback.format_exc())
            if(remote_dump):
                sftp_obj.close()
                ssh_client.close()
        else:
            if(multi_process == False):
                skipped_counter = skipped_counter + 1
                update_pbar(str(i), start_time, 0, f'Skipped {i}')
            else:
                with skipped_counter.get_lock():
                    skipped_counter.value += 1
                update_pbar(str(i), start_time, 0, f'Skipped {i}')
            
    




    