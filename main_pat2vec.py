
import sys
#stuff paths for portability
sys.path.insert(0,'/home/aliencat/samora/gloabl_files')
sys.path.insert(0,'/data/AS/Samora/gloabl_files')
sys.path.insert(0,'/home/jovyan/work/gloabl_files')
sys.path.insert(0, '/home/cogstack/samora/_data/gloabl_files')
sys.path.insert(0, '/home/cogstack/samora/_data/gloabl_files/pat2vec')
import csv
import multiprocessing
import os
#import tqdm
import re


from pat2vec_pat_list.get_patient_treatment_list import get_all_patients_list


import random
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

from pat2vec_get_methods.current_pat_annotations_to_file import (
    get_current_pat_annotations_batch_to_file,
    get_current_pat_annotations_mct_batch_to_file)
from pat2vec_main_methods.main_batch import main_batch
from patvec_get_batch_methods.main import (get_pat_batch_bloods,
                                           get_pat_batch_bmi,
                                           get_pat_batch_demo,
                                           get_pat_batch_diagnostics,
                                           get_pat_batch_drugs,
                                           get_pat_batch_epr_docs,
                                           get_pat_batch_mct_docs,
                                           get_pat_batch_news,
                                           get_pat_batch_obs)
from util import config_pat2vec
from util.methods_get import filter_stripped_list, generate_date_list, list_dir_wrapper, update_pbar
from util.methods_get_medcat import get_cat


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
import pickle
import random
import subprocess
import time
import traceback
from datetime import datetime, timedelta, timezone
from io import StringIO
from os.path import exists
from pathlib import Path

import paramiko
from cogstack_v8_lite import *  # wrap with option and put behind boolean check, no wildcard in function.
from COGStats import *
from credentials import *
from dateutil.relativedelta import relativedelta
from medcat.cat import CAT
from scipy import stats

#from util.config_pat2vec import config_class


class main:
    def __init__(self, cogstack=True, verbosity = 0, use_filter=False,
             json_filter_path = None, random_seed_val=42,
             hostname =None, config_obj = None, ):


    

        # Additional parameters
        self.aliencat = config_obj.aliencat
        self.dgx = config_obj.dgx
        self.dhcap = config_obj.dhcap
        self.dhcap02 = config_obj.dhcap02
        self.batch_mode = config_obj.batch_mode
        self.remote_dump = config_obj.remote_dump
        self.negated_presence_annotations = config_obj.negated_presence_annotations
        self.store_annot = config_obj.store_annot
        self.share_sftp = config_obj.share_sftp
        self.multi_process = config_obj.multi_process
        self.annot_first = config_obj.annot_first
        self.strip_list = config_obj.strip_list
        self.verbosity = config_obj.verbosity
        self.random_seed_val = config_obj.random_seed_val
        #self.treatment_client_id_list = config_obj.treatment_client_id_list
        self.hostname = config_obj.hostname

        self.config_obj = config_obj
        
        
        if(self.config_obj==None):
            print("Init default config on config_pat2vec")
            self.config_obj = config_pat2vec.config_class()
            
        
        
    
        #config parameters
        self.suffix = config_obj.suffix
        self.treatment_doc_filename = config_obj.treatment_doc_filename
        self.treatment_control_ratio_n = config_obj.treatment_control_ratio_n
        self.pre_annotation_path = config_obj.pre_annotation_path
        self.pre_annotation_path_mrc = config_obj.pre_annotation_path_mrc
        self.proj_name = config_obj.proj_name
        self.gpu_mem_threshold = config_obj.gpu_mem_threshold

        

        self.all_patient_list = get_all_patients_list(config_obj)
        
        
        self.current_pat_line_path = config_obj.current_pat_line_path
        self.current_pat_lines_path = config_obj.current_pat_lines_path
        self.sftp_client = config_obj.sftp_obj

        
        # Create a folder for logs if it doesn't exist
        log_folder = "logs"
        os.makedirs(log_folder, exist_ok=True)

        # Create a logger
        self.logger = logging.getLogger(__name__)

        # Create a handler that writes log messages to a file with a timestamp
        log_file = f"{log_folder}/logfile_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
            
        else:
            gpu_index,free_mem = -1, self.gpu_mem_threshold -1

        if(int(free_mem)>self.gpu_mem_threshold):
            os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_index)
            print(f"Setting gpu with {free_mem} free")
        else:
            print(f"Setting NO gpu, most free memory: {free_mem} !")
            os.environ["CUDA_VISIBLE_DEVICES"] = "-1"




        random.shuffle(self.all_patient_list)
        
        if(self.verbosity > 0):
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
            
            
            
        
        self.stripped_list_start = [x.replace(".csv","") for x in list_dir_wrapper(path = self.current_pat_lines_path,  config_obj=config_obj)]
        
        
        print(len(self.stripped_list_start))

                
                
        
        stripped_list = [x.replace(".csv","") for x in list_dir_wrapper(path = self.current_pat_lines_path, config_obj=config_obj)]
        


        random.seed()
        random.shuffle(self.all_patient_list)

        skipped_counter = 0
        self.t = trange(len(self.all_patient_list), desc='Bar desc', leave=True, colour="GREEN", position=0, total=len(self.all_patient_list))
        
        
        self.cat = get_cat(config_obj)
        
        
        
        self.stripped_list = [x.replace(".csv","") for x in list_dir_wrapper(path = self.current_pat_lines_path, config_obj=config_obj)]
        
        
        self.stripped_list = filter_stripped_list(self.stripped_list, config_obj = self.config_obj)
        
        
        self.date_list = config_obj.date_list
        
        self.n_pat_lines = config_obj.n_pat_lines
        
    #------------------------------------begin main----------------------------------       
    
            

        
        
    
            

    def pat_maker(self, i):
        #global skipped_counter
        #global stripped_list
        
        skipped_counter = self.config_obj.skipped_counter
        stripped_list = self.stripped_list
        all_patient_list = self.all_patient_list
        skipped_counter = self.config_obj.skipped_counter
        
        remote_dump = self.config_obj.remote_dump
        hostname = self.config_obj.hostname
        username = self.config_obj.username
        password = self.config_obj.password
        annot_first = self.config_obj.annot_first
        
        stripped_list_start = self.stripped_list_start
        
        date_list = self.date_list
        
        multi_process = self.config_obj.multi_process
        
        
        
        
        
        
        current_pat_client_id_code = all_patient_list[i]
        
        p_bar_entry = current_pat_client_id_code
        
        start_time = time.time()
        #update_pbar(current_pat_client_id_code, start_time, stage_int, stage_str, t, config_obj, skipped_counter=None, **n_docs_to_annotate)
        update_pbar(p_bar_entry, start_time, 0, f'Pat_maker called on {i}...', self.t, self.config_obj, skipped_counter)
        
        #time.sleep(random.randint(1, 50))
        #i, sftp_obj = i[0], i[1]
        
            
            
        sftp_obj = self.config_obj.sftp_obj
        
        
        
        

        
        #get_pat batches
        
        stripped_list = stripped_list_start.copy()
        
        
        if(current_pat_client_id_code not in stripped_list_start):
            
            update_pbar(p_bar_entry, start_time, 0, 'Getting batches...', self.t, self.config_obj, skipped_counter)
        
        
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

            update_pbar(p_bar_entry, start_time, 0, f'Done batches in {time.time()-start_time}', self.t, self.config_obj, skipped_counter)


            run_on_pat = False

            only_check_last = True

            last_check = all_patient_list[i] not in stripped_list

            skip_check = last_check

            for j in range(0, len(date_list)):
                try:
                    if(only_check_last):
                        run_on_pat = last_check
                    else:
                        run_on_pat = all_patient_list[i]  not in stripped_list


                    if(run_on_pat):   
                        if(annot_first):

                            get_current_pat_annotations_batch_to_file(all_patient_list[i], date_list[j], batch_epr, sftp_obj, skip_check=skip_check)

                            get_current_pat_annotations_mct_batch_to_file(all_patient_list[i], date_list[j], batch_mct, sftp_obj, skip_check=skip_check)

                        else:
                            main_batch(all_patient_list[i],
                            date_list[j],
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
                    print(f"Exception in patmaker on {all_patient_list[i], date_list[j]}")
                    print(traceback.format_exc())
            if(remote_dump):
                sftp_obj.close()
                self.config_obj.ssh_client.close()
        else:
            if(multi_process == False):
                skipped_counter = skipped_counter + 1
                update_pbar(str(i), start_time, 0, f'Skipped {i}', self.t, self.config_obj, skipped_counter)
            else:
                with skipped_counter.get_lock():
                    skipped_counter.value += 1
                update_pbar(str(i), start_time, 0, f'Skipped {i}', self.t, self.config_obj, skipped_counter)
            
    





    

