import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import paramiko

#stuff paths for portability
sys.path.insert(0,'/home/aliencat/samora/gloabl_files')
sys.path.insert(0,'/data/AS/Samora/gloabl_files')
sys.path.insert(0,'/home/jovyan/work/gloabl_files')
sys.path.insert(0, '/home/cogstack/samora/_data/gloabl_files')
sys.path.insert(0, '/home/cogstack/samora/_data/gloabl_files/pat2vec')

from util.methods_get import generate_date_list


class config_class:
    def __init__(self,
                 remote_dump=False,
                 suffix='',
                 treatment_doc_filename='treatment_docs.csv',
                 treatment_control_ratio_n=1,
                 proj_name='new_project',
                 current_path_dir=".",
                 main_options = None,
                 start_date = (datetime(1995, 1, 1)),
                 years = 0,
                 months = 0, 
                 days = 1,
                 aliencat = False,
                 dgx = False, 
                 dhcap = False,
                 dhcap02 = True,
                 batch_mode = True,
                 store_annot = False,
                 share_sftp = True,
                 multi_process = False,
                 annot_first = False,
                 strip_list = True,
                 verbosity = 3,
                 random_seed_val = 42,
                 hostname = None,
                 username= None,
                 password=None,
                 gpu_mem_threshold = 4000,
                 testing=False,
                 use_controls = False,
                 medcat = False,
                 global_start_year=None,
                 global_start_month=None,
                 global_end_year=None,
                 global_end_month=None,
                 skip_additional_listdir = False,
                 start_time = None,
                 root_path = None,
                 negate_biochem = False,
                 patient_id_column_name='client_idcode',
                 overwrite_stored_pat_docs = False,
                 store_pat_batch_docs = True,
                 annot_filter_options = None,
                 shuffle_pat_list = False
                 
                 ):
        
        
        self.suffix = suffix
        self.treatment_doc_filename = treatment_doc_filename
        self.treatment_control_ratio_n = treatment_control_ratio_n
        self.pre_annotation_path = f'current_pat_annots_parts{self.suffix}/'
        self.pre_annotation_path_mrc = f'current_pat_annots_mrc_parts{self.suffix}/'
        
        #self.pre_document_day_path = f'current_pat_documents{self.suffix}/'
        self.pre_document_annotation_batch_path = f'current_pat_documents_annotations_batches{self.suffix}/'
        self.pre_document_annotation_batch_path_mct = f'current_pat_documents_annotations_batches_mct{self.suffix}/'
        self.pre_document_batch_path = f"current_pat_document_batches{self.suffix}/"
        self.pre_document_batch_path_mct = f"current_pat_document_batches_mct{self.suffix}/"
        
        self.store_pat_batch_docs = store_pat_batch_docs
        
        self.proj_name = proj_name
        self.main_options = main_options
        
        self.negate_biochem = negate_biochem
        self.patient_id_column_name = patient_id_column_name
        
        
        self.aliencat = aliencat
        self.dgx = dgx
        self.dhcap = dhcap
        self.dhcap02 = dhcap02
        self.batch_mode = batch_mode
        self.remote_dump = remote_dump
        
        self.store_annot = store_annot
        self.share_sftp = share_sftp
        self.multi_process = multi_process
        self.annot_first = annot_first
        self.strip_list = strip_list
        self.verbosity = verbosity
        self.random_seed_val = random_seed_val
        
        self.hostname = hostname
        self.username = username
        self.password = password
        
        self.gpu_mem_threshold = gpu_mem_threshold
        
        self.testing = testing
        self.use_controls = use_controls
        
        self.skipped_counter = 0 #init start
        
        self.medcat = medcat
        
        self.root_path = root_path
        
        self.overwrite_stored_pat_docs = overwrite_stored_pat_docs
        
        self.annot_filter_options = annot_filter_options
        
        self.start_time = start_time
        
        self.shuffle_pat_list = shuffle_pat_list
        
        if(start_time ==None):
            self.start_time = datetime.now()

        
        if(self.main_options == None):
            if(self.verbosity >= 1):
                print('default main_options set!')
            
            self.main_options = {'demo':True,
                'bmi':False,
                'bloods':False,
                'drugs':False,
                'diagnostics':False,
                
                'core_02':False,
                'bed':False,
                'vte_status':False,
                'hosp_site':False,
                'core_resus':False,
                'news':False,
                
                'annotations':False,
                'annotations_mrc': False,
                'negated_presence_annotations':False
                
               }
            if(self.verbosity >= 1):
                print(self.main_options)
            
        if(self.annot_filter_options ==None):
            self.filter_arguments = {
                'Confidence': 0.8,
                'Accuracy': 0.8,
                'types': ['qualifier value', 'procedure', 'substance', 'finding', 'environment', 'disorder', 'observable entity'],
                'Time_Value': ['Recent', 'Past'],  # Specify the values you want to include in a list
                'Time_Confidence': 0.8,  # Specify the confidence threshold as a float
                'Presence_Value': ['True'],  # Specify the values you want to include in a list
                'Presence_Confidence': 0.8,  # Specify the confidence threshold as a float
                'Subject_Value': ['Patient'],  # Specify the values you want to include in a list
                'Subject_Confidence': 0.8  # Specify the confidence threshold as a float
            }
            
        self.negated_presence_annotations = self.main_options.get('negated_presence_annotations')


        if(remote_dump==False):
        
            if(self.root_path == None):
                self.root_path = f'{os.getcwd()}/{self.proj_name}/'
            
            self.pre_annotation_path = self.root_path + self.pre_annotation_path
            
            self.pre_annotation_path_mrc = self.root_path + self.pre_annotation_path_mrc
            
            #Make document batch paths for epr and mct
            
            self.pre_document_batch_path = self.root_path + self.pre_document_batch_path 
            
            self.pre_document_batch_path_mct = self.root_path + self.pre_document_batch_path_mct 
            
            #Make annotation batch paths for epr and mct
            
            self.pre_document_annotation_batch_path = self.root_path + self.pre_document_annotation_batch_path
            
            self.pre_document_annotation_batch_path_mct = self.root_path + self.pre_document_annotation_batch_path_mct
            
            self.output_folder = 'outputs'
            
            self.output_folder = os.path.join(self.root_path, self.output_folder)
            
            Path(self.pre_annotation_path).mkdir(parents=True, exist_ok=True)
            Path(self.pre_annotation_path_mrc).mkdir(parents=True, exist_ok=True)
            
            Path(self.output_folder).mkdir(parents=True, exist_ok=True)
            
            Path(self.pre_document_batch_path).mkdir(parents=True, exist_ok=True)
            Path(self.pre_document_batch_path_mct).mkdir(parents=True, exist_ok=True)
            
            Path(self.pre_document_annotation_batch_path).mkdir(parents=True, exist_ok=True)
            Path(self.pre_document_annotation_batch_path_mct).mkdir(parents=True, exist_ok=True)
            
            
            
            print(self.pre_annotation_path)
            print(self.pre_annotation_path_mrc)
            
            print(self.pre_document_batch_path)
            print(self.pre_document_batch_path_mct)
            
            print(self.pre_document_annotation_batch_path)
            print(self.pre_document_annotation_batch_path_mct)
            
            print(self.output_folder)
            
            
            
        self.current_pat_line_path = f"current_pat_lines_parts{self.suffix}/"

        if(remote_dump==False):
            
            self.current_pat_line_path = self.root_path + self.current_pat_line_path
            
            self.current_pat_lines_path = self.current_pat_line_path
            
            Path(self.current_pat_line_path).mkdir(parents=True, exist_ok=True)
        

        print(self.current_pat_line_path)
        
        self.start_date = start_date
        self.years = years
        self.months = months
        self.days = days
        
        months = [x for x in range(1,4)]
        years = [x for x in range(2023, 2024)]
        days = [x for x in range(1,32)]
        import itertools
        combinations = list(itertools.product(years, months, days))
        len(combinations)
        
        self.slow_execution_threshold_low = timedelta(seconds=10)
        self.slow_execution_threshold_high = timedelta(seconds=30)
        self.slow_execution_threshold_extreme = timedelta(seconds=60)
        
        priority_list_bool = False

        if(priority_list_bool):
            #add logic to prioritise pats from list.
            
            df_old_done = pd.read_csv('/data/AS/Samora/HFE/HFE/v18/current_pat_lines_parts/current_pat_lines__part_0_merged.csv',usecols=['client_idcode', 'Hemochromatosis (disorder)_count_subject_present'])
            
            priority_list = df_old_done[df_old_done['Hemochromatosis (disorder)_count_subject_present']>0]['client_idcode'].to_list()
            
            all_patient_list = priority_list #+ all_patient_list
            
        
        
        
        if(self.testing):
            self.treatment_doc_filename = '/home/cogstack/samora/_data/pat2vec_tests/' + treatment_doc_filename
        
        if(self.remote_dump == False):
            self.sftp_obj = None
        
        
        if(self.remote_dump):

            if(self.root_path == None):
                
                self.root_path = f'/mnt/hdd1/samora/{self.proj_name}/'
                print(f"sftp root_path: {self.root_path}")
                
            else:
                print(f"sftp root_path: {self.root_path}")

            # Set the hostname, username, and password for the remote machine
            
            hostname = self.hostname
            
            username = self.username
            password = self.password

            # Create an SSH client and connect to the remote machine
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(hostname=self.hostname, username=self.username, password=self.password)

            self.sftp_client = self.ssh_client.open_sftp()

            if(self.remote_dump):
                try:
                    self.sftp_client.chdir(self.root_path)  # Test if remote_path exists
                except IOError:
                    self.sftp_client.mkdir(self.root_path)  # Create remote_path



            self.pre_annotation_path = f"{self.root_path}{self.pre_annotation_path}"
            self.pre_annotation_path_mrc = f"{self.root_path}{self.pre_annotation_path_mrc}"
            self.current_pat_line_path = f"{self.root_path}{self.current_pat_line_path}"
            self.current_pat_lines_path = self.current_pat_line_path
            
            
            if(self.remote_dump==False):
                Path(self.current_pat_annot_path).mkdir(parents=True, exist_ok=True)
                Path(self.pre_annotation_path_mrc).mkdir(parents=True, exist_ok=True)

            elif( root_path == f'/mnt/hdd1/samora/{self.proj_name}/'):
                
                
                try:
                    self.sftp_client.chdir(self.pre_annotation_path)  # Test if remote_path exists
                except IOError:
                    self.sftp_client.mkdir(self.pre_annotation_path)  # Create remote_path

                try:
                    self.sftp_client.chdir(self.pre_annotation_path_mrc)  # Test if remote_path exists
                except IOError:
                    self.sftp_client.mkdir(self.pre_annotation_path_mrc)  # Create remote_path
                    
                try:
                    self.sftp_client.chdir(self.current_pat_line_path)  # Test if remote_path exists
                except IOError:
                    self.sftp_client.mkdir(self.current_pat_line_path)  # Create remote_path
            
            self.sftp_obj = self.sftp_client        
                    
                    
        else:
            self.sftp_client = None
        
        
        self.date_list = generate_date_list(self.start_date,self.years, self.months, self.days)
        
        if(self.verbosity>0):
            for date in self.date_list[0:5]:
                print(date)
                
        self.n_pat_lines = len(self.date_list)
        
        self.model_paths = {
            
        'aliencat': '/home/aliencat/samora/HFE/HFE/medcat_models/medcat_model_pack_316666b47dfaac07.zip',
        'dgx': '/data/AS/Samora/HFE/HFE/v18/medcat_models/20230328_trained_model_hfe_redone/medcat_model_pack_316666b47dfaac07',
        'dhcap': '/home/jovyan/work/medcat_models/medcat_model_pack_316666b47dfaac07.zip',
        'dhcap02': '/home/cogstack/samora/_data/medcat_models/medcat_model_pack_316666b47dfaac07.zip'
        
        }
        
        if(global_start_year == None):
            self.global_start_year, self.global_start_month, self.global_end_year, self.global_end_month = '1995', '01', '2023', '11' 
        else:
            
            self.global_start_year = global_start_year
            self.global_start_month = global_start_month
            self.global_end_year = global_end_year
            self.global_end_month = global_end_month
            
        if(self.verbosity>1):
            
            print("Debug message: global_start_year =", self.global_start_year)
            print("Debug message: global_start_month =", self.global_start_month)
            print("Debug message: global_end_year =", self.global_end_year)
            print("Debug message: global_end_month =", self.global_end_month)


        self.skip_additional_listdir = skip_additional_listdir