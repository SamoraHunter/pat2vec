

import datetime as dt
import os
import pickle
import subprocess
import time
from datetime import datetime, timedelta
from io import StringIO
from os.path import exists

import numpy as np
import pandas as pd
import paramiko
from colorama import Back, Fore, Style
from dateutil.relativedelta import relativedelta

color_bars = [Fore.RED,
    Fore.GREEN,
    Fore.BLUE,
    Fore.MAGENTA,
    Fore.YELLOW,
    Fore.CYAN,
    Fore.WHITE]

def list_dir_wrapper(path, config_obj=None):
        
        hostname = config_obj.hostname
        
        username = config_obj.username
        
        password = config_obj.password
        
        remote_dump = config_obj.remote_dump
        
        share_sftp = config_obj.share_sftp
        
        sftp_obj = config_obj.sftp_obj
        
        
        #global sftp_client
        if(remote_dump):
            if(share_sftp == False):
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(hostname=hostname, username=username, password=password)

                sftp_client = ssh_client.open_sftp()
                sftp_obj = sftp_client
            elif(sftp_obj ==None):
                sftp_obj = sftp_client
                
            res = sftp_obj.listdir(path)
            
            
            return res
            
        else:
            
            return os.listdir(path)



def get_start_end_year_month(target_date_range, n=1):
    
    start_year, start_month, start_day = target_date_range[0], target_date_range[1], target_date_range[2]
    
    start_date = dt.date(start_year, start_month, start_day)
    end_date = start_date + dt.timedelta(days=n)
    return (
        start_date.year,
        start_date.month,
        end_date.year,
        end_date.month,
        start_date.day,
        end_date.day
    )

# def get_empty_date_vector(config_object):

#     #start date. Other days are for duration of time window
    
#     start_date = config_object.start_date
    
    
    
#     years = config_object.years
#     months = config_object.months
#     days = config_object.days
    
#     combinations = generate_date_list(start_date, years, months, days)
    
#     combinations = [str(item) + '_' + 'date_time_stamp' for item in combinations]
    
#     return pd.DataFrame(data=0.0, index=np.arange(1), columns = combinations).astype(float) #untested float cast




def convert_timestamp_to_tuple(timestamp):
    # parse the timestamp string into a datetime object
    dt = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
    
    # extract the year and month from the datetime object
    year = dt.year
    month = dt.month
    
    # return the tuple of year and month
    return (year, month)



def enum_target_date_vector(target_date_range, current_pat_client_id_code, config_obj):
    
    empty_date_vector = get_empty_date_vector(config_obj=config_obj)
    
    empty_date_vector.at[0,str(target_date_range)+"_date_time_stamp"] = 1
    
    empty_date_vector['client_idcode'] = current_pat_client_id_code
    
    return empty_date_vector
    
    


def generate_date_list(start_date, years, months, days):
    
    end_date = start_date + relativedelta(years=years, months=months, days=days)
    
    date_list = []
    current_date = start_date
    
    while current_date <= end_date:
        date_list.append((current_date.year, current_date.month, current_date.day))
        current_date += timedelta(days=1)
    
    return date_list
    
    


        
        
def filter_dataframe_by_timestamp(df, start_year, start_month, end_year, end_month, start_day, end_day, timestamp_string):
        # Convert timestamp column to datetime format
    df[timestamp_string] = pd.to_datetime(df[timestamp_string], utc=True)


    #imputed from elastic. Mirror:
    #start_day = 1
    #end_day = 1
    hour = 23
    minute = 59
    second = 59


    # Filter based on year and month ranges
    try:
        filtered_df = df[(df[timestamp_string] >= str(datetime(start_year, int(start_month), start_day, hour, minute, second))) & 
                         (df[timestamp_string] <= str(datetime(end_year, int(end_month), end_day, hour, minute, second)))
                        ]
    except Exception as e:
        print(e)
        filtered_df = df[df[timestamp_string] =="2323"]

    return filtered_df


def dump_results(file_data, path, config_obj=None):
    
        share_sftp = config_obj.share_sftp
        hostname = config_obj.hostname
        username = config_obj.username
        password = config_obj.password
        
        sftp_obj = config_obj.sftp_obj
        
        remote_dump = config_obj.remote_dump
    
        if(remote_dump):
            if(share_sftp == False):
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(hostname=hostname, username=username, password=password)

                sftp_client = ssh_client.open_sftp()
                sftp_obj = sftp_client
            
            
            with sftp_obj.open(path, 'w') as file:
        
                pickle.dump(file_data, file)
            if(share_sftp == False):
                sftp_obj.close()
                sftp_obj.close()
            
        else:
            with open(path, 'wb') as f:
                pickle.dump(file_data, f)
                
                
                

def update_pbar(current_pat_client_id_code, start_time, stage_int, stage_str, t, config_obj, skipped_counter=None, **n_docs_to_annotate):
    #global colour_val
    #global t
    #global skipped_counter
    
    #global skipped_counter
    #colour_val = color_bars[stage_int] + stage_str
    
    start_time = config_obj.start_time
    #print("start time debug", start_time)
    
    multi_process = config_obj.multi_process
    slow_execution_threshold_low = config_obj.slow_execution_threshold_low
    slow_execution_threshold_high = config_obj.slow_execution_threshold_high
    slow_execution_threshold_extreme = config_obj.slow_execution_threshold_extreme
    
    # print(type(slow_execution_threshold_low), slow_execution_threshold_low)
    # print(type(start_time), start_time)
    # print(type(time.time()), time.time())
    
    colour_val = Fore.GREEN +  Style.BRIGHT + stage_str
    
    if(multi_process):
        
        counter_disp = skipped_counter.value
    
    else:
        counter_disp = skipped_counter
    
    t.set_description(f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}" )
    if((datetime.now() - start_time)>slow_execution_threshold_low):
        t.colour = Fore.YELLOW
        colour_val = Fore.YELLOW + stage_str
        t.set_description(f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}" )
        
    elif((datetime.now() - start_time)>slow_execution_threshold_high):
        t.colour = Fore.RED +  Style.BRIGHT
        colour_val = Fore.RED +  Style.BRIGHT + stage_str
        t.set_description(f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}" )
        
    elif((datetime.now() - start_time)>slow_execution_threshold_extreme):
        t.colour = Fore.RED +  Style.DIM
        colour_val = Fore.RED +  Style.DIM + stage_str
        t.set_description(f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}" )
        
    else:
        t.colour =  Fore.GREEN +  Style.DIM
        colour_val = Fore.GREEN +  Style.DIM + stage_str
        t.set_description(f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}" )
        
        

    t.refresh()
    
    
    
    
def get_demographics3_batch(patlist, target_date_range, pat_batch, config_obj = None, cohort_searcher_with_terms_and_search=None):
    
    batch_mode = config_obj.batch_mode
    
    #patlist = config_obj.patlist #is present?

    
    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)
    
    
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
        
        
        








# def list_dir_wrapper(self, path, sftp_obj=None, config_obj = None):
    
#     hostname = config_obj.hostname
    
#     username = config_obj.username
    
#     password = config_obj.password
    
#     #global sftp_client
#     if(self.remote_dump):
#         if(self.share_sftp == False):
#             ssh_client = paramiko.SSHClient()
#             ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#             ssh_client.connect(hostname=hostname, username=username, password=password)

#             sftp_client = ssh_client.open_sftp()
#             sftp_obj = sftp_client
#         elif(sftp_obj ==None):
#             sftp_obj = sftp_client
            
#         res = sftp_obj.listdir(path)
        
        
#         return res
        
#     else:
        
#         return os.listdir(path)





def get_free_gpu():
    ## move to cogstats?
    gpu_stats = subprocess.check_output(["nvidia-smi", "--format=csv", "--query-gpu=memory.used,memory.free"])
    gpu_df = pd.read_csv(StringIO(gpu_stats.decode('utf-8')),
                        names=['memory.used', 'memory.free'],
                        skiprows=1)
    print('GPU usage:\n{}'.format(gpu_df))
    gpu_df['memory.free'] = gpu_df['memory.free'].map(lambda x: x.rstrip(' [MiB]'))
    idx = gpu_df['memory.free'].astype(int).idxmax()
    print('Returning GPU{} with {} free MiB'.format(idx, gpu_df.iloc[idx]['memory.free']))
    return int(idx), gpu_df.iloc[idx]['memory.free']



def method1(self):
    
    self.logger.debug("This is a debug message from your_method.")
    self.logger.warning("This is a warning message from your_method.")

    # Code for method1
    pass

def method2(self):
    # Code for method2
    pass

def __str__(self):
    return f"MyClass instance with parameters: {self.parameter1}, {self.parameter2}"


def write_remote(path, csv_file, config_obj = None):
    
    hostname = config_obj.hostname
    
    username = config_obj.username
    
    password = config_obj.password
    
    share_sftp = config_obj.share_sftp
    
    sftp_obj = config_obj.sftp_obj 
    
    
    #print("writing remote")
    if(share_sftp == False):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname, username=username, password=password)

        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client


    with sftp_obj.open(path, 'w') as file:
        csv_file.to_csv(file)

    if(share_sftp == False):
        sftp_obj.close()
        sftp_obj.close()
        
        




import datetime as dt
import os
import pickle
import subprocess
import time
from datetime import datetime, timedelta
from io import StringIO
from os.path import exists

import numpy as np
import pandas as pd
import paramiko
from colorama import Back, Fore, Style
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

color_bars = [Fore.RED,
    Fore.GREEN,
    Fore.BLUE,
    Fore.MAGENTA,
    Fore.YELLOW,
    Fore.CYAN,
    Fore.WHITE]

def list_dir_wrapper(path, config_obj=None):
        
        hostname = config_obj.hostname
        
        username = config_obj.username
        
        password = config_obj.password
        
        remote_dump = config_obj.remote_dump
        
        share_sftp = config_obj.share_sftp
        
        sftp_obj = config_obj.sftp_obj
        
        
        #global sftp_client
        if(remote_dump):
            if(share_sftp == False):
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(hostname=hostname, username=username, password=password)

                sftp_client = ssh_client.open_sftp()
                sftp_obj = sftp_client
            elif(sftp_obj ==None):
                sftp_obj = sftp_client
                
            res = sftp_obj.listdir(path)
            
            
            return res
            
        else:
            
            return os.listdir(path)
        
        
        

def convert_date(date_string):
    date_string = date_string.split("T")[0]
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    return date_object



def get_start_end_year_month(target_date_range, n=1):
    
    start_year, start_month, start_day = target_date_range[0], target_date_range[1], target_date_range[2]
    
    start_date = dt.date(start_year, start_month, start_day)
    end_date = start_date + dt.timedelta(days=n)
    return (
        start_date.year,
        start_date.month,
        end_date.year,
        end_date.month,
        start_date.day,
        end_date.day
    )

def get_empty_date_vector(config_obj):

    #start date. Other days are for duration of time window
    
    start_date = config_obj.start_date
    
    
    
    years = config_obj.years
    months = config_obj.months
    days = config_obj.days
    
    combinations = generate_date_list(start_date, years, months, days)
    
    combinations = [str(item) + '_' + 'date_time_stamp' for item in combinations]
    
    return pd.DataFrame(data=0.0, index=np.arange(1), columns = combinations).astype(float) #untested float cast




    

# def get_demographics3_batch(patlist, target_date_range, pat_batch, config_obj = None, cohort_searcher_with_terms_and_search=None):
    
#     batch_mode = config_obj.batch_mode
    
#     #patlist = config_obj.patlist #is present?

    
#     start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)
    
    
#     if(batch_mode):
        
#         demo = filter_dataframe_by_timestamp(pat_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'updatetime')

        
        
#     else:
#         demo = cohort_searcher_with_terms_and_search(index_name="epr_documents", 
#                                             fields_list=["client_idcode", "client_firstname", "client_lastname", "client_dob", "client_gendercode", "client_racecode", "client_deceaseddtm", "updatetime"], 
#                                             term_name="client_idcode.keyword", 
#                                             entered_list=patlist,
#                                             search_string= f'updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}] '
#                                                 )
    
    
#     demo["updatetime"] = pd.to_datetime(demo["updatetime"], utc=True)
#     demo = demo.sort_values(["client_idcode", "updatetime"]) #.drop_duplicates(subset = ["client_idcode"], keep = "last", inplace = True)
    
#     #if more than one in the range return the nearest the end of the period
#     if(len(demo)> 1):
#         try:
#             #print("case1")
#             return demo.tail(1)
#             #return demo.iloc[-1].to_frame()
#         except Exception as e:
#             print(e)
            
#     #if only one return it        
#     elif len(demo)==1:
#         return demo
    
#     #otherwise return only the client id
#     else:
#         demo = pd.DataFrame(data=None, columns=None)
#         demo['client_idcode'] = patlist
#         return demo
        
        
        



def sftp_exists(path, config_obj=None):
        
        hostname = config_obj.hostname
        
        username = config_obj.username
        
        password = config_obj.password
        
        sftp_obj = config_obj.sftp_obj
        
        share_sftp = config_obj.share_sftp
        
        try:
            if(share_sftp == False):
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(hostname=hostname, username=username, password=password)

                sftp_obj = ssh_client.open_sftp()
            
            sftp_obj.stat(path)
            
            if(share_sftp == False):
                sftp_obj.close()
                sftp_obj.close()
            return True
        except FileNotFoundError:
            return False







def list_dir_wrapper(path, config_obj = None):
    
    hostname = config_obj.hostname
    
    username = config_obj.username
    
    password = config_obj.password
    
    sftp_obj = config_obj.sftp_obj
    
    remote_dump = config_obj.remote_dump
    
    share_sftp = config_obj.share_sftp
    
    #global sftp_client
    if(remote_dump):
        if(share_sftp == False):
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client
        elif(sftp_obj ==None):
            sftp_obj = sftp_client
            
        res = sftp_obj.listdir(path)
        
        
        return res
        
    else:
        
        return os.listdir(path)




def exist_check(path, config_obj=None):
    
    
    sftp_obj = config_obj.sftp_obj
    remote_dump = config_obj.remote_dump
    
    if(remote_dump):
        return sftp_exists(path, config_obj)
    else:
        return exists(path)
    
def check_sftp_connection(self, remote_directory, config_obj):
    
    hostname = config_obj.hostname
    port = config_obj.port
    username = config_obj.username
    password = config_obj.password
    
    try:
        # Create an SSH client
        ssh = paramiko.SSHClient()

        # Automatically add the server's host key
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the server
        ssh.connect(hostname, port, username, password)

        # Open an SFTP session
        sftp = ssh.open_sftp()

        # Check if the connection is successful by listing the remote directory
        remote_directory = sftp.listdir()
        print(f"Connection successful. Remote directory contents: {remote_directory}")

        # Close the SFTP session and the SSH connection
        sftp.close()
        ssh.close()

    except Exception as e:
        print(f"Error: {e}")
    
    

def get_free_gpu():
    ## move to cogstats?
    gpu_stats = subprocess.check_output(["nvidia-smi", "--format=csv", "--query-gpu=memory.used,memory.free"])
    gpu_df = pd.read_csv(StringIO(gpu_stats.decode('utf-8')),
                        names=['memory.used', 'memory.free'],
                        skiprows=1)
    print('GPU usage:\n{}'.format(gpu_df))
    gpu_df['memory.free'] = gpu_df['memory.free'].map(lambda x: x.rstrip(' [MiB]'))
    idx = gpu_df['memory.free'].astype(int).idxmax()
    print('Returning GPU{} with {} free MiB'.format(idx, gpu_df.iloc[idx]['memory.free']))
    return int(idx), gpu_df.iloc[idx]['memory.free']







def method1(self):
    
    self.logger.debug("This is a debug message from your_method.")
    self.logger.warning("This is a warning message from your_method.")

    # Code for method1
    pass

def method2(self):
    # Code for method2
    pass

def __str__(self):
    return f"MyClass instance with parameters: {self.parameter1}, {self.parameter2}"


def write_remote(path, csv_file, config_obj = None):
    
    hostname = config_obj.hostname
    
    username = config_obj.username
    
    password = config_obj.password
    
    share_sftp = config_obj.share_sftp
    if(share_sftp):
        sftp_obj = config_obj.sftp_obj
    
    
    #print("writing remote")
    if(share_sftp == False):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname, username=username, password=password)

        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client


    with sftp_obj.open(path, 'w') as file:
        csv_file.to_csv(file)

    if(share_sftp == False):
        sftp_obj.close()
        sftp_obj.close()
        
        






def filter_stripped_list(stripped_list, config_obj=None):
    
    strip_list = config_obj.strip_list
    remote_dump = config_obj.remote_dump
    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password
    current_pat_lines_path = config_obj.current_pat_lines_path
    n_pat_lines = config_obj.n_pat_lines
    
    
    if strip_list:
        #stripped_list_start_copy = stripped_list.copy()
        container_list = []

        if remote_dump:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()

            for i in range(len(stripped_list)):
                try:
                    if len(sftp_client.listdir(current_pat_lines_path + stripped_list[i])) >= n_pat_lines:
                        container_list.append(stripped_list[i])
                except:
                    pass
        else:
            if(config_obj.verbosity>0):
                print("Stripping list...")
            for i in tqdm(range(len(stripped_list))):
                if len(list_dir_wrapper(current_pat_lines_path + stripped_list[i], config_obj=config_obj)) >= n_pat_lines:
                    container_list.append(stripped_list[i])

        stripped_list_start = container_list.copy()
        stripped_list = container_list.copy()
        
        if remote_dump:
            sftp_client.close()
            ssh_client.close()
    else:
        stripped_list = []
        stripped_list_start = []

    return stripped_list, stripped_list_start

# Example usage:
# stripped_list, stripped_list_start = filter_stripped_list(your_stripped_list, strip_list=True, remote_dump=True, hostname="your_host", username="your_username", password="your_password", current_pat_lines_path="your_path", n_pat_lines=your_n)



# def create_folders(all_patient_list, config_obj=None):
#     pre_annotation_path = config_obj.pre_annotation_path
#     pre_annotation_path_mrc = config_obj.pre_annotation_path_mrc
#     current_pat_line_path = config_obj.current_pat_line_path
#     #all_patient_list = config_obj.all_patient_list

#     for i in tqdm(range(len(all_patient_list))):
#         for path in [pre_annotation_path, pre_annotation_path_mrc, current_pat_line_path]:
#             folder_path = os.path.join(path, str(all_patient_list[i]))

#             if not os.path.exists(folder_path):
#                 os.makedirs(folder_path)
def create_folders(all_patient_list, config_obj=None):
    """
    Create folders for each patient in the specified paths.

    Parameters:
    - all_patient_list (list): List of patient IDs.
    - config_obj (object): Configuration object containing paths and verbosity level.

    Returns:
    None
    """
    pre_annotation_path = config_obj.pre_annotation_path
    pre_annotation_path_mrc = config_obj.pre_annotation_path_mrc
    current_pat_line_path = config_obj.current_pat_line_path

    for patient_id in all_patient_list:
        for path in [pre_annotation_path, pre_annotation_path_mrc, current_pat_line_path]:
            folder_path = os.path.join(path, str(patient_id))

            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
    if config_obj.verbosity > 0:
                print(f"Folders created: {current_pat_line_path}...") 
                    


def convert_date(date_string):
    date_string = date_string.split("T")[0]
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    return date_object