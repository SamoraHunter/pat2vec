

import datetime as dt
import os
import pickle
import subprocess
import time
from datetime import datetime, timedelta
from io import StringIO
from os.path import exists
from pathlib import Path

import numpy as np
import pandas as pd
import paramiko
from colorama import Back, Fore, Style
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from IPython.display import display
from tqdm import tqdm
import pytz

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

    # global sftp_client
    if (remote_dump):
        if (share_sftp == False):
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname,
                               username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client
        elif (sftp_obj == None):
            sftp_obj = sftp_client

        res = sftp_obj.listdir(path)

        return res

    else:

        return os.listdir(path)


def get_start_end_year_month(target_date_range, n=1):

    start_year, start_month, start_day = target_date_range[
        0], target_date_range[1], target_date_range[2]

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

    empty_date_vector.at[0, str(target_date_range)+"_date_time_stamp"] = 1

    empty_date_vector['client_idcode'] = current_pat_client_id_code

    return empty_date_vector

def enum_exact_target_date_vector(target_date_range, current_pat_client_id_code, config_obj):

    #empty_date_vector = get_empty_date_vector(config_obj=config_obj)

    empty_date_vector = pd.DataFrame(columns=['client_idcode', str(target_date_range)+"_date_time_stamp"])

    empty_date_vector[str(target_date_range)+"_date_time_stamp"] = 1

    empty_date_vector.at[0, str(target_date_range)+"_date_time_stamp"] = 1

    empty_date_vector['client_idcode'] = current_pat_client_id_code

    return empty_date_vector


# def generate_date_list(start_date, years, months, days, time_window_interval_delta=relativedelta(days=1), config_obj=None):
##working
#     lookback = config_obj.lookback

#     config_obj.global_start_year, config_obj.global_start_month, config_obj.global_end_year, config_obj.global_end_month, config_obj.global_start_day, config_obj.global_end_day

#     if (lookback == False):
#         end_date = start_date + \
#             relativedelta(years=years, months=months, days=days)
#     else:
#         end_date = start_date - \
#             relativedelta(years=years, months=months, days=days)

#     date_list = []
#     current_date = start_date

#     if (lookback == False):
#         # look forward...
#         while current_date <= end_date:
#             date_list.append(
#                 (current_date.year, current_date.month, current_date.day))
#             current_date += time_window_interval_delta  # timedelta(days=1)
#     else:
#         # look back
#         while current_date >= end_date:
#             date_list.append(
#                 (current_date.year, current_date.month, current_date.day))
#             current_date += time_window_interval_delta

#     return date_list


def generate_date_list(start_date, years, months, days, time_window_interval_delta=relativedelta(days=1), config_obj=None):

    lookback = config_obj.lookback

    config_obj.global_start_year, config_obj.global_start_month, config_obj.global_end_year, config_obj.global_end_month, config_obj.global_start_day, config_obj.global_end_day

    if (lookback == False):
        end_date = start_date + \
            relativedelta(years=years, months=months, days=days)
    else:
        end_date = start_date - \
            relativedelta(years=years, months=months, days=days)

    global_start_date = datetime.strptime(f"{config_obj.global_start_year}-{config_obj.global_start_month}-{config_obj.global_start_day}", "%Y-%m-%d")
    global_end_date = datetime.strptime(f"{config_obj.global_end_year}-{config_obj.global_end_month}-{config_obj.global_end_day}", "%Y-%m-%d")

    #import pytz

    # Assuming your timezone is 'UTC', you can replace it with your actual timezone
    timezone = pytz.UTC

    # Check if start_date and end_date are timezone-aware
    if not start_date.tzinfo:
        start_date = timezone.localize(start_date)

    if not end_date.tzinfo:
        end_date = timezone.localize(end_date)

    # Check if global_start_date and global_end_date are timezone-aware
    if not global_start_date.tzinfo:
        global_start_date = timezone.localize(global_start_date)

    if not global_end_date.tzinfo:
        global_end_date = timezone.localize(global_end_date)

    # Adjust start_date and end_date based on global start and end dates
    if start_date < global_start_date:
        start_date = global_start_date

    if end_date > global_end_date:
        end_date = global_end_date

    # Rest of your code



    date_list = []
    current_date = start_date

    if (lookback == False):
        # look forward...
        while current_date <= end_date:
            date_list.append(
                (current_date.year, current_date.month, current_date.day))
            current_date += time_window_interval_delta  # timedelta(days=1)
    else:
        # look back
        #limit lookback dates by global dates. 
        if start_date < global_start_date:
            
            start_date = global_start_date
            if(config_obj.verbose >= 1):
                print("start_date < global_start_date", start_date)

        if end_date > global_end_date:
            end_date = global_end_date
            if(config_obj.verbose >= 1):
                print("end_date > global_end_date", end_date)

        while current_date >= end_date:
            date_list.append(
                (current_date.year, current_date.month, current_date.day))
            current_date += time_window_interval_delta

    return date_list


# def filter_dataframe_by_timestamp(df, start_year, start_month, end_year, end_month, start_day, end_day, timestamp_string, dropna=False):
#     # Convert timestamp column to datetime format

#     # if(dropna):
#     #     df[timestamp_string] = pd.to_datetime(df[timestamp_string], errors='coerce')
#     #     df.dropna(subset=[timestamp_string], inplace=True)

#     df[timestamp_string] = pd.to_datetime(df[timestamp_string], utc=True)

#     # df[timestamp_string] = pd.to_datetime(df[timestamp_string])

#     # imputed from elastic. Mirror:
#     # start_day = 1
#     # end_day = 1
#     hour = 23
#     minute = 59
#     second = 59

#     # Filter based on year and month ranges
#     try:
#         filtered_df = df[(df[timestamp_string] >= str(datetime(start_year, int(start_month), int(start_day), hour, minute, second))) &
#                          (df[timestamp_string] <= str(datetime(end_year, int(
#                              end_month), int(end_day), hour, minute, second)))
#                          ]
#     except Exception as e:
#         print("error in filter_dataframe_by_timestamp")
#         print(e)
#         display(df)
#         print(start_year, start_month, end_year, end_month,
#               start_day, end_day, timestamp_string)
#         raise e
#         # filtered_df = df[df[timestamp_string] =="2323"]

#     return filtered_df
def filter_dataframe_by_timestamp(df, start_year, start_month, end_year, end_month, start_day, end_day, timestamp_string, dropna=False):
    
    # Convert timestamp column to datetime format
    df[timestamp_string] = pd.to_datetime(df[timestamp_string], utc=True)

    # Ensure start date is earlier than end date
    start_datetime = pd.Timestamp(datetime(start_year, int(start_month), int(start_day)), tz='UTC')
    end_datetime = pd.Timestamp(datetime(end_year, int(end_month), int(end_day)), tz='UTC')
    if start_datetime > end_datetime:
        start_datetime, end_datetime = end_datetime, start_datetime

    # Filter based on datetime range
    filtered_df = df[(df[timestamp_string] >= start_datetime) & (df[timestamp_string] <= end_datetime)]

    if dropna:
        filtered_df.dropna(subset=[timestamp_string], inplace=True)
    #display(filtered_df)
    return filtered_df

# def filter_dataframe_by_timestamp(df, start_year, start_month, end_year, end_month, start_day, end_day, timestamp_string, dropna=False):
    
#     #print("filter_dataframe_by_timestamp", "methods_get")
#     #raise
#     # Convert timestamp column to datetime format
#     df[timestamp_string] = pd.to_datetime(df[timestamp_string], utc=True)

#     # Ensure start date is earlier than end date
#     start_datetime = datetime(start_year, int(start_month), int(start_day))
#     end_datetime = datetime(end_year, int(end_month), int(end_day))
#     if start_datetime > end_datetime:
#         start_datetime, end_datetime = end_datetime, start_datetime

#     # Filter based on datetime range
#     filtered_df = df[(df[timestamp_string] >= start_datetime) & (df[timestamp_string] <= end_datetime)]

#     if dropna:
#         filtered_df.dropna(subset=[timestamp_string], inplace=True)
#     display(filtered_df)
#     return filtered_df


def dump_results(file_data, path, config_obj=None):

    share_sftp = config_obj.share_sftp
    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password

    sftp_obj = config_obj.sftp_obj

    remote_dump = config_obj.remote_dump

    if (remote_dump):
        if (share_sftp == False):
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname,
                               username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client

        with sftp_obj.open(path, 'w') as file:

            pickle.dump(file_data, file)
        if (share_sftp == False):
            sftp_obj.close()
            sftp_obj.close()

    else:
        with open(path, 'wb') as f:
            pickle.dump(file_data, f)


def update_pbar(current_pat_client_id_code, start_time, stage_int, stage_str, t, config_obj, skipped_counter=None, **n_docs_to_annotate):

    start_time = config_obj.start_time

    multi_process = config_obj.multi_process
    slow_execution_threshold_low = config_obj.slow_execution_threshold_low
    slow_execution_threshold_high = config_obj.slow_execution_threshold_high
    slow_execution_threshold_extreme = config_obj.slow_execution_threshold_extreme

    colour_val = Fore.GREEN + Style.BRIGHT + stage_str

    if (multi_process):

        counter_disp = skipped_counter.value

    else:
        counter_disp = skipped_counter

    t.set_description(
        f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}")
    if ((datetime.now() - start_time) > slow_execution_threshold_low):
        t.colour = Fore.YELLOW
        colour_val = Fore.YELLOW + stage_str
        t.set_description(
            f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}")

    elif ((datetime.now() - start_time) > slow_execution_threshold_high):
        t.colour = Fore.RED + Style.BRIGHT
        colour_val = Fore.RED + Style.BRIGHT + stage_str
        t.set_description(
            f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}")

    elif ((datetime.now() - start_time) > slow_execution_threshold_extreme):
        t.colour = Fore.RED + Style.DIM
        colour_val = Fore.RED + Style.DIM + stage_str
        t.set_description(
            f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}")

    else:
        t.colour = Fore.GREEN + Style.DIM
        colour_val = Fore.GREEN + Style.DIM + stage_str
        t.set_description(
            f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}")

    t.refresh()


def get_demographics3_batch(patlist, target_date_range, pat_batch, config_obj=None, cohort_searcher_with_terms_and_search=None):

    batch_mode = config_obj.batch_mode

    # patlist = config_obj.patlist #is present?

    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(
        target_date_range, config_obj=config_obj)

    if (batch_mode):

        demo = filter_dataframe_by_timestamp(
            pat_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'updatetime')

    else:
        demo = cohort_searcher_with_terms_and_search(index_name="epr_documents",
                                                     fields_list=["client_idcode", "client_firstname", "client_lastname", "client_dob",
                                                                  "client_gendercode", "client_racecode", "client_deceaseddtm", "updatetime"],
                                                     term_name="client_idcode.keyword",
                                                     entered_list=patlist,
                                                     search_string=f'updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}] '
                                                     )

    demo["updatetime"] = pd.to_datetime(demo["updatetime"], utc=True)
    # .drop_duplicates(subset = ["client_idcode"], keep = "last", inplace = True)
    demo = demo.sort_values(["client_idcode", "updatetime"])

    # if more than one in the range return the nearest the end of the period
    if (len(demo) > 1):
        try:
            # print("case1")
            return demo.tail(1)
            # return demo.iloc[-1].to_frame()
        except Exception as e:
            print(e)

    # if only one return it
    elif len(demo) == 1:
        return demo

    # otherwise return only the client id
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
    # move to cogstats?
    gpu_stats = subprocess.check_output(
        ["nvidia-smi", "--format=csv", "--query-gpu=memory.used,memory.free"])
    gpu_df = pd.read_csv(StringIO(gpu_stats.decode('utf-8')),
                         names=['memory.used', 'memory.free'],
                         skiprows=1)
    print('GPU usage:\n{}'.format(gpu_df))
    gpu_df['memory.free'] = gpu_df['memory.free'].map(
        lambda x: x.rstrip(' [MiB]'))
    idx = gpu_df['memory.free'].astype(int).idxmax()
    print('Returning GPU{} with {} free MiB'.format(
        idx, gpu_df.iloc[idx]['memory.free']))
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

    # global sftp_client
    if (remote_dump):
        if (share_sftp == False):
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname,
                               username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client
        elif (sftp_obj == None):
            sftp_obj = sftp_client

        res = sftp_obj.listdir(path)

        return res

    else:

        return os.listdir(path)


def convert_date(date_string):
    date_string = date_string.split("T")[0]
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    return date_object


def get_start_end_year_month(target_date_range, config_obj=None):

    if config_obj is None:
        raise ValueError("config_obj cannot be None")

    time_window_interval_delta = config_obj.time_window_interval_delta

    start_year, start_month, start_day = target_date_range[
        0], target_date_range[1], target_date_range[2]

    start_date = dt.date(start_year, start_month, start_day)
    # end_date = start_date + dt.timedelta(days=n)
    end_date = start_date + time_window_interval_delta

    return (
        start_date.year,
        start_date.month,
        end_date.year,
        end_date.month,
        start_date.day,
        end_date.day
    )


def get_empty_date_vector(config_obj):

    # start date. Other days are for duration of time window

    start_date = config_obj.start_date

    years = config_obj.years
    months = config_obj.months
    days = config_obj.days

    interval_window_delta = config_obj.time_window_interval_delta

    combinations = generate_date_list(
        start_date, years, months, days, interval_window_delta, config_obj = config_obj)

    combinations = [
        str(item) + '_' + 'date_time_stamp' for item in combinations]

    # untested float cast
    return pd.DataFrame(data=0.0, index=np.arange(1), columns=combinations).astype(float)


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
        if (share_sftp == False):
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname,
                               username=username, password=password)

            sftp_obj = ssh_client.open_sftp()

        sftp_obj.stat(path)

        if (share_sftp == False):
            sftp_obj.close()
            sftp_obj.close()
        return True
    except FileNotFoundError:
        return False


def list_dir_wrapper(path, config_obj=None):

    hostname = config_obj.hostname

    username = config_obj.username

    password = config_obj.password

    sftp_obj = config_obj.sftp_obj

    remote_dump = config_obj.remote_dump

    share_sftp = config_obj.share_sftp

    # global sftp_client
    if (remote_dump):
        if (share_sftp == False):
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname,
                               username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client
        elif (sftp_obj == None):
            sftp_obj = sftp_client

        res = sftp_obj.listdir(path)

        return res

    else:

        return os.listdir(path)


def exist_check(path, config_obj=None):

    sftp_obj = config_obj.sftp_obj
    remote_dump = config_obj.remote_dump

    if (remote_dump):
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
        print(
            f"Connection successful. Remote directory contents: {remote_directory}")

        # Close the SFTP session and the SSH connection
        sftp.close()
        ssh.close()

    except Exception as e:
        print(f"Error: {e}")


def get_free_gpu():
    # move to cogstats?
    gpu_stats = subprocess.check_output(
        ["nvidia-smi", "--format=csv", "--query-gpu=memory.used,memory.free"])
    gpu_df = pd.read_csv(StringIO(gpu_stats.decode('utf-8')),
                         names=['memory.used', 'memory.free'],
                         skiprows=1)
    print('GPU usage:\n{}'.format(gpu_df))
    gpu_df['memory.free'] = gpu_df['memory.free'].map(
        lambda x: x.rstrip(' [MiB]'))
    idx = gpu_df['memory.free'].astype(int).idxmax()
    print('Returning GPU{} with {} free MiB'.format(
        idx, gpu_df.iloc[idx]['memory.free']))
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


def write_remote(path, csv_file, config_obj=None):
    """
    Write a Pandas DataFrame to a remote file using SFTP or SSH.

    Parameters:
    - path (str): The remote path where the file should be written.
    - csv_file (pd.DataFrame): The DataFrame to be written to the remote file.
    - config_obj (ConfigObject, optional): An object containing configuration details.
                                           Should have 'hostname', 'username', 'password',
                                           'share_sftp', and 'sftp_obj' attributes.

    Returns:
    None
    """

    if config_obj is None:
        raise ValueError("Config object cannot be None.")

    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password
    share_sftp = config_obj.share_sftp

    if share_sftp:
        sftp_obj = config_obj.sftp_obj
    else:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname,
                           username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client

    with sftp_obj.open(path, 'w') as file:
        csv_file.to_csv(file)

    if not share_sftp:
        sftp_obj.close()
        ssh_client.close()


def write_csv_wrapper(path, csv_file_data=None, config_obj=None):
    """
    Write CSV data to a file either locally or remotely based on the configuration.

    Parameters:
    - path (str): The path to the CSV file.
    - csv_file_data (pd.DataFrame): The DataFrame containing the CSV data.
    - config_obj (ConfigObject): An object containing configuration settings, including 'remote_dump'.

    Returns:
    None
    """
    remote_dump = config_obj.remote_dump

    if not remote_dump:
        # Write data locally
        csv_file_data.to_csv(path, index=False)
    else:
        # Write data remotely using a custom function (write_remote)
        write_remote(path, csv_file_data, config_obj=config_obj)


def read_remote(path, config_obj=None):
    """
    Read a remote CSV file using SFTP or SSH and return a Pandas DataFrame.

    Parameters:
    - path (str): The remote path from where the file should be read.
    - config_obj (ConfigObject, optional): An object containing configuration details.
                                           Should have 'hostname', 'username', 'password',
                                           'share_sftp', and 'sftp_obj' attributes.

    Returns:
    pd.DataFrame: The DataFrame containing the data read from the remote CSV file.
    """

    if config_obj is None:
        raise ValueError("Config object cannot be None.")

    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password
    share_sftp = config_obj.share_sftp

    if share_sftp:
        sftp_obj = config_obj.sftp_obj
    else:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname,
                           username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client

    with sftp_obj.open(path, 'r') as file:
        # Read CSV content into a Pandas DataFrame
        csv_content = file.read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))

    if not share_sftp:
        sftp_obj.close()
        ssh_client.close()

    return df


def read_csv_wrapper(path, config_obj=None):
    """
    Read CSV data from a file either locally or remotely based on the configuration.

    Parameters:
    - path (str): The path to the CSV file.
    - config_obj (ConfigObject): An object containing configuration settings, including 'remote_dump'.

    Returns:
    pd.DataFrame: The DataFrame containing the data read from the CSV file.
    """
    remote_dump = config_obj.remote_dump

    if not remote_dump:
        # Read data locally
        df = pd.read_csv(path)
    else:
        # Read data remotely using a custom function (read_remote)
        df = read_remote(path, config_obj=config_obj)

    return df


def create_local_folders(config_obj=None):
    """
    Create local folders for patent documents and annotated vectors.

    Parameters:
    - root_path (str): The root path where the folders should be created.
    - project_name (str): The name of the project.

    Returns:
    None
    """
    project_name = config_obj.project_name

    root_path = config_obj.root_path

    pat_doc_folder_path = root_path + "/" + project_name + "/pat_docs/"
    Path(pat_doc_folder_path).mkdir(parents=True, exist_ok=True)

    pat_doc_annot_vec_folder_path = root_path + \
        "/" + project_name + "/pat_docs_annot_vecs/"
    Path(pat_doc_annot_vec_folder_path).mkdir(parents=True, exist_ok=True)


def create_remote_folders(config_obj=None):
    """
    Create remote folders for patent documents and annotated vectors using SFTP or SSH.

    Parameters:
    - root_path (str): The root path where the folders should be created remotely.
    - project_name (str): The name of the project.
    - config_obj (ConfigObject, optional): An object containing configuration details.
                                           Should have 'hostname', 'username', 'password',
                                           'share_sftp', and 'sftp_obj' attributes.

    Returns:
    None
    """
    root_path = config_obj.root_path
    project_name = config_obj.proj_name

    if config_obj is None:
        raise ValueError("Config object cannot be None.")

    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password
    share_sftp = config_obj.share_sftp

    if share_sftp:
        sftp_obj = config_obj.sftp_obj
    else:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname,
                           username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client

    pat_doc_folder_path = root_path + "/" + project_name + "/pat_docs/"
    pat_doc_annot_vec_folder_path = root_path + \
        "/" + project_name + "/pat_docs_annot_vecs/"

    try:
        # Create the remote directory if it doesn't exist
        sftp_obj.stat(pat_doc_folder_path)
    except FileNotFoundError:
        sftp_obj.mkdir(pat_doc_folder_path)

    try:
        # Create the remote directory if it doesn't exist
        sftp_obj.stat(pat_doc_annot_vec_folder_path)
    except FileNotFoundError:
        sftp_obj.mkdir(pat_doc_annot_vec_folder_path)

    if not share_sftp:
        sftp_obj.close()
        ssh_client.close()


def create_folders_annot_csv_wrapper(config_obj=None):
    """
    Create folders locally or remotely and read CSV data based on the configuration.

    Parameters:
    - root_path (str): The root path where the folders should be created.
    - project_name (str): The name of the project.
    - csv_file_path (str): The path to the CSV file.
    - config_obj (ConfigObject, optional): An object containing configuration details.
                                           Should have 'remote_dump', 'hostname', 'username',
                                           'password', 'share_sftp', and 'sftp_obj' attributes.

    Returns:
    pd.DataFrame: The DataFrame containing the data read from the CSV file.
    """
    root_path = config_obj.root_path
    project_name = config_obj.proj_name

    # Create folders
    if not config_obj or not config_obj.remote_dump:
        # Create local folders
        create_local_folders(config_obj=config_obj)
    else:
        # Create remote folders
        create_remote_folders(config_obj=config_obj)


def filter_stripped_list(stripped_list, config_obj=None):

    strip_list = config_obj.strip_list
    remote_dump = config_obj.remote_dump
    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password
    current_pat_lines_path = config_obj.current_pat_lines_path
    n_pat_lines = config_obj.n_pat_lines

    if strip_list:
        # stripped_list_start_copy = stripped_list.copy()
        container_list = []

        if remote_dump:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname,
                               username=username, password=password)

            sftp_client = ssh_client.open_sftp()

            for i in range(len(stripped_list)):
                try:
                    if len(sftp_client.listdir(current_pat_lines_path + stripped_list[i])) >= n_pat_lines:
                        container_list.append(stripped_list[i])
                except:
                    pass
        else:
            if (config_obj.verbosity > 0):
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
    current_pat_lines_path = config_obj.current_pat_lines_path

    for patient_id in all_patient_list:
        for path in [pre_annotation_path, pre_annotation_path_mrc, current_pat_lines_path]:
            folder_path = os.path.join(path, str(patient_id))

            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
    if config_obj.verbosity > 0:
        print(f"Folders created: {current_pat_lines_path}...")


def create_folders_for_pat(patient_id, config_obj=None):
    """
    Create folders for a single patient in the specified paths.

    Parameters:
    - patient_id (str): Patient ID.
    - config_obj (object): Configuration object containing paths and verbosity level.

    Returns:
    None
    """
    pre_annotation_path = config_obj.pre_annotation_path
    pre_annotation_path_mrc = config_obj.pre_annotation_path_mrc
    current_pat_lines_path = config_obj.current_pat_lines_path

    for path in [pre_annotation_path, pre_annotation_path_mrc, current_pat_lines_path]:
        folder_path = os.path.join(path, str(patient_id))

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    if config_obj.verbosity > 0:
        print(
            f"Folders created for patient {patient_id}: {current_pat_lines_path}...")


def convert_date(date_string):
    date_string = date_string.split("T")[0]
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    return date_object


def add_offset_column(dataframe, start_column_name, offset_column_name, time_offset):
    """
    Adds a new column with the offset from the start time to the provided DataFrame.

    Parameters:
    - dataframe: pandas DataFrame
    - start_column_name: str, the name of the column with the starting datetime
    - offset_column_name: str, the name of the new column to be created with the offset
    - time_offset: relativedelta, the time period offset to be added to the start time

    Returns:
    - None (modifies the input DataFrame in place)
    """

    try:
        # attempt to fix human time stamp inconsistencies:
        dataframe[start_column_name] = pd.to_datetime(dataframe[start_column_name].astype(
            str), infer_datetime_format=True, format='%d/%m/%y %H.%M.%S')
    except:
        dataframe[start_column_name] = pd.to_datetime(dataframe[start_column_name].astype(
            str), infer_datetime_format=True, format='mixed')

    # cast back to str for parser
    dataframe[start_column_name] = dataframe[start_column_name].astype(str)

    # Attempt to parse datetime using dateutil.parser.parse
    try:
        dataframe[start_column_name + "_converted"] = dataframe[start_column_name].apply(
            lambda x: parse(x, fuzzy=True))
    except ValueError:
        # Fallback to specified format if parsing fails
        dataframe[start_column_name + "_converted"] = pd.to_datetime(
            dataframe[start_column_name], format='%m/%d/%y %H.%M.%S', errors='coerce')

    # Ensure the start column is now in datetime format
    if not pd.api.types.is_datetime64_any_dtype(dataframe[start_column_name + "_converted"]):
        raise ValueError(
            f"Column '{start_column_name}_converted' does not exist or cannot be converted to datetime format.")

    # Define a function to apply the offset individually to each element
    def apply_offset(dt):
        if pd.notna(dt):
            return dt + time_offset
        else:
            return dt

    # Apply the offset function to create the new column
    dataframe[offset_column_name] = dataframe[start_column_name +
                                              "_converted"].apply(apply_offset)

    return dataframe

# add_offset_column(df, 'ADMISSION_DTTM', offset_column_name, time_offset)


def build_patient_dict(dataframe, patient_id_column, start_column, end_column):
    """
    Builds a dictionary with patient_id as key and (start, end) as values.

    Parameters:
    - dataframe: pandas DataFrame
    - patient_id_column: str, the name of the column containing patient IDs
    - start_column: str, the name of the column containing start datetime
    - end_column: str, the name of the column containing end datetime

    Returns:
    - patient_dict: dict, a dictionary with patient_id as key and (start, end) as values
    """
    patient_dict = {}

    for index, row in dataframe.iterrows():
        patient_id = row[patient_id_column]
        start_time = row[start_column]
        end_time = row[end_column]

        # Check if start and end times are valid datetime objects
        if pd.notnull(start_time) and pd.notnull(end_time):
            # Add the patient_id and corresponding start and end times to the dictionary
            patient_dict[patient_id] = (start_time, end_time)

    return patient_dict

    # patient_dict = build_patient_dict(df, 'PATIENT_ID', 'ADMISSION_DTTM_converted', 'ADMISSION_DTTM_offset')
