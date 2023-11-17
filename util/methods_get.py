

import datetime as dt

from datetime import datetime

import numpy as np

from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta 

import pandas as pd

import paramiko

from os.path import exists


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

def get_empty_date_vector(config_object):

    #start date. Other days are for duration of time window
    
    start_date = config_object.start_date
    
    
    
    years = config_object.years
    months = config_object.months
    days = config_object.days
    
    combinations = generate_date_list(start_date, years, months, days)
    
    combinations = [str(item) + '_' + 'date_time_stamp' for item in combinations]
    
    return pd.DataFrame(data=0.0, index=np.arange(1), columns = combinations).astype(float) #untested float cast




def convert_timestamp_to_tuple(timestamp):
    # parse the timestamp string into a datetime object
    dt = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f%z')
    
    # extract the year and month from the datetime object
    year = dt.year
    month = dt.month
    
    # return the tuple of year and month
    return (year, month)



def enum_target_date_vector(target_date_range, current_pat_client_id_code):
    
    empty_date_vector = get_empty_date_vector()
    
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
    
    
def exist_check(path, sftp_obj=None, remote_dump =False):
        if(remote_dump):
            return sftp_exists(path, sftp_obj)
        else:
            return exists(path)
        
        
def sftp_exists(path, sftp_obj=None, config_obj=None):
    
        share_sftp = config_obj.share_sftp
        hostname =config_obj.hostname
        username = config_obj.username
        password = config_obj.password
        
        
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