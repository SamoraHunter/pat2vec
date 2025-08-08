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

import pandas as pd
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import warnings

color_bars = [
    Fore.RED,
    Fore.GREEN,
    Fore.BLUE,
    Fore.MAGENTA,
    Fore.YELLOW,
    Fore.CYAN,
    Fore.WHITE,
]


def list_dir_wrapper(path, config_obj=None):

    hostname = config_obj.hostname

    username = config_obj.username

    password = config_obj.password

    remote_dump = config_obj.remote_dump

    share_sftp = config_obj.share_sftp

    sftp_obj = config_obj.sftp_obj

    # global sftp_client
    if remote_dump:
        if share_sftp == False:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client
        elif sftp_obj == None:
            sftp_obj = sftp_client

        res = sftp_obj.listdir(path)

        return res

    else:

        return os.listdir(path)


def convert_timestamp_to_tuple(timestamp):
    # parse the timestamp string into a datetime object
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")

    # extract the year and month from the datetime object
    year = dt.year
    month = dt.month

    # return the tuple of year and month
    return (year, month)


def enum_target_date_vector(target_date_range, current_pat_client_id_code, config_obj):

    empty_date_vector = get_empty_date_vector(config_obj=config_obj)

    empty_date_vector.at[0, str(target_date_range) + "_date_time_stamp"] = 1

    empty_date_vector["client_idcode"] = current_pat_client_id_code

    return empty_date_vector


def enum_exact_target_date_vector(
    target_date_range, current_pat_client_id_code, config_obj
):

    # empty_date_vector = get_empty_date_vector(config_obj=config_obj)

    empty_date_vector = pd.DataFrame(
        columns=["client_idcode", str(target_date_range) + "_date_time_stamp"]
    )

    empty_date_vector[str(target_date_range) + "_date_time_stamp"] = 1

    empty_date_vector.at[0, str(target_date_range) + "_date_time_stamp"] = 1

    empty_date_vector["client_idcode"] = current_pat_client_id_code

    return empty_date_vector


def generate_date_list(
    start_date,
    years,
    months,
    days,
    time_window_interval_delta=relativedelta(days=1),
    config_obj=None,
):

    lookback = config_obj.lookback

    config_obj.global_start_year, config_obj.global_start_month, config_obj.global_end_year, config_obj.global_end_month, config_obj.global_start_day, config_obj.global_end_day

    if lookback == False:
        end_date = start_date + relativedelta(years=years, months=months, days=days)
    else:
        end_date = start_date - relativedelta(years=years, months=months, days=days)

    global_start_date = datetime.strptime(
        f"{config_obj.global_start_year}-{config_obj.global_start_month}-{config_obj.global_start_day}",
        "%Y-%m-%d",
    )
    global_end_date = datetime.strptime(
        f"{config_obj.global_end_year}-{config_obj.global_end_month}-{config_obj.global_end_day}",
        "%Y-%m-%d",
    )

    # import pytz

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

    if lookback == False:
        # look forward...
        while current_date <= end_date:
            date_list.append((current_date.year, current_date.month, current_date.day))
            current_date += time_window_interval_delta  # timedelta(days=1)
    else:
        # look back
        # limit lookback dates by global dates.
        if start_date < global_start_date:

            start_date = global_start_date
            if config_obj.verbose >= 1:
                print("start_date < global_start_date", start_date)

        if end_date > global_end_date:
            end_date = global_end_date
            if config_obj.verbose >= 1:
                print("end_date > global_end_date", end_date)

        while current_date >= end_date:
            date_list.append((current_date.year, current_date.month, current_date.day))
            current_date += time_window_interval_delta

        # date_list.reverse() # cohort searcher date order fix

    return date_list


def filter_dataframe_by_timestamp(
    df,
    start_year,
    start_month,
    end_year,
    end_month,
    start_day,
    end_day,
    timestamp_string,
    dropna=False,
):

    # Convert timestamp column to datetime format
    df[timestamp_string] = pd.to_datetime(
        df[timestamp_string], utc=True, errors="coerce"
    )

    df = df.dropna(subset=[timestamp_string])

    # Ensure start date is earlier than end date
    start_datetime = pd.Timestamp(
        datetime(start_year, int(start_month), int(start_day)), tz="UTC"
    )
    end_datetime = pd.Timestamp(
        datetime(end_year, int(end_month), int(end_day)), tz="UTC"
    )
    if start_datetime > end_datetime:
        start_datetime, end_datetime = end_datetime, start_datetime

    # Filter based on datetime range
    filtered_df = df[
        (df[timestamp_string] >= start_datetime)
        & (df[timestamp_string] <= end_datetime)
    ]

    if dropna:
        filtered_df.dropna(subset=[timestamp_string], inplace=True)
    # display(filtered_df)
    return filtered_df


def dump_results(file_data, path, config_obj=None):

    share_sftp = config_obj.share_sftp
    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password

    sftp_obj = config_obj.sftp_obj

    remote_dump = config_obj.remote_dump

    if remote_dump:
        if share_sftp == False:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client

        with sftp_obj.open(path, "w") as file:

            pickle.dump(file_data, file)
        if share_sftp == False:
            sftp_obj.close()
            sftp_obj.close()

    else:
        with open(path, "wb") as f:
            pickle.dump(file_data, f)


def update_pbar(
    current_pat_client_id_code,
    start_time,
    stage_int,
    stage_str,
    t,
    config_obj,
    skipped_counter=None,
    **n_docs_to_annotate,
):

    start_time = config_obj.start_time

    multi_process = config_obj.multi_process
    slow_execution_threshold_low = config_obj.slow_execution_threshold_low
    slow_execution_threshold_high = config_obj.slow_execution_threshold_high
    slow_execution_threshold_extreme = config_obj.slow_execution_threshold_extreme

    colour_val = Fore.GREEN + Style.BRIGHT + stage_str

    if multi_process:

        counter_disp = skipped_counter.value

    else:
        counter_disp = skipped_counter

    t.set_description(
        f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}"
    )
    if (datetime.now() - start_time) > slow_execution_threshold_low:
        t.colour = Fore.YELLOW
        colour_val = Fore.YELLOW + stage_str
        t.set_description(
            f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}"
        )

    elif (datetime.now() - start_time) > slow_execution_threshold_high:
        t.colour = Fore.RED + Style.BRIGHT
        colour_val = Fore.RED + Style.BRIGHT + stage_str
        t.set_description(
            f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}"
        )

    elif (datetime.now() - start_time) > slow_execution_threshold_extreme:
        t.colour = Fore.RED + Style.DIM
        colour_val = Fore.RED + Style.DIM + stage_str
        t.set_description(
            f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}"
        )

    else:
        t.colour = Fore.GREEN + Style.DIM
        colour_val = Fore.GREEN + Style.DIM + stage_str
        t.set_description(
            f"s: {counter_disp} | {current_pat_client_id_code} | task: {colour_val} | {n_docs_to_annotate}"
        )

    t.refresh()


def get_free_gpu():
    """Identifies and returns the GPU with the most available free memory.

    This function executes the `nvidia-smi` command-line utility to query the
    current memory usage of all available NVIDIA GPUs. It parses the output to
    determine which GPU has the maximum amount of free memory and returns its
    index along with the amount of free memory.

    This is particularly useful for automatically selecting a GPU for a
    compute-intensive task in a multi-GPU system.

    Returns:
        tuple[int, str]: A tuple where the first element is the integer index
        of the GPU with the most free memory, and the second element is a
        string representing the amount of free memory in MiB (e.g., "1024").

    Raises:
        FileNotFoundError: If the `nvidia-smi` command is not found in the
            system's PATH.
        subprocess.CalledProcessError: If the `nvidia-smi` command fails or
            returns a non-zero exit code.
    """

    gpu_stats = subprocess.check_output(
        ["nvidia-smi", "--format=csv", "--query-gpu=memory.used,memory.free"]
    )
    gpu_df = pd.read_csv(
        StringIO(gpu_stats.decode("utf-8")),
        names=["memory.used", "memory.free"],
        skiprows=1,
    )
    print("GPU usage:\n{}".format(gpu_df))
    gpu_df["memory.free"] = gpu_df["memory.free"].map(lambda x: x.rstrip(" [MiB]"))
    idx = gpu_df["memory.free"].astype(int).idxmax()
    print(
        "Returning GPU{} with {} free MiB".format(idx, gpu_df.iloc[idx]["memory.free"])
    )
    return int(idx), gpu_df.iloc[idx]["memory.free"]


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


color_bars = [
    Fore.RED,
    Fore.GREEN,
    Fore.BLUE,
    Fore.MAGENTA,
    Fore.YELLOW,
    Fore.CYAN,
    Fore.WHITE,
]


def list_dir_wrapper(path, config_obj=None):

    hostname = config_obj.hostname

    username = config_obj.username

    password = config_obj.password

    remote_dump = config_obj.remote_dump

    share_sftp = config_obj.share_sftp

    sftp_obj = config_obj.sftp_obj

    # global sftp_client
    if remote_dump:
        if share_sftp == False:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client
        elif sftp_obj == None:
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
    """Calculates start and end date components based on a time interval.

    This function takes a starting date and adds a time interval defined in a
    configuration object to determine the end date. It then returns the year,
    month, and day for both the start and end dates.

    Args:
        target_date_range (tuple): A tuple of (year, month, day) representing
            the start date.
        config_obj (object, optional): A configuration object that must contain
            the `time_window_interval_delta` attribute. This delta is added
            to the start date to calculate the end date. Defaults to None.

    Returns:
        tuple: A tuple of six integers: (start_year, start_month, end_year,
            end_month, start_day, end_day).

    Raises:
        ValueError: If `config_obj` is not provided.
    """

    if config_obj is None:
        raise ValueError("config_obj cannot be None")

    time_window_interval_delta = config_obj.time_window_interval_delta

    start_year, start_month, start_day = (
        target_date_range[0],
        target_date_range[1],
        target_date_range[2],
    )

    start_date = dt.date(start_year, start_month, start_day)
    # end_date = start_date + dt.timedelta(days=n)
    end_date = start_date + time_window_interval_delta

    return (
        start_date.year,
        start_date.month,
        end_date.year,
        end_date.month,
        start_date.day,
        end_date.day,
    )


def get_empty_date_vector(config_obj):

    # start date. Other days are for duration of time window

    start_date = config_obj.start_date

    years = config_obj.years
    months = config_obj.months
    days = config_obj.days

    interval_window_delta = config_obj.time_window_interval_delta

    combinations = generate_date_list(
        start_date, years, months, days, interval_window_delta, config_obj=config_obj
    )

    combinations = [str(item) + "_" + "date_time_stamp" for item in combinations]

    # untested float cast
    return pd.DataFrame(data=0.0, index=np.arange(1), columns=combinations).astype(
        float
    )


def sftp_exists(path, config_obj=None):

    hostname = config_obj.hostname

    username = config_obj.username

    password = config_obj.password

    sftp_obj = config_obj.sftp_obj

    share_sftp = config_obj.share_sftp

    try:
        if share_sftp == False:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_obj = ssh_client.open_sftp()

        sftp_obj.stat(path)

        if share_sftp == False:
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
    if remote_dump:
        if share_sftp == False:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client
        elif sftp_obj == None:
            sftp_obj = sftp_client

        res = sftp_obj.listdir(path)

        return res

    else:

        return os.listdir(path)


def exist_check(path, config_obj=None):

    sftp_obj = config_obj.sftp_obj
    remote_dump = config_obj.remote_dump

    if remote_dump:
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
        ssh_client.connect(hostname=hostname, username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client

    with sftp_obj.open(path, "w") as file:
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
        ssh_client.connect(hostname=hostname, username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client

    with sftp_obj.open(path, "r") as file:
        # Read CSV content into a Pandas DataFrame
        csv_content = file.read().decode("utf-8")
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

    pat_doc_annot_vec_folder_path = (
        root_path + "/" + project_name + "/pat_docs_annot_vecs/"
    )
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
        ssh_client.connect(hostname=hostname, username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client

    pat_doc_folder_path = root_path + "/" + project_name + "/pat_docs/"
    pat_doc_annot_vec_folder_path = (
        root_path + "/" + project_name + "/pat_docs_annot_vecs/"
    )

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
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()

            for i in range(len(stripped_list)):
                try:
                    if (
                        len(
                            sftp_client.listdir(
                                current_pat_lines_path + stripped_list[i]
                            )
                        )
                        >= n_pat_lines
                    ):
                        container_list.append(stripped_list[i])
                except:
                    pass
        else:
            if config_obj.verbosity > 0:
                print("Stripping list...")
            for i in tqdm(range(len(stripped_list))):
                if (
                    len(
                        list_dir_wrapper(
                            current_pat_lines_path + stripped_list[i],
                            config_obj=config_obj,
                        )
                    )
                    >= n_pat_lines
                ):
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
        for path in [
            pre_annotation_path,
            pre_annotation_path_mrc,
            current_pat_lines_path,
        ]:
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
    pre_annotation_path_reports = config_obj.pre_document_annotation_batch_path_reports

    for path in [
        pre_annotation_path,
        pre_annotation_path_mrc,
        current_pat_lines_path,
    ]:  # pre_annotation_path_reports]:
        folder_path = os.path.join(path, str(patient_id))

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    if config_obj.verbosity > 0:
        print(f"Folders created for patient {patient_id}: {current_pat_lines_path}...")


def convert_date(date_string):
    date_string = date_string.split("T")[0]
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    return date_object


def add_offset_column(
    dataframe, start_column_name, offset_column_name, time_offset, verbose=1
):
    """
    Adds a new column with the offset from the start time to the provided DataFrame.
    Handles multiple datetime formats flexibly.

    Parameters:
    - dataframe: pandas DataFrame
    - start_column_name: str, the name of the column with the starting datetime
    - offset_column_name: str, the name of the new column to be created with the offset
    - time_offset: relativedelta or timedelta, the time period offset to be added to the start time
    - verbose: int, verbosity level (0=silent, 1=basic, 2=detailed)

    Returns:
    - pandas DataFrame (modified dataframe with new offset column)
    """

    if start_column_name not in dataframe.columns:
        raise ValueError(f"Column '{start_column_name}' does not exist.")

    # Create a copy to avoid modifying the original
    df = dataframe.copy()

    # Common datetime formats to try
    common_formats = [
        "%d/%m/%y %H.%M.%S",
        "%d/%m/%Y %H.%M.%S",
        "%m/%d/%y %H.%M.%S",
        "%m/%d/%Y %H.%M.%S",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H.%M.%S",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H.%M.%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H.%M.%S",
        "%d/%m/%y",
        "%m/%d/%y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%d.%m.%y",
        "%Y.%m.%d",
    ]

    def parse_datetime_flexible(value):
        """Try multiple methods to parse datetime"""
        if pd.isna(value):
            return pd.NaT

        # Convert to string if not already
        str_value = str(value).strip()

        # Skip if empty or 'nan'
        if not str_value or str_value.lower() in ["nan", "nat", "none", "null"]:
            return pd.NaT

        # Method 1: Try pandas to_datetime with infer_datetime_format
        try:
            result = pd.to_datetime(str_value, infer_datetime_format=True)
            if pd.notna(result):
                return result
        except:
            pass

        # Method 2: Try common formats
        for fmt in common_formats:
            try:
                result = pd.to_datetime(str_value, format=fmt)
                if pd.notna(result):
                    return result
            except:
                continue

        # Method 3: Try dateutil parser (fuzzy parsing)
        try:
            result = parse(str_value, fuzzy=True)
            return pd.to_datetime(result)
        except:
            pass

        # Method 4: Try pandas to_datetime with errors='coerce'
        try:
            result = pd.to_datetime(str_value, errors="coerce")
            if pd.notna(result):
                return result
        except:
            pass

        # If all methods fail, return NaT
        return pd.NaT

    # Apply the flexible parsing
    if verbose >= 1:
        print(f"Processing {len(df)} rows for datetime conversion...")

    # Convert the datetime column
    converted_column = df[start_column_name].apply(parse_datetime_flexible)

    # Count successful conversions
    successful_conversions = converted_column.notna().sum()
    total_rows = len(df)

    if verbose >= 1:
        print(
            f"Successfully converted {successful_conversions}/{total_rows} datetime values"
        )
        if successful_conversions < total_rows:
            failed_count = total_rows - successful_conversions
            print(
                f"Warning: {failed_count} values could not be converted and will be NaT"
            )

    if verbose >= 2:
        # Show some examples of the original and converted values
        print("\nSample conversions:")
        sample_size = min(5, len(df))
        for i in range(sample_size):
            orig = df[start_column_name].iloc[i]
            conv = converted_column.iloc[i]
            print(f"  '{orig}' -> {conv}")

    # Define a function to apply the offset
    def apply_offset(dt):
        if pd.notna(dt):
            try:
                return dt + time_offset
            except Exception as e:
                if verbose >= 2:
                    print(f"Error applying offset to {dt}: {e}")
                return pd.NaT
        else:
            return pd.NaT

    # Apply the offset function to create the new column
    df[offset_column_name] = converted_column.apply(apply_offset)

    # Count successful offset applications
    successful_offsets = df[offset_column_name].notna().sum()

    if verbose >= 1:
        print(
            f"Successfully applied offset to {successful_offsets}/{successful_conversions} converted values"
        )

    return df


# Example usage and testing function
def test_datetime_formats():
    """Test the function with various datetime formats"""
    import pandas as pd
    from dateutil.relativedelta import relativedelta

    # Create test data with various formats
    test_data = {
        "timestamps": [
            "25/12/23 14.30.45",
            "12/25/23 14.30.45",
            "25/12/2023 14:30:45",
            "2023-12-25 14:30:45",
            "2023/12/25 14:30:45",
            "25-12-2023 14:30:45",
            "25.12.2023 14:30:45",
            "25/12/23",
            "2023-12-25",
            None,
            "invalid_date",
            "01/01/24 00.00.00",
        ]
    }

    df = pd.DataFrame(test_data)

    # Test with 1 month offset
    offset = relativedelta(months=1)

    result = add_offset_column(df, "timestamps", "offset_timestamps", offset, verbose=2)

    print("\nTest Results:")
    print(result[["timestamps", "offset_timestamps"]])

    return result


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
    if patient_id_column not in dataframe.columns:
        raise ValueError(f"Column '{patient_id_column}' does not exist.")

    if start_column not in dataframe.columns:
        raise ValueError(f"Column '{start_column}' does not exist.")

    if end_column not in dataframe.columns:
        raise ValueError(f"Column '{end_column}' does not exist.")

    patient_dict = {}

    for index, row in dataframe.iterrows():
        patient_id = row[patient_id_column]
        start_time = row[start_column]
        end_time = row[end_column]

        # Check if start and end times are valid datetime objects
        if pd.notnull(start_time) and pd.notnull(end_time):
            # Add the patient_id and corresponding start and end times to the dictionary
            patient_dict[patient_id] = (start_time, end_time)
        else:
            print(
                f"Ignoring patient {patient_id}: start or end time is null. "
                f"start: {start_time}, end: {end_time}"
            )

    return patient_dict
