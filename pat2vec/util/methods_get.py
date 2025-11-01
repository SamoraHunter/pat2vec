import os
import pickle
import subprocess
from datetime import datetime, timedelta
from io import StringIO
from os.path import exists
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import paramiko
from colorama import Fore, Style
from dateutil.parser import parse
from tqdm import tqdm

import pandas as pd

# Use the modern standard library for timezones
import logging

from pat2vec.util.generate_date_list import generate_date_list

logger = logging.getLogger(__name__)
# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def list_dir_wrapper(path: str, config_obj: Any = None) -> List[str]:
    """Lists the contents of a directory, either locally or remotely via SFTP.

    This function acts as a wrapper around `os.listdir` and `sftp.listdir`
    to provide a consistent interface for listing directory contents based on
    the `remote_dump` setting in the configuration object.

    Args:
        path: The path to the directory to list.
        config_obj: The configuration object containing SFTP credentials and
            settings if `remote_dump` is True.

    Returns:
        A list of filenames in the specified directory.
    """
    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password
    remote_dump = config_obj.remote_dump
    share_sftp = config_obj.share_sftp
    sftp_obj = config_obj.sftp_obj
    sftp_client = None  # Initialize to avoid UnboundLocalError
    if remote_dump:
        if not share_sftp:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client

        res = sftp_obj.listdir(path)
        if not share_sftp and sftp_client:
            sftp_client.close()

        return res
    else:
        return os.listdir(path)


def convert_timestamp_to_tuple(timestamp: str) -> Tuple[int, int]:
    """Converts a timestamp string to a (year, month) tuple.

    Args:
        timestamp: The timestamp string to convert, expected in the format
            `%Y-%m-%dT%H:%M:%S.%f%z`.

    Returns:
        A tuple containing the year and month as integers.
    """

    # parse the timestamp string into a datetime object
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")

    # extract the year and month from the datetime object
    year = dt.year
    month = dt.month

    # return the tuple of year and month
    return (year, month)


def enum_target_date_vector(
    target_date_range: Tuple[int, int, int],
    current_pat_client_id_code: str,
    config_obj: Any,
) -> pd.DataFrame:
    """Creates a one-hot encoded date vector for a target date.

    Args:
        target_date_range: A tuple of (year, month, day) for the target date.
        current_pat_client_id_code: The patient's ID.
        config_obj: The configuration object.

    Returns:
        A single-row DataFrame with a one-hot encoded column for the target date.
    """
    empty_date_vector = get_empty_date_vector(config_obj=config_obj)

    empty_date_vector.at[0, str(target_date_range) + "_date_time_stamp"] = 1

    empty_date_vector["client_idcode"] = current_pat_client_id_code

    return empty_date_vector


def enum_exact_target_date_vector(
    target_date_range: Tuple[int, int, int],
    current_pat_client_id_code: str,
    config_obj: Any,
) -> pd.DataFrame:
    """Creates a one-hot encoded date vector for a specific target date.

    Args:
        target_date_range: A tuple of (year, month, day) for the target date.
        current_pat_client_id_code: The patient's ID.
        config_obj: The configuration object (currently unused).

    Returns:
        A single-row DataFrame with a one-hot encoded column for the target date.
    """
    # empty_date_vector = get_empty_date_vector(config_obj=config_obj)

    empty_date_vector = pd.DataFrame(
        columns=["client_idcode", str(target_date_range) + "_date_time_stamp"]
    )

    empty_date_vector[str(target_date_range) + "_date_time_stamp"] = 1

    empty_date_vector.at[0, str(target_date_range) + "_date_time_stamp"] = 1

    empty_date_vector["client_idcode"] = current_pat_client_id_code

    return empty_date_vector


def dump_results(file_data: Any, path: str, config_obj: Any = None) -> None:
    """Saves data to a file using pickle, either locally or remotely via SFTP.

    Args:
        file_data: The Python object to be pickled.
        path: The destination file path.
        config_obj: The configuration object containing SFTP credentials and
            settings if `remote_dump` is True.
    """
    share_sftp = config_obj.share_sftp
    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password

    sftp_obj = config_obj.sftp_obj

    remote_dump = config_obj.remote_dump
    sftp_client = None
    if remote_dump:
        if not share_sftp:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, username=username, password=password)

            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client

        with sftp_obj.open(path, "w") as file:
            pickle.dump(file_data, file)
        if not share_sftp:
            if sftp_client:
                sftp_client.close()
            sftp_obj.close()

    else:
        with open(path, "wb") as f:
            pickle.dump(file_data, f)


def update_pbar(
    current_pat_client_id_code: str,
    start_time: datetime,
    stage_int: int,
    stage_str: str,
    t: tqdm,
    config_obj: Any,
    skipped_counter: Optional[Union[int, Any]] = None,
    **n_docs_to_annotate: Any,
) -> None:
    """Updates a tqdm progress bar with formatted information about the current processing state.

    This function dynamically sets the description and color of a tqdm progress bar
    to reflect the current patient, processing stage, and execution time. The color
    changes to indicate slow performance if the elapsed time exceeds predefined
    thresholds.

    Args:
        current_pat_client_id_code: The identifier of the patient currently being processed.
        start_time: The start time of the current operation.
            Note: This parameter is currently overwritten by `config_obj.start_time`.
        stage_int: An integer representing the processing stage. Note: This parameter is currently unused.
        stage_str: A string describing the current processing stage (e.g., "demo", "annotating").
        t: The tqdm progress bar instance to update.
        config_obj: A configuration object containing settings like `start_time`,
            `multi_process`, and various `slow_execution_threshold` values.
        skipped_counter: A counter for the number of
            skipped items. Can be a standard integer or a multiprocessing-safe value. Defaults to None.
        **n_docs_to_annotate: Arbitrary keyword arguments that are displayed at the end of the
            progress bar description. Useful for showing counts like the number of documents
            to annotate.
    """
    start_time = config_obj.start_time

    multi_process = config_obj.multi_process
    slow_execution_threshold_low = config_obj.slow_execution_threshold_low
    slow_execution_threshold_high = config_obj.slow_execution_threshold_high
    slow_execution_threshold_extreme = config_obj.slow_execution_threshold_extreme

    colour_val = Fore.GREEN + Style.BRIGHT + stage_str
    counter_disp = 0
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


def get_free_gpu() -> Tuple[int, str]:
    """Identifies and returns the GPU with the most available free memory.

    This function executes the `nvidia-smi` command-line utility to query the
    current memory usage of all available NVIDIA GPUs. It parses the output to
    determine which GPU has the maximum amount of free memory and returns its
    index along with the amount of free memory.

    This is particularly useful for automatically selecting a GPU for a
    compute-intensive task in a multi-GPU system.

    Returns:
        A tuple where the first element is the integer index of the GPU with
        the most free memory, and the second element is a string representing
        the amount of free memory in MiB (e.g., "1024").

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
    logger.info("GPU usage:\n{}".format(gpu_df))
    gpu_df["memory.free"] = gpu_df["memory.free"].map(lambda x: x.rstrip(" [MiB]"))
    idx = gpu_df["memory.free"].astype(int).idxmax()  # type: ignore
    logger.info(
        "Returning GPU{} with {} free MiB".format(idx, gpu_df.iloc[idx]["memory.free"])
    )
    return int(idx), gpu_df.iloc[idx]["memory.free"]


def convert_date(date_string: str) -> datetime:
    """Converts a date string in 'YYYY-MM-DD' format to a datetime object.

    Args:
        date_string: The string to convert, which may include a time part
            (e.g., 'YYYY-MM-DDTHH:MM:SS').

    Returns:
        A datetime object representing the date part of the string.
    """
    date_string = date_string.split("T")[0]
    date_object = datetime.strptime(date_string, "%Y-%m-%d")
    return date_object


def write_csv_wrapper(
    path: str, csv_file_data: Optional[pd.DataFrame] = None, config_obj: Any = None
) -> None:
    """Writes CSV data to a file either locally or remotely.

    Args:
        path: The path to the destination CSV file.
        csv_file_data: The DataFrame to write.
        config_obj: An object containing configuration settings, including
            'remote_dump'.
    """
    remote_dump = config_obj.remote_dump

    if not remote_dump:
        # Write data locally
        if csv_file_data is not None:
            csv_file_data.to_csv(path, index=False)
    else:
        # Write data remotely using a custom function (write_remote)
        write_remote(path, csv_file_data, config_obj=config_obj)


def read_remote(path: str, config_obj: Any = None) -> pd.DataFrame:
    """Reads a remote CSV file via SFTP and returns a pandas DataFrame.

    Args:
        path: The remote path of the CSV file to read.
        config_obj: An object containing configuration details.

    Returns:
        The DataFrame containing the data read from the remote CSV file.
    """
    if config_obj is None:
        raise ValueError("Config object cannot be None.")

    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password
    share_sftp = config_obj.share_sftp
    sftp_client = None  # Initialize to ensure it's defined for the finally block
    if share_sftp:
        sftp_obj = config_obj.sftp_obj
    else:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname, username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client
    try:
        with sftp_obj.open(path, "r") as file:
            # Read CSV content into a Pandas DataFrame
            csv_content = file.read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_content))
        return df
    finally:
        if not share_sftp and sftp_client:
            sftp_client.close()
            ssh_client.close()


def read_csv_wrapper(path: str, config_obj: Any = None) -> pd.DataFrame:
    """Reads CSV data from a file, handling both local and remote paths.

    This function is a wrapper that calls either `pd.read_csv` for local files
    or `read_remote` for SFTP paths, based on the `remote_dump` flag in the
    configuration.

    Args:
        path: The path to the CSV file (local or remote).
        config_obj: An object containing configuration settings, including 'remote_dump'.

    Returns:
        The DataFrame containing the data read from the CSV file.
    """
    remote_dump = config_obj.remote_dump

    if not remote_dump:
        # Read data locally
        df = pd.read_csv(path)
    else:
        # Read data remotely using a custom function (read_remote)
        df = read_remote(path, config_obj=config_obj)

    return df


def create_local_folders(config_obj: Any = None) -> None:
    """Creates local project directories for storing intermediate files.

    Args:
        config_obj: The configuration object containing `root_path` and
            `proj_name`.
    """
    project_name = config_obj.proj_name

    root_path = config_obj.root_path

    pat_doc_folder_path = root_path + "/" + project_name + "/pat_docs/"
    Path(pat_doc_folder_path).mkdir(parents=True, exist_ok=True)

    pat_doc_annot_vec_folder_path = (
        root_path + "/" + project_name + "/pat_docs_annot_vecs/"
    )
    Path(pat_doc_annot_vec_folder_path).mkdir(parents=True, exist_ok=True)


def create_remote_folders(config_obj: Any = None) -> None:
    """Creates remote project directories for storing intermediate files via SFTP.

    Args:
        config_obj: An object containing configuration details like `root_path`,
            `proj_name`, and SFTP credentials.

    Raises:
        ValueError: If `config_obj` is not provided.
    """
    root_path = config_obj.root_path
    project_name = config_obj.proj_name

    if config_obj is None:
        raise ValueError("Config object cannot be None.")

    hostname = config_obj.hostname
    username = config_obj.username
    password = config_obj.password
    share_sftp = config_obj.share_sftp
    sftp_client = None  # Initialize for the finally block
    if share_sftp:
        sftp_obj = config_obj.sftp_obj
    else:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=hostname, username=username, password=password)
        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client
    try:
        pat_doc_folder_path = root_path + "/" + project_name + "/pat_docs/"
        pat_doc_annot_vec_folder_path = (
            root_path + "/" + project_name + "/pat_docs_annot_vecs/"
        )

        try:
            sftp_obj.stat(pat_doc_folder_path)
        except FileNotFoundError:
            sftp_obj.mkdir(pat_doc_folder_path)

        try:
            sftp_obj.stat(pat_doc_annot_vec_folder_path)
        except FileNotFoundError:
            sftp_obj.mkdir(pat_doc_annot_vec_folder_path)

    finally:
        if not share_sftp and sftp_client:
            sftp_client.close()
            ssh_client.close()


def create_folders_annot_csv_wrapper(config_obj: Any = None) -> None:
    """Creates folders locally or remotely based on the configuration.

    This function is a wrapper that calls either `create_local_folders` or
    `create_remote_folders` based on the `remote_dump` flag in the config.

    Args:
        config_obj: The configuration object.
    """

    # Create folders
    if not config_obj or not config_obj.remote_dump:
        # Create local folders
        create_local_folders(config_obj=config_obj)
    else:
        # Create remote folders
        create_remote_folders(config_obj=config_obj)


def get_empty_date_vector(config_obj: Any) -> pd.DataFrame:
    """Creates an empty DataFrame with one-hot encoded date columns.

    The columns are generated based on the time window settings in the
    configuration object.

    Args:
        config_obj: The configuration object with time window settings.

    Returns:
        A single-row DataFrame with columns for each date in the time window,
        initialized to 0.0.
    """
    start_date = config_obj.start_date
    years = config_obj.years
    months = config_obj.months
    days = config_obj.days
    interval_window_delta = config_obj.time_window_interval_delta

    combinations = generate_date_list(
        start_date, years, months, days, interval_window_delta, config_obj=config_obj
    )

    combinations = [str(item) + "_" + "date_time_stamp" for item in combinations]

    return pd.DataFrame(data=0.0, index=np.arange(1), columns=combinations).astype(
        float
    )


def sftp_exists(path: str, config_obj: Any) -> bool:
    """Checks if a file or directory exists on a remote SFTP server.

    Args:
        path: The remote path to check.
        config_obj: The configuration object containing SFTP credentials and
            settings.

    Returns:
        True if the path exists, False otherwise.
    """
    sftp_client = None
    ssh_client = None
    try:
        if config_obj.share_sftp:
            sftp_obj = config_obj.sftp_obj
        else:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(
                hostname=config_obj.hostname,
                username=config_obj.username,
                password=config_obj.password,
            )
            sftp_client = ssh_client.open_sftp()
            sftp_obj = sftp_client

        sftp_obj.stat(path)
        return True
    except FileNotFoundError:
        return False
    finally:
        if not config_obj.share_sftp and sftp_client:
            sftp_client.close()
            if ssh_client:
                ssh_client.close()


def exist_check(path: str, config_obj: Any = None) -> bool:
    """Checks if a file or directory exists, either locally or remotely.

    This is a wrapper around `os.path.exists` and `sftp_exists` that checks
    the `remote_dump` flag in the configuration object.

    Args:
        path: The path to check.
        config_obj: The configuration object.

    Returns:
        True if the path exists, False otherwise.
    """
    remote_dump = config_obj.remote_dump

    if remote_dump:
        return sftp_exists(path, config_obj)
    else:
        return exists(path)


def filter_stripped_list(
    stripped_list: List[str], config_obj: Any = None
) -> Tuple[List[str], List[str]]:
    """Filters a list of patients to exclude those already processed.

    Checks if a patient's output directory contains at least `n_pat_lines`
    files, indicating that processing for that patient is complete.

    Args:
        stripped_list: The initial list of patient IDs to process.
        config_obj: The configuration object containing paths and settings.

    Returns:
        A tuple containing two lists: the filtered list of patients to be
        processed, and the original filtered list (for reference).
    """
    strip_list = config_obj.strip_list
    remote_dump = config_obj.remote_dump
    current_pat_lines_path = config_obj.current_pat_lines_path
    n_pat_lines = config_obj.n_pat_lines
    sftp_client = None
    ssh_client = None

    if strip_list:
        # stripped_list_start_copy = stripped_list.copy()
        container_list = []

        if remote_dump:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(
                hostname=config_obj.hostname,
                username=config_obj.username,
                password=config_obj.password,
            )
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
                logger.info("Stripping list...")
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
            if sftp_client:
                sftp_client.close()
            if ssh_client:
                ssh_client.close()
    else:
        stripped_list = []
        stripped_list_start = []

    return stripped_list, stripped_list_start


def create_folders(all_patient_list: List[str], config_obj: Any = None) -> None:
    """Creates folders for each patient in the specified paths.

    Args:
        all_patient_list: List of patient IDs.
        config_obj: Configuration object containing paths and verbosity level.
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
        logger.info(f"Folders created: {current_pat_lines_path}...")


def create_folders_for_pat(patient_id: str, config_obj: Any = None) -> None:
    """Creates folders for a single patient in the specified paths.

    Args:
        patient_id: The patient's ID.
        config_obj: Configuration object containing paths and verbosity level.
    """
    pre_annotation_path = config_obj.pre_annotation_path
    pre_annotation_path_mrc = config_obj.pre_annotation_path_mrc
    current_pat_lines_path = config_obj.current_pat_lines_path

    for path in [
        pre_annotation_path,
        pre_annotation_path_mrc,
        current_pat_lines_path,
    ]:  # pre_annotation_path_reports]:
        folder_path = os.path.join(path, str(patient_id))

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    if config_obj.verbosity > 0:
        logger.info(
            f"Folders created for patient {patient_id}: {current_pat_lines_path}..."
        )


def add_offset_column(
    dataframe: pd.DataFrame,
    start_column_name: str,
    offset_column_name: str,
    time_offset: Union[timedelta, Any],
    verbose: int = 1,
) -> pd.DataFrame:
    """Adds a new column with a time offset from a starting datetime column.

    Handles multiple datetime formats flexibly.

    Args:
        dataframe: The input DataFrame.
        start_column_name: The name of the column with the starting datetime.
        offset_column_name: The name for the new column to be created.
        time_offset: The time period offset to add to the start time.
        verbose: Verbosity level (0=silent, 1=basic, 2=detailed).

    Returns:
        The modified DataFrame with the new offset column.
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
        logger.info(f"Processing {len(df)} rows for datetime conversion...")

    # Convert the datetime column
    converted_column = df[start_column_name].apply(parse_datetime_flexible)

    # Count successful conversions
    successful_conversions = converted_column.notna().sum()
    total_rows = len(df)

    if verbose >= 1:
        logger.info(
            f"Successfully converted {successful_conversions}/{total_rows} datetime values"
        )
        if successful_conversions < total_rows:
            failed_count = total_rows - successful_conversions
            logger.warning(
                f"Warning: {failed_count} values could not be converted and will be NaT"
            )

    if verbose >= 2:
        # Show some examples of the original and converted values
        logger.info("\nSample conversions:")
        sample_size = min(5, len(df))
        for i in range(sample_size):
            orig = df[start_column_name].iloc[i]
            conv = converted_column.iloc[i]
            logger.info(f"  '{orig}' -> {conv}")

    # Define a function to apply the offset
    def apply_offset(dt):
        if pd.notna(dt):
            try:
                return dt + time_offset
            except Exception as e:
                if verbose >= 2:
                    logger.error(f"Error applying offset to {dt}: {e}")
                return pd.NaT
        else:
            return pd.NaT

    # Apply the offset function to create the new column
    df[offset_column_name] = converted_column.apply(apply_offset)

    # Count successful offset applications
    successful_offsets = df[offset_column_name].notna().sum()

    if verbose >= 1:
        logger.info(
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

    logger.info(
        "\nTest Results:\n%s", result[["timestamps", "offset_timestamps"]].to_string()
    )

    return result


# add_offset_column(df, 'ADMISSION_DTTM', offset_column_name, time_offset)


def build_patient_dict(
    dataframe: pd.DataFrame,
    patient_id_column: str,
    start_column: str,
    end_column: str,
) -> Dict[str, Tuple[datetime, datetime]]:
    """Builds a dictionary mapping patient IDs to (start, end) datetime tuples.

    Args:
        dataframe: The input DataFrame.
        patient_id_column: The name of the column containing patient IDs.
        start_column: The name of the column containing start datetimes.
        end_column: The name of the column containing end datetimes.

    Returns:
        A dictionary where keys are patient IDs and values are (start, end) tuples.
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
            logger.warning(
                f"Ignoring patient {patient_id}: start or end time is null. "
                f"start: {start_time}, end: {end_time}"
            )

    return patient_dict


def write_remote(path, csv_file, config_obj=None):
    """Writes a pandas DataFrame to a remote file via SFTP.

    Args:
        path: The remote path where the file should be written.
        csv_file: The DataFrame to be written.
        config_obj: An object containing SFTP configuration details.

    Raises:
        ValueError: If `config_obj` is not provided.
    """

    if config_obj is None:
        raise ValueError("Config object cannot be None.")

    share_sftp = config_obj.share_sftp
    sftp_client = None
    ssh_client = None
    if share_sftp:
        sftp_obj = config_obj.sftp_obj
    else:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=config_obj.hostname,
            username=config_obj.username,
            password=config_obj.password,
        )
        sftp_client = ssh_client.open_sftp()
        sftp_obj = sftp_client

    with sftp_obj.open(path, "w") as file:
        csv_file.to_csv(file)

    if not share_sftp:
        if sftp_client:
            sftp_client.close()
        if ssh_client:
            ssh_client.close()
