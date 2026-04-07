import os
import logging
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial
from typing import Any, List, Optional, Tuple
from pat2vec.util.helper_functions import get_df_from_db

from pat2vec.util.clinical_note_splitter import split_and_append_chunks
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.methods_annotation_regex import append_regex_term_counts

from pat2vec.util.filter_methods import filter_dataframe_by_fuzzy_terms
from pat2vec.util.filter_methods import (
    apply_bloods_data_type_filter,
    apply_data_type_mct_docs_filters,
)


def verify_split_data_concatenated(
    original_df: pd.DataFrame, client_idcode_column: str, save_folder: str
) -> None:
    """Verifies split data by concatenating and comparing with the original.

    This function provides a fast verification method by reading all the split
    CSV files, concatenating them, and comparing the result to the original
    DataFrame. It assumes the files can be sorted in a way that matches the
    original DataFrame's order.

    Args:
        original_df: The original DataFrame before splitting.
        client_idcode_column: The name of the column used for splitting.
        save_folder: The directory where the split CSV files are saved.
    """
    # Check for missing/extra files
    expected_clients = set(original_df[client_idcode_column].unique())
    saved_files = os.listdir(save_folder)
    saved_clients = {f.replace(".csv", "") for f in saved_files if f.endswith(".csv")}

    missing = expected_clients - saved_clients
    extra = saved_clients - expected_clients

    if missing:
        raise ValueError(f"Missing CSV files for clients: {missing}")
    if extra:
        raise ValueError(f"Extra CSV files with no matching clients: {extra}")

    # Read and concatenate all CSVs in sorted order (adjust sorting logic as needed)
    csv_files = sorted(
        [f for f in saved_files if f.endswith(".csv")],
        key=lambda x: int(x.split("_")[1].split(".")[0]),
    )

    concatenated_df = pd.concat(
        [pd.read_csv(os.path.join(save_folder, f)) for f in csv_files],
        ignore_index=True,
    )

    # Compare with original DataFrame
    if not concatenated_df.equals(original_df):
        raise ValueError("Concatenated CSV data does not match the original DataFrame.")
    logging.info("Verification successful: All CSVs match the original DataFrame.")


def verify_split_data_individual(
    original_df: pd.DataFrame, client_idcode_column: str, save_folder: str
) -> None:
    """Verifies split data by checking each individual CSV file.

    This function performs a more thorough verification by iterating through
    each expected client ID, reading its corresponding CSV file, and comparing
    its content to the relevant slice of the original DataFrame.

    Args:
        original_df: The original DataFrame before splitting.
        client_idcode_column: The name of the column used for splitting.
        save_folder: The directory where the split CSV files are saved.
    """
    # Check for missing/extra files (same as above)
    expected_clients = set(original_df[client_idcode_column].unique())
    saved_files = os.listdir(save_folder)
    saved_clients = {f.replace(".csv", "") for f in saved_files if f.endswith(".csv")}

    missing = expected_clients - saved_clients
    extra = saved_clients - expected_clients

    if missing:
        raise ValueError(f"Missing CSV files for clients: {missing}")
    if extra:
        raise ValueError(f"Extra CSV files with no matching clients: {extra}")

    # Check each CSV's data matches the original group
    for client in expected_clients:
        csv_path = os.path.join(save_folder, f"{client}.csv")
        csv_data = pd.read_csv(csv_path)
        original_data = original_df[
            original_df[client_idcode_column] == client
        ].reset_index(drop=True)

        # Compare data (ignore row order by sorting)
        if not original_data.sort_values(by=original_data.columns.tolist()).equals(
            csv_data.sort_values(by=csv_data.columns.tolist())
        ):
            raise ValueError(f"Data mismatch for client: {client}")
    logging.info("Verification successful: All CSVs match the original DataFrame.")


def save_group(client_idcode_group: Tuple[str, pd.DataFrame], save_folder: str) -> None:
    """Saves a single patient's data group to a CSV file.

    Args:
        client_idcode_group: A tuple containing the client ID and their data as a DataFrame.
        save_folder: The directory where the CSV file will be saved.
    """
    client_idcode, group = client_idcode_group
    file_path = os.path.join(save_folder, f"{client_idcode}.csv")

    # Check if the file already exists
    if os.path.exists(file_path):
        return

    # Save the group to CSV
    group.to_csv(file_path, index=False, float_format="%.6f")


def split_and_save_csv(
    df: pd.DataFrame,
    client_idcode_column: str,
    save_folder: str,
    num_processes: Optional[int] = None,
) -> None:
    """Splits a DataFrame by a key and saves each subset as a CSV using multiprocessing.

    This function groups a large DataFrame by the `client_idcode_column` and
    saves the data for each client into a separate CSV file in the `save_folder`.

    Args:
        df: The pandas DataFrame to split.
        client_idcode_column: The name of the column to group by.
        save_folder: The path to the folder where CSVs will be saved.
        num_processes: The number of processes to use. Defaults to all available CPUs.
    """
    # Ensure the save folder exists
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    # Group the DataFrame and convert groups to a list of tuples
    grouped = df.groupby(client_idcode_column)
    groups = list(grouped)

    # Use all CPUs if num_processes is None
    if num_processes is None:
        num_processes = cpu_count()

    # Create a partial function to fix the save_folder argument
    worker = partial(save_group, save_folder=save_folder)

    # Process groups in parallel
    with Pool(processes=num_processes) as pool:
        pool.map(worker, groups)


# Example usage:
# data = {'client_idcode': ['A123', 'B456', 'A123'], 'value': [10, 20, 30]}
# df = pd.DataFrame(data)
# split_and_save_csv(df, 'client_idcode', 'client_data', num_processes=4)


def get_merged_pat_batch_bloods(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of blood test observations for a list of patients.

    This function queries the `basic_observations` index for all patients in
    `client_idcode_list` in a single search operation.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The term to search for (currently unused in the query).
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of blood test observations.
    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "storage_backend",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations
    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day
    bloods_time_field = config_obj.bloods_time_field

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_bloods"
            schema_name = "raw_data"

            # 1. Check if we can load from the database (caching mechanism)
            if not overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load bloods data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    # Assuming that if we find data, it's complete enough for this run,
                    # as the alternative is to fetch and replace the entire table.
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            # 2. If we are here, we need to fetch from Elasticsearch
            logging.info("Fetching bloods data from Elasticsearch.")
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "basicobs_itemname_analysed",
                    "basicobs_value_numeric",
                    "basicobs_entered",
                    "clientvisit_serviceguid",
                    "updatetime",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"basicobs_value_numeric:* AND {bloods_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            # Apply data type filters if specified
            if (
                config_obj.data_type_filter_dict
                and config_obj.data_type_filter_dict.get("filter_term_lists", {}).get(
                    "bloods"
                )
            ):
                if config_obj.verbosity >= 1:
                    logging.info(
                        "Applying doc type filter to bloods",
                        config_obj.data_type_filter_dict,
                    )
                filter_term_list = config_obj.data_type_filter_dict[
                    "filter_term_lists"
                ]["bloods"]
                batch_target = filter_dataframe_by_fuzzy_terms(
                    batch_target,
                    filter_term_list,
                    column_name="basicobs_itemname_analysed",
                    verbose=config_obj.verbosity,
                )

            batch_target = apply_bloods_data_type_filter(config_obj, batch_target)

            # 3. Save the fetched data to the database
            if store_pat_batch_observations or overwrite_stored_pat_observations:
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged bloods."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,  # Good practice for large datasets
                )
                logging.info("Finished writing to database.")

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for bloods: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(input_directory, "merged_bloods_batches.csv")

        if not overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "basicobs_itemname_analysed",
                    "basicobs_value_numeric",
                    "basicobs_entered",
                    "clientvisit_serviceguid",
                    "updatetime",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"basicobs_value_numeric:* AND {bloods_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.data_type_filter_dict
                and config_obj.data_type_filter_dict.get("filter_term_lists", {}).get(
                    "bloods"
                )
            ):
                if config_obj.verbosity >= 1:
                    logging.info(
                        "Applying doc type filter to bloods",
                        config_obj.data_type_filter_dict,
                    )
                filter_term_list = config_obj.data_type_filter_dict[
                    "filter_term_lists"
                ]["bloods"]
                batch_target = filter_dataframe_by_fuzzy_terms(
                    batch_target,
                    filter_term_list,
                    column_name="basicobs_itemname_analysed",
                    verbose=config_obj.verbosity,
                )

            batch_target = apply_bloods_data_type_filter(config_obj, batch_target)

            if store_pat_batch_observations or overwrite_stored_pat_observations:
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(
                f"Error retrieving batch blood test-related observations: {e}"
            )
            return pd.DataFrame()


def get_merged_pat_batch_drugs(
    client_idcode_list: List[str],
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of drug orders for a list of patients.

    This function queries the `order` index for all patients in
    `client_idcode_list` in a single search operation, filtering for medication orders.

    Args:
        client_idcode_list: A list of client ID codes.
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of drug orders.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "storage_backend",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations
    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day
    drug_time_field = config_obj.drug_time_field

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_drugs"
            schema_name = "raw_data"

            # 1. Check if we can load from the database
            if not overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load drugs data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            # 2. Fetch from Elasticsearch
            logging.info("Fetching drugs data from Elasticsearch.")
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="order",
                fields_list=[
                    "client_idcode",
                    "order_guid",
                    "order_name",
                    "order_summaryline",
                    "order_holdreasontext",
                    "order_entered",
                    "clientvisit_visitidcode",
                    "order_performeddtm",
                    "order_createdwhen",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'order_typecode:"medication" AND {drug_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]',
            )

            # Apply data type filters
            if (
                config_obj.data_type_filter_dict
                and config_obj.data_type_filter_dict.get("filter_term_lists", {}).get(
                    "drugs"
                )
            ):
                if config_obj.verbosity >= 1:
                    logging.info(
                        "Applying doc type filter to drugs",
                        config_obj.data_type_filter_dict,
                    )
                filter_term_list = config_obj.data_type_filter_dict[
                    "filter_term_lists"
                ]["drugs"]
                batch_target = filter_dataframe_by_fuzzy_terms(
                    batch_target,
                    filter_term_list,
                    column_name="order_name",
                    verbose=config_obj.verbosity,
                )

            # 3. Save to the database
            if store_pat_batch_observations or overwrite_stored_pat_observations:
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged drugs."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )
                logging.info("Finished writing to database.")

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for drugs: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(input_directory, "merged_drugs_batches.csv")

        if not overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="order",
                fields_list=[
                    "client_idcode",
                    "order_guid",
                    "order_name",
                    "order_summaryline",
                    "order_holdreasontext",
                    "order_entered",
                    "clientvisit_visitidcode",
                    "order_performeddtm",
                    "order_createdwhen",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'order_typecode:"medication" AND {drug_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]',
            )

            if (
                config_obj.data_type_filter_dict
                and config_obj.data_type_filter_dict.get("filter_term_lists", {}).get(
                    "drugs"
                )
            ):
                if config_obj.verbosity >= 1:
                    logging.info(
                        "Applying doc type filter to drugs",
                        config_obj.data_type_filter_dict,
                    )
                filter_term_list = config_obj.data_type_filter_dict[
                    "filter_term_lists"
                ]["drugs"]
                batch_target = filter_dataframe_by_fuzzy_terms(
                    batch_target,
                    filter_term_list,
                    column_name="order_name",
                    verbose=config_obj.verbosity,
                )

            if store_pat_batch_observations or overwrite_stored_pat_observations:
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch drug orders: {e}")
            return pd.DataFrame()


def get_merged_pat_batch_diagnostics(
    client_idcode_list: List[str],
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of diagnostic orders for a list of patients.

    This function queries the `order` index for all patients in
    `client_idcode_list` in a single search operation, filtering for diagnostic orders.

    Args:
        client_idcode_list: A list of client ID codes.
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of diagnostic orders.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "storage_backend",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    diagnostic_time_field = config_obj.diagnostic_time_field

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_diagnostics"
            schema_name = "raw_data"

            if not overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load diagnostics data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="order",
                fields_list=[
                    "client_idcode",
                    "order_guid",
                    "order_name",
                    "order_summaryline",
                    "order_holdreasontext",
                    "order_entered",
                    "clientvisit_visitidcode",
                    "order_performeddtm",
                    "order_createdwhen",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'order_typecode:"diagnostic" AND {diagnostic_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]',
            )

            if (
                config_obj.data_type_filter_dict
                and config_obj.data_type_filter_dict.get("filter_term_lists", {}).get(
                    "diagnostics"
                )
            ):
                if config_obj.verbosity >= 1:
                    logging.info(
                        "Applying doc type filter to diagnostics",
                        config_obj.data_type_filter_dict,
                    )
                filter_term_list = config_obj.data_type_filter_dict[
                    "filter_term_lists"
                ]["diagnostics"]
                batch_target = filter_dataframe_by_fuzzy_terms(
                    batch_target,
                    filter_term_list,
                    column_name="order_name",
                    verbose=config_obj.verbosity,
                )

            if store_pat_batch_observations or overwrite_stored_pat_observations:
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged diagnostics."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for diagnostics: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(
            input_directory, "merged_diagnostics_batches.csv"
        )

        if not overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="order",
                fields_list=[
                    "client_idcode",
                    "order_guid",
                    "order_name",
                    "order_summaryline",
                    "order_holdreasontext",
                    "order_entered",
                    "clientvisit_visitidcode",
                    "order_performeddtm",
                    "order_createdwhen",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'order_typecode:"diagnostic" AND {diagnostic_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]',
            )

            if config_obj.data_type_filter_dict is not None:
                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists").get(
                        "diagnostics"
                    )
                    is not None
                ):
                    if config_obj.verbosity >= 1:
                        logging.info(
                            "Applying doc type filter to diagnostics",
                            config_obj.data_type_filter_dict,
                        )
                    filter_term_list = config_obj.data_type_filter_dict.get(
                        "filter_term_lists"
                    ).get("diagnostics")
                    batch_target = filter_dataframe_by_fuzzy_terms(
                        batch_target,
                        filter_term_list,
                        column_name="order_name",
                        verbose=config_obj.verbosity,
                    )

            if store_pat_batch_observations or overwrite_stored_pat_observations:
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch diagnostic orders: {e}")
            return pd.DataFrame()


def get_merged_pat_batch_mct_docs(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of MCT documents for a list of patients.

    This function queries the `observations` index for all patients in
    `client_idcode_list`, filtering for 'AoMRC_ClinicalSummary_FT' documents.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The term to search for (currently unused).
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of MCT documents.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "storage_backend",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs
    store_pat_batch_docs = config_obj.store_pat_batch_docs

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    split_clinical_notes_bool = config_obj.split_clinical_notes

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_mct_docs"
            schema_name = "raw_data"

            if not overwrite_stored_pat_docs:
                logging.info(
                    f"Attempting to load MCT docs data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'obscatalogmasteritem_displayname:("AoMRC_ClinicalSummary_FT") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            batch_target = apply_data_type_mct_docs_filters(config_obj, batch_target)

            col_list_drop_nan = [
                "observation_valuetext_analysed",
                "observationdocument_recordeddtm",
                "client_idcode",
            ]
            batch_target = batch_target.dropna(subset=col_list_drop_nan).copy()

            if split_clinical_notes_bool:
                batch_target = split_and_append_chunks(
                    batch_target, epr=False, mct=True
                )

            if store_pat_batch_docs or overwrite_stored_pat_docs:
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged MCT docs."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for MCT docs: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(
            input_directory, "merged_mct_docs_batches.csv"
        )

        if not overwrite_stored_pat_docs and os.path.exists(merged_batches_path):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'obscatalogmasteritem_displayname:("AoMRC_ClinicalSummary_FT") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            batch_target = apply_data_type_mct_docs_filters(config_obj, batch_target)

            col_list_drop_nan = [
                "observation_valuetext_analysed",
                "observationdocument_recordeddtm",
                "client_idcode",
            ]
            batch_target = batch_target.dropna(subset=col_list_drop_nan).copy()

            if split_clinical_notes_bool:
                batch_target = split_and_append_chunks(
                    batch_target, epr=False, mct=True
                )

            if store_pat_batch_docs or overwrite_stored_pat_docs:
                batch_target.to_csv(merged_batches_path, index=False)

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch MCT documents: {e}")
            return pd.DataFrame()


def get_merged_pat_batch_epr_docs(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of EPR documents for a list of patients.

    This function queries the `epr_documents` index for all patients in
    `client_idcode_list` within the globally defined time window.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The term to search for (currently unused).
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of EPR documents.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "storage_backend",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs
    store_pat_batch_docs = config_obj.store_pat_batch_docs
    split_clinical_notes_bool = config_obj.split_clinical_notes
    global_start_year = str(config_obj.global_start_year).zfill(4)
    global_start_month = str(config_obj.global_start_month).zfill(2)
    global_end_year = str(config_obj.global_end_year).zfill(4)
    global_end_month = str(config_obj.global_end_month).zfill(2)
    global_start_day = str(config_obj.global_start_day).zfill(2)
    global_end_day = str(config_obj.global_end_day).zfill(2)

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_epr_docs"
            schema_name = "raw_data"

            if not overwrite_stored_pat_docs:
                logging.info(
                    f"Attempting to load EPR docs data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="epr_documents",
                fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if config_obj.data_type_filter_dict is not None and not batch_target.empty:
                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists", {}).get(
                        "epr_docs"
                    )
                    is not None
                ):
                    filter_term_list = config_obj.data_type_filter_dict[
                        "filter_term_lists"
                    ]["epr_docs"]
                    batch_target = filter_dataframe_by_fuzzy_terms(
                        batch_target,
                        filter_term_list,
                        column_name="document_description",
                        verbose=config_obj.verbosity,
                    )
                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists", {}).get(
                        "epr_docs_term_regex"
                    )
                    is not None
                ):
                    batch_target = append_regex_term_counts(
                        df=batch_target,
                        terms=config_obj.data_type_filter_dict["filter_term_lists"][
                            "epr_docs_term_regex"
                        ],
                        text_column="body_analysed",
                        debug=config_obj.verbosity > 5,
                    )

            col_list_drop_nan = ["body_analysed", "updatetime", "client_idcode"]
            batch_target = batch_target.dropna(subset=col_list_drop_nan).copy()

            if split_clinical_notes_bool:
                batch_target = split_and_append_chunks(batch_target, epr=True)
                if config_obj.filter_split_notes:
                    batch_target = filter_dataframe_by_timestamp(
                        df=batch_target,
                        start_year=int(global_start_year),
                        start_month=int(global_start_month),
                        end_year=int(global_end_year),
                        end_month=int(global_end_month),
                        start_day=int(global_start_day),
                        end_day=int(global_end_day),
                        timestamp_string="updatetime",
                        dropna=False,
                    )

            if store_pat_batch_docs or overwrite_stored_pat_docs:
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged EPR docs."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for EPR docs: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(
            input_directory, "merged_epr_docs_batches.csv"
        )

        if not overwrite_stored_pat_docs and os.path.exists(merged_batches_path):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="epr_documents",
                fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if config_obj.data_type_filter_dict is not None and not batch_target.empty:
                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists", {}).get(
                        "epr_docs"
                    )
                    is not None
                ):
                    filter_term_list = config_obj.data_type_filter_dict[
                        "filter_term_lists"
                    ]["epr_docs"]
                    batch_target = filter_dataframe_by_fuzzy_terms(
                        batch_target,
                        filter_term_list,
                        column_name="document_description",
                        verbose=config_obj.verbosity,
                    )
                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists", {}).get(
                        "epr_docs_term_regex"
                    )
                    is not None
                ):
                    batch_target = append_regex_term_counts(
                        df=batch_target,
                        terms=config_obj.data_type_filter_dict["filter_term_lists"][
                            "epr_docs_term_regex"
                        ],
                        text_column="body_analysed",
                        debug=config_obj.verbosity > 5,
                    )

            col_list_drop_nan = ["body_analysed", "updatetime", "client_idcode"]
            batch_target = batch_target.dropna(subset=col_list_drop_nan).copy()

            if split_clinical_notes_bool:
                batch_target = split_and_append_chunks(batch_target, epr=True)
                if config_obj.filter_split_notes:
                    batch_target = filter_dataframe_by_timestamp(
                        df=batch_target,
                        start_year=int(global_start_year),
                        start_month=int(global_start_month),
                        end_year=int(global_end_year),
                        end_month=int(global_end_month),
                        start_day=int(global_start_day),
                        end_day=int(global_end_day),
                        timestamp_string="updatetime",
                        dropna=False,
                    )

            if store_pat_batch_docs or overwrite_stored_pat_docs:
                batch_target.to_csv(merged_batches_path, index=False)

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch EPR documents: {e}")
            raise UnboundLocalError("Error retrieving batch EPR documents.")


def get_merged_pat_batch_textual_obs_docs(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of textual observations for a list of patients.

    This function queries the `basic_observations` index for all patients in
    `client_idcode_list` and filters for rows containing non-empty `textualObs`.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The term to search for (currently unused).
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of textual observation documents.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "pre_merged_input_batches_path",
            "proj_name",  # Ensure proj_name is available in config_obj
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations

    bloods_time_field = config_obj.bloods_time_field

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_textual_obs"
            schema_name = "raw_data"

            if not overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load textual obs data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "basicobs_itemname_analysed",
                    "basicobs_value_numeric",
                    "basicobs_value_analysed",
                    "basicobs_entered",
                    "clientvisit_serviceguid",
                    "basicobs_guid",
                    "updatetime",
                    "textualObs",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"{bloods_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            batch_target = batch_target.dropna(subset=["textualObs"])
            batch_target = batch_target[batch_target["textualObs"] != ""]
            batch_target["body_analysed"] = batch_target["textualObs"].astype(str)

            if store_pat_batch_observations or overwrite_stored_pat_observations:
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged textual obs."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for textual obs: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(
            input_directory, "merged_textual_obs_batches.csv"
        )

        if not overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "basicobs_itemname_analysed",
                    "basicobs_value_numeric",
                    "basicobs_value_analysed",
                    "basicobs_entered",
                    "clientvisit_serviceguid",
                    "basicobs_guid",
                    "updatetime",
                    "textualObs",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"{bloods_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            batch_target = batch_target.dropna(subset=["textualObs"])
            batch_target = batch_target[batch_target["textualObs"] != ""]
            batch_target["body_analysed"] = batch_target["textualObs"].astype(str)

            if store_pat_batch_observations or overwrite_stored_pat_observations:
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch textual observations: {e}")
            return pd.DataFrame()


def get_merged_pat_batch_appointments(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of appointments for a list of patients.

    This function queries the `pims_apps*` index for all patients in
    `client_idcode_list` within the globally defined time window.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The term to search for (currently unused).
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of appointments.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "pre_merged_input_batches_path",
            "proj_name",  # Ensure proj_name is available in config_obj
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    appointments_time_field = config_obj.appointments_time_field

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_appointments"
            schema_name = "raw_data"

            if not config_obj.overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load appointments data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj,
                    schema_name,
                    table_name,
                    patient_ids=client_idcode_list,
                    patient_id_column="HospitalID",
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="pims_apps*",
                fields_list=[
                    "Popular",
                    "AppointmentType",
                    "AttendanceReference",
                    "ClinicCode",
                    "ClinicDesc",
                    "Consultant",
                    "DateModified",
                    "DNA",
                    "HospitalID",
                    "PatNHSNo",
                    "Specialty",
                    "AppointmentDateTime",
                    "Attended",
                    "CancDesc",
                    "CancRefNo",
                    "ConsultantCode",
                    "DateCreated",
                    "Ethnicity",
                    "Gender",
                    "NHSNoStatusCode",
                    "NotSpec",
                    "PatDateOfBirth",
                    "PatForename",
                    "PatPostCode",
                    "PatSurname",
                    "PiMsPatRefNo",
                    "Primarykeyfieldname",
                    "Primarykeyfieldvalue",
                    "SessionCode",
                    "SpecialtyCode",
                ],
                term_name="HospitalID.keyword",
                entered_list=client_idcode_list,
                search_string=f"{appointments_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or config_obj.overwrite_stored_pat_observations
            ):
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged appointments."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for appointments: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(
            input_directory, "merged_appointments_batches.csv"
        )

        if not config_obj.overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="pims_apps*",
                fields_list=[
                    "Popular",
                    "AppointmentType",
                    "AttendanceReference",
                    "ClinicCode",
                    "ClinicDesc",
                    "Consultant",
                    "DateModified",
                    "DNA",
                    "HospitalID",
                    "PatNHSNo",
                    "Specialty",
                    "AppointmentDateTime",
                    "Attended",
                    "CancDesc",
                    "CancRefNo",
                    "ConsultantCode",
                    "DateCreated",
                    "Ethnicity",
                    "Gender",
                    "NHSNoStatusCode",
                    "NotSpec",
                    "PatDateOfBirth",
                    "PatForename",
                    "PatPostCode",
                    "PatSurname",
                    "PiMsPatRefNo",
                    "Primarykeyfieldname",
                    "Primarykeyfieldvalue",
                    "SessionCode",
                    "SpecialtyCode",
                ],
                term_name="HospitalID.keyword",
                entered_list=client_idcode_list,
                search_string=f"{appointments_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or config_obj.overwrite_stored_pat_observations
            ):
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch appointments: {e}")
            return pd.DataFrame()


def get_merged_pat_batch_demo(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of demographic information for a list of patients.

    This function queries the `epr_documents` index for all patients in
    `client_idcode_list` to get their demographic data.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The term to search for (currently unused).
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of demographic information.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "pre_merged_input_batches_path",
            "proj_name",  # Ensure proj_name is available in config_obj
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_demographics"
            schema_name = "raw_data"

            if not config_obj.overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load demographics data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="epr_documents",
                fields_list=[
                    "client_idcode",
                    "client_firstname",
                    "client_lastname",
                    "client_dob",
                    "client_gendercode",
                    "client_racecode",
                    "client_deceaseddtm",
                    "updatetime",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or config_obj.overwrite_stored_pat_observations
            ):
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged demographics."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for demographics: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(input_directory, "merged_demo_batches.csv")

        if not config_obj.overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="epr_documents",
                fields_list=[
                    "client_idcode",
                    "client_firstname",
                    "client_lastname",
                    "client_dob",
                    "client_gendercode",
                    "client_racecode",
                    "client_deceaseddtm",
                    "updatetime",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or config_obj.overwrite_stored_pat_observations
            ):
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch demographic information: {e}")
            return pd.DataFrame()


def get_merged_pat_batch_bmi(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of BMI-related observations for a list of patients.

    This function queries the `observations` index for all patients in
    `client_idcode_list`, filtering for BMI, Weight, and Height observations.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The term to search for (currently unused).
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of BMI-related observations.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "storage_backend",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_bmi"
            schema_name = "raw_data"

            if not overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load BMI data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'obscatalogmasteritem_displayname:("OBS BMI" OR "OBS Weight" OR "OBS height") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or overwrite_stored_pat_observations
            ):
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged BMI data."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for BMI: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(input_directory, "merged_bmi_batches.csv")

        if not config_obj.overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'obscatalogmasteritem_displayname:("OBS BMI" OR "OBS Weight" OR "OBS height") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or config_obj.overwrite_stored_pat_observations
            ):
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch BMI-related observations: {e}")
            return pd.DataFrame()


def get_merged_pat_batch_obs(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of specific observations for a list of patients.

    This function queries the `observations` index for all patients in
    `client_idcode_list`, filtering for a specific `search_term`.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The specific observation term to search for.
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of specified observations.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "storage_backend",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            # Sanitize search_term for table name
            safe_search_term = "".join(
                e for e in search_term if e.isalnum() or e == "_"
            ).lower()
            table_name = f"raw_obs_{safe_search_term}"
            schema_name = "raw_data"

            if not overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load '{search_term}' data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'obscatalogmasteritem_displayname:("{search_term}") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or overwrite_stored_pat_observations
            ):
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        f"DB engine not initialized, cannot save merged obs for '{search_term}'."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(
                f"Database operation failed for observation '{search_term}': {e}"
            )
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(
            input_directory, f"merged_{search_term}_batches.csv"
        )

        if not config_obj.overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f'obscatalogmasteritem_displayname:("{search_term}") AND '
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or config_obj.overwrite_stored_pat_observations
            ):
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch observations: {e}")
            return pd.DataFrame()


def get_merged_pat_batch_news(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of NEWS observations for a list of patients.

    This function queries the `observations` index for all patients in
    `client_idcode_list`, filtering for 'NEWS' or 'NEWS2' observations.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The term to search for (currently unused).
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of NEWS observations.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "storage_backend",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_news"
            schema_name = "raw_data"

            if not overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load NEWS data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj, schema_name, table_name, patient_ids=client_idcode_list
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"obscatalogmasteritem_displayname:(NEWS*) AND "
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or overwrite_stored_pat_observations
            ):
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged NEWS data."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for NEWS: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(input_directory, "merged_news_batches.csv")

        if not config_obj.overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                                observation_valuetext_analysed observationdocument_recordeddtm
                                clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"obscatalogmasteritem_displayname:(NEWS*) AND "
                f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if (
                config_obj.store_pat_batch_observations
                or config_obj.overwrite_stored_pat_observations
            ):
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch NEWS observations: {e}")
            return pd.DataFrame()


def get_merged_pat_batch_reports(
    client_idcode_list: List[str],
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a merged batch of reports for a list of patients.

    This function queries the `basic_observations` index for all patients in
    `client_idcode_list`, filtering for documents where the item name is 'report'.

    Args:
        client_idcode_list: A list of client ID codes.
        search_term: The specific report type to search for.
        config_obj: The configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the merged batch of reports.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "pre_merged_input_batches_path",
            "proj_name",  # Ensure proj_name is available in config_obj
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_observations = config_obj.overwrite_stored_pat_observations
    store_pat_batch_observations = config_obj.store_pat_batch_observations

    global_start_year = str(config_obj.global_start_year).zfill(4)
    global_start_month = str(config_obj.global_start_month).zfill(2)
    global_end_year = str(config_obj.global_end_year).zfill(4)
    global_end_month = str(config_obj.global_end_month).zfill(2)
    global_start_day = str(config_obj.global_start_day).zfill(2)
    global_end_day = str(config_obj.global_end_day).zfill(2)

    # --- DATABASE BACKEND LOGIC ---
    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_reports"
            schema_name = "raw_data"

            if not overwrite_stored_pat_observations:
                logging.info(
                    f"Attempting to load reports data for {len(client_idcode_list)} patients from database '{schema_name}.{table_name}'."
                )
                df = get_df_from_db(
                    config_obj,
                    schema_name,
                    table_name,
                    patient_ids=client_idcode_list,
                    patient_id_column="HospitalID",
                )
                if not df.empty:
                    logging.info(
                        f"Successfully loaded {len(df)} records from database cache."
                    )
                    return df

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "HospitalID",
                    "updatetime",
                    "textualObs",
                    "basicobs_guid",
                    "basicobs_value_analysed",
                    "basicobs_itemname_analysed",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"basicobs_itemname_analysed:{search_term} AND updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            batch_target["body_analysed"] = (
                batch_target["textualObs"].astype(str)
                + "\n"
                + batch_target["basicobs_value_analysed"].astype(str)
            )

            if store_pat_batch_observations or overwrite_stored_pat_observations:
                engine = config_obj.db_engine
                if not engine:
                    logging.error(
                        "DB engine not initialized, cannot save merged reports."
                    )
                    return batch_target

                db_table_name = (
                    f"{schema_name}_{table_name}"
                    if engine.name == "sqlite"
                    else table_name
                )
                db_schema = None if engine.name == "sqlite" else schema_name

                logging.info(
                    f"Writing {len(batch_target)} records to database table '{db_schema}.{db_table_name}'..."
                )
                batch_target.to_sql(
                    name=db_table_name,
                    con=engine,
                    schema=db_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=10000,
                )

            return batch_target

        except Exception as e:
            logging.error(f"Database operation failed for reports: {e}")
            return pd.DataFrame()

    # --- FILE-BASED (LEGACY) BACKEND LOGIC ---
    elif config_obj.storage_backend == "file":
        input_directory = config_obj.pre_merged_input_batches_path
        os.makedirs(input_directory, exist_ok=True)
        merged_batches_path = os.path.join(
            input_directory, "merged_reports_batches.csv"
        )

        if not overwrite_stored_pat_observations and os.path.exists(
            merged_batches_path
        ):
            logging.info(
                f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
            )
            return pd.read_csv(merged_batches_path)

        try:
            batch_target = cohort_searcher_with_terms_and_search(
                index_name="basic_observations",
                fields_list=[
                    "client_idcode",
                    "updatetime",
                    "textualObs",
                    "basicobs_guid",
                    "basicobs_value_analysed",
                    "basicobs_itemname_analysed",
                ],
                term_name=config_obj.client_idcode_term_name,
                entered_list=client_idcode_list,
                search_string=f"basicobs_itemname_analysed:{search_term} AND updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            batch_target["body_analysed"] = (
                batch_target["textualObs"].astype(str)
                + "\n"
                + batch_target["basicobs_value_analysed"].astype(str)
            )

            if store_pat_batch_observations or overwrite_stored_pat_observations:
                batch_target.to_csv(merged_batches_path, index=False)
                if config_obj.verbosity >= 1:
                    logging.info(f"Merged batches saved to {merged_batches_path}")

            return batch_target

        except Exception as e:
            logging.error(f"Error retrieving batch reports: {e}")
            return pd.DataFrame()
