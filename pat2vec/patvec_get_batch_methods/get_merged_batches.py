import os
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial
from clinical_note_splitter.clinical_notes_splitter import split_and_append_chunks
from pat2vec.util.methods_get import filter_dataframe_by_timestamp
from pat2vec.util.methods_annotation_regex import append_regex_term_counts

from pat2vec.util.filter_methods import filter_dataframe_by_fuzzy_terms
from pat2vec.util.filter_methods import (
    apply_bloods_data_type_filter,
    apply_data_type_epr_docs_filters,
    apply_data_type_mct_docs_filters,
    filter_dataframe_by_fuzzy_terms,
)


def verify_split_data_concatenated(original_df, client_idcode_column, save_folder):
    """
    Fast verification by concatenating all CSVs and comparing to the original DataFrame.
    Only works if client_idcode values are sorted/named in a way that matches the original DataFrame order.
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
    print("Verification successful: All CSVs match the original DataFrame.")


def verify_split_data_individual(original_df, client_idcode_column, save_folder):
    """Thorough verification by checking each CSV individually."""
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
    print("Verification successful: All CSVs match the original DataFrame.")


def save_group(client_idcode_group, save_folder):
    """Helper function to save a single group to CSV."""
    client_idcode, group = client_idcode_group
    file_path = os.path.join(save_folder, f"{client_idcode}.csv")
    group.to_csv(file_path, index=False, float_format="%.6f")
    # print(f"Saved {file_path}")  # Optional: Might print out of order in multiprocessing


def split_and_save_csv(df, client_idcode_column, save_folder, num_processes=None):
    """
    Splits a DataFrame by client_idcode and saves each subset as a CSV file using multiprocessing.

    Parameters:
    - df: pandas DataFrame containing the data.
    - client_idcode_column: str, the column name for client_idcode.
    - save_folder: str, path to the folder where CSVs will be saved.
    - num_processes: int, number of processes to use (default: all CPUs).
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
    client_idcode_list,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve batch basic observations for a list of patients based on the given parameters, specifically for blood tests.

    Args:
        client_idcode_list (list): A list of client ID codes for the patients.
        search_term (str): The term used for searching blood test-related observations.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        pd.DataFrame: Merged batch of blood test-related observations for all patients.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
    """

    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
            "pre_bloods_batch_path",
            "proj_name",  # Ensure proj_name is available in config_obj
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

    # Define the output directory using config_obj.proj_name
    input_directory = os.path.join(config_obj.proj_name, "merged_input_pat_batches")

    input_directory = config_obj.pre_merged_input_batches_path

    os.makedirs(input_directory, exist_ok=True)  # Ensure the directory exists

    # Define the path for the merged batches output
    merged_batches_path = os.path.join(input_directory, "merged_bloods_batches.csv")

    # Check if the merged file already exists and overwrite is not enabled
    if not overwrite_stored_pat_observations and os.path.exists(merged_batches_path):
        print(
            f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
        )
        return pd.read_csv(merged_batches_path)

    try:
        # Retrieve batch observations for all clients in one go
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
            entered_list=client_idcode_list,  # Pass the entire list of client IDs
            search_string=f"basicobs_value_numeric:* AND "
            f"{bloods_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
        )

        # Apply data type filters if specified
        if config_obj.data_type_filter_dict is not None:
            if (
                config_obj.data_type_filter_dict.get("filter_term_lists").get("bloods")
                is not None
            ):
                if config_obj.verbosity >= 1:
                    print(
                        "Applying doc type filter to bloods",
                        config_obj.data_type_filter_dict,
                    )

                filter_term_list = config_obj.data_type_filter_dict.get(
                    "filter_term_lists"
                ).get("bloods")

                batch_target = filter_dataframe_by_fuzzy_terms(
                    batch_target,
                    filter_term_list,
                    column_name="basicobs_itemname_analysed",
                    verbose=config_obj.verbosity,
                )

        batch_target = apply_bloods_data_type_filter(config_obj, batch_target)

        # Save the merged DataFrame to the dynamically constructed directory
        if store_pat_batch_observations or overwrite_stored_pat_observations:
            batch_target.to_csv(merged_batches_path, index=False)
            if config_obj.verbosity >= 1:
                print(f"Merged batches saved to {merged_batches_path}")

        return batch_target

    except Exception as e:
        print(f"Error retrieving batch blood test-related observations: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def get_merged_pat_batch_drugs(
    client_idcode_list,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve and merge batch drug orders for a list of patients based on the given parameters.

    Args:
        client_idcode_list (list): A list of client ID codes for the patients.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        pd.DataFrame: Merged batch of drug orders for all patients.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    drug_time_field = config_obj.drug_time_field  # Ensure this is defined in config_obj

    # Define the output directory using config_obj.pre_merged_input_batches_path
    input_directory = config_obj.pre_merged_input_batches_path
    os.makedirs(input_directory, exist_ok=True)  # Ensure the directory exists

    # Define the path for the merged batches output
    merged_batches_path = os.path.join(input_directory, "merged_drugs_batches.csv")

    # Check if the merged file already exists and overwrite is not enabled
    if not overwrite_stored_pat_observations and os.path.exists(merged_batches_path):
        print(
            f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
        )
        return pd.read_csv(merged_batches_path)

    try:
        # Retrieve batch drug orders for all clients in one go
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
            entered_list=client_idcode_list,  # Pass the entire list of client IDs
            search_string='order_typecode:"medication" AND '
            + f"{drug_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
        )

        # Apply data type filters if specified
        if config_obj.data_type_filter_dict is not None:
            if (
                config_obj.data_type_filter_dict.get("filter_term_lists").get("drugs")
                is not None
            ):
                if config_obj.verbosity >= 1:
                    print(
                        "Applying doc type filter to drugs",
                        config_obj.data_type_filter_dict,
                    )

                filter_term_list = config_obj.data_type_filter_dict.get(
                    "filter_term_lists"
                ).get("drugs")

                batch_target = filter_dataframe_by_fuzzy_terms(
                    batch_target,
                    filter_term_list,
                    column_name="order_name",  # Adjust column name for drugs
                    verbose=config_obj.verbosity,
                )

        # Save the merged DataFrame to the dynamically constructed directory
        if store_pat_batch_observations or overwrite_stored_pat_observations:
            batch_target.to_csv(merged_batches_path, index=False)
            if config_obj.verbosity >= 1:
                print(f"Merged batches saved to {merged_batches_path}")

        return batch_target

    except Exception as e:
        print(f"Error retrieving batch drug orders: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


import os
import pandas as pd


def get_merged_pat_batch_diagnostics(
    client_idcode_list,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve and merge batch diagnostic orders for a list of patients based on the given parameters.

    Args:
        client_idcode_list (list): A list of client ID codes for the patients.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        pd.DataFrame: Merged batch of diagnostic orders for all patients.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    diagnostic_time_field = (
        config_obj.diagnostic_time_field
    )  # Ensure this is defined in config_obj

    # Define the output directory using config_obj.pre_merged_input_batches_path
    input_directory = config_obj.pre_merged_input_batches_path
    os.makedirs(input_directory, exist_ok=True)  # Ensure the directory exists

    # Define the path for the merged batches output
    merged_batches_path = os.path.join(
        input_directory, "merged_diagnostics_batches.csv"
    )

    # Check if the merged file already exists and overwrite is not enabled
    if not overwrite_stored_pat_observations and os.path.exists(merged_batches_path):
        print(
            f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
        )
        return pd.read_csv(merged_batches_path)

    try:
        # Retrieve batch diagnostic orders for all clients in one go
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
            entered_list=client_idcode_list,  # Pass the entire list of client IDs
            search_string='order_typecode:"diagnostic" AND '
            + f"{diagnostic_time_field}:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
        )

        # Apply data type filters if specified
        if config_obj.data_type_filter_dict is not None:
            if (
                config_obj.data_type_filter_dict.get("filter_term_lists").get(
                    "diagnostics"
                )
                is not None
            ):
                if config_obj.verbosity >= 1:
                    print(
                        "Applying doc type filter to diagnostics",
                        config_obj.data_type_filter_dict,
                    )

                filter_term_list = config_obj.data_type_filter_dict.get(
                    "filter_term_lists"
                ).get("diagnostics")

                batch_target = filter_dataframe_by_fuzzy_terms(
                    batch_target,
                    filter_term_list,
                    column_name="order_name",  # Adjust column name for diagnostics
                    verbose=config_obj.verbosity,
                )

        # Save the merged DataFrame to the dynamically constructed directory
        if store_pat_batch_observations or overwrite_stored_pat_observations:
            batch_target.to_csv(merged_batches_path, index=False)
            if config_obj.verbosity >= 1:
                print(f"Merged batches saved to {merged_batches_path}")

        return batch_target

    except Exception as e:
        print(f"Error retrieving batch diagnostic orders: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


import os
import pandas as pd


def get_merged_pat_batch_mct_docs(
    client_idcode_list,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve and merge batch MCT documents for a list of patients based on the given parameters.

    Args:
        client_idcode_list (list): A list of client ID codes for the patients.
        search_term (str): The term used for searching MCT documents.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        pd.DataFrame: Merged batch of MCT documents for all patients.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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

    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs
    store_pat_batch_docs = config_obj.store_pat_batch_docs

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    split_clinical_notes_bool = config_obj.split_clinical_notes

    # Define the output directory using config_obj.pre_merged_input_batches_path
    input_directory = config_obj.pre_merged_input_batches_path
    os.makedirs(input_directory, exist_ok=True)  # Ensure the directory exists

    # Define the path for the merged batches output
    merged_batches_path = os.path.join(input_directory, "merged_mct_docs_batches.csv")

    # Check if the merged file already exists and overwrite is not enabled
    if not overwrite_stored_pat_docs and os.path.exists(merged_batches_path):
        print(
            f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
        )
        return pd.read_csv(merged_batches_path)

    try:
        # Retrieve batch MCT documents for all clients in one go
        batch_target = cohort_searcher_with_terms_and_search(
            index_name="observations",
            fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname
                            observation_valuetext_analysed observationdocument_recordeddtm 
                            clientvisit_visitidcode""".split(),
            term_name=config_obj.client_idcode_term_name,
            entered_list=client_idcode_list,  # Pass the entire list of client IDs
            search_string=f'obscatalogmasteritem_displayname:("AoMRC_ClinicalSummary_FT") AND '
            f"observationdocument_recordeddtm:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
        )

        # Apply data type filters for MCT documents
        batch_target = apply_data_type_mct_docs_filters(config_obj, batch_target)

        # Drop rows with NaN values in critical columns
        col_list_drop_nan = [
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "client_idcode",
        ]
        rows_with_nan = batch_target[batch_target[col_list_drop_nan].isna().any(axis=1)]
        batch_target = batch_target.drop(rows_with_nan.index).copy()

        if config_obj.verbosity >= 3:
            print(f"Post-drop NaN rows: {len(batch_target)}")

        # Split clinical notes if enabled
        if split_clinical_notes_bool:
            batch_target = split_and_append_chunks(batch_target, epr=False, mct=True)

        # Save the merged DataFrame to the dynamically constructed directory
        if store_pat_batch_docs or overwrite_stored_pat_docs:
            batch_target.to_csv(merged_batches_path, index=False)
            if config_obj.verbosity >= 1:
                print(f"Merged batches saved to {merged_batches_path}")

        return batch_target

    except Exception as e:
        print(f"Error retrieving batch MCT documents: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def get_merged_pat_batch_epr_docs(
    client_idcode_list,
    search_term,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieve and merge batch EPR documents for a list of patients based on the given parameters.

    Args:
        client_idcode_list (list): A list of client ID codes for the patients.
        search_term (str): The term used for searching EPR documents.
        config_obj (ConfigObject): An object containing global start and end year/month.
        cohort_searcher_with_terms_and_search (function): A function for searching a cohort with terms.

    Returns:
        pd.DataFrame: Merged batch of EPR documents for all patients.

    Raises:
        ValueError: If config_obj is None or missing required attributes.
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

    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs
    store_pat_batch_docs = config_obj.store_pat_batch_docs

    split_clinical_notes_bool = config_obj.split_clinical_notes

    global_start_year = str(config_obj.global_start_year).zfill(4)
    global_start_month = str(config_obj.global_start_month).zfill(2)
    global_end_year = str(config_obj.global_end_year).zfill(4)
    global_end_month = str(config_obj.global_end_month).zfill(2)
    global_start_day = str(config_obj.global_start_day).zfill(2)
    global_end_day = str(config_obj.global_end_day).zfill(2)

    # Define the output directory using config_obj.pre_merged_input_batches_path
    input_directory = config_obj.pre_merged_input_batches_path
    os.makedirs(input_directory, exist_ok=True)  # Ensure the directory exists

    # Define the path for the merged batches output
    merged_batches_path = os.path.join(input_directory, "merged_epr_docs_batches.csv")

    # Check if the merged file already exists and overwrite is not enabled
    if not overwrite_stored_pat_docs and os.path.exists(merged_batches_path):
        print(
            f"Merged batches file already exists at {merged_batches_path}. Loading from disk."
        )
        return pd.read_csv(merged_batches_path)

    try:
        # Retrieve batch EPR documents for all clients in one go
        batch_target = cohort_searcher_with_terms_and_search(
            index_name="epr_documents",
            fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
            term_name=config_obj.client_idcode_term_name,
            entered_list=client_idcode_list,  # Pass the entire list of client IDs
            search_string=f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
        )

        # Apply data type filters if specified
        if config_obj.data_type_filter_dict is not None and not batch_target.empty:
            if (
                config_obj.data_type_filter_dict.get("filter_term_lists").get(
                    "epr_docs"
                )
                is not None
            ):
                if config_obj.verbosity >= 1:
                    print(
                        "Applying doc type filter to EPR docs",
                        config_obj.data_type_filter_dict,
                    )

                filter_term_list = config_obj.data_type_filter_dict.get(
                    "filter_term_lists"
                ).get("epr_docs")

                batch_target = filter_dataframe_by_fuzzy_terms(
                    batch_target,
                    filter_term_list,
                    column_name="document_description",
                    verbose=config_obj.verbosity,
                )

            if (
                config_obj.data_type_filter_dict.get("filter_term_lists").get(
                    "epr_docs_term_regex"
                )
                is not None
            ):
                if config_obj.verbosity > 1:
                    print("Appending regex term counts...")
                batch_target = append_regex_term_counts(
                    df=batch_target,
                    terms=config_obj.data_type_filter_dict.get("filter_term_lists").get(
                        "epr_docs_term_regex"
                    ),
                    text_column="body_analysed",
                    debug=config_obj.verbosity > 5,
                )

        # Drop rows with NaN values in critical columns
        col_list_drop_nan = ["body_analysed", "updatetime", "client_idcode"]
        rows_with_nan = batch_target[batch_target[col_list_drop_nan].isna().any(axis=1)]
        batch_target = batch_target.drop(rows_with_nan.index).copy()

        if config_obj.verbosity >= 3:
            print(f"Post-drop NaN rows: {len(batch_target)}")

        # Split clinical notes if enabled
        if split_clinical_notes_bool:
            batch_target = split_and_append_chunks(batch_target, epr=True)

            # Filter split notes by global date range if enabled
            if config_obj.filter_split_notes:
                pre_filter_split_notes_len = len(batch_target)
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
                if config_obj.verbosity > 2:
                    print(
                        f"Pre-filter split notes length: {pre_filter_split_notes_len}"
                    )
                    print(f"Post-filter split notes length: {len(batch_target)}")

        # Save the merged DataFrame to the dynamically constructed directory
        if store_pat_batch_docs or overwrite_stored_pat_docs:
            batch_target.to_csv(merged_batches_path, index=False)
            if config_obj.verbosity >= 1:
                print(f"Merged batches saved to {merged_batches_path}")

        return batch_target

    except Exception as e:
        print(f"Error retrieving batch EPR documents: {e}")
        raise UnboundLocalError("Error retrieving batch EPR documents.")
