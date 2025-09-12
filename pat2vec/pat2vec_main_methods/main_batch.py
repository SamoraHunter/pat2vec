import time
import traceback
import pandas as pd
from pat2vec.pat2vec_get_methods.get_method_appointments import get_appointments
from pat2vec.pat2vec_get_methods.get_method_report_annotations import (
    get_current_pat_report_annotations,
)
from pat2vec.pat2vec_get_methods.get_method_textual_obs_annotations import (
    get_current_pat_textual_obs_annotations,
)
from pat2vec.util.methods_get import exist_check

from pat2vec.pat2vec_get_methods.get_method_bed import get_bed
from pat2vec.pat2vec_get_methods.get_method_bloods import get_current_pat_bloods
from pat2vec.pat2vec_get_methods.get_method_bmi import get_bmi_features
from pat2vec.pat2vec_get_methods.get_method_core02 import get_core_02
from pat2vec.pat2vec_get_methods.get_method_core_resus import get_core_resus
from pat2vec.pat2vec_get_methods.get_method_current_pat_annotations_mrc_cs import (
    get_current_pat_annotations_mrc_cs,
)
from pat2vec.pat2vec_get_methods.get_method_demographics import get_demo
from pat2vec.pat2vec_get_methods.get_method_diagnostics import (
    get_current_pat_diagnostics,
)
from pat2vec.pat2vec_get_methods.get_method_drugs import get_current_pat_drugs
from pat2vec.pat2vec_get_methods.get_method_hosp_site import get_hosp_site
from pat2vec.pat2vec_get_methods.get_method_news import get_news
from pat2vec.pat2vec_get_methods.get_method_pat_annotations import (
    get_current_pat_annotations,
)
from pat2vec.pat2vec_get_methods.get_method_smoking import get_smoking
from pat2vec.pat2vec_get_methods.get_method_vte_status import get_vte_status
from pat2vec.util.methods_get import (
    enum_target_date_vector,
    enum_exact_target_date_vector,
    list_dir_wrapper,
    update_pbar,
    write_remote,
)


def main_batch(
    current_pat_client_id_code,
    target_date_range,
    batches=None,
    config_obj=None,
    stripped_list_start=None,
    t=None,
    cohort_searcher_with_terms_and_search=None,
    cat=None,
):
    """Orchestrates the feature extraction process for a single patient within a specific time window.

    This function serves as the main entry point for processing a patient's data in batch mode.
    It iterates through a list of predefined feature configurations. For each feature enabled
    in `config_obj.main_options`, it calls the corresponding `get_*` function, passing the
    pre-fetched data from the `batches` dictionary. The resulting feature DataFrames are
    concatenated into a single feature vector for the given patient and time slice.

    The final feature vector is saved as a CSV file to a specified directory, effectively creating
    a time-slice representation of the patient's state.

    Args:
        current_pat_client_id_code (str): The unique identifier for the patient being processed.
        target_date_range (tuple): A tuple representing the specific time window (e.g., (YYYY, MM, DD))
            for which to generate the feature vector.
        batches (dict[str, pd.DataFrame], optional): A dictionary containing all pre-fetched
            data batches for the patient, keyed by batch name (e.g., 'batch_demo').
        config_obj (object, optional): A configuration object containing settings like `main_options`,
            paths, and verbosity.
        stripped_list_start (list, optional): A list of patient IDs that have already been processed
            to avoid redundant computation.
        t (object, optional): A tqdm progress bar instance for updating progress.
        cohort_searcher_with_terms_and_search (callable, optional): A function to query the data source,
            used by some feature extraction methods in non-batch mode.
        cat (object, optional): A MedCAT instance for clinical text annotation. Required if any
            annotation options are enabled.

    Raises:
        ValueError: If `config_obj`, `cohort_searcher_with_terms_and_search`, `t`, or `cat` (when required)
            are not provided.

    Side Effects:
        - Writes a CSV file containing the patient's feature vector for the specified time slice.
        - Updates the tqdm progress bar `t`.
    """
    if config_obj is None:
        raise ValueError(
            "config_obj cannot be None. Please provide a valid configuration. (main_batch)"
        )

    if batches is None:
        raise ValueError(
            "batches cannot be None. Please provide a valid dictionary of dataframes. (main_batch)"
        )

    if cohort_searcher_with_terms_and_search is None:
        raise ValueError(
            "cohort_searcher_with_terms_and_search cannot be None. Please provide a valid configuration. (main_batch)"
        )

    if t is None:
        raise ValueError(
            "t cannot be None. Please provide a valid configuration. (main_batch)"
        )

    if (
        config_obj.main_options.get("annotations")
        or config_obj.main_options.get("annotations_mrc")
        or config_obj.main_options.get("annotations_reports")
        or config_obj.main_options.get("textual_obs")
    ):
        if cat is None:
            raise ValueError(
                "cat cannot be None with annotations or annotations_mrc or annotations_reports or textual_obs. Please provide a valid configuration. (main_batch)"
            )

    current_pat_client_id_code = str(current_pat_client_id_code)

    start_time = time.time()

    skipped_counter = config_obj.skipped_counter
    n_pat_lines = config_obj.n_pat_lines
    skip_additional_listdir = config_obj.skip_additional_listdir
    current_pat_lines_path = config_obj.current_pat_lines_path

    remote_dump = config_obj.remote_dump
    sftp_client = config_obj.sftp_client
    sftp_obj = config_obj.sftp_obj
    multi_process = config_obj.multi_process
    main_options = config_obj.main_options

    start_time = config_obj.start_time

    already_done = False

    done_list = []
    if current_pat_client_id_code not in stripped_list_start:

        if skip_additional_listdir:
            stripped_list = stripped_list_start
        else:

            if exist_check(
                current_pat_lines_path + str(current_pat_client_id_code),
                config_obj=config_obj,
            ):

                if (n_pat_lines is not None) and (
                    len(
                        list_dir_wrapper(
                            current_pat_lines_path + str(current_pat_client_id_code),
                            config_obj,
                        )
                    )
                    >= n_pat_lines
                ):
                    already_done = True
                    stripped_list_start.append(current_pat_client_id_code)
            stripped_list = stripped_list_start.copy()

        if current_pat_client_id_code not in stripped_list and already_done == False:

            try:
                patient_vector = []
                p_bar_entry = current_pat_client_id_code + "_" + str(target_date_range)

                # Define which functions require special or optional arguments.
                # This makes the calling mechanism more robust and easier to extend.
                funcs_with_cohort_searcher = {
                    get_current_pat_drugs, get_current_pat_diagnostics, get_current_pat_annotations,
                    get_current_pat_annotations_mrc_cs, get_core_02, get_bed, get_vte_status,
                    get_hosp_site, get_core_resus, get_news, get_smoking,
                    get_current_pat_report_annotations, get_current_pat_textual_obs_annotations,
                    get_appointments
                }
                funcs_with_cat = {
                    get_current_pat_annotations, get_current_pat_annotations_mrc_cs
                }
                funcs_with_t = {
                    get_current_pat_annotations, get_current_pat_annotations_mrc_cs,
                    get_current_pat_textual_obs_annotations
                }

                feature_configs = [
                    # Simple functions (no optional cohort_searcher, cat, or t)
                    {"option": "demo", "pbar": "demo", "func": get_demo, "batch_arg": "pat_batch", "batch_key": "batch_demo"},
                    {"option": "bmi", "pbar": "bmi", "func": get_bmi_features, "batch_arg": "pat_batch", "batch_key": "batch_bmi"},
                    {"option": "bloods", "pbar": "bloods", "func": get_current_pat_bloods, "batch_arg": "pat_batch", "batch_key": "batch_bloods"},

                    # Functions that can accept cohort_searcher
                    {"option": "drugs", "pbar": "drugs", "func": get_current_pat_drugs, "batch_arg": "pat_batch", "batch_key": "batch_drugs"},
                    {"option": "diagnostics", "pbar": "diagnostics", "func": get_current_pat_diagnostics, "batch_arg": "pat_batch", "batch_key": "batch_diagnostics"},
                    {"option": "core_02", "pbar": "core_02", "func": get_core_02, "batch_arg": "pat_batch", "batch_key": "batch_core_02"},
                    {"option": "bed", "pbar": "bed", "func": get_bed, "batch_arg": "pat_batch", "batch_key": "batch_bednumber"},
                    {"option": "vte_status", "pbar": "vte_status", "func": get_vte_status, "batch_arg": "pat_batch", "batch_key": "batch_vte"},
                    {"option": "hosp_site", "pbar": "hosp_site", "func": get_hosp_site, "batch_arg": "pat_batch", "batch_key": "batch_hospsite"},
                    {"option": "core_resus", "pbar": "core_resus", "func": get_core_resus, "batch_arg": "pat_batch", "batch_key": "batch_resus"},
                    {"option": "news", "pbar": "news", "func": get_news, "batch_arg": "pat_batch", "batch_key": "batch_news"},
                    {"option": "smoking", "pbar": "smoking", "func": get_smoking, "batch_arg": "pat_batch", "batch_key": "batch_smoking"},
                    {"option": "appointments", "pbar": "appointments", "func": get_appointments, "batch_arg": "pat_batch", "batch_key": "batch_appointments"},

                    # Annotation functions with more complex signatures
                    {
                        "option": "annotations", "pbar": "annotations_epr", "func": get_current_pat_annotations,
                        "batch_arg": "batch_epr_docs_annotations", "batch_key": "batch_epr_docs_annotations"
                    },
                    {
                        "option": "annotations_mrc", "pbar": "annotations_mrc", "func": get_current_pat_annotations_mrc_cs,
                        "batch_arg": "batch_mct_docs_annotations", "batch_key": "batch_epr_docs_annotations_mct"
                    },
                    {
                        "option": "annotations_reports", "pbar": "annotations_reports", "func": get_current_pat_report_annotations,
                        "batch_arg": "report_annotations", "batch_key": "batch_report_docs_annotations"
                    },
                    {
                        "option": "textual_obs", "pbar": "textual_obs", "func": get_current_pat_textual_obs_annotations,
                        "batch_arg": "textual_obs_annotations", "batch_key": "batch_textual_obs_annotations"
                    },
                ]

                for i, config in enumerate(feature_configs):
                    if main_options.get(config["option"]):
                        update_pbar(p_bar_entry, start_time, i, config["pbar"], t, config_obj)

                        # Dynamically build the arguments dictionary for each function
                        args = {
                            "current_pat_client_id_code": current_pat_client_id_code,
                            "target_date_range": target_date_range,
                            "config_obj": config_obj,
                        }

                        # Add the specific batch dataframe for the function
                        args[config["batch_arg"]] = batches[config["batch_key"]]

                        # Add optional arguments only if the function expects them
                        if config["func"] in funcs_with_cohort_searcher:
                            args["cohort_searcher_with_terms_and_search"] = cohort_searcher_with_terms_and_search
                        if config["func"] in funcs_with_cat:
                            args["cat"] = cat
                        if config["func"] in funcs_with_t:
                            args["t"] = t

                        # Call the function with the prepared arguments
                        feature_df = config["func"](**args)
                        patient_vector.append(feature_df)

                update_pbar(p_bar_entry, start_time, 2, "concatenating", t, config_obj)

                if config_obj.individual_patient_window:
                    target_date_vector = enum_exact_target_date_vector(
                        target_date_range,
                        current_pat_client_id_code,
                        config_obj=config_obj,
                    )
                else:
                    target_date_vector = enum_target_date_vector(
                        target_date_range,
                        current_pat_client_id_code,
                        config_obj=config_obj,
                    )

                patient_vector.append(target_date_vector)

                pat_concatted = pd.concat(patient_vector, axis=1)

                pat_concatted.drop("client_idcode", axis=1, inplace=True)

                pat_concatted.insert(0, "client_idcode", current_pat_client_id_code)

                update_pbar(p_bar_entry, start_time, 2, "saving...", t, config_obj)

                output_path = (
                    config_obj.current_pat_lines_path
                    + current_pat_client_id_code
                    + "/"
                    + str(current_pat_client_id_code)
                    + "_"
                    + str(target_date_range)
                    + ".csv"
                )

                if remote_dump == False:
                    if len(pat_concatted) > 1:
                        print(pat_concatted)
                        raise ValueError("Batch too large for local dump")

                    pat_concatted.to_csv(output_path)
                else:

                    if multi_process == True:

                        write_remote(output_path, pat_concatted, config_obj=config_obj)
                    else:
                        with sftp_client.open(output_path, "w") as file:
                            pat_concatted.to_csv(file)

                try:
                    update_pbar(
                        p_bar_entry,
                        start_time,
                        2,
                        f"Done {len(pat_concatted.columns)} cols in {int(time.time() - start_time)}s, {int((len(pat_concatted.columns)+1)/int(time.time() - start_time)+1)} p/s",
                        t,
                        config_obj,
                    )
                except:
                    update_pbar(
                        p_bar_entry,
                        start_time,
                        2,
                        f"Columns n={len(pat_concatted.columns)}",
                        t,
                        config_obj,
                    )
                    pass

                if config_obj.verbosity >= 9:
                    print("Reached end main batch")
            except RuntimeError as RuntimeError_exception:
                print("Caught runtime error... is torch?")
                print(RuntimeError)
                print("sleeping 1h")
                time.sleep(3600)

            except Exception as e:
                print(e)
                print(traceback.format_exc())
                print(f"Reproduce on {current_pat_client_id_code, target_date_range}")
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(e).__name__, e.args)
                print(message)
                raise

        else:
            if multi_process == False:
                skipped_counter = skipped_counter + 1
            else:
                with skipped_counter.get_lock():
                    skipped_counter.value += 1
            pass
