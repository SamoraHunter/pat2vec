import os
import random
import time
import traceback
from datetime import datetime
from pat2vec.pat2vec_search.cogstack_search_methods import initialize_cogstack_client
from multiprocessing import Pool
import pandas as pd
from pat2vec.pat2vec_search.cogstack_search_methods import (
    cohort_searcher_with_terms_and_search,
)
from pat2vec.patvec_get_batch_methods.get_prefetch_batches import prefetch_batches
#from pat2vec.pat2vec_search.cogstack_search_methods import *
from colorama import Back, Fore, Style

from pat2vec.util.generate_date_list import generate_date_list
from pat2vec.util.get_best_gpu import set_best_gpu

from tqdm import trange
from pat2vec.pat2vec_main_methods.main_batch import main_batch
from pat2vec.pat2vec_pat_list.get_patient_treatment_list import get_all_patients_list
from pat2vec.patvec_get_batch_methods.main import (
    get_pat_batch_bloods,
    get_pat_batch_bmi,
    get_pat_batch_demo,
    get_pat_batch_diagnostics,
    get_pat_batch_drugs,
    get_pat_batch_epr_docs,
    get_pat_batch_epr_docs_annotations,
    get_pat_batch_mct_docs,
    get_pat_batch_mct_docs_annotations,
    get_pat_batch_news,
    get_pat_batch_obs,
    get_pat_batch_reports,
    get_pat_batch_reports_docs_annotations,
    get_pat_batch_appointments,
    get_pat_batch_textual_obs_docs,
    get_pat_batch_textual_obs_annotations,
)
from pat2vec.util import config_pat2vec
from pat2vec.util.methods_get import (
    create_folders_for_pat,
    filter_stripped_list,
    get_free_gpu,
    list_dir_wrapper,
    update_pbar,
)
from pat2vec.util.methods_get_medcat import get_cat
from pat2vec.util.get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
)


color_bars = [
    Fore.RED,
    Fore.GREEN,
    Fore.BLUE,
    Fore.MAGENTA,
    Fore.YELLOW,
    Fore.CYAN,
    Fore.WHITE,
]


class main:
    """The main orchestrator for the pat2vec feature extraction pipeline.

    This class manages the entire workflow of processing patient data to generate
    time-sliced feature vectors. It initializes the pipeline based on a configuration
    object, connects to data sources like CogStack, prepares a list of patients,
    and orchestrates the feature extraction process for each patient.

    The typical workflow is as follows:
    1.  An instance of this class is created with a `config_obj` that defines
        all pipeline parameters (e.g., time windows, enabled features, paths).
    2.  It establishes a connection to the data source (e.g., Elasticsearch via CogStack).
    3.  It retrieves or generates a list of patients to be processed.
    4.  It can pre-fetch all necessary raw data batches for the entire patient cohort
        if `prefetch_pat_batches` is enabled in the configuration.
    5.  For each patient, it iterates through the defined time windows.
    6.  For each time slice, it calls the `main_batch` function, which in turn calls
        the individual feature extraction modules (e.g., for demographics, bloods,
        NLP annotations) to generate a feature vector.
    7.  The resulting feature vector is saved to a file.

    This class relies heavily on the `config_obj` for its behavior.

    Attributes:
        config_obj (config_class): The configuration object that controls the pipeline.
        cs (CogStack): An instance of the CogStack client for data retrieval.
        all_patient_list (list): The list of patient IDs to be processed.
        cat (MedCAT): A MedCAT instance for clinical text annotation if required.
        t (tqdm.trange): A progress bar for monitoring the process.
    """

    def __init__(
        self,
        cogstack=True,
        use_filter=False,
        json_filter_path=None,
        random_seed_val=42,
        hostname=None,
        config_obj=None,
    ):
        """Initializes the main pat2vec pipeline orchestrator.

        This constructor sets up the pipeline environment, including data source
        connections, patient lists, and NLP models, based on the provided
        configuration.

        Args:
            cogstack (bool, optional): If True, connects to a CogStack Elasticsearch
                instance for data retrieval. If False, a dummy searcher is used,
                which is useful for testing. Defaults to True.
            use_filter (bool, optional): If True, applies a CUI filter to the
                MedCAT model to restrict annotations to a specific set of concepts.
                Requires `json_filter_path`. Defaults to False.
            json_filter_path (str, optional): Path to a JSON file containing the
                CUI filter for MedCAT. Required if `use_filter` is True.
                Defaults to None.
            random_seed_val (int, optional): The random seed for reproducibility of
                operations like patient list shuffling. Defaults to 42.
            hostname (str, optional): Deprecated. The hostname for the SFTP server.
                SFTP settings are now managed within the config object. Defaults to None.
            config_obj (config_class, optional): The main configuration object that
                drives the pipeline's behavior. If None, a default configuration
                is created. Defaults to None.
        """
        self.batch_mode = config_obj.batch_mode
        self.remote_dump = config_obj.remote_dump  # Deprecated
        self.negated_presence_annotations = config_obj.negated_presence_annotations
        self.store_annot = config_obj.store_annot
        self.share_sftp = config_obj.share_sftp  # Deprecated
        self.multi_process = config_obj.multi_process  # Deprecated
        self.strip_list = config_obj.strip_list
        self.verbosity = config_obj.verbosity
        self.random_seed_val = config_obj.random_seed_val
        self.hostname = config_obj.hostname  # Deprecated
        self.config_obj = config_obj

        if self.config_obj == None:
            print("Init default config on config_pat2vec")
            self.config_obj = config_pat2vec.config_class()

        # config parameters
        self.suffix = config_obj.suffix
        self.treatment_doc_filename = config_obj.treatment_doc_filename
        self.treatment_control_ratio_n = config_obj.treatment_control_ratio_n
        self.pre_annotation_path = config_obj.pre_annotation_path
        self.pre_annotation_path_mrc = config_obj.pre_annotation_path_mrc
        self.proj_name = config_obj.proj_name
        self.gpu_mem_threshold = config_obj.gpu_mem_threshold  # For medCat
        self.all_patient_list = get_all_patients_list(self.config_obj)
        self.current_pat_lines_path = config_obj.current_pat_lines_path
        self.sftp_client = config_obj.sftp_obj

        if cogstack == True:
            # Initialize the CogStack client with the config object
            self.cs = initialize_cogstack_client(self.config_obj)

            if config_obj.testing:
                self.cohort_searcher_with_terms_and_search = (
                    cohort_searcher_with_terms_and_search_dummy
                )
                print("Init cohort_searcher_with_terms_and_search_dummy function")
            else:
                if self.config_obj.verbosity > 0:
                    print("Init cohort_searcher_with_terms_and_search function")
                self.cohort_searcher_with_terms_and_search = (
                    cohort_searcher_with_terms_and_search
                )
        else:
            if self.config_obj.verbosity > 0:
                print("Warning cohort_searcher_with_terms_and_search disabled")
            self.cohort_searcher_with_terms_and_search = None

        if self.verbosity > 0:
            print(self.pre_annotation_path)
            print(self.pre_annotation_path_mrc)

        self.use_filter = use_filter  # Using a medcat CUI filter for annotations data.

        if self.use_filter:
            self.json_filter_path = json_filter_path
            import json

            with open(self.json_filter_path, "r") as f:
                json_data = json.load(f)

            len(json_data["projects"][0])
            json_cuis = json_data["projects"][0]["cuis"].split(",")
            self.cat.cdb.filter_by_cui(json_cuis)

        set_best_gpu(config_obj.gpu_mem_threshold)

        random.seed(self.config_obj.random_seed_val)
        if config_obj.shuffle_pat_list == True:
            random.shuffle(self.all_patient_list)

        if self.config_obj.verbosity > 0:
            print(f"remote_dump {self.remote_dump}")
            print(self.pre_annotation_path)
            print(self.pre_annotation_path_mrc)

        self.stripped_list_start = [
            x.replace(".csv", "")
            for x in list_dir_wrapper(
                path=self.current_pat_lines_path, config_obj=config_obj
            )
        ]

        (
            print(f"Length of stripped_list_start: {len(self.stripped_list_start)}")
            if self.config_obj.verbosity > 0
            else None
        )

        stripped_list = [
            x.replace(".csv", "")
            for x in list_dir_wrapper(
                path=self.current_pat_lines_path, config_obj=config_obj
            )
        ]

        skipped_counter = 0
        self.t = trange(
            len(self.all_patient_list),
            desc="Bar desc",
            leave=True,
            colour="GREEN",
            position=0,
            total=len(self.all_patient_list),
        )

        self.cat = get_cat(config_obj)

        self.stripped_list = [
            x.replace(".csv", "")
            for x in list_dir_wrapper(
                path=self.current_pat_lines_path, config_obj=config_obj
            )
        ]
        if self.config_obj.individual_patient_window == False:
            self.stripped_list, self.stripped_list_start = filter_stripped_list(
                self.stripped_list, config_obj=self.config_obj
            )
        else:
            print("skipped strip list because ipw is enabled")

        self.n_pat_lines = config_obj.n_pat_lines

        if self.config_obj.prefetch_pat_batches:
            if self.config_obj.verbosity > 0:
                print("Prefetching patient batches...")

            prefetch_batches(pat2vec_obj=self)

    def _get_patient_data_batches(self, current_pat_client_id_code):
        """Fetches and organizes all data batches for a single patient.

        This method centralizes and modularizes the data fetching logic previously
        handled directly within `pat_maker`. It uses a configuration-driven
        approach to retrieve various types of patient data (e.g., clinical notes,
        lab results, demographics) based on the settings in `self.config_obj`.

        The core of this method is two configuration lists:
        - `batch_configs`: For standard data types like bloods, drugs, and demographics.
        - `annotation_batch_configs`: For text-derived annotations from MedCAT.

        Each entry in these lists is a dictionary that specifies:
        - `option`: The key in `self.config_obj.main_options` that enables/disables
          this data source.
        - `var`: The variable name to use as the key in the returned dictionary.
        - `func`: The specific `get_pat_batch_*` function responsible for fetching
          the data.
        - `args`: A dictionary of additional arguments for the fetch function (e.g.,
          a specific `search_term`).
        - `empty`: An empty DataFrame with the correct schema to use as a
          fallback if the data source is disabled or returns no data.

        This design makes the system highly extensible. To add a new data source,
        a developer only needs to:
        1. Create a new `get_pat_batch_<new_source>` function.
        2. Add a corresponding configuration dictionary to the `batch_configs` or
           `annotation_batch_configs` list within this method.

        Args:
            current_pat_client_id_code (str): The unique identifier for the patient
                for whom to fetch data batches.

        Returns:
            dict[str, pd.DataFrame]: A dictionary where keys are the batch names
                (e.g., 'batch_epr', 'batch_bloods') and values are the fetched
                pandas DataFrames. If a data source is disabled in the main
                configuration or if no data is found for the patient, the
                corresponding value will be an appropriately structured empty
                DataFrame.
        """
        empty_return = pd.DataFrame()
        empty_return_epr = pd.DataFrame(columns=["updatetime", "body_analysed"])
        empty_return_mct = pd.DataFrame(
            columns=["observationdocument_recordeddtm", "observation_valuetext_analysed"]
        )
        empty_return_textual_obs = pd.DataFrame(columns=["basicobs_entered", "textualObs"])
        empty_return_reports = pd.DataFrame(
            columns=["updatetime", "observation_valuetext_analysed"]
        )

        # Configuration for standard data batches
        batch_configs = [
            {
                "option": "annotations", "var": "batch_epr", "func": get_pat_batch_epr_docs,
                "args": {"search_term": None}, "empty": empty_return_epr
            },
            {
                "option": "annotations_mrc", "var": "batch_mct", "func": get_pat_batch_mct_docs,
                "args": {"search_term": None}, "empty": empty_return_mct
            },
            {
                "option": "textual_obs", "var": "batch_textual_obs_docs", "func": get_pat_batch_textual_obs_docs,
                "args": {"search_term": None}, "empty": empty_return_textual_obs
            },
            {
                "option": "annotations_reports", "var": "batch_reports", "func": get_pat_batch_reports,
                "args": {"search_term": None}, "empty": empty_return_reports
            },
            {
                "option": "smoking", "var": "batch_smoking", "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_SmokingStatus"}, "empty": empty_return
            },
            {
                "option": "core_02", "var": "batch_core_02", "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_SpO2"}, "empty": empty_return
            },
            {
                "option": "bed", "var": "batch_bednumber", "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_BedNumber3"}, "empty": empty_return
            },
            {
                "option": "vte_status", "var": "batch_vte", "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_VTE_STATUS"}, "empty": empty_return
            },
            {
                "option": "hosp_site", "var": "batch_hospsite", "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_HospitalSite"}, "empty": empty_return
            },
            {
                "option": "core_resus", "var": "batch_resus", "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_RESUS_STATUS"}, "empty": empty_return
            },
            {
                "option": "news", "var": "batch_news", "func": get_pat_batch_news,
                "args": {"search_term": None}, "empty": empty_return
            },
            {
                "option": "bmi", "var": "batch_bmi", "func": get_pat_batch_bmi,
                "args": {"search_term": None}, "empty": empty_return
            },
            {
                "option": "diagnostics", "var": "batch_diagnostics", "func": get_pat_batch_diagnostics,
                "args": {"search_term": None}, "empty": empty_return
            },
            {
                "option": "drugs", "var": "batch_drugs", "func": get_pat_batch_drugs,
                "args": {"search_term": None}, "empty": empty_return
            },
            {
                "option": "demo", "var": "batch_demo", "func": get_pat_batch_demo,
                "args": {"search_term": None}, "empty": empty_return
            },
            {
                "option": "bloods", "var": "batch_bloods", "func": get_pat_batch_bloods,
                "args": {"search_term": None}, "empty": empty_return
            },
            {
                "option": "appointments", "var": "batch_appointments", "func": get_pat_batch_appointments,
                "args": {"search_term": None}, "empty": empty_return
            },
        ]

        # Configuration for annotation batches
        annotation_batch_configs = [
            {
                "option": "annotations", "var": "batch_epr_docs_annotations",
                "func": get_pat_batch_epr_docs_annotations, "empty": empty_return_epr
            },
            {
                "option": "annotations_mrc", "var": "batch_epr_docs_annotations_mct",
                "func": get_pat_batch_mct_docs_annotations, "empty": empty_return_mct
            },
            {
                "option": "textual_obs", "var": "batch_textual_obs_annotations",
                "func": get_pat_batch_textual_obs_annotations, "empty": empty_return_textual_obs
            },
            {
                "option": "annotations_reports", "var": "batch_reports_docs_annotations",
                "func": get_pat_batch_reports_docs_annotations, "empty": empty_return_reports
            },
        ]

        batches = {}

        # Fetch standard batches
        for config in batch_configs:
            if self.config_obj.main_options.get(config["option"], True):
                batches[config["var"]] = config["func"](
                    current_pat_client_id_code=current_pat_client_id_code,
                    config_obj=self.config_obj,
                    cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    **config["args"]
                )
            else:
                batches[config["var"]] = config["empty"]

        # Fetch annotation batches
        for config in annotation_batch_configs:
            if self.config_obj.main_options.get(config["option"], True):
                batch_result = config["func"](
                    current_pat_client_id_code,
                    config_obj=self.config_obj,
                    cat=self.cat,
                    t=self.t,
                )
                # Handle cases where annotation functions might return None
                if batch_result is None:
                    if self.config_obj.verbosity > 2:
                        print(f"{config['var']} empty")
                    batches[config["var"]] = config["empty"]
                else:
                    batches[config["var"]] = batch_result
            else:
                batches[config["var"]] = config["empty"]

        return batches

    def _setup_patient_time_window(self, current_pat_client_id_code):
        """
        Sets up and returns the date list for a patient, handling IPW logic.
        """
        if self.config_obj.verbosity >= 4:
            print("main_pat2vec>self.config_obj.individual_patient_window: ")

        # Default to global date list if not using IPW
        if not self.config_obj.individual_patient_window:
            return self.config_obj.date_list

        pat_dates = self.config_obj.patient_dict.get(current_pat_client_id_code)

        if not pat_dates:  # It's a control patient
            if self.config_obj.individual_patient_window_controls_method == "full":
                current_pat_start_date = datetime(
                    int(self.config_obj.initial_global_start_year),
                    int(self.config_obj.initial_global_start_month),
                    int(self.config_obj.initial_global_start_day),
                )
                current_pat_end_date = datetime(
                    int(self.config_obj.initial_global_end_year),
                    int(self.config_obj.initial_global_end_month),
                    int(self.config_obj.initial_global_end_day),
                )
                if self.config_obj.verbosity >= 4:
                    print(f"Control pat full {current_pat_client_id_code} ipw dates set:")
                    print("Start Date:", current_pat_start_date)
                    print("End Date:", current_pat_end_date)

            elif self.config_obj.individual_patient_window_controls_method == "random":
                # Select a random treatment's time window for application.
                patient_ids = list(self.config_obj.patient_dict.keys())
                if not patient_ids:
                    print("Warning: Cannot use 'random' control method with an empty patient_dict. Skipping.")
                    return None
                random_pat_id = random.choice(patient_ids)
                pat_dates = self.config_obj.patient_dict.get(random_pat_id)
                current_pat_start_date, current_pat_end_date = pat_dates
            else:
                print(f"Unknown control method: {self.config_obj.individual_patient_window_controls_method}")
                return None
        else:  # It's a treatment patient
            if len(pat_dates) != 2:
                print(f"Warning: Invalid dates for patient {current_pat_client_id_code}. Skipping.")
                return None
            current_pat_start_date, current_pat_end_date = pat_dates

        # Safeguard against invalid date types
        if pd.isna(current_pat_start_date) or pd.isna(current_pat_end_date) or not isinstance(current_pat_start_date, datetime) or not isinstance(current_pat_end_date, datetime):
            print(f"Warning: Dates for patient {current_pat_client_id_code} are invalid. Skipping.")
            return None

        # Determine anchor date for generation and clamping boundaries
        p_real_start, p_real_end = min(current_pat_start_date, current_pat_end_date), max(current_pat_start_date, current_pat_end_date)
        date_for_generate = p_real_end if self.config_obj.lookback else p_real_start

        # Override global dates as a workaround for generate_date_list
        self.config_obj.global_start_year = str(p_real_start.year).zfill(4)
        self.config_obj.global_start_month = str(p_real_start.month).zfill(2)
        self.config_obj.global_start_day = str(p_real_start.day).zfill(2)
        self.config_obj.global_end_year = str(p_real_end.year).zfill(4)
        self.config_obj.global_end_month = str(p_real_end.month).zfill(2)
        self.config_obj.global_end_day = str(p_real_end.day).zfill(2)
        self.config_obj.start_date = date_for_generate

        date_list = generate_date_list(
            date_for_generate,
            self.config_obj.years,
            self.config_obj.months,
            self.config_obj.days,
            self.config_obj.time_window_interval_delta,
            config_obj=self.config_obj,
        )

        if self.config_obj.verbosity >= 4:
            print("ipw, datelist", current_pat_client_id_code)
            print(date_list[0:5] if date_list else "date_list is empty")

        self.n_pat_lines = len(date_list)
        return date_list

    def _clean_document_batches(self, batches):
        """
        Cleans timestamp columns for all document-related batches.
        """
        doc_configs = [
            {"key": "batch_epr", "time_col": "updatetime", "text_col": "body_analysed", "option": "annotations"},
            {"key": "batch_mct", "time_col": "observationdocument_recordeddtm", "text_col": "observation_valuetext_analysed", "option": "annotations_mrc"},
            {"key": "batch_reports", "time_col": "updatetime", "text_col": None, "option": "annotations_reports"},
            {"key": "batch_textual_obs_docs", "time_col": "basicobs_entered", "text_col": None, "option": "textual_obs"},
            {"key": "batch_epr_docs_annotations", "time_col": "updatetime", "text_col": None, "option": "annotations"},
            {"key": "batch_epr_docs_annotations_mct", "time_col": "observationdocument_recordeddtm", "text_col": None, "option": "annotations_mrc"},
            {"key": "batch_reports_docs_annotations", "time_col": "updatetime", "text_col": None, "option": "annotations_reports"},
        ]

        for config in doc_configs:
            if self.config_obj.main_options.get(config["option"], True):
                batch = batches.get(config["key"])
                if batch is not None and not batch.empty:
                    time_col = config["time_col"]
                    text_col = config["text_col"]

                    try:
                        batch[time_col] = pd.to_datetime(batch[time_col], errors="coerce", utc=True)
                        batch.dropna(subset=[time_col], inplace=True)

                        if text_col:
                            batch.dropna(subset=[text_col], inplace=True)
                            batch = batch[batch[text_col].apply(lambda x: isinstance(x, str))]

                        batches[config["key"]] = batch
                    except Exception as e:
                        print(f"Error cleaning batch {config['key']}: {e}")
                        print(type(batch))
                        print(batch.columns)

        if self.config_obj.verbosity > 3:
            print("post batch timestamp na drop:")
            print("EPR:", len(batches["batch_epr"]))
            print("MCT:", len(batches["batch_mct"]))
            print("EPR annotations:", len(batches["batch_epr_docs_annotations"]))
            print("EPR annotations mct:", len(batches["batch_epr_docs_annotations_mct"]))
            print("textual obs docs:", len(batches["batch_textual_obs_docs"]))
            print("textual obs annotations:", len(batches["batch_textual_obs_annotations"]))
            print("batch_report_docs_annotations:", len(batches["batch_reports_docs_annotations"]))

        return batches

    def _process_patient_slices(self, current_pat_client_id_code, date_list, batches):
        """
        Iterates through time slices and calls main_batch to generate feature vectors.
        """
        run_on_pat = current_pat_client_id_code not in self.stripped_list

        if not run_on_pat:
            return

        for j, date_slice in enumerate(date_list):
            try:
                if self.config_obj.verbosity > 5:
                    print(f"Processing date {date_slice} for patient {current_pat_client_id_code}...")

                if self.config_obj.calculate_vectors:
                    main_batch(
                        current_pat_client_id_code,
                        date_slice,
                        batch_demo=batches["batch_demo"],
                        batch_smoking=batches["batch_smoking"],
                        batch_core_02=batches["batch_core_02"],
                        batch_bednumber=batches["batch_bednumber"],
                        batch_vte=batches["batch_vte"],
                        batch_hospsite=batches["batch_hospsite"],
                        batch_resus=batches["batch_resus"],
                        batch_news=batches["batch_news"],
                        batch_bmi=batches["batch_bmi"],
                        batch_diagnostics=batches["batch_diagnostics"],
                        batch_epr=batches["batch_epr"],
                        batch_mct=batches["batch_mct"],
                        batch_bloods=batches["batch_bloods"],
                        batch_drugs=batches["batch_drugs"],
                        batch_epr_docs_annotations=batches["batch_epr_docs_annotations"],
                        batch_epr_docs_annotations_mct=batches["batch_epr_docs_annotations_mct"],
                        batch_report_docs_annotations=batches["batch_reports_docs_annotations"],
                        batch_textual_obs_annotations=batches["batch_textual_obs_annotations"],
                        batch_appointments=batches["batch_appointments"],
                        config_obj=self.config_obj,
                        stripped_list_start=self.stripped_list_start,
                        t=self.t,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                        cat=self.cat,
                    )

            except Exception as e:
                print(e)
                print(f"Exception in patmaker on {current_pat_client_id_code, date_slice}")
                print(traceback.format_exc())
                raise e

    def pat_maker(self, i):
        """Orchestrates the entire feature extraction process for a single patient.

        This method is the primary worker function for processing one patient from the
        cohort. It manages the patient's specific time window, pre-fetches all
        necessary raw data, and then iterates through each time slice to generate
        feature vectors.

        The key steps for each patient are:
        1.  **Check for Completion**: Skips the patient if their feature vectors have
            already been generated, based on the `stripped_list_start`.
        2.  **Set Time Window**: If `individual_patient_window` is enabled, it
            calculates and sets the specific start and end dates for this patient,
            overriding the global time window. It handles both primary and control
            patients differently.
        3.  **Pre-fetch Data Batches**: It calls various `get_pat_batch_*` functions
            to retrieve all required data for the patient across their entire
            time window. This includes demographics, bloods, medications, clinical
            notes (EPR, MRC), reports, and other observations.
        4.  **Pre-generate Annotations**: If text-based features are enabled (e.g.,
            `annotations`, `annotations_mrc`), it processes the fetched clinical
            notes with MedCAT to generate all annotations for the patient upfront.
        5.  **Data Cleaning**: Performs initial cleaning on the fetched batches, such
            as dropping records with missing timestamps.
        6.  **Iterate and Process Slices**: It loops through each time slice defined
            in the patient's `date_list`. For each slice, it calls `main_batch`,
            passing all the pre-fetched data. `main_batch` is responsible for
            filtering the data for that specific slice and generating the final
            feature vector CSV file.

        Args:
            i (int): The index of the patient within `self.all_patient_list` to be
                processed.

        Side Effects:
            - Creates output directories for the patient's feature vectors if they
              do not exist.
            - Fetches potentially large amounts of data from the source (e.g.,
              Elasticsearch) and holds it in memory for processing.
            - Calls `main_batch` which results in writing one CSV file per time
              slice for the patient.
            - Updates the `tqdm` progress bar to reflect the current status.
            - Can modify `self.config_obj` attributes (like `date_list` and global
              start/end dates) on-the-fly when `individual_patient_window` is enabled.

        Returns:
            None: This method orchestrates the processing pipeline and manages file
                I/O, but it does not return any value.
        """

        if self.config_obj.verbosity > 3:
            print(f"Processing patient {i} at {self.all_patient_list[i]}...")

        skipped_counter = self.config_obj.skipped_counter
        stripped_list = self.stripped_list
        all_patient_list = self.all_patient_list
        skipped_counter = self.config_obj.skipped_counter
        current_pat_client_id_code = str(self.all_patient_list[i])

        remote_dump = self.config_obj.remote_dump
        hostname = self.config_obj.hostname
        username = self.config_obj.username
        password = self.config_obj.password

        stripped_list_start = self.stripped_list_start

        if not self.config_obj.individual_patient_window:
            date_list = self.config_obj.date_list
            if self.config_obj.verbosity > 3:
                print("Date list> pat_maker")
                print(date_list[0:5])

        multi_process = self.config_obj.multi_process

        if skipped_counter == None:
            skipped_counter = 0

        current_pat_client_id_code = str(all_patient_list[i])

        create_folders_for_pat(current_pat_client_id_code, self.config_obj)

        p_bar_entry = current_pat_client_id_code

        if self.config_obj.individual_patient_window:

        # Check if patient has already been processed
        if current_pat_client_id_code in self.stripped_list_start:
            if self.config_obj.verbosity >= 4:
                print("main_pat2vec>self.config_obj.individual_patient_window: ")

            if all_patient_list[i] not in self.config_obj.patient_dict.keys():
                # is control pat

                if self.config_obj.individual_patient_window_controls_method == "full":
                    # Use the initally set global time window
                    # Assuming you have access to the global variables
                    global_start_month = self.config_obj.initial_global_start_month
                    global_start_year = self.config_obj.initial_global_start_year
                    global_end_month = self.config_obj.initial_global_end_month
                    global_end_year = self.config_obj.initial_global_end_year
                    global_start_day = self.config_obj.initial_global_start_day
                    global_end_day = self.config_obj.initial_global_end_day

                    # Convert global variables to integers
                    global_start_month = int(global_start_month)
                    global_start_year = int(global_start_year)
                    global_end_month = int(global_end_month)
                    global_end_year = int(global_end_year)
                    global_start_day = int(global_start_day)
                    global_end_day = int(global_end_day)

                    # Create datetime objects for start and end dates
                    current_pat_start_date = datetime(
                        global_start_year, global_start_month, global_start_day
                    )
                    current_pat_end_date = datetime(
                        global_end_year, global_end_month, global_end_day
                    )

                    if self.config_obj.verbosity >= 4:
                        print(f"Control pat full {all_patient_list[i]} ipw dates set:")
                        # Print the datetime objects for verification (optional)
                        print("Start Date:", current_pat_start_date)
                        print("End Date:", current_pat_end_date)

                elif (
                    self.config_obj.individual_patient_window_controls_method
                    == "random"
                ):
                    # Select a random treatments time window for application.
                    index = random.randint(0, len(all_patient_list))
                    current_pat_start_date = self.config_obj.patient_dict.get(
                        all_patient_list[index]
                    )[0]

                    current_pat_end_date = self.config_obj.patient_dict.get(
                        all_patient_list[index]
                    )[1]

                print(f"patient {i} in stripped_list_start")
            skipped_counter = self.config_obj.skipped_counter
            if self.config_obj.multi_process is False:
                skipped_counter += 1
            else:
                with skipped_counter.get_lock():
                    skipped_counter.value += 1
            if self.config_obj.verbosity > 0:
                print(f"Skipped {i}")
            return

                # Pat is in patient_dict.keys... Simplified and robust date retrieval.
                pat_dates = self.config_obj.patient_dict.get(all_patient_list[i])

                if not pat_dates or len(pat_dates) != 2:
                    print(
                        f"Warning: Invalid or missing dates for patient {all_patient_list[i]} in patient_dict. Skipping."
                    )
                    return

                # The patient_dict contains a tuple of (start, end) or (end, start) if lookback=True.
                # These should already be datetime objects from the config setup.
                current_pat_start_date, current_pat_end_date = pat_dates

                print("Debug date routine:")
                print("current_pat_start_date", current_pat_start_date)
                print("current_pat_end_date", current_pat_end_date)

                # Safeguard against non-datetime or NaT values which can cause crashes.
                if (
                    pd.isna(current_pat_start_date)
                    or pd.isna(current_pat_end_date)
                    or not isinstance(current_pat_start_date, datetime)
                    or not isinstance(current_pat_end_date, datetime)
                ):
                    print(
                        f"Warning: Dates for patient {all_patient_list[i]} are invalid or not datetime objects. Skipping."
                    )
                    return

                # print("Debug date routine:")
                # print("current_pat_start_date", current_pat_start_date)
                # print("current_pat_end_date", current_pat_end_date)
                # print(self.config_obj.patient_dict.get(all_patient_list[i])[0])
                # print(self.config_obj.patient_dict.get(all_patient_list[i])[1])

            # The patient_dict contains a tuple of (start, end) dates.
            # We assign the real start/end and then determine the anchor date for generation and clamping boundaries.
            p_real_start = current_pat_start_date
            p_real_end = current_pat_end_date

            if p_real_start > p_real_end:
                p_real_start, p_real_end = p_real_end, p_real_start

            if self.config_obj.lookback:
                # For lookback, the generation starts from the end of the patient's window and goes backward.
                date_for_generate = p_real_end
            else:
                # For forward generation, it starts from the beginning of the window and goes forward.
                date_for_generate = p_real_start

            # The clamping boundaries are always the patient's full, ordered time window.
            g_start, g_end = p_real_start, p_real_end

            # Override global dates with the correctly ordered patient window for clamping.
            # This is a workaround to pass patient-specific boundaries to generate_date_list.
            self.config_obj.global_start_month = g_start.month
            self.config_obj.global_start_year = g_start.year
            self.config_obj.global_start_day = g_start.day
            self.config_obj.global_end_month = g_end.month
            self.config_obj.global_end_year = g_end.year
            self.config_obj.global_end_day = g_end.day

            self.config_obj.start_date = date_for_generate

            self.config_obj.global_start_year = str(
                self.config_obj.global_start_year
            ).zfill(4)
            self.config_obj.global_start_month = str(
                self.config_obj.global_start_month
            ).zfill(2)

            self.config_obj.global_end_year = str(
                self.config_obj.global_end_year
            ).zfill(4)
            self.config_obj.global_end_month = str(
                self.config_obj.global_end_month
            ).zfill(2)

            self.config_obj.global_start_day = str(
                self.config_obj.global_start_day
            ).zfill(2)
            self.config_obj.global_end_day = str(self.config_obj.global_end_day).zfill(
                2
            )

            if self.config_obj.verbosity >= 4:
                print("main_pat2vec > ipw dates:")
                self.config_obj.global_start_year = str(
                    self.config_obj.global_start_year
                ).zfill(4)
                self.config_obj.global_start_month = str(
                    self.config_obj.global_start_month
                ).zfill(2)
                self.config_obj.global_end_year = str(
                    self.config_obj.global_end_year
                ).zfill(4)
                self.config_obj.global_end_month = str(
                    self.config_obj.global_end_month
                ).zfill(2)

                self.config_obj.global_start_day = str(
                    self.config_obj.global_start_day
                ).zfill(2)

                self.config_obj.global_end_day = str(
                    self.config_obj.global_end_day
                ).zfill(2)

            # calculate for ipw
            interval_window_delta = self.config_obj.time_window_interval_delta

            # self.config_obj.date_list = generate_date_list(self.config_obj.start_date,
            #                                                self.config_obj.years,
            #                                                self.config_obj.months,
            #                                                self.config_obj.days,
            #                                                interval_window_delta,
            #                                                lookback=self.config_obj.lookback
            #                                                )

            if self.config_obj.verbosity >= 4:
                print(
                    "overwriting datelist in ipw",
                    self.config_obj.start_date,
                    interval_window_delta,
                )
                print("current_pat_end_date", current_pat_end_date)
                print("current_pat_start_date", current_pat_start_date)

            if self.config_obj.verbosity >= 4:

                print("date for generate", date_for_generate)

            self.config_obj.date_list = generate_date_list(
                date_for_generate,
                self.config_obj.years,
                self.config_obj.months,
                self.config_obj.days,
                interval_window_delta,
                config_obj=self.config_obj,
            )
            if self.config_obj.verbosity >= 4:
                print("overwriting temp datetime with ipw")
                print("date for generate:", date_for_generate)
                print("self.config_obj.date_list:", self.config_obj.date_list)
            date_list = self.config_obj.date_list

            if self.config_obj.verbosity >= 4:
                print("self.config_obj.date_list:", self.config_obj.date_list)

            self.n_pat_lines = len(self.config_obj.date_list)

            if self.config_obj.verbosity >= 4:
                print("ipw, datelist", current_pat_client_id_code)
                print(self.config_obj.date_list[0:5])

        create_folders_for_pat(current_pat_client_id_code, self.config_obj)
        start_time = time.time()

        if self.config_obj.verbosity >= 4:
            print("pat maker called: opts: ", self.config_obj.main_options)
        # 1. Set up time window for the patient
        date_list = self._setup_patient_time_window(current_pat_client_id_code)
        if date_list is None:
            return  # Skip patient if time window setup fails

        # 2. Update progress and fetch data batches
        update_pbar(
            p_bar_entry,
            current_pat_client_id_code,
            start_time,
            0,
            f"Pat_maker called on {i}...",
            self.t,
            self.config_obj,
            skipped_counter,
            self.config_obj.skipped_counter,
        )
        batches = self._get_patient_data_batches(current_pat_client_id_code)

        sftp_obj = self.config_obj.sftp_obj
        update_pbar(
            current_pat_client_id_code,
            start_time,
            0,
            f"Done batches in {time.time()-start_time}",
            self.t,
            self.config_obj,
            self.config_obj.skipped_counter,
        )

        # get_pat batches
        # 3. Clean document batches if required
        if self.config_obj.dropna_doc_timestamps:
            batches = self._clean_document_batches(batches)

        stripped_list = stripped_list_start.copy()
        # 4. Process patient data in time slices
        self._process_patient_slices(current_pat_client_id_code, date_list, batches)

        if self.config_obj.verbosity >= 4:
            print("stripped_list_start")
            print(stripped_list_start)

        if current_pat_client_id_code not in stripped_list_start:
            if self.config_obj.verbosity >= 6:
                print(f"Getting batches for patient {i}...")

            update_pbar(
                p_bar_entry,
                start_time,
                0,
                "Getting batches...",
                self.t,
                self.config_obj,
                skipped_counter,
            )

            # Fetch all data batches for the patient
            batches = self._get_patient_data_batches(current_pat_client_id_code)
            batch_epr = batches["batch_epr"]
            batch_mct = batches["batch_mct"]
            batch_textual_obs_docs = batches["batch_textual_obs_docs"]
            batch_reports = batches["batch_reports"]
            batch_smoking = batches["batch_smoking"]
            batch_core_02 = batches["batch_core_02"]
            batch_bednumber = batches["batch_bednumber"]
            batch_vte = batches["batch_vte"]
            batch_hospsite = batches["batch_hospsite"]
            batch_resus = batches["batch_resus"]
            batch_news = batches["batch_news"]
            batch_bmi = batches["batch_bmi"]
            batch_diagnostics = batches["batch_diagnostics"]
            batch_drugs = batches["batch_drugs"]
            batch_demo = batches["batch_demo"]
            batch_bloods = batches["batch_bloods"]
            batch_appointments = batches["batch_appointments"]
            batch_epr_docs_annotations = batches["batch_epr_docs_annotations"]
            batch_epr_docs_annotations_mct = batches["batch_epr_docs_annotations_mct"]
            batch_textual_obs_annotations = batches["batch_textual_obs_annotations"]
            batch_reports_docs_annotations = batches["batch_reports_docs_annotations"]

            update_pbar(
                p_bar_entry,
                start_time,
                0,
                f"Done batches in {time.time()-start_time}",
                self.t,
                self.config_obj,
                skipped_counter,
            )

            if self.config_obj.verbosity > 3:

                print("Batch Sizes:")
                print("EPR:", len(batch_epr))
                print("MCT:", len(batch_mct))
                print("Smoking:", len(batch_smoking))
                print("SpO2:", len(batch_core_02))
                print("BedNumber:", len(batch_bednumber))
                print("VTE:", len(batch_vte))
                print("HospitalSite:", len(batch_hospsite))
                print("RESUS:", len(batch_resus))
                print("NEWS:", len(batch_news))
                print("BMI:", len(batch_bmi))
                print("Diagnostics:", len(batch_diagnostics))
                print("Drugs:", len(batch_drugs))
                print("Demo:", len(batch_demo))
                print("Bloods:", len(batch_bloods))
                print("EPR annotations:", len(batch_epr_docs_annotations))
                print("EPR annotations mct:", len(batch_epr_docs_annotations_mct))
                print("batch_reports:", len(batch_reports))
                print(
                    "batch_report_docs_annotations:",
                    len(batch_reports_docs_annotations),
                )
                print("TextualObs:", len(batch_textual_obs_docs))
                print("TextualObs annotations:", len(batch_textual_obs_annotations))

            if self.config_obj.verbosity > 3:
                print(f"Done batches in {time.time() - start_time}")

            run_on_pat = False
            only_check_last = True
            last_check = all_patient_list[i] not in stripped_list
            skip_check = last_check

            if self.config_obj.dropna_doc_timestamps:
                # clean epr and mct:

                if self.config_obj.main_options.get("annotations", True):
                    target_column_string = "updatetime"
                    batch_epr[target_column_string] = pd.to_datetime(
                        batch_epr[target_column_string], errors="coerce", utc=True
                    )
                    batch_epr.dropna(subset=[target_column_string], inplace=True)
                    batch_epr.dropna(subset=["body_analysed"], inplace=True)
                    batch_epr = batch_epr[
                        batch_epr["body_analysed"].apply(lambda x: isinstance(x, str))
                    ]

                if self.config_obj.main_options.get("annotations_mrc", True):
                    target_column_string = "observationdocument_recordeddtm"
                    batch_mct[target_column_string] = pd.to_datetime(
                        batch_mct[target_column_string], errors="coerce", utc=True
                    )
                    batch_mct.dropna(subset=[target_column_string], inplace=True)
                    batch_mct.dropna(
                        subset=["observation_valuetext_analysed"], inplace=True
                    )
                    batch_mct = batch_mct[
                        batch_mct["observation_valuetext_analysed"].apply(
                            lambda x: isinstance(x, str)
                        )
                    ]

                if self.config_obj.main_options.get("annotations", True):
                    target_column_string = "updatetime"
                    try:
                        batch_epr_docs_annotations[target_column_string] = (
                            pd.to_datetime(
                                batch_epr_docs_annotations[target_column_string],
                                errors="coerce",
                                utc=True,
                            )
                        )
                    except Exception as e:
                        print(e)
                        print(type(batch_epr_docs_annotations))
                        print(batch_epr_docs_annotations.columns)
                        print(batch_epr_docs_annotations)

                    try:
                        batch_epr_docs_annotations.dropna(
                            subset=[target_column_string], inplace=True
                        )
                    except Exception as e:
                        print(e)
                        print(type(batch_epr_docs_annotations))
                        print(batch_epr_docs_annotations.columns)
                        print(batch_epr_docs_annotations)

                if self.config_obj.main_options.get("annotations_mrc", True):
                    target_column_string = "observationdocument_recordeddtm"
                    batch_epr_docs_annotations_mct[target_column_string] = (
                        pd.to_datetime(
                            batch_epr_docs_annotations_mct[target_column_string],
                            errors="coerce",
                            utc=True,
                        )
                    )
                    batch_epr_docs_annotations_mct.dropna(
                        subset=[target_column_string], inplace=True
                    )

                if self.config_obj.main_options.get("annotations_reports", True):
                    target_column_string = "updatetime"
                    batch_reports_docs_annotations[target_column_string] = (
                        pd.to_datetime(
                            batch_reports_docs_annotations[target_column_string],
                            errors="coerce",
                            utc=True,
                        )
                    )
                    batch_reports_docs_annotations.dropna(
                        subset=[target_column_string], inplace=True
                    )

                if self.config_obj.main_options.get("annotations_reports", True):
                    target_column_string = "updatetime"
                    batch_reports[target_column_string] = pd.to_datetime(
                        batch_reports[target_column_string], errors="coerce", utc=True
                    )
                    batch_reports.dropna(subset=[target_column_string], inplace=True)

                if self.config_obj.main_options.get("textual_obs", True):
                    target_column_string = "basicobs_entered"
                    batch_textual_obs_docs[target_column_string] = pd.to_datetime(
                        batch_textual_obs_docs[target_column_string],
                        errors="coerce",
                        utc=True,
                    )
                    batch_textual_obs_docs.dropna(
                        subset=[target_column_string], inplace=True
                    )

                # batch_epr_docs_annotations_mct.dropna(subset=['observation_valuetext_analysed'], inplace=True)

                # target_column_string = 'body_analysed'
                # batch_epr[target_column_string] = pd.to_datetime(batch_epr[target_column_string], errors='coerce', utc=True)
                # batch_epr.dropna(subset=[target_column_string], inplace=True)

                # target_column_string = 'observation_valuetext_analysed'
                # batch_mct[target_column_string] = pd.to_datetime(batch_mct[target_column_string], errors='coerce', utc=True)
                # batch_mct.dropna(subset=[target_column_string], inplace=True)

                # target_column_string = 'body_analysed'
                # batch_epr_docs_annotations[target_column_string] = pd.to_datetime(batch_epr_docs_annotations[target_column_string], errors='coerce', utc=True)
                # batch_epr_docs_annotations.dropna(subset=[target_column_string], inplace=True)

                # target_column_string = 'observation_valuetext_analysed'
                # batch_epr_docs_annotations_mct[target_column_string] = pd.to_datetime(batch_epr_docs_annotations_mct[target_column_string], errors='coerce', utc=True)
                # batch_epr_docs_annotations_mct.dropna(subset=[target_column_string], inplace=True)

                if self.config_obj.verbosity > 3:
                    print("post batch timestamp na drop:")
                    print("EPR:", len(batch_epr))
                    print("MCT:", len(batch_mct))
                    print("EPR annotations:", len(batch_epr_docs_annotations))
                    print("EPR annotations mct:", len(batch_epr_docs_annotations_mct))
                    print("textual obs docs:", len(batch_textual_obs_docs))
                    print(
                        "textual obs annotations:", len(batch_textual_obs_annotations)
                    )
                    print(
                        "batch_report_docs_annotations:",
                        len(batch_reports_docs_annotations),
                    )

            for j in range(0, len(date_list)):
                try:
                    if only_check_last:
                        run_on_pat = last_check
                    else:
                        run_on_pat = all_patient_list[i] not in stripped_list

                    if run_on_pat:
                        if self.config_obj.verbosity > 5:
                            print(f"Processing date {date_list[j]} for patient {i}...")

                        if self.config_obj.calculate_vectors:
                            main_batch(
                                all_patient_list[i],
                                date_list[j],
                                batch_demo=batch_demo,
                                batch_smoking=batch_smoking,
                                batch_core_02=batch_core_02,
                                batch_bednumber=batch_bednumber,
                                batch_vte=batch_vte,
                                batch_hospsite=batch_hospsite,
                                batch_resus=batch_resus,
                                batch_news=batch_news,
                                batch_bmi=batch_bmi,
                                batch_diagnostics=batch_diagnostics,
                                batch_epr=batch_epr,
                                batch_mct=batch_mct,
                                batch_bloods=batch_bloods,
                                batch_drugs=batch_drugs,
                                batch_epr_docs_annotations=batch_epr_docs_annotations,
                                batch_epr_docs_annotations_mct=batch_epr_docs_annotations_mct,
                                batch_report_docs_annotations=batch_reports_docs_annotations,
                                batch_textual_obs_annotations=batch_textual_obs_annotations, # This was passed to main_batch
                                batch_appointments=batch_appointments,
                                config_obj=self.config_obj,
                                stripped_list_start=stripped_list_start,
                                t=self.t,
                                cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                                cat=self.cat,
                            )
                        else:
                            pass

                except Exception as e:
                    print(e)
                    print(
                        f"Exception in patmaker on {all_patient_list[i], date_list[j]}"
                    )
                    print(traceback.format_exc())
                    raise e

            if remote_dump:
                self.sftp_obj.close()
                self.config_obj.ssh_client.close()
        else:
            if self.config_obj.verbosity >= 4:
                print(f"patient {i} in stripped_list_start")

            if multi_process is False:
                skipped_counter = skipped_counter + 1
                if self.config_obj.verbosity > 0:
                    print(f"Skipped {i}")
            else:
                with skipped_counter.get_lock():
                    skipped_counter.value += 1
                if self.config_obj.verbosity > 0:
                    print(f"Skipped {i}")
        # 5. Finalize
        if self.config_obj.remote_dump:
            self.sftp_obj.close()
            self.config_obj.ssh_client.close()
