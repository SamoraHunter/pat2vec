import random
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

# from pat2vec.pat2vec_search.cogstack_search_methods import *
from tqdm import trange

from pat2vec.pat2vec_main_methods.main_batch import main_batch
from pat2vec.pat2vec_pat_list.get_patient_treatment_list import get_all_patients_list
from pat2vec.pat2vec_search.cogstack_search_methods import (
    cohort_searcher_with_terms_and_search,
    initialize_cogstack_client,
)
from pat2vec.patvec_get_batch_methods.get_prefetch_batches import prefetch_batches
from pat2vec.patvec_get_batch_methods.main import (
    get_pat_batch_appointments,
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
    get_pat_batch_textual_obs_annotations,
    get_pat_batch_textual_obs_docs,
)
from pat2vec.util import config_pat2vec
from pat2vec.util.generate_date_list import generate_date_list
from pat2vec.util.get_best_gpu import set_best_gpu
from pat2vec.util.get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
)
from pat2vec.util.methods_get import (
    create_folders_for_pat,
    filter_stripped_list,
    list_dir_wrapper,
    update_pbar,
)
from pat2vec.util.methods_get_medcat import get_cat


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
        cogstack: bool = True,
        use_filter: bool = False,
        json_filter_path: Optional[str] = None,
        random_seed_val: int = 42,
        hostname: Optional[str] = None,
        config_obj: Optional[Any] = None,
    ):
        """Initializes the main pat2vec pipeline orchestrator.

        This constructor sets up the pipeline environment, including data source
        connections, patient lists, and NLP models, based on the provided
        configuration.

        Args:
            cogstack: If True, connects to a CogStack Elasticsearch instance.
                If False, a dummy searcher is used for testing.
            use_filter: If True, applies a CUI filter to the MedCAT model.
            json_filter_path: Path to a JSON file containing the CUI filter.
            random_seed_val: The random seed for reproducibility.
            hostname: Deprecated. SFTP settings are now in the config object.
            config_obj: The main configuration object. If None, a default
                configuration is created.
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

        # Using a medcat CUI filter for annotations data.
        self.use_filter = use_filter

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

    def _get_patient_data_batches(
        self, current_pat_client_id_code: str
    ) -> Dict[str, pd.DataFrame]:
        """Fetches and organizes all data batches for a single patient.

        This method uses a configuration-driven approach to retrieve various
        types of patient data (e.g., clinical notes, lab results, demographics)
        based on the settings in `self.config_obj`. It iterates through a list
        of predefined batch configurations, calling the appropriate fetch
        function for each enabled data type.

        This design is highly extensible. To add a new data source, a developer
        only needs to:
        1.  Create a new `get_pat_batch_<new_source>` function.
        2.  Add a corresponding configuration dictionary to the `batch_configs`
            or `annotation_batch_configs` list within this method.

        Args:
            current_pat_client_id_code: The unique identifier for the patient
                for whom to fetch data.

        Returns:
            A dictionary where keys are batch names (e.g., 'batch_epr') and
            values are the corresponding pandas DataFrames. If a data source is
            disabled or returns no data, the value will be an empty DataFrame.
        """
        empty_return = pd.DataFrame()
        empty_return_epr = pd.DataFrame(columns=["updatetime", "body_analysed"])
        empty_return_mct = pd.DataFrame(
            columns=[
                "observationdocument_recordeddtm",
                "observation_valuetext_analysed",
            ]
        )
        empty_return_textual_obs = pd.DataFrame(
            columns=["basicobs_entered", "textualObs"]
        )
        empty_return_reports = pd.DataFrame(
            columns=["updatetime", "observation_valuetext_analysed"]
        )

        # Configuration for standard data batches
        batch_configs = [
            {
                "option": "annotations",
                "var": "batch_epr",
                "func": get_pat_batch_epr_docs,
                "args": {"search_term": None},
                "empty": empty_return_epr,
            },
            {
                "option": "annotations_mrc",
                "var": "batch_mct",
                "func": get_pat_batch_mct_docs,
                "args": {"search_term": None},
                "empty": empty_return_mct,
            },
            {
                "option": "textual_obs",
                "var": "batch_textual_obs_docs",
                "func": get_pat_batch_textual_obs_docs,
                "args": {"search_term": None},
                "empty": empty_return_textual_obs,
            },
            {
                "option": "annotations_reports",
                "var": "batch_reports",
                "func": get_pat_batch_reports,
                "args": {"search_term": None},
                "empty": empty_return_reports,
            },
            {
                "option": "smoking",
                "var": "batch_smoking",
                "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_SmokingStatus"},
                "empty": empty_return,
            },
            {
                "option": "core_02",
                "var": "batch_core_02",
                "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_SpO2"},
                "empty": empty_return,
            },
            {
                "option": "bed",
                "var": "batch_bednumber",
                "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_BedNumber3"},
                "empty": empty_return,
            },
            {
                "option": "vte_status",
                "var": "batch_vte",
                "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_VTE_STATUS"},
                "empty": empty_return,
            },
            {
                "option": "hosp_site",
                "var": "batch_hospsite",
                "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_HospitalSite"},
                "empty": empty_return,
            },
            {
                "option": "core_resus",
                "var": "batch_resus",
                "func": get_pat_batch_obs,
                "args": {"search_term": "CORE_RESUS_STATUS"},
                "empty": empty_return,
            },
            {
                "option": "news",
                "var": "batch_news",
                "func": get_pat_batch_news,
                "args": {"search_term": None},
                "empty": empty_return,
            },
            {
                "option": "bmi",
                "var": "batch_bmi",
                "func": get_pat_batch_bmi,
                "args": {"search_term": None},
                "empty": empty_return,
            },
            {
                "option": "diagnostics",
                "var": "batch_diagnostics",
                "func": get_pat_batch_diagnostics,
                "args": {"search_term": None},
                "empty": empty_return,
            },
            {
                "option": "drugs",
                "var": "batch_drugs",
                "func": get_pat_batch_drugs,
                "args": {"search_term": None},
                "empty": empty_return,
            },
            {
                "option": "demo",
                "var": "batch_demo",
                "func": get_pat_batch_demo,
                "args": {"search_term": None},
                "empty": empty_return,
            },
            {
                "option": "bloods",
                "var": "batch_bloods",
                "func": get_pat_batch_bloods,
                "args": {"search_term": None},
                "empty": empty_return,
            },
            {
                "option": "appointments",
                "var": "batch_appointments",
                "func": get_pat_batch_appointments,
                "args": {"search_term": None},
                "empty": empty_return,
            },
        ]

        # Configuration for annotation batches
        annotation_batch_configs = [
            {
                "option": "annotations",
                "var": "batch_epr_docs_annotations",
                "func": get_pat_batch_epr_docs_annotations,
                "empty": empty_return_epr,
            },
            {
                "option": "annotations_mrc",
                "var": "batch_epr_docs_annotations_mct",
                "func": get_pat_batch_mct_docs_annotations,
                "empty": empty_return_mct,
            },
            {
                "option": "textual_obs",
                "var": "batch_textual_obs_annotations",
                "func": get_pat_batch_textual_obs_annotations,
                "empty": empty_return_textual_obs,
            },
            {
                "option": "annotations_reports",
                "var": "batch_reports_docs_annotations",
                "func": get_pat_batch_reports_docs_annotations,
                "empty": empty_return_reports,
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
                    **config["args"],
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

    def _setup_patient_time_window(
        self, current_pat_client_id_code: str
    ) -> Optional[List[tuple]]:
        """Sets up and returns the date list for a patient, handling IPW logic.

        If `individual_patient_window` is enabled, this method calculates a
        patient-specific date list. Otherwise, it returns the global date list.

        Args:
            current_pat_client_id_code: The patient's unique identifier.

        Returns:
            A list of date tuples, or None if the time window cannot be set up.
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
                    print(
                        f"Control pat full {current_pat_client_id_code} ipw dates set:"
                    )
                    print("Start Date:", current_pat_start_date)
                    print("End Date:", current_pat_end_date)

            elif self.config_obj.individual_patient_window_controls_method == "random":
                # Select a random treatment's time window for application.
                patient_ids = list(self.config_obj.patient_dict.keys())
                if not patient_ids:
                    print(
                        "Warning: Cannot use 'random' control method with an empty patient_dict. Skipping."
                    )
                    return None
                random_pat_id = random.choice(patient_ids)
                pat_dates = self.config_obj.patient_dict.get(random_pat_id)
                current_pat_start_date, current_pat_end_date = pat_dates
            else:
                print(
                    f"Unknown control method: {self.config_obj.individual_patient_window_controls_method}"
                )
                return None
        else:  # It's a treatment patient
            if len(pat_dates) != 2:
                print(
                    f"Warning: Invalid dates for patient {current_pat_client_id_code}. Skipping."
                )
                return None
            current_pat_start_date, current_pat_end_date = pat_dates

        # Safeguard against invalid date types
        if (
            pd.isna(current_pat_start_date)
            or pd.isna(current_pat_end_date)
            or not isinstance(current_pat_start_date, datetime)
            or not isinstance(current_pat_end_date, datetime)
        ):
            print(
                f"Warning: Dates for patient {current_pat_client_id_code} are invalid. Skipping."
            )
            return None

        # Determine anchor date for generation and clamping boundaries
        p_real_start, p_real_end = min(
            current_pat_start_date, current_pat_end_date
        ), max(current_pat_start_date, current_pat_end_date)
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

    def _clean_document_batches(
        self, batches: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """Cleans timestamp columns for all document-related batches.

        Args:
            batches: A dictionary of DataFrames, keyed by batch name.

        Returns:
            The dictionary of DataFrames with cleaned timestamp columns.
        """
        doc_configs = [
            {
                "key": "batch_epr",
                "time_col": "updatetime",
                "text_col": "body_analysed",
                "option": "annotations",
            },
            {
                "key": "batch_mct",
                "time_col": "observationdocument_recordeddtm",
                "text_col": "observation_valuetext_analysed",
                "option": "annotations_mrc",
            },
            {
                "key": "batch_reports",
                "time_col": "updatetime",
                "text_col": None,
                "option": "annotations_reports",
            },
            {
                "key": "batch_textual_obs_docs",
                "time_col": "basicobs_entered",
                "text_col": None,
                "option": "textual_obs",
            },
            {
                "key": "batch_epr_docs_annotations",
                "time_col": "updatetime",
                "text_col": None,
                "option": "annotations",
            },
            {
                "key": "batch_epr_docs_annotations_mct",
                "time_col": "observationdocument_recordeddtm",
                "text_col": None,
                "option": "annotations_mrc",
            },
            {
                "key": "batch_reports_docs_annotations",
                "time_col": "updatetime",
                "text_col": None,
                "option": "annotations_reports",
            },
        ]

        for config in doc_configs:
            if self.config_obj.main_options.get(config["option"], True):
                batch = batches.get(config["key"])
                if batch is not None and not batch.empty:
                    time_col = config["time_col"]
                    text_col = config["text_col"]

                    try:
                        batch[time_col] = pd.to_datetime(
                            batch[time_col], errors="coerce", utc=True
                        )
                        batch.dropna(subset=[time_col], inplace=True)

                        if text_col:
                            batch.dropna(subset=[text_col], inplace=True)
                            batch = batch[
                                batch[text_col].apply(lambda x: isinstance(x, str))
                            ]

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
            print(
                "EPR annotations mct:", len(batches["batch_epr_docs_annotations_mct"])
            )
            print("textual obs docs:", len(batches["batch_textual_obs_docs"]))
            print(
                "textual obs annotations:",
                len(batches["batch_textual_obs_annotations"]),
            )
            print(
                "batch_report_docs_annotations:",
                len(batches["batch_reports_docs_annotations"]),
            )

        return batches

    def _process_patient_slices(
        self,
        current_pat_client_id_code: str,
        date_list: List[tuple],
        batches: Dict[str, pd.DataFrame],
    ) -> None:
        """Iterates through time slices and calls main_batch to generate feature vectors.

        Args:
            current_pat_client_id_code: The patient's unique identifier.
            date_list: The list of date tuples representing time slices.
            batches: A dictionary of pre-fetched data batches for the patient.
        """
        # The main pat_maker function already checks if the patient is in stripped_list_start.
        # This check is a safeguard, but the main logic for skipping is at a higher level.
        if current_pat_client_id_code in self.stripped_list_start:
            if self.config_obj.verbosity > 3:
                print(
                    f"Patient {current_pat_client_id_code} already processed, skipping slice processing."
                )
            return

        # The only_check_last logic from the original function is implicitly handled by this loop.
        for date_slice in date_list:
            try:
                if self.config_obj.verbosity > 5:
                    print(
                        f"Processing date {date_slice} for patient {current_pat_client_id_code}..."
                    )

                if self.config_obj.calculate_vectors:
                    main_batch(
                        current_pat_client_id_code,
                        date_slice,
                        batches=batches,
                        config_obj=self.config_obj,
                        stripped_list_start=self.stripped_list_start,
                        t=self.t,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                        cat=self.cat,
                    )

            except Exception as e:
                print(e)
                print(
                    f"Exception in patmaker on {current_pat_client_id_code, date_slice}"
                )
                print(traceback.format_exc())
                raise e

    def pat_maker(self, i: int) -> None:
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

        current_pat_client_id_code = str(self.all_patient_list[i])

        # Check if patient has already been processed
        if current_pat_client_id_code in self.stripped_list_start:
            if self.config_obj.verbosity >= 4:
                print(f"patient {i} in stripped_list_start")
            if self.config_obj.multi_process is False:
                self.config_obj.skipped_counter += 1
            else:
                with self.config_obj.skipped_counter.get_lock():
                    self.config_obj.skipped_counter.value += 1
            if self.config_obj.verbosity > 0:
                print(f"Skipped {i}")
            return

        create_folders_for_pat(current_pat_client_id_code, self.config_obj)
        start_time = time.time()

        # 1. Set up time window for the patient
        date_list = self._setup_patient_time_window(current_pat_client_id_code)
        if date_list is None:
            return  # Skip patient if time window setup fails

        # 2. Update progress and fetch data batches
        update_pbar(
            current_pat_client_id_code,
            start_time,
            0,
            f"Pat_maker called on {i}...",
            self.t,
            self.config_obj,
            self.config_obj.skipped_counter,
        )
        batches = self._get_patient_data_batches(current_pat_client_id_code)

        update_pbar(
            current_pat_client_id_code,
            start_time,
            0,
            f"Done batches in {time.time()-start_time}",
            self.t,
            self.config_obj,
            self.config_obj.skipped_counter,
        )

        # 3. Clean document batches if required
        if self.config_obj.dropna_doc_timestamps:
            batches = self._clean_document_batches(batches)

        # 4. Process patient data in time slices
        self._process_patient_slices(current_pat_client_id_code, date_list, batches)

        # 5. Finalize
        if self.config_obj.remote_dump:
            self.sftp_obj.close()
            self.config_obj.ssh_client.close()
