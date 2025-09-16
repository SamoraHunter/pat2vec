import math
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Type, TypeVar, Union
import pandas as pd
import paramiko
from dateutil.relativedelta import relativedelta
from IPython.display import display
from pat2vec.util.calculate_interval import calculate_interval
from pat2vec.util.current_pat_batch_path_methods import PathsClass
from pat2vec.util.generate_date_list import generate_date_list
from pat2vec.util.methods_get import (
    add_offset_column,
    build_patient_dict,
)


T_config = TypeVar("T_config", bound="config_class")


# NOTE: This class is very large. Consider refactoring into smaller, more focused
# configuration objects (e.g., `PathConfig`, `TimeWindowConfig`, `FeatureConfig`).
class config_class:
    """Initializes the configuration object for the pat2vec pipeline."""

    def __init__(
        self,
        remote_dump: bool = False,
        suffix: str = "",
        treatment_doc_filename: str = "treatment_docs.csv",
        treatment_control_ratio_n: int = 1,
        proj_name: str = "new_project",
        current_path_dir: str = ".",
        main_options: Optional[Dict[str, bool]] = None,
        start_date: datetime = datetime(1995, 1, 1),
        years: int = 0,
        months: int = 0,
        days: int = 1,
        batch_mode: bool = True,
        store_annot: bool = False,
        share_sftp: bool = True,
        multi_process: bool = False,
        strip_list: bool = True,
        verbosity: int = 3,
        random_seed_val: int = 42,
        hostname: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        gpu_mem_threshold: int = 4000,
        testing: bool = False,
        dummy_medcat_model: bool = False,
        use_controls: bool = False,
        medcat: bool = False,
        global_start_year: Optional[Union[int, str]] = None,
        global_start_month: Optional[Union[int, str]] = None,
        global_end_year: Optional[Union[int, str]] = None,
        global_end_month: Optional[Union[int, str]] = None,
        global_start_day: Optional[Union[int, str]] = None,
        global_end_day: Optional[Union[int, str]] = None,
        skip_additional_listdir: bool = False,
        start_time: Optional[datetime] = None,
        root_path: Optional[str] = None,
        negate_biochem: bool = False,
        patient_id_column_name: str = "client_idcode",
        overwrite_stored_pat_docs: bool = False,
        overwrite_stored_pat_observations: bool = False,
        store_pat_batch_docs: bool = True,
        store_pat_batch_observations: bool = True,
        annot_filter_options: Optional[Dict[str, Any]] = None,
        shuffle_pat_list: bool = False,
        individual_patient_window: bool = False,
        individual_patient_window_df: Optional[pd.DataFrame] = None,
        individual_patient_window_start_column_name: Optional[str] = None,
        individual_patient_id_column_name: Optional[str] = None,
        individual_patient_window_controls_method: str = "full",  # full, random
        dropna_doc_timestamps: bool = True,
        time_window_interval_delta: relativedelta = relativedelta(days=1),
        feature_engineering_arg_dict: Optional[Dict[str, Any]] = None,
        split_clinical_notes: bool = True,
        lookback: bool = True,
        add_icd10: bool = False,
        add_opc4s: bool = False,
        all_epr_patient_list_path: str = "/home/samorah/_data/gloabl_files/all_client_idcodes_epr_unique.csv",
        override_medcat_model_path: Optional[str] = None,
        data_type_filter_dict: Optional[Dict[str, Any]] = None,
        filter_split_notes: bool = True,
        client_idcode_term_name: str = "client_idcode.keyword",
        sanitize_pat_list: bool = True,
        calculate_vectors: bool = True,
        prefetch_pat_batches: bool = False,
        sample_treatment_docs: int = 0,
        test_data_path: Optional[str] = None,
        credentials_path: str = "../../credentials.py",
    ) -> None:
        """Initializes the configuration object for the pat2vec pipeline.

        This class holds all configuration parameters for a pat2vec run, including
        file paths, time window settings, feature selection, and operational flags.

        Args:
            remote_dump: If `True`, data will be dumped to a remote server via SFTP.
            suffix: A suffix to append to output folder names.
            treatment_doc_filename: The filename for the input document containing the
                primary cohort list.
            treatment_control_ratio_n: The ratio of treatment to control patients.
            proj_name: The name of the current project, used for creating
                project-specific folders where patient data batches and vectors are stored.
            current_path_dir: The current working directory.
            main_options: A dictionary of boolean flags to enable or disable specific
                feature extractions (e.g., 'demo', 'bloods', 'annotations').
                If `None`, a default dictionary is used.
            start_date: The anchor date for generating time windows. For global windows,
                this is the start. For individual windows, this is overridden per patient.
            years: The number of years in the time window duration.
            months: Number of months to add to the `start_date`.
            days: The number of days in the time window duration.
            batch_mode: Flag for batch processing mode. This is currently the only
                functioning mode.
            store_annot: Flag to store annotations. Partially deprecated.
            share_sftp: Flag for sharing SFTP connection. Partially deprecated.
            multi_process: Flag for multi-process execution. Deprecated.
            strip_list: If `True`, this will check for completed patients before
                starting to avoid redundancy.
            verbosity: Verbosity level for logging (0-9).
            random_seed_val: Random seed for reproducibility.
            hostname: Hostname for SFTP connection.
            username: Username for SFTP connection.
            password: Password for SFTP connection.
            gpu_mem_threshold: GPU memory threshold in MB for MedCAT.
            testing: If `True`, enables testing mode, which may use dummy data generators.
            dummy_medcat_model: If `True` and in testing mode, simulates a MedCAT model.
            use_controls: If `True`, this will add the desired ratio of controls at
                random from the global pool, requiring configuration with a master list
                of patients.
            medcat: Flag for MedCAT processing. If `True`, MedCAT will load into memory
                and be used for annotating.
            global_start_year: Global start year for the overall data extraction window.
            global_start_month: Global start month.
            global_start_day: Global start day.
            global_end_year: Global end year.
            global_end_month: Global end month.
            global_end_day: Global end day.
            skip_additional_listdir: If `True`, skips some `listdir` calls for performance.
            start_time: Start time for logging. Defaults to `datetime.now()`.
            root_path: The root directory for the project. If `None`, defaults to the
                current working directory.
            negate_biochem: Flag for negating biochemistry features.
            patient_id_column_name: Column name for patient IDs in input files.
            overwrite_stored_pat_docs: If `True`, overwrites existing stored patient documents.
            overwrite_stored_pat_observations: If `True`, overwrites existing stored
                patient observations.
            store_pat_batch_docs: If `True`, stores patient document batches.
            store_pat_batch_observations: If `True`, stores patient observation batches.
            annot_filter_options: Dictionary for filtering MedCAT annotations.
            shuffle_pat_list: Flag for shuffling the patient list.
            individual_patient_window: If `True`, uses patient-specific time windows
                defined in `individual_patient_window_df`.
            individual_patient_window_df: DataFrame with patient IDs and their individual
                start dates. Required if `individual_patient_window` is `True`.
            individual_patient_window_start_column_name: The column name for start dates
                in `individual_patient_window_df`.
            individual_patient_id_column_name: The column name for patient IDs in
                `individual_patient_window_df`.
            individual_patient_window_controls_method: Method for handling control
                patients in IPW mode ('full' or 'random').
            dropna_doc_timestamps: If `True`, drops documents with missing timestamps.
            time_window_interval_delta: The step/interval for each time slice vector.
            feature_engineering_arg_dict: Dictionary of arguments for feature engineering.
            split_clinical_notes: If `True`, clinical notes will be split by date and
                treated as individual documents with extracted dates. Requires a note
                splitter module.
            lookback: If `True`, the time window is calculated backward from the start
                date. If `False`, it's calculated forward.
            add_icd10: If `True`, appends ICD-10 codes to annotation batches.
            add_opc4s: Requires `add_icd10` to be `True`. If `True`, appends OPC4S codes
                to annotation batches.
            all_epr_patient_list_path: Path to a file containing all patient IDs, used
                for sampling controls.
            override_medcat_model_path: Path to a MedCAT model pack to override the default.
            data_type_filter_dict: Dictionary for data type filtering.
            filter_split_notes: If enabled (`True`), the global time window filter will be
                reapplied after clinical note splitting.
            client_idcode_term_name: The Elasticsearch field name for patient ID searches.
            sanitize_pat_list: If `True`, sanitizes the patient list (e.g., to uppercase).
            calculate_vectors: If `True`, calculates feature vectors. If `False`, only
                extracts batches.
            prefetch_pat_batches: If `True`, fetches all raw data for all patients before
                processing. May use significant memory.
            sample_treatment_docs: Number of patients to sample from the initial cohort
                list. `0` means no sampling.
            test_data_path: The path to the test data file, used when `testing` is `True`.
            credentials_path: Path to the credentials file.
        """

        if prefetch_pat_batches and individual_patient_window:
            print(
                "Warning: 'prefetch_pat_batches' is not compatible with 'individual_patient_window'."
            )
            print(
                "The prefetch mechanism uses a global time window, while IPW requires patient-specific windows."
            )
            print(
                "Disabling 'prefetch_pat_batches' to ensure correct time windows are used for each patient."
            )
            prefetch_pat_batches = False

        #: If `True`, fetches all raw data for all patients before processing. May use significant memory.
        self.prefetch_pat_batches = prefetch_pat_batches

        #: If `True`, calculates feature vectors. If `False`, only extracts batches.
        self.calculate_vectors = calculate_vectors  # Calculate vectors for each patient else just extract batches

        #: Validate IPW configuration early to prevent TypeErrors
        if individual_patient_window and individual_patient_window_df is None:
            raise ValueError(
                "individual_patient_window_df must be provided when individual_patient_window is True."
            )


        self.sanitize_pat_list = (
            sanitize_pat_list
        )
        #: If `True`, skips some `listdir` calls for performance.
        self.skip_additional_listdir = skip_additional_listdir

        #: If enabled (`True`), the global time window filter will be reapplied after clinical note splitting.
        self.filter_split_notes = filter_split_notes

        #: The path to the test data file, used when `testing` is `True`.
        self.test_data_path = test_data_path

        #: Path to the credentials file.
        self.credentials_path = credentials_path
        print("credentials_path:" , credentials_path)

        #: A suffix to append to output folder names.
        self.suffix = suffix
        #: The filename for the input document containing the primary cohort list.
        self.treatment_doc_filename = treatment_doc_filename
        #: The ratio of treatment to control patients.
        self.treatment_control_ratio_n = treatment_control_ratio_n

        #: Path to the pre-annotation parts directory.
        self.pre_annotation_path = f"current_pat_annots_parts{self.suffix}/"
        #: Path to the MRC pre-annotation parts directory.
        self.pre_annotation_path_mrc = f"current_pat_annots_mrc_parts{self.suffix}/"

        #: Path to the document annotation batches directory.
        self.pre_document_annotation_batch_path = (
            f"current_pat_documents_annotations_batches{self.suffix}/"
        )
        #: Path to the MCT document annotation batches directory.
        self.pre_document_annotation_batch_path_mct = (
            f"current_pat_documents_annotations_batches_mct{self.suffix}/"
        )
        #: Path to the report annotation batches directory.
        self.pre_report_annotation_batch_path_report = (
            f"current_pat_documents_annotations_batches_report{self.suffix}/"
        )

        #: Path to the document batches directory.
        self.pre_document_batch_path = f"current_pat_document_batches{self.suffix}/"
        #: Path to the MCT document batches directory.
        self.pre_document_batch_path_mct = (
            f"current_pat_document_batches_mct{self.suffix}/"
        )

        #: Path to the textual observation document batches directory.
        self.pre_textual_obs_document_batch_path = (
            f"current_pat_textual_obs_document_batches{self.suffix}/"
        )

        #: Path to the textual observation annotation batches directory.
        self.pre_textual_obs_annotation_batch_path = (
            f"current_pat_textual_obs_annotation_batches{self.suffix}/"
        )

        #: Path to the report batches directory.
        self.pre_report_batch_path = f"current_pat_report_batches{self.suffix}/"

        #: Path to the bloods batches directory.
        self.pre_bloods_batch_path = f"current_pat_bloods_batches{self.suffix}/"

        #: Path to the drugs batches directory.
        self.pre_drugs_batch_path = f"current_pat_drugs_batches{self.suffix}/"

        #: Path to the diagnostics batches directory.
        self.pre_diagnostics_batch_path = (
            f"current_pat_diagnostics_batches{self.suffix}/"
        )

        #: Path to the NEWS batches directory.
        self.pre_news_batch_path = f"current_pat_news_batches{self.suffix}/"

        #: Path to the observations batches directory.
        self.pre_obs_batch_path = f"current_pat_obs_batches{self.suffix}/"

        #: Path to the BMI batches directory.
        self.pre_bmi_batch_path = f"current_pat_bmi_batches{self.suffix}/"

        #: Path to the demographics batches directory.
        self.pre_demo_batch_path = f"current_pat_demo_batches{self.suffix}/"

        #: Path to the miscellaneous batches directory.
        self.pre_misc_batch_path = f"current_pat_misc_batches{self.suffix}/"

        #: Path to the patient line directory.
        self.current_pat_line_path = f"current_pat_line_path{self.suffix}/"

        #: Path to the appointments batches directory.
        self.pre_appointments_batch_path = (
            f"current_pat_appointments_batches{self.suffix}/"
        )

        #: If `True`, stores patient document batches.
        self.store_pat_batch_docs = store_pat_batch_docs

        #: If `True`, stores patient observation batches.
        self.store_pat_batch_observations = store_pat_batch_observations

        #: The name of the current project, used for creating project-specific folders.
        self.proj_name = proj_name
        #: A dictionary of boolean flags to enable or disable specific feature extractions.
        self.main_options = main_options

        #: Flag for negating biochemistry features.
        self.negate_biochem = negate_biochem
        #: Column name for patient IDs in input files.
        self.patient_id_column_name = patient_id_column_name

        #: If `True`, appends ICD-10 codes to annotation batches.
        self.add_icd10 = add_icd10
        #: If `True`, appends OPC4S codes to annotation batches. Requires `add_icd10` to be `True`.
        self.add_opc4s = add_opc4s

        #: Dictionary for data type filtering.
        self.data_type_filter_dict = data_type_filter_dict

        #: Flag for batch processing mode.
        self.batch_mode = batch_mode
        #: If `True`, data will be dumped to a remote server via SFTP.
        self.remote_dump = remote_dump

        #: Flag to store annotations. Partially deprecated.
        self.store_annot = store_annot
        #: Flag for sharing SFTP connection. Partially deprecated.
        self.share_sftp = share_sftp
        #: Flag for multi-process execution. Deprecated.
        self.multi_process = multi_process
        #: If `True`, checks for completed patients before starting to avoid redundancy.
        self.strip_list = strip_list
        #: Verbosity level for logging (0-9).
        self.verbosity = verbosity
        #: Random seed for reproducibility.
        self.random_seed_val = random_seed_val

        #: Hostname for SFTP connection.
        self.hostname = hostname
        #: Username for SFTP connection.
        self.username = username
        #: Password for SFTP connection.
        self.password = password

        #: GPU memory threshold in MB for MedCAT.
        self.gpu_mem_threshold = gpu_mem_threshold

        #: If `True`, enables testing mode, which may use dummy data generators.
        self.testing = testing
        #: If `True`, adds the desired ratio of controls at random from the global pool.
        self.use_controls = use_controls

        #: Counter for skipped items.
        self.skipped_counter = 0  # init start

        #: Flag for MedCAT processing. If `True`, MedCAT will be used for annotating.
        self.medcat = medcat

        #: The root directory for the project.
        self.root_path = root_path

        #: If `True`, overwrites existing stored patient documents.
        self.overwrite_stored_pat_docs = overwrite_stored_pat_docs

        #: If `True`, overwrites existing stored patient observations.
        self.overwrite_stored_pat_observations = overwrite_stored_pat_observations

        #: Dictionary for filtering MedCAT annotations.
        self.annot_filter_options = annot_filter_options

        #: Start time for logging.
        self.start_time = start_time

        #: Flag for shuffling the patient list.
        self.shuffle_pat_list = shuffle_pat_list

        #: If `True`, uses patient-specific time windows.
        self.individual_patient_window = individual_patient_window

        #: DataFrame with patient IDs and their individual start dates.
        self.individual_patient_window_df = individual_patient_window_df

        #: The column name for start dates in `individual_patient_window_df`.
        self.individual_patient_window_start_column_name = (
            individual_patient_window_start_column_name
        )

        #: The column name for patient IDs in `individual_patient_window_df`.
        self.individual_patient_id_column_name = individual_patient_id_column_name

        #: Method for handling control patients in IPW mode ('full' or 'random').
        self.individual_patient_window_controls_method = (
            individual_patient_window_controls_method
        )

        #: Path to the control list pickle file.
        self.control_list_path = "control_path.pkl"

        #: If `True`, drops documents with missing timestamps.
        self.dropna_doc_timestamps = dropna_doc_timestamps

        #: The step/interval for each time slice vector.
        self.time_window_interval_delta = time_window_interval_delta

        #: If `True`, clinical notes will be split by date.
        self.split_clinical_notes = split_clinical_notes

        #: Path to a file containing all patient IDs, used for sampling controls.
        self.all_epr_patient_list_path = all_epr_patient_list_path

        #: If `True`, the time window is calculated backward from the start date.
        self.lookback = lookback

        #: Path to a MedCAT model pack to override the default.
        self.override_medcat_model_path = override_medcat_model_path

        if dummy_medcat_model == None:
            self.dummy_medcat_model = True
        else:
            self.dummy_medcat_model = dummy_medcat_model

        if start_time == None:
            self.start_time = datetime.now()

        #: The time field to use for drug orders.
        self.drug_time_field = "order_createdwhen"  # order_createdwhen: none missing, #"order_performeddtm" order performed empty for medication  # alt #order_createdwhen

        #: The time field to use for diagnostic orders.
        self.diagnostic_time_field = "order_createdwhen"  # order_createdwhen: none missing, #"order_performeddtm" order performed empty for medication  # alt #order_createdwhen

        #: The time field to use for appointments.
        self.appointments_time_field = "AppointmentDateTime"  # alt #DateModified

        #: The time field to use for bloods.
        self.bloods_time_field = "basicobs_entered"

        if client_idcode_term_name is None:
            self.client_idcode_term_name = "client_idcode.keyword"  # alt client_idcode.keyword #warn excludes on case basis.
        else:
            #: The Elasticsearch field name for patient ID searches.
            self.client_idcode_term_name = client_idcode_term_name

        #: If `True` and in testing mode, simulates a MedCAT model.
        if self.client_idcode_term_name == "client_idcode.keyword":
            print("Warning keyword not case inclusive.")

        if self.verbosity >= 1:
            print("self.drug_time_field", self.drug_time_field)

            print("self.diagnostic_time_field", self.diagnostic_time_field)

            print("self.appointments_time_field", self.appointments_time_field)

            print("self.bloods_time_field", self.bloods_time_field)

        if self.main_options == None:
            if self.verbosity >= 1:
                print("default main_options set!")

            self.main_options = {
                "demo": True,
                "bmi": False,
                "bloods": False,
                "drugs": False,
                "diagnostics": False,
                "core_02": False,
                "bed": False,
                "vte_status": False,
                "hosp_site": False,
                "core_resus": False,
                "news": False,
                "annotations": False,
                "annotations_mrc": False,
                "negated_presence_annotations": False,
                "appointments": False,
                "annotations_reports": False,
                "textual_obs": False,
            }
            if self.verbosity >= 1:
                print(self.main_options)

        if self.annot_filter_options == None:
            self.filter_arguments = {
                "Confidence": 0.8,
                "Accuracy": 0.8,
                "types": [
                    "qualifier value",
                    "procedure",
                    "substance",
                    "finding",
                    "environment",
                    "disorder",
                    "observable entity",
                ],
                # Specify the values you want to include in a list
                "Time_Value": ["Recent", "Past"],
                "Time_Confidence": 0.8,  # Specify the confidence threshold as a float
                # Specify the values you want to include in a list
                "Presence_Value": ["True"],
                "Presence_Confidence": 0.8,  # Specify the confidence threshold as a float
                # Specify the values you want to include in a list
                "Subject_Value": ["Patient"],
                "Subject_Confidence": 0.8,  # Specify the confidence threshold as a float
            }

        if feature_engineering_arg_dict == None:
            #: Dictionary of arguments for feature engineering.
            self.feature_engineering_arg_dict = {
                "drugs": {
                    "_num-drug-order": True,
                    "_days-since-last-drug-order": True,
                    "_days-between-first-last-drug": True,
                }
            }
        else:
            self.feature_engineering_arg_dict = feature_engineering_arg_dict

        #: Flag for handling negated presence annotations.
        self.negated_presence_annotations = self.main_options.get(
            "negated_presence_annotations"
        )

        if remote_dump == False:

            if self.root_path == None:
                self.root_path = f"{os.getcwd()}/{self.proj_name}/"

            #: Path to the pre-annotation parts directory.
            self.pre_annotation_path = os.path.join(
                self.root_path, f"current_pat_annots_parts{self.suffix}/"
            )
            self.pre_annotation_path_mrc = os.path.join(
                self.root_path, f"current_pat_annots_mrc_parts{self.suffix}/"
            )

            #: Path to the document annotation batches directory.
            self.pre_document_annotation_batch_path = os.path.join(
                self.root_path,
                f"current_pat_documents_annotations_batches{self.suffix}/",
            )

            self.pre_document_annotation_batch_path_mct = os.path.join(
                self.root_path,
                f"current_pat_documents_annotations_batches_mct{self.suffix}/",
            )

            #: Path to the textual observation annotation batches directory.
            self.pre_textual_obs_annotation_batch_path = os.path.join(
                self.root_path,
                f"current_pat_textual_obs_annotation_batches{self.suffix}/",
            )

            self.pre_textual_obs_document_batch_path = os.path.join(
                self.root_path,
                f"current_pat_textual_obs_document_batches{self.suffix}/",
            )

            #: Path to the document batches directory.
            self.pre_document_batch_path = os.path.join(
                self.root_path, f"current_pat_document_batches{self.suffix}/"
            )
            self.pre_document_batch_path_mct = os.path.join(
                self.root_path, f"current_pat_document_batches_mct{self.suffix}/"
            )

            #: Path to the report document batches directory.
            self.pre_document_batch_path_reports = os.path.join(
                self.root_path, f"current_pat_document_batches_reports{self.suffix}/"
            )

            self.pre_document_annotation_batch_path_reports = os.path.join(
                self.root_path,
                f"current_pat_documents_annotations_batches_reports{self.suffix}/",
            )

            #: Path to the bloods batches directory.
            self.pre_bloods_batch_path = os.path.join(
                self.root_path, f"current_pat_bloods_batches{self.suffix}/"
            )

            self.pre_drugs_batch_path = os.path.join(
                self.root_path, f"current_pat_drugs_batches{self.suffix}/"
            )

            #: Path to the diagnostics batches directory.
            self.pre_diagnostics_batch_path = os.path.join(
                self.root_path, f"current_pat_diagnostics_batches{self.suffix}/"
            )

            self.pre_news_batch_path = os.path.join(
                self.root_path, f"current_pat_news_batches{self.suffix}/"
            )

            #: Path to the observations batches directory.
            self.pre_obs_batch_path = os.path.join(
                self.root_path, f"current_pat_obs_batches{self.suffix}/"
            )

            self.pre_bmi_batch_path = os.path.join(
                self.root_path, f"current_pat_bmi_batches{self.suffix}/"
            )

            #: Path to the demographics batches directory.
            self.pre_demo_batch_path = os.path.join(
                self.root_path, f"current_pat_demo_batches{self.suffix}/"
            )

            self.pre_misc_batch_path = os.path.join(
                self.root_path, f"current_pat_misc_batches{self.suffix}/"
            )

            #: Path to the patient lines parts directory.
            self.current_pat_lines_path = os.path.join(
                self.root_path, f"current_pat_lines_parts{self.suffix}/"
            )

            self.pre_appointments_batch_path = os.path.join(
                self.root_path, f"current_pat_appointments_batches{self.suffix}/"
            )

            #: Path to the merged input batches directory.
            self.pre_merged_input_batches_path = os.path.join(
                self.root_path, f"merged_input_pat_batches{self.suffix}/"
            )

            #: The name of the output folder.
            self.output_folder = "outputs"

            #: An instance of the PathsClass for managing directory paths.
            self.PathsClass_instance = PathsClass(
                self.root_path, self.suffix, self.output_folder
            )

        print(f"Setting start_date to: {start_date}")
        #: The anchor date for generating time windows.
        self.start_date = start_date

        print(f"Setting years to: {years}")
        #: The number of years in the time window duration.
        self.years = years

        print(f"Setting months to: {months}")
        #: The number of months in the time window duration.
        self.months = months

        print(f"Setting days to: {days}")
        #: The number of days in the time window duration.
        self.days = days

        #: The total time delta for the window.
        self.time_delta = relativedelta(days=days, weeks=0, months=months, years=years)

        num = calculate_interval(
            start_date=start_date,
            total_delta=self.time_delta,
            interval_delta=time_window_interval_delta,
        )

        print(
            f"Number of {time_window_interval_delta} intervals in {self.time_delta}: {num}"
        )
        print("Expected time interval vectors per patient:", num)

        print(
            "Time interval vectors will span the following dates: ",
            start_date,
            start_date + self.time_delta,
        )

        #: Threshold for low slow execution warning.
        self.slow_execution_threshold_low = timedelta(seconds=10)
        #: Threshold for high slow execution warning.
        self.slow_execution_threshold_high = timedelta(seconds=30)
        #: Threshold for extreme slow execution warning.
        self.slow_execution_threshold_extreme = timedelta(seconds=60)

        #: Number of patients to sample from the initial cohort list. `0` means no sampling.
        self.sample_treatment_docs = sample_treatment_docs

        priority_list_bool: bool = False

        if priority_list_bool:
            # add logic to prioritise pats from list.

            df_old_done = pd.read_csv(
                "..current_pat_lines__part_0_merged.csv",
                usecols=[
                    "client_idcode",
                    "Hemochromatosis (disorder)_count_subject_present",
                ],
            )

            priority_list = df_old_done[
                df_old_done["Hemochromatosis (disorder)_count_subject_present"] > 0
            ]["client_idcode"].to_list()

            all_patient_list = priority_list  # + all_patient_list

        if self.testing:
            print("Setting test options")

            # If in testing mode and no specific test data path is provided,
            # set it to the default path. This allows overriding it if needed.
            if self.test_data_path is None:
                self.test_data_path = "test_files/treatment_docs.csv"
                print(f"Defaulting test_data_path to: {self.test_data_path}")

            print("Updating main options with implemented test options")
            # Enforce implemented testing options
            self._update_main_options()

        if self.remote_dump == False:
            self.sftp_obj = None

        if self.remote_dump:
            #: SFTP client object.

            if self.root_path == None:

                self.root_path = f"../{self.proj_name}/"
                print(f"sftp root_path: {self.root_path}")

            else:
                print(f"sftp root_path: {self.root_path}")

            # Set the hostname, username, and password for the remote machine

            hostname = self.hostname

            username = self.username
            password = self.password

            #: SSH client for remote connections.
            # Create an SSH client and connect to the remote machine
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if not all([self.hostname, self.username, self.password]):
                raise ValueError(
                    "Hostname, username, and password must be provided for remote dump."
                )

            self.ssh_client.connect(
                hostname=self.hostname, username=self.username, password=self.password
            )

            #: SFTP client for remote file operations.
            self.sftp_client = self.ssh_client.open_sftp()

            if self.remote_dump:
                try:
                    # Test if remote_path exists
                    self.sftp_client.chdir(self.root_path)
                except IOError:
                    # Create remote_path
                    self.sftp_client.mkdir(self.root_path)

            #: Path to the pre-annotation parts directory on the remote server.
            self.pre_annotation_path = f"{self.root_path}{self.pre_annotation_path}"
            #: Path to the MRC pre-annotation parts directory on the remote server.
            self.pre_annotation_path_mrc = (
                f"{self.root_path}{self.pre_annotation_path_mrc}"
            )
            #: Path to the patient line directory on the remote server.
            self.current_pat_line_path = f"{self.root_path}{self.current_pat_line_path}"
            self.current_pat_lines_path = self.current_pat_line_path

            if self.remote_dump == False:
                # Path(self.current_pat_annot_path).mkdir(parents=True, exist_ok=True)
                # Path(self.pre_annotation_path_mrc).mkdir(parents=True, exist_ok=True)
                pass  #'deprecated'

            elif root_path == f"../{self.proj_name}/":

                try:
                    # Test if remote_path exists
                    self.sftp_client.chdir(self.pre_annotation_path)
                except IOError:
                    # Create remote_path
                    self.sftp_client.mkdir(self.pre_annotation_path)

                try:
                    # Test if remote_path exists
                    self.sftp_client.chdir(self.pre_annotation_path_mrc)
                except IOError:
                    # Create remote_path
                    self.sftp_client.mkdir(self.pre_annotation_path_mrc)

                try:
                    # Test if remote_path exists
                    self.sftp_client.chdir(self.current_pat_line_path)
                except IOError:
                    # Create remote_path
                    self.sftp_client.mkdir(self.current_pat_line_path)

            self.sftp_obj = self.sftp_client
            #: SFTP object for file operations.

        else:
            self.sftp_client = None

        if global_start_year == None:
            (
                self.global_start_year,
                self.global_start_month,
                self.global_end_year,
                self.global_end_month,
                self.global_start_day,
                self.global_end_day,
            ) = ("1995", "01", "2023", "11", "01", "01")
        else:

            self.global_start_year = str(global_start_year).zfill(4)
            self.global_start_month = str(global_start_month).zfill(2)
            self.global_end_year = str(global_end_year).zfill(4)
            self.global_end_month = str(global_end_month).zfill(2)
            self.global_start_day = str(global_start_day).zfill(2)
            self.global_end_day = str(global_end_day).zfill(2)

        self.initial_global_start_year = self.global_start_year
        self.initial_global_start_month = self.global_start_month
        self.initial_global_end_year = self.global_end_year
        self.initial_global_end_month = self.global_end_month
        self.initial_global_start_day = self.global_start_day
        self.initial_global_end_day = self.global_end_day

        # CRITICAL: Ensure global dates are in correct order for Elasticsearch
        try:
            self.global_start_date = datetime(
                int(self.global_start_year),
                int(self.global_start_month),
                int(self.global_start_day),
            )
            self.global_end_date = datetime(
                int(self.global_end_year),
                int(self.global_end_month),
                int(self.global_end_day),
            )
        except (ValueError, TypeError):
            self.global_start_date = None
            self.global_end_date = None
        self._validate_and_fix_global_dates()

        # Update global start date based on the provided start_date (only for forward looking)
        self = update_global_start_date(self, self.start_date)

        if not self.individual_patient_window:
            #: List of datetime objects for time window generation.
            self.date_list: List[datetime] = generate_date_list(
                self.start_date,
                self.years,
                self.months,
                self.days,
                time_window_interval_delta=self.time_window_interval_delta,
                config_obj=self,
            )

            if self.verbosity > 0:
                for date in self.date_list[0:5]:
                    print(date)

            #: Number of patient lines (time slices) to generate.
            self.n_pat_lines = len(self.date_list)

        if self.individual_patient_window:
            print("individual_patient_window set!")

            # check if user uploaded ipw dataframe already contains offset, if so do not compute and print warning.

            start_column_name = self.individual_patient_window_start_column_name
            id_column_name = self.individual_patient_id_column_name
            offset_column_name = start_column_name + "_offset"
            end_date_column_name = start_column_name + "_end_date"

            # Print a warning if the start column name is not in the dataframe
            if start_column_name not in self.individual_patient_window_df.columns:
                raise ValueError(f"Column '{start_column_name}' does not exist.")
            if id_column_name not in self.individual_patient_window_df.columns:
                raise ValueError(f"Column '{id_column_name}' does not exist.")
            # print debug message about start column name, offset column name, end date column name, median time between start and end date
            if self.verbosity >= 1:
                print(f"Start column name: {start_column_name}")
                print(f"Offset column name: {offset_column_name}")
                print(f"End date column name: {end_date_column_name}")
                if end_date_column_name in self.individual_patient_window_df.columns:
                    median_days = (
                        (
                            pd.to_datetime(
                                self.individual_patient_window_df[end_date_column_name],
                                errors="coerce",
                                utc=True,
                            )
                            - pd.to_datetime(
                                self.individual_patient_window_df[start_column_name],
                                errors="coerce",
                                utc=True,
                            )
                        )
                        .median()
                        .days
                    )
                    print(
                        f"Median time between {start_column_name} and {end_date_column_name}: {median_days} days"
                    )

            if self.lookback:
                time_offset = -relativedelta(days=days, months=months, years=years)

            else:
                time_offset = relativedelta(days=days, months=months, years=years)

            if offset_column_name in self.individual_patient_window_df.columns:
                print("individual_patient_window already contains offset column")
                print("skipping offset computation", "using existing offset column")
                print(
                    "if you want to recompute offset, delete offset column from dataframe"
                )
                print('using existing offset column: "{}"'.format(offset_column_name))
                self.individual_patient_window_df[
                    self.individual_patient_window_start_column_name
                ] = pd.to_datetime(
                    self.individual_patient_window_df[
                        self.individual_patient_window_start_column_name
                    ],
                    errors="coerce",
                    utc=True,
                )
                self.individual_patient_window_df[offset_column_name] = pd.to_datetime(
                    self.individual_patient_window_df[offset_column_name],
                    errors="coerce",
                    utc=True,
                )
                self.individual_patient_window_df[end_date_column_name] = (
                    pd.to_datetime(
                        self.individual_patient_window_df[end_date_column_name],
                        errors="coerce",
                        utc=True,
                    )
                )

                self.patient_dict = build_patient_dict(
                    dataframe=self.individual_patient_window_df,
                    patient_id_column=self.individual_patient_id_column_name,
                    start_column=offset_column_name,
                    end_column=end_date_column_name,
                )

            else:
                print("computing offset column")

                self.individual_patient_window_df = add_offset_column(
                    self.individual_patient_window_df,
                    start_column_name,
                    offset_column_name,
                    time_offset,
                    verbose=self.verbosity,
                )

                # Ensure the start_column_name is converted to datetime before building the patient_dict
                # The add_offset_column function internally converts it for offset calculation,
                # but the original column in the DataFrame might still be of a different type.
                self.individual_patient_window_df[
                    self.individual_patient_window_start_column_name
                ] = pd.to_datetime(
                    self.individual_patient_window_df[
                        self.individual_patient_window_start_column_name
                    ],
                    errors="coerce",
                    utc=True,
                )
                # Also convert the newly created offset column to ensure it's a timezone-aware datetime
                self.individual_patient_window_df[offset_column_name] = pd.to_datetime(
                    self.individual_patient_window_df[offset_column_name],
                    errors="coerce",
                    utc=True,
                )

                self.patient_dict = build_patient_dict(
                    dataframe=self.individual_patient_window_df,
                    patient_id_column=self.individual_patient_id_column_name,
                    start_column=self.individual_patient_window_start_column_name,  # Use the now-converted original column
                    end_column=f"{start_column_name}_offset",
                )

            #: Number of patient lines, dynamic for IPW.
            self.n_pat_lines = (
                None  # N_pat_lines will be dynamic for each pat... or potentially?
            )
            self.date_list = None  # We will generate this in main_pat2vec under individiual patient window

        if self.verbosity > 1:

            print("Debug message: global_start_year =", self.global_start_year)
            print("Debug message: global_start_month =", self.global_start_month)
            print("Debug message: global_end_year =", self.global_end_year)
            print("Debug message: global_end_month =", self.global_end_month)
            print("Debug message: global_start_day =", self.global_start_day)
            print("Debug message: global_end_day =", self.global_end_day)

            if self.individual_patient_window:
                if self.patient_dict:
                    first_key = next(iter(self.patient_dict))
                    display(self.patient_dict[first_key])
            if self.data_type_filter_dict is not None:
                print("data_type_filter_dict")
                print(self.data_type_filter_dict)
                print(self.data_type_filter_dict.keys())

    def _get_test_options_dict(self) -> Dict[str, bool]:
        """Returns a dictionary of implemented testing functions.

        The dictionary contains boolean flags for various features that have a
        dummy data generator implemented for testing purposes.

        Returns:
            A dictionary with feature names as keys and a boolean indicating
            if a test implementation exists.
        """
        return {
            "demo": True,
            "bmi": False,
            "bloods": True,
            "drugs": True,
            "diagnostics": True,
            "core_02": False,
            "bed": False,
            "vte_status": False,
            "hosp_site": False,
            "core_resus": False,
            "news": False,
            "smoking": False,
            "annotations": True,
            "annotations_mrc": True,
            "negated_presence_annotations": False,
            "appointments": True,
            "annotations_reports": False,
            "textual_obs": True,
        }

    def _update_main_options(self) -> None:
        """Disables main options that are not implemented for testing.

        This ensures that when `self.testing` is True, only features with
        available dummy data generators are enabled. It modifies `self.main_options`
        in place.
        """
        test_options_dict = self._get_test_options_dict()
        for option, value in self.main_options.items():
            if value and not test_options_dict.get(option, False):
                self.main_options[option] = False

    def _validate_and_fix_global_dates(self) -> None:
        """Ensures global start date is before the global end date.

        If the start date is after the end date, it swaps them to ensure
        compatibility with Elasticsearch range queries and warns the user.
        Modifies date attributes in place.
        """
        try:
            global_start = datetime(
                int(self.global_start_year),
                int(self.global_start_month),
                int(self.global_start_day),
            )
            global_end = datetime(
                int(self.global_end_year),
                int(self.global_end_month),
                int(self.global_end_day),
            )


            if self.global_start_date and self.global_end_date and self.global_start_date > self.global_end_date:
                print(
                    f"Warning: Global start date ({global_start.date()}) is after "
                    f"global end date ({global_end.date()})."
                    f"Warning: Global start date ({self.global_start_date.date()}) is after "
                    f"global end date ({self.global_end_date.date()})."
                )
                print("Swapping dates to ensure Elasticsearch compatibility...")

                # Swap the date attributes
                (
                    self.global_start_year,
                    self.global_start_month,
                    self.global_start_day,
                    self.global_end_year,
                    self.global_end_month,
                    self.global_end_day,
                ) = (
                    self.global_end_year,
                    self.global_end_month,
                    self.global_end_day,
                    self.global_start_year,
                    self.global_start_month,
                    self.global_start_day,
                )
                # Also swap the datetime objects
                (
                    self.global_start_date,
                    self.global_end_date
                ) = (
                    self.global_end_date,
                    self.global_start_date
                )

        except (ValueError, TypeError) as e:
            print(f"Warning: Could not validate global dates due to invalid values: {e}")


def update_global_start_date(self: T_config, start_date: datetime) -> T_config:
    """Updates the global start date if the provided start_date is later.

    This logic only applies when looking forward (lookback=False).

    Args:
        self: The configuration object instance.
        start_date: The new start date to compare against the global start date.

    Returns:
        The configuration object instance.
    """
    # This function is now defined outside the class but operates on an instance.
    # It's kept for compatibility but would be better as a private method.
    # The logic has been simplified and made more robust.
    if self.lookback:
        return self

    try:
        if self.global_start_date and start_date > self.global_start_date:
            print(
                "Warning: Updating global start date because the provided "
                "start_date is later."
            )
            self.global_start_year = str(start_date.year)
            self.global_start_month = str(start_date.month).zfill(2)
            self.global_start_day = str(start_date.day).zfill(2)
            # Also update the datetime object to maintain consistency
            self.global_start_date = start_date
    except (ValueError, TypeError):
        print("Warning: Invalid global date attributes in config. Cannot update.")

    return self


def validate_and_fix_global_dates(config: T_config) -> T_config:
    """Ensures global start date is before the global end date.

    If the start date is after the end date, it swaps them to ensure
    compatibility with Elasticsearch range queries and warns the user.

    Args:
        config: The configuration object instance.

    Returns:
        The modified configuration object.
    """
    try:
        global_start = datetime(
            int(config.global_start_year),
            int(config.global_start_month),
            int(config.global_start_day),
        )
        global_end = datetime(
            int(config.global_end_year),
            int(config.global_end_month),
            int(config.global_end_day),
        )

        if global_start > global_end:
            print(
                f"Warning: Global start date ({global_start.date()}) is after "
                f"global end date ({global_end.date()})."
            )
            print("Swapping dates to ensure Elasticsearch compatibility...")

            # Swap the date attributes
            (
                config.global_start_year, config.global_start_month, config.global_start_day,
                config.global_end_year, config.global_end_month, config.global_end_day,
            ) = (
                config.global_end_year, config.global_end_month, config.global_end_day,
                config.global_start_year, config.global_start_month, config.global_start_day,
            )
            # Also swap the datetime objects if they exist
            if hasattr(config, 'global_start_date') and hasattr(config, 'global_end_date'):
                (
                    config.global_start_date,
                    config.global_end_date
                ) = (
                    config.global_end_date,
                    config.global_start_date
                )

    except (ValueError, TypeError) as e:
        print(f"Warning: Could not validate global dates due to invalid values: {e}")

    return config
