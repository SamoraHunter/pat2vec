import math
import os
from datetime import datetime, timedelta
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


def get_test_options_dict():
    # contains implemented testing functions, i.e have a dummy data generator implemented.
    """
    Returns a dictionary of main options for testing configurations.

    The dictionary contains boolean flags for various features to be enabled
    or disabled during testing. These options include demographic information,
    BMI tracking, blood-related information, drug-related information,
    diagnostic information, and various annotations, among others.

    Returns:
        dict: A dictionary with keys as option names and values as boolean
        indicating whether the feature is enabled (True) or disabled (False).
    """

    main_options_dict = {
        # Enable demographic information (Ethnicity mapped to UK census, age, death)
        "demo": True,
        "bmi": False,  # Enable BMI (Body Mass Index) tracking
        "bloods": True,  # Enable blood-related information
        "drugs": True,  # Enable drug-related information
        "diagnostics": True,  # Enable diagnostic information
        "core_02": False,  # Enable core_02 information
        "bed": False,  # Enable bed n information
        "vte_status": False,  # Enable VTE () status tracking
        "hosp_site": False,  # Enable hospital site information
        "core_resus": False,  # Enable core resuscitation information
        "news": False,  # Enable NEWS (National Early Warning Score) tracking
        "smoking": False,  # Enable smoking-related information
        "annotations": True,  # Enable EPR annotations
        # Enable MRC (Additional clinical note observations index) annotations
        "annotations_mrc": True,
        # Enable or disable negated presence annotations
        "negated_presence_annotations": False,
        "appointments": True,
        "annotations_reports": False,
        "textual_obs": True,
    }
    return main_options_dict


def update_global_start_date(self, start_date):
    """
    Updates the global start date if the provided start_date is later.
    This logic only applies when looking forward (lookback=False).
    """
    # This function should only operate when looking forward.
    if self.lookback:
        return self  # Do nothing if it's a lookback calculation

    # Construct a complete datetime object from the global start attributes for a clean comparison.
    # This also handles the case where the attributes might still be strings.
    try:
        global_start = datetime(
            int(self.global_start_year),
            int(self.global_start_month),
            int(self.global_start_day),
        )
    except (ValueError, TypeError):
        # Handle cases where global date attributes are invalid to prevent crashes.
        print("Warning: Invalid global date attributes in config. Cannot update.")
        return self

    # Compare the full date objects for simplicity and accuracy.
    if start_date > global_start:
        print(
            "Warning: Updating global start date because the provided start_date is later."
        )

        # Assign the new values as integers, not strings.
        self.global_start_year = start_date.year
        self.global_start_month = start_date.month
        self.global_start_day = start_date.day

    return self


def validate_and_fix_global_dates(self):
    """
    Ensures that global_start_date < global_end_date for Elasticsearch compatibility.
    If they're in wrong order, swaps them and warns the user.
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

        if global_start > global_end:
            print(
                f"Warning: Global start date ({global_start}) is after global end date ({global_end})"
            )
            print("Swapping dates to ensure Elasticsearch compatibility...")

            # Swap the dates
            temp_year, temp_month, temp_day = (
                self.global_start_year,
                self.global_start_month,
                self.global_start_day,
            )
            (
                self.global_start_year,
                self.global_start_month,
                self.global_start_day,
            ) = (self.global_end_year, self.global_end_month, self.global_end_day)
            self.global_end_year, self.global_end_month, self.global_end_day = (
                temp_year,
                temp_month,
                temp_day,
            )

            print(
                f"New global start date: {self.global_start_year}-{self.global_start_month}-{self.global_start_day}"
            )
            print(
                f"New global end date: {self.global_end_year}-{self.global_end_month}-{self.global_end_day}"
            )

    except (ValueError, TypeError) as e:
        print(f"Warning: Could not validate global dates due to invalid values: {e}")

    return self


class config_class:
    def __init__(
        self,
        remote_dump=False,
        suffix="",
        treatment_doc_filename="treatment_docs.csv",
        treatment_control_ratio_n=1,
        proj_name="new_project",
        current_path_dir=".",
        main_options=None,
        start_date=(datetime(1995, 1, 1)),
        years=0,
        months=0,
        days=1,
        batch_mode=True,
        store_annot=False,
        share_sftp=True,
        multi_process=False,
        strip_list=True,
        verbosity=3,
        random_seed_val=42,
        hostname=None,
        username=None,
        password=None,
        gpu_mem_threshold=4000,
        testing=False,
        dummy_medcat_model=False,
        use_controls=False,
        medcat=False,
        global_start_year=None,
        global_start_month=None,
        global_end_year=None,
        global_end_month=None,
        global_start_day=None,
        global_end_day=None,
        skip_additional_listdir=False,
        start_time=None,
        root_path=None,
        negate_biochem=False,
        patient_id_column_name="client_idcode",
        overwrite_stored_pat_docs=False,
        overwrite_stored_pat_observations=False,
        store_pat_batch_docs=True,
        store_pat_batch_observations=True,
        annot_filter_options=None,
        shuffle_pat_list=False,
        individual_patient_window=False,
        individual_patient_window_df=None,
        individual_patient_window_start_column_name=None,
        individual_patient_id_column_name=None,
        individual_patient_window_controls_method="full",  # full, random
        dropna_doc_timestamps=True,
        time_window_interval_delta=relativedelta(days=1),
        feature_engineering_arg_dict=None,
        split_clinical_notes=True,
        lookback=True,  # look back or forward from the start date
        add_icd10=False,  # append ICD 10 codes to the output of annotations
        add_opc4s=False,  # append OPC4s codes to the output of annotations
        all_epr_patient_list_path="/home/samorah/_data/gloabl_files/all_client_idcodes_epr_unique.csv",  # Used for control patient sampling
        override_medcat_model_path=None,
        data_type_filter_dict=None,
        filter_split_notes=True,  # Apply global time window to notes post clinical note splitting.
        client_idcode_term_name="client_idcode.keyword",  # Used for elastic search index keyword search
        sanitize_pat_list=True,
        calculate_vectors=True,
        prefetch_pat_batches=False,
        sample_treatment_docs=0,  # 0 for no sampling, provide an int for the number of samples.
    ):
        """Initializes the configuration object for the pat2vec pipeline.

        This class holds all configuration parameters for a pat2vec run, including
        file paths, time window settings, feature selection, and operational flags.

        Args:
            remote_dump (bool): If **True**, data will be dumped to a remote server via SFTP. Defaults to **False**.
            suffix (str): A suffix to append to output folder names. Defaults to **""**.
            treatment_doc_filename (str): The filename for the input document containing the primary cohort list. Defaults to **'treatment_docs.csv'**.
            treatment_control_ratio_n (int): The ratio of treatment to control patients. Defaults to **1**.
            proj_name (str): The name of the current project, used for creating project-specific folders where patient data batches and vectors are stored. Defaults to **'new_project'**.
            current_path_dir (str): The current working directory. Defaults to **"."**.
            main_options (dict, optional): A dictionary of boolean flags to enable or disable specific feature extractions (e.g., 'demo', 'bloods', 'annotations'). If **None**, a default dictionary is used. Defaults to **None**.
            start_date (datetime): The anchor date for generating time windows. For global windows, this is the start. For individual windows, this is overridden per patient. Defaults to **datetime(1995, 1, 1)**.
            years (int): The number of years in the time window duration. Defaults to **0**.
            months (int): Number of months to add to the `start_date`. Defaults to **0**.
            days (int): The number of days in the time window duration. Defaults to **1**.
            batch_mode (bool): Flag for batch processing mode. This is currently the **only functioning mode**. Defaults to **True**.
            store_annot (bool): Flag to store annotations. Partially deprecated. Defaults to **False**.
            share_sftp (bool): Flag for sharing SFTP connection. Partially deprecated. Defaults to **True**.
            multi_process (bool): Flag for multi-process execution. Deprecated. Defaults to **False**.
            strip_list (bool): If **True**, this will check for completed patients before starting to avoid redundancy. Defaults to **True**.
            verbosity (int): Verbosity level for logging (0-9). Defaults to **3**.
            random_seed_val (int): Random seed for reproducibility. Defaults to **42**.
            hostname (str, optional): Hostname for SFTP connection. Defaults to **None**.
            username (str, optional): Username for SFTP connection. Defaults to **None**.
            password (str, optional): Password for SFTP connection. Defaults to **None**.
            gpu_mem_threshold (int): GPU memory threshold in MB for MedCAT. Defaults to **4000**.
            testing (bool): If **True**, enables testing mode, which may use dummy data generators. Defaults to **False**.
            dummy_medcat_model (bool): If **True** and in testing mode, simulates a MedCAT model. Defaults to **False**.
            use_controls (bool): If **True**, this will add the desired ratio of controls at random from the global pool, requiring configuration with a master list of patients. Defaults to **False**.
            medcat (bool): Flag for MedCAT processing. If **True**, MedCAT will load into memory and be used for annotating. Defaults to **False**.
            global_start_year (int, optional): Global start year for the overall data extraction window. Defaults to **None**, then set to '1995'.
            global_start_month (int, optional): Global start month. Defaults to **None**, then set to '01'.
            global_start_day (int, optional): Global start day. Defaults to **None**, then set to '01'.
            global_end_year (int, optional): Global end year. Defaults to **None**, then set to '2023'.
            global_end_month (int, optional): Global end month. Defaults to **None**, then set to '11'.
            global_end_day (int, optional): Global end day. Defaults to **None**, then set to '01'.
            skip_additional_listdir (bool): If **True**, skips some `listdir` calls for performance. Defaults to **False**.
            start_time (datetime, optional): Start time for logging. Defaults to `datetime.now()`.
            root_path (str, optional): The root directory for the project. If **None**, defaults to the current working directory. Defaults to **None**.
            negate_biochem (bool): Flag for negating biochemistry features. Defaults to **False**.
            patient_id_column_name (str): Column name for patient IDs in input files. Defaults to **'client_idcode'**.
            overwrite_stored_pat_docs (bool): If **True**, overwrites existing stored patient documents. Defaults to **False**.
            overwrite_stored_pat_observations (bool): If **True**, overwrites existing stored patient observations. Defaults to **False**.
            store_pat_batch_docs (bool): If **True**, stores patient document batches. Defaults to **True**.
            store_pat_batch_observations (bool): If **True**, stores patient observation batches. Defaults to **True**.
            annot_filter_options (dict, optional): Dictionary for filtering MedCAT annotations. Defaults to **None**.
            shuffle_pat_list (bool): Flag for shuffling the patient list. Defaults to **False**.
            individual_patient_window (bool): If **True**, uses patient-specific time windows defined in `individual_patient_window_df`. Defaults to **False**.
            individual_patient_window_df (pd.DataFrame, optional): DataFrame with patient IDs and their individual start dates. Required if `individual_patient_window` is **True**. Defaults to **None**.
            individual_patient_window_start_column_name (str, optional): The column name for start dates in `individual_patient_window_df`. Defaults to **None**.
            individual_patient_id_column_name (str, optional): The column name for patient IDs in `individual_patient_window_df`. Defaults to **None**.
            individual_patient_window_controls_method (str): Method for handling control patients in IPW mode ('full' or 'random'). Defaults to **'full'**.
            dropna_doc_timestamps (bool): If **True**, drops documents with missing timestamps. Defaults to **True**.
            time_window_interval_delta (relativedelta): The step/interval for each time slice vector. Defaults to **relativedelta(days=1)**.
            feature_engineering_arg_dict (dict, optional): Dictionary of arguments for feature engineering. Defaults to **None**.
            split_clinical_notes (bool): If **True**, clinical notes will be split by date and treated as individual documents with extracted dates. Requires a note splitter module. Defaults to **True**.
            lookback (bool): If **True**, the time window is calculated backward from the start date. If **False**, it's calculated forward. Defaults to **True**.
            add_icd10 (bool): If **True**, appends ICD-10 codes to annotation batches. These can be found under `current_pat_documents_annotations/%client_idcode%.csv`. Defaults to **False**.
            add_opc4s (bool): Requires `add_icd10` to be **True**. If **True**, appends OPC4S codes to annotation batches. These can be found under `current_pat_documents_annotations/%client_idcode%.csv`. Defaults to **False**.
            all_epr_patient_list_path (str): Path to a file containing all patient IDs, used for sampling controls.
            override_medcat_model_path (str, optional): Path to a MedCAT model pack to override the default. Defaults to **None**.
            data_type_filter_dict (dict, optional): Dictionary for data type filtering. See examples provided in the configuration. Defaults to **None**.
            filter_split_notes (bool): If enabled (**True**), the global time window filter will be reapplied after clinical note splitting. This is recommended to enable if `split_clinical_notes` is enabled. Defaults to **True**.
            client_idcode_term_name (str): The Elasticsearch field name for patient ID searches. Defaults to **"client_idcode.keyword"**.
            sanitize_pat_list (bool): If **True**, sanitizes the patient list (e.g., to uppercase). Defaults to **True**.
            calculate_vectors (bool): If **True**, calculates feature vectors. If **False**, only extracts batches. Defaults to **True**.
            prefetch_pat_batches (bool): If **True**, fetches all raw data for all patients before processing. May use significant memory. Defaults to **False**.
            sample_treatment_docs (int): Number of patients to sample from the initial cohort list. `0` means no sampling. Defaults to **0**.
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

        self.prefetch_pat_batches = prefetch_pat_batches

        self.calculate_vectors = calculate_vectors  # Calculate vectors for each patient else just extract batches

        self.sanitize_pat_list = (
            sanitize_pat_list  # Enforce all characters capitalized in patient list.
        )

        self.skip_additional_listdir = skip_additional_listdir

        self.filter_split_notes = filter_split_notes

        self.suffix = suffix
        self.treatment_doc_filename = treatment_doc_filename
        self.treatment_control_ratio_n = treatment_control_ratio_n

        self.pre_annotation_path = f"current_pat_annots_parts{self.suffix}/"
        self.pre_annotation_path_mrc = f"current_pat_annots_mrc_parts{self.suffix}/"

        self.pre_document_annotation_batch_path = (
            f"current_pat_documents_annotations_batches{self.suffix}/"
        )
        self.pre_document_annotation_batch_path_mct = (
            f"current_pat_documents_annotations_batches_mct{self.suffix}/"
        )
        self.pre_report_annotation_batch_path_report = (
            f"current_pat_documents_annotations_batches_report{self.suffix}/"
        )

        self.pre_document_batch_path = f"current_pat_document_batches{self.suffix}/"
        self.pre_document_batch_path_mct = (
            f"current_pat_document_batches_mct{self.suffix}/"
        )

        self.pre_textual_obs_document_batch_path = (
            f"current_pat_textual_obs_document_batches{self.suffix}/"
        )

        self.pre_textual_obs_annotation_batch_path = (
            f"current_pat_textual_obs_annotation_batches{self.suffix}/"
        )

        self.pre_report_batch_path = f"current_pat_report_batches{self.suffix}/"

        self.pre_bloods_batch_path = f"current_pat_bloods_batches{self.suffix}/"

        self.pre_drugs_batch_path = f"current_pat_drugs_batches{self.suffix}/"

        self.pre_diagnostics_batch_path = (
            f"current_pat_diagnostics_batches{self.suffix}/"
        )

        self.pre_news_batch_path = f"current_pat_news_batches{self.suffix}/"

        self.pre_obs_batch_path = f"current_pat_obs_batches{self.suffix}/"

        self.pre_bmi_batch_path = f"current_pat_bmi_batches{self.suffix}/"

        self.pre_demo_batch_path = f"current_pat_demo_batches{self.suffix}/"

        self.pre_misc_batch_path = f"current_pat_misc_batches{self.suffix}/"

        self.current_pat_line_path = f"current_pat_line_path{self.suffix}/"

        self.pre_appointments_batch_path = (
            f"current_pat_appointments_batches{self.suffix}/"
        )

        self.store_pat_batch_docs = store_pat_batch_docs

        self.store_pat_batch_observations = store_pat_batch_observations

        self.proj_name = proj_name
        self.main_options = main_options

        self.negate_biochem = negate_biochem
        self.patient_id_column_name = patient_id_column_name

        self.add_icd10 = add_icd10
        self.add_opc4s = add_opc4s

        self.data_type_filter_dict = data_type_filter_dict

        self.batch_mode = batch_mode
        self.remote_dump = remote_dump

        self.store_annot = store_annot
        self.share_sftp = share_sftp
        self.multi_process = multi_process
        self.strip_list = strip_list
        self.verbosity = verbosity
        self.random_seed_val = random_seed_val

        self.hostname = hostname
        self.username = username
        self.password = password

        self.gpu_mem_threshold = gpu_mem_threshold

        self.testing = testing
        self.use_controls = use_controls

        self.skipped_counter = 0  # init start

        self.medcat = medcat

        self.root_path = root_path

        self.overwrite_stored_pat_docs = overwrite_stored_pat_docs

        self.overwrite_stored_pat_observations = overwrite_stored_pat_observations

        self.annot_filter_options = annot_filter_options

        self.start_time = start_time

        self.shuffle_pat_list = shuffle_pat_list

        self.individual_patient_window = individual_patient_window

        self.individual_patient_window_df = individual_patient_window_df

        self.individual_patient_window_start_column_name = (
            individual_patient_window_start_column_name
        )

        self.individual_patient_id_column_name = individual_patient_id_column_name

        self.individual_patient_window_controls_method = (
            individual_patient_window_controls_method
        )

        self.control_list_path = "control_path.pkl"

        self.dropna_doc_timestamps = dropna_doc_timestamps

        self.time_window_interval_delta = time_window_interval_delta

        self.split_clinical_notes = split_clinical_notes

        self.all_epr_patient_list_path = all_epr_patient_list_path

        self.lookback = lookback

        self.override_medcat_model_path = override_medcat_model_path

        if dummy_medcat_model == None:
            self.dummy_medcat_model = True
        else:
            self.dummy_medcat_model = dummy_medcat_model

        if start_time == None:
            self.start_time = datetime.now()

        self.drug_time_field = "order_createdwhen"  # order_createdwhen: none missing, #"order_performeddtm" order performed empty for medication  # alt #order_createdwhen

        self.diagnostic_time_field = "order_createdwhen"  # order_createdwhen: none missing, #"order_performeddtm" order performed empty for medication  # alt #order_createdwhen

        self.appointments_time_field = "AppointmentDateTime"  # alt #DateModified

        self.bloods_time_field = "basicobs_entered"

        if client_idcode_term_name is None:
            self.client_idcode_term_name = "client_idcode.keyword"  # alt client_idcode.keyword #warn excludes on case basis.
        else:
            self.client_idcode_term_name = client_idcode_term_name

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
            self.feature_engineering_arg_dict = {
                "drugs": {
                    "_num-drug-order": True,
                    "_days-since-last-drug-order": True,
                    "_days-between-first-last-drug": True,
                }
            }
        else:
            self.feature_engineering_arg_dict = feature_engineering_arg_dict

        self.negated_presence_annotations = self.main_options.get(
            "negated_presence_annotations"
        )

        if remote_dump == False:

            if self.root_path == None:
                self.root_path = f"{os.getcwd()}/{self.proj_name}/"

            self.pre_annotation_path = os.path.join(
                self.root_path, f"current_pat_annots_parts{self.suffix}/"
            )
            self.pre_annotation_path_mrc = os.path.join(
                self.root_path, f"current_pat_annots_mrc_parts{self.suffix}/"
            )

            self.pre_document_annotation_batch_path = os.path.join(
                self.root_path,
                f"current_pat_documents_annotations_batches{self.suffix}/",
            )

            self.pre_document_annotation_batch_path_mct = os.path.join(
                self.root_path,
                f"current_pat_documents_annotations_batches_mct{self.suffix}/",
            )

            self.pre_textual_obs_annotation_batch_path = os.path.join(
                self.root_path,
                f"current_pat_textual_obs_annotation_batches{self.suffix}/",
            )

            self.pre_textual_obs_document_batch_path = os.path.join(
                self.root_path,
                f"current_pat_textual_obs_document_batches{self.suffix}/",
            )

            self.pre_document_batch_path = os.path.join(
                self.root_path, f"current_pat_document_batches{self.suffix}/"
            )
            self.pre_document_batch_path_mct = os.path.join(
                self.root_path, f"current_pat_document_batches_mct{self.suffix}/"
            )

            self.pre_document_batch_path_reports = os.path.join(
                self.root_path, f"current_pat_document_batches_reports{self.suffix}/"
            )

            self.pre_document_annotation_batch_path_reports = os.path.join(
                self.root_path,
                f"current_pat_documents_annotations_batches_reports{self.suffix}/",
            )

            self.pre_bloods_batch_path = os.path.join(
                self.root_path, f"current_pat_bloods_batches{self.suffix}/"
            )

            self.pre_drugs_batch_path = os.path.join(
                self.root_path, f"current_pat_drugs_batches{self.suffix}/"
            )

            self.pre_diagnostics_batch_path = os.path.join(
                self.root_path, f"current_pat_diagnostics_batches{self.suffix}/"
            )

            self.pre_news_batch_path = os.path.join(
                self.root_path, f"current_pat_news_batches{self.suffix}/"
            )

            self.pre_obs_batch_path = os.path.join(
                self.root_path, f"current_pat_obs_batches{self.suffix}/"
            )

            self.pre_bmi_batch_path = os.path.join(
                self.root_path, f"current_pat_bmi_batches{self.suffix}/"
            )

            self.pre_demo_batch_path = os.path.join(
                self.root_path, f"current_pat_demo_batches{self.suffix}/"
            )

            self.pre_misc_batch_path = os.path.join(
                self.root_path, f"current_pat_misc_batches{self.suffix}/"
            )

            self.current_pat_lines_path = os.path.join(
                self.root_path, f"current_pat_lines_parts{self.suffix}/"
            )

            self.pre_appointments_batch_path = os.path.join(
                self.root_path, f"current_pat_appointments_batches{self.suffix}/"
            )

            self.pre_merged_input_batches_path = os.path.join(
                self.root_path, f"merged_input_pat_batches{self.suffix}/"
            )

            self.output_folder = "outputs"

            self.PathsClass_instance = PathsClass(
                self.root_path, self.suffix, self.output_folder
            )

        print(f"Setting start_date to: {start_date}")
        self.start_date = start_date

        print(f"Setting years to: {years}")
        self.years = years

        print(f"Setting months to: {months}")
        self.months = months

        print(f"Setting days to: {days}")
        self.days = days

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

        self.slow_execution_threshold_low = timedelta(seconds=10)
        self.slow_execution_threshold_high = timedelta(seconds=30)
        self.slow_execution_threshold_extreme = timedelta(seconds=60)

        self.sample_treatment_docs = sample_treatment_docs

        priority_list_bool = False

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

        def update_main_options(self):
            """
            Updates the main options by setting any enabled options not present in the
            test options dictionary to False.

            This is used to ensure that only enabled options that are implemented in the
            test data are used when running tests.

            Parameters
            ----------
            None

            Returns
            -------
            None
            """
            test_options_dict = get_test_options_dict()
            for option, value in self.main_options.items():
                if value and not test_options_dict[option]:
                    self.main_options[option] = False

        if self.testing:
            print("Setting test options")

            self.treatment_doc_filename = os.path.join(
                os.getcwd(), "test_files", "treatment_docs.csv"
            )
            print("updating main options with implemented test options")
            # Enforce implemented testing options
            update_main_options(self)

        if self.remote_dump == False:
            self.sftp_obj = None

        if self.remote_dump:

            if self.root_path == None:

                self.root_path = f"../{self.proj_name}/"
                print(f"sftp root_path: {self.root_path}")

            else:
                print(f"sftp root_path: {self.root_path}")

            # Set the hostname, username, and password for the remote machine

            hostname = self.hostname

            username = self.username
            password = self.password

            # Create an SSH client and connect to the remote machine
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(
                hostname=self.hostname, username=self.username, password=self.password
            )

            self.sftp_client = self.ssh_client.open_sftp()

            if self.remote_dump:
                try:
                    # Test if remote_path exists
                    self.sftp_client.chdir(self.root_path)
                except IOError:
                    # Create remote_path
                    self.sftp_client.mkdir(self.root_path)

            self.pre_annotation_path = f"{self.root_path}{self.pre_annotation_path}"
            self.pre_annotation_path_mrc = (
                f"{self.root_path}{self.pre_annotation_path_mrc}"
            )
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
        self = validate_and_fix_global_dates(self)

        if self.lookback:
            if self.verbosity >= 1:
                print(
                    "Lookback mode enabled - this affects time window calculation direction"
                )
                print("Global dates remain ordered for Elasticsearch compatibility")
                print(
                    f"Global range: {self.global_start_year}-{self.global_start_month}-{self.global_start_day} to {self.global_end_year}-{self.global_end_month}-{self.global_end_day}"
                )

        # Update global start date based on the provided start_date (only for forward looking)
        self = update_global_start_date(self, self.start_date)

        if not self.individual_patient_window:
            self.date_list = generate_date_list(
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
