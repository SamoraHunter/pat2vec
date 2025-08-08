import os
from datetime import datetime, timedelta
import pandas as pd
import paramiko
from dateutil.relativedelta import relativedelta
from IPython.display import display
from pat2vec.util.current_pat_batch_path_methods import PathsClass
from pat2vec.util.methods_get import (
    add_offset_column,
    build_patient_dict,
    generate_date_list,
)


class MultiStream:
    # This class is a simple container for multiple streams that can be used
    # interchangeably with a single stream. For example, you can create an
    # instance of this class and pass it to the logging module as the stream
    # to be used for logging. Then, any messages that are logged will be
    # written to all of the streams that are contained within this class.
    # This is useful for situations where you want to log to multiple
    # places at the same time.
    #
    # The constructor takes a single argument, which is a list of streams.
    # The streams can be any type of stream, including strings (in which
    # case they will be used as file names), or already open file objects.
    #
    # For example, to create a MultiStream that logs to the console and to
    # a file, you could do the following:
    #
    #     stream_list = [sys.stdout, 'logfile.txt']
    #     multi_stream = MultiStream(stream_list)
    #     logging.basicConfig(stream=multi_stream, level=logging.INFO)
    #
    # Then, any messages that are logged will be written to both the console
    # and the logfile.

    def __init__(self, streams):
        self.streams = streams

    def write(self, text):
        for stream in self.streams:
            stream.write(text)

    def flush(self):
        for stream in self.streams:
            stream.flush()


def calculate_interval(start_date, time_delta, m=1):
    # adjust for time interval width
    end_date = start_date + time_delta
    interval_days = (end_date - start_date).days

    n_intervals = interval_days // m
    return n_intervals


def update_global_start_date(self, start_date):
    print("updating global start date")
    # Compare and update individual elements of global start date if necessary
    if self.lookback == False:
        if start_date.year > int(self.global_start_year):
            self.global_start_year = str(start_date.year)
        if start_date.month > int(self.global_start_month):
            self.global_start_month = str(start_date.month)
        if start_date.day > int(self.global_start_day):
            self.global_start_day = str(start_date.day)

        print(
            "Warning: Updated global start date as start date later than global start date."
        )

    return self


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
        aliencat=False,
        dgx=False,
        dhcap=False,
        dhcap02=True,
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
    ):
        """
        Initializes the config_class instance with various configuration parameters for data processing and analysis.

        This constructor sets up paths, time windows, and other settings for handling patient data,
        including options for local or remote data dumps, MedCAT models, and feature engineering.

        Args:
            remote_dump (bool): If **True**, data will be dumped to a remote server via SFTP. Defaults to **False**.
            suffix (str): A suffix to append to output folder names. Defaults to **""**.
            treatment_doc_filename (str): The filename for treatment documents. Defaults to **'test_files/treatment_docs.csv'**.
            treatment_control_ratio_n (int): The ratio of treatment to control patients. Defaults to **1**.
            proj_name (str): The name of the current project, used for creating project-specific folders where patient data batches and vectors are stored. Defaults to **'new_project'**.
            current_path_dir (str): The current working directory. Defaults to **""**.
            main_options (dict): A dictionary of boolean options for enabling or disabling various data types, such as demographic information ('demo'), BMI ('bmi'), blood-related information ('bloods'), drug-related information ('drugs'), and diagnostic information ('diagnostics'). It also includes options for 'core_02', 'bed', 'vte_status', 'hosp_site', 'core_resus', 'news', 'smoking', 'annotations' (EPR documents via MedCat), 'annotations_mrc' (MRC annotations via MedCat), 'negated_presence_annotations', 'appointments', 'annotations_reports', and 'textual_obs'. Defaults to `main_options_dict`.
            start_date (datetime): The starting date for processing. Defaults to **datetime(1995, 1, 1)**.
            years (int): Number of years to add to the `start_date`. This sets the duration of the time window, which is defined as the `start_date` plus the specified `years`, `months`, and `days`. Defaults to **30**.
            months (int): Number of months to add to the `start_date`. Defaults to **0**.
            days (int): Number of days to add to the `start_date`. Defaults to **0**.
            dgx (bool): Flag for DGX environment. Set to **True** if in this environment; each environment needs specific paths configured. Defaults to **False**.
            dhcap (bool): Flag for DHCap. Defaults to **False**.
            dhcap02 (bool): Flag for DHCap02. Defaults to **True**.
            batch_mode (bool): Flag for batch processing mode. This is currently the **only functioning mode**. Defaults to **True**.
            store_annot (bool): Flag to store annotations. Partially deprecated. Defaults to **True**.
            share_sftp (bool): Flag for sharing via SFTP. Partially deprecated. Defaults to **True**.
            multi_process (bool): Flag for multi-process execution. Deprecated. Defaults to **False**.
            strip_list (bool): If **True**, this will check for completed patients before starting to avoid redundancy. Defaults to **True**.
            verbosity (int): Verbosity level for printing debug messages (0-9). Defaults to **0**.
            random_seed_val (int): Random seed value for reproducibility of controls.
            testing (bool): Flag for testing mode. If **True**, the system will use dummy data. Defaults to **True**.
            dummy_medcat_model (bool): Flag for a dummy MedCAT model. Used if `testing` is **True**; this will simulate a MedCAT model. Defaults to **True**.
            use_controls (bool): If **True**, this will add the desired ratio of controls at random from the global pool, requiring configuration with a master list of patients. Defaults to **False**.
            medcat (bool): Flag for MedCAT processing. If **True**, MedCAT will load into memory and be used for annotating. Defaults to **False**.
            start_time (datetime): Current timestamp as the start time for logging and the progress bar. Defaults to `datetime.now()`.
            patient_id_column_name (str): Column name for patient ID. Setting to **'auto'** will attempt to find it. Example: "client_idcode". Defaults to **'auto'**.
            annot_filter_options (dict): Dictionary for annotation filtering. Only base annotations meeting this threshold will be included. It includes options for base concept accuracy ('acc'), UMLS list of types for MedCAT filter ('types'), and confidence thresholds for 'Time_Value', 'Presence_Value', and 'Subject_Value'. Defaults to `annot_filter_arguments`.
            global_start_year (int): Global start year for setting the limits of the time window data can be drawn from. The start should not precede the `start_date` set above. Ensure that the global start year/month/day is before the end year/month/day. Defaults to **1995**.
            global_start_month (int): Global start month. Defaults to **1**.
            global_end_year (int): Global end year. Defaults to **2023**.
            global_end_month (int): Global end month. Defaults to **1**.
            global_start_day (int): Global start day. Defaults to **1**.
            global_end_day (int): Global end day. Defaults to **1**.
            shuffle_pat_list (bool): Flag for shuffling the patient list. Defaults to **False**.
            time_window_interval_delta (relativedelta): Specifies the time window to collapse each feature vector into (e.g., `years=1` results in one vector per year within the global time window). Defaults to `relativedelta(years=30)`.
            split_clinical_notes (bool): If **True**, clinical notes will be split by date and treated as individual documents with extracted dates. Requires a note splitter module. Defaults to **True**.
            lookback (bool): Determines the direction of the time length window. If **True**, it calculates backward in time; otherwise, it calculates forward. This applies when calculating individual patient windows from a table of start dates or from the global start date. Defaults to **False**.
            add_icd10 (bool): If **True**, appends ICD-10 codes to annotation batches. These can be found under `current_pat_documents_annotations/%client_idcode%.csv`. Defaults to **False**.
            add_opc4s (bool): Requires `add_icd10` to be **True**. If **True**, appends OPC4S codes to annotation batches. These can be found under `current_pat_documents_annotations/%client_idcode%.csv`. Defaults to **False**.
            override_medcat_model_path (str, optional): Forces the MedCAT model path. If **None**, it uses the defaults for the environment. Can be set in `paths.py` with `medcat_path = "%path to medcat model pack.zip"`. Defaults to `path_to_medcat_model_pack`.
            data_type_filter_dict (dict, optional): Dictionary for data type filtering. See examples provided in the configuration. Defaults to **None**.
            filter_split_notes (bool): If enabled (**True**), the global time window filter will be reapplied after clinical note splitting. This is recommended to enable if `split_clinical_notes` is enabled. Defaults to **True**.
            prefetch_pat_batches (bool): If enabled (**True**), this will fetch batches for the entire patient list and pre-populate batch folders with individual patient batches. Note that this may lead to out-of-memory issues. Defaults to **False**.
        """

        self.prefetch_pat_batches = prefetch_pat_batches

        self.calculate_vectors = calculate_vectors  # Calculate vectors for each patient else just extract batches

        self.sanitize_pat_list = (
            sanitize_pat_list  # Enforce all characters capitalized in patient list.
        )

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

        self.aliencat = aliencat
        self.dgx = dgx
        self.dhcap = dhcap
        self.dhcap02 = dhcap02
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

        m = 1

        # self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.time_delta = relativedelta(days=days, weeks=0, months=months, years=years)

        # timedelta()

        result = calculate_interval(
            start_date=self.start_date, time_delta=self.time_delta, m=m
        )

        print(
            f"Number of {m}-day intervals between {start_date} and the calculated end date: {result}"
        )

        self.slow_execution_threshold_low = timedelta(seconds=10)
        self.slow_execution_threshold_high = timedelta(seconds=30)
        self.slow_execution_threshold_extreme = timedelta(seconds=60)

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

        if self.lookback:
            self.time_window_interval_delta = -self.time_window_interval_delta
            print("looking back with ", self.time_window_interval_delta)
        else:
            print("looking forward with ", self.time_window_interval_delta)

        self.model_paths = {
            "aliencat": "../medcat_model_pack_316666b47dfaac07.zip",
            "dgx": "../medcat_models/20230328_trained_model_hfe_redone/medcat_model_pack_316666b47dfaac07",
            "dhcap": "../medcat_model_pack_316666b47dfaac07.zip",
            "dhcap02": "../medcat_model_pack_316666b47dfaac07.zip",
            "override_medcat_model_path": None,
        }

        if self.lookback == True:
            print("Swapping global values")
            # Swapping values
            global_start_year, global_end_year = global_end_year, global_start_year
            global_start_month, global_end_month = global_end_month, global_start_month
            global_start_day, global_end_day = global_end_day, global_start_day

            # Output the swapped values
            print("global_start_year:", global_start_year)
            print("global_start_month:", global_start_month)
            print("global_end_year:", global_end_year)
            print("global_end_month:", global_end_month)
            print("global_start_day:", global_start_day)
            print("global_end_day:", global_end_day)

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

        if self.lookback:

            self = swap_start_end(self)
            if self.verbosity >= 1:
                print("Swapping start and end dates, lookback True")
                # Output
                print(
                    "Start:",
                    self.global_start_year,
                    self.global_start_month,
                    self.global_start_day,
                )
                print(
                    "End:",
                    self.global_end_year,
                    self.global_end_month,
                    self.global_end_day,
                )

        self.initial_global_start_year = self.global_start_year
        self.initial_global_start_month = self.global_start_month
        self.initial_global_end_year = self.global_end_year
        self.initial_global_end_month = self.global_end_month
        self.initial_global_start_day = self.global_start_day
        self.initial_global_end_day = self.global_end_day

        self = update_global_start_date(self, self.start_date)

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
            offset_column_name = start_column_name + "_offset"

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

                print(
                    pd.to_datetime(
                        self.individual_patient_window_df[
                            self.individual_patient_window_start_column_name
                        ],
                        errors="coerce",
                        utc=True,
                        format="ISO8601",
                    )
                    .isna()
                    .value_counts(),
                )

                self.patient_dict = build_patient_dict(
                    dataframe=self.individual_patient_window_df,
                    patient_id_column=self.individual_patient_id_column_name,
                    start_column=self.individual_patient_window_start_column_name,
                    end_column=self.individual_patient_window_start_column_name
                    + "_offset",
                )
                self.individual_patient_window_df[
                    self.individual_patient_window_start_column_name
                ] = pd.to_datetime(
                    self.individual_patient_window_df[
                        self.individual_patient_window_start_column_name
                    ],
                    errors="coerce",
                    utc=True,
                    format="ISO8601",
                )

                self.individual_patient_window_df[
                    self.individual_patient_window_start_column_name + "_offset"
                ] = pd.to_datetime(
                    self.individual_patient_window_df[
                        self.individual_patient_window_start_column_name + "_offset"
                    ],
                    errors="coerce",
                    utc=True,
                    format="ISO8601",
                )

            else:
                print("computing offset column")

                self.individual_patient_window_df = add_offset_column(
                    self.individual_patient_window_df,
                    start_column_name,
                    offset_column_name,
                    time_offset,
                )

                self.patient_dict = build_patient_dict(
                    dataframe=self.individual_patient_window_df,
                    patient_id_column=self.individual_patient_id_column_name,
                    start_column=f"{start_column_name}_converted",
                    end_column=f"{start_column_name}_offset",
                )

            if self.lookback == True:
                # print("skipping reverse")

                reversed_patient_dict = {
                    key: tuple(reversed(value))
                    for key, value in self.patient_dict.items()
                }
                print("reversed_patient_dict")
                self.patient_dict = reversed_patient_dict

        if self.verbosity > 1:

            print("Debug message: global_start_year =", self.global_start_year)
            print("Debug message: global_start_month =", self.global_start_month)
            print("Debug message: global_end_year =", self.global_end_year)
            print("Debug message: global_end_month =", self.global_end_month)
            print("Debug message: global_start_day =", self.global_start_day)
            print("Debug message: global_end_day =", self.global_end_day)

            if self.individual_patient_window:
                first_key = next(iter(self.patient_dict))
                display(self.patient_dict[first_key])

        self.skip_additional_listdir = skip_additional_listdir

        if self.verbosity >= 1:
            if self.data_type_filter_dict is not None:
                print("data_type_filter_dict")
                print(self.data_type_filter_dict)
                print(self.data_type_filter_dict.keys())


def swap_start_end(self):
    # Temporary variables to hold start values
    """
    Swap the values of start and end dates for all patients in the global and individual patient time windows.

    This method is used to reverse the direction of the time windows, e.g. from forward to backward in time or vice versa.
    It is used internally in the IndividualPatientWindow class when `lookback` is set to `True`.
    """
    temp_year = self.global_start_year
    temp_month = self.global_start_month
    temp_day = self.global_start_day

    # Assigning end values to start variables
    self.global_start_year = self.global_end_year
    self.global_start_month = self.global_end_month
    self.global_start_day = self.global_end_day

    # Assigning temporary values to end variables
    self.global_end_year = temp_year
    self.global_end_month = temp_month
    self.global_end_day = temp_day

    # Return self for method chaining
    return self
