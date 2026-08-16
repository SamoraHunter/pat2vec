import logging
import random
import time
import traceback
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text

# from pat2vec.pat2vec_search.cogstack_search_methods import *
from tqdm import trange

from pat2vec.pat2vec_get_methods.get_method_covid import SEARCH_TERM_ES

# epic_imaging_reports is now only available via annotations - commented out
# from pat2vec.pat2vec_get_methods.get_method_epic_imaging_reports import (
#     search_epic_imaging_reports,
# )
from pat2vec.pat2vec_get_methods.get_method_epic_clinical_notes_appointments import (
    search_epic_clinical_notes_appointments,
)
from pat2vec.pat2vec_get_methods.get_method_epic_encounters import (
    search_epic_encounters,
)

# epic_clinical_notes, epic_medical_history, epic_orders are now only available via annotations
# from pat2vec.pat2vec_get_methods.get_method_epic_clinical_notes import (
#     search_epic_clinical_notes,
# )
# from pat2vec.pat2vec_get_methods.get_method_epic_medical_history import (
#     search_epic_medical_history,
# )
# from pat2vec.pat2vec_get_methods.get_method_epic_orders import search_epic_orders
from pat2vec.pat2vec_get_methods.get_method_epic_lab_results import (
    search_epic_lab_results,
)
from pat2vec.pat2vec_get_methods.get_method_epic_patients import search_epic_patients
from pat2vec.pat2vec_main_methods.main_batch import main_batch
from pat2vec.pat2vec_pat_list.get_patient_treatment_list import get_all_patients_list
from pat2vec.pat2vec_search.cogstack_search_methods import (
    cohort_searcher_with_terms_and_search,
    initialize_cogstack_client,
)
from pat2vec.patvec_get_batch_methods.get_prefetch_batches import prefetch_batches
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_appointments import (
    get_pat_batch_appointments,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_bloods import (
    get_pat_batch_bloods,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_bmi import get_pat_batch_bmi
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_demo import get_pat_batch_demo
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_diagnostics import (
    get_pat_batch_diagnostics,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_drugs import (
    get_pat_batch_drugs,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_clinical_notes_annotations import (
    get_pat_batch_epic_clinical_notes_annotations,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_clinical_notes_appointments_annotations import (
    get_pat_batch_epic_clinical_notes_appointments_annotations,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_imaging_reports_annotations import (
    get_pat_batch_epic_imaging_reports_annotations,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_medical_history_annotations import (
    get_pat_batch_epic_medical_history_annotations,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epic_orders_annotations import (
    get_pat_batch_epic_orders_annotations,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epr_docs import (
    get_pat_batch_epr_docs,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_epr_docs_annotations import (
    get_pat_batch_epr_docs_annotations,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_mct_docs import (
    get_pat_batch_mct_docs,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_mct_docs_annotations import (
    get_pat_batch_mct_docs_annotations,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_news import get_pat_batch_news
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_obs import get_pat_batch_obs
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_reports import (
    get_pat_batch_reports,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_reports_docs_annotations import (
    get_pat_batch_reports_docs_annotations,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_textual_obs_annotations import (
    get_pat_batch_textual_obs_annotations,
)
from pat2vec.patvec_get_batch_methods.main_get_pat_batch_textual_obs_docs import (
    get_pat_batch_textual_obs_docs,
)
from pat2vec.util import config_pat2vec, helper_functions
from pat2vec.util.generate_date_list import generate_date_list
from pat2vec.util.get_best_gpu import set_best_gpu
from pat2vec.util.get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
)
from pat2vec.util.helper_functions import (
    clear_patient_features,
    save_patient_features,
    save_raw_patient_batch,
)
from pat2vec.util.methods_get import (
    create_folders_for_pat,
    list_dir_wrapper,
    update_pbar,
)
from pat2vec.util.methods_get_medcat import get_cat
from pat2vec.util.retrieve_data import retrieve_patient_data


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
    7.  The resulting feature vector is saved to a file or database.

    This class relies heavily on the `config_obj` for its behavior.

    Attributes:
        config_obj: The configuration object that controls the pipeline. Can be an instance of
            config_class or None (will create a default instance internally).
        cs (CogStack): An instance of the CogStack client for data retrieval.
        all_patient_list (list): The list of patient IDs to be processed.
        cat: MedCAT model instance for clinical text annotation if required. Set via
            `get_cat()` in `__init__`.
        t (tqdm.trange): Progress bar object for monitoring the process. Created via
            `trange()` in `__init__`.
        cohort_searcher_with_terms_and_search: Dynamically-assigned search function
            (either CogStack or dummy variant). Set in `__init__` based on cogstack flag.

    """

    cohort_searcher_with_terms_and_search = None

    def __init__(
        self,
        cogstack: bool = True,
        use_filter: bool = False,
        json_filter_path: str | None = None,
        random_seed_val: int = 42,
        hostname: str | None = None,
        config_obj: Any | None = None,
    ):
        """Initializes the main pat2vec pipeline orchestrator.

        This constructor sets up the pipeline environment, including data source
        connections, patient lists, and NLP models, based on the provided
        configuration.

        Args:
            cogstack: If True, connects to a CogStack Elasticsearch instance.
                If False, a dummy searcher is used for testing.
            use_filter: If True, applies a CUI filter to the MedCAT model.
            json_filter_path: Path to a JSON file containing the CUI filter. Defaults
                to None.
            random_seed_val: The random seed for reproducibility.
            hostname: Deprecated. SFTP settings are now in the config object. This
                parameter is stored as `self.hostname` but should not be used for new code.
            config_obj: The main configuration object. If None, a default
                configuration is created. Can also accept an instance of config_class
                or None (will create internally).

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

        if self.config_obj is None:
            logging.info("Initializing default config from config_pat2vec.")
            self.config_obj = config_pat2vec.config_class()

        # config parameters
        self.suffix = config_obj.suffix
        self.treatment_doc_filename = config_obj.treatment_doc_filename
        self.treatment_control_ratio_n = config_obj.treatment_control_ratio_n
        self.pre_annotation_path = config_obj.pre_annotation_path
        self.pre_annotation_path_mrc = config_obj.pre_annotation_path_mrc
        self.proj_name = config_obj.proj_name
        self.gpu_mem_threshold = config_obj.gpu_mem_threshold  # For medCat

        if cogstack:
            # Initialize the CogStack client with the config object
            self.cs = initialize_cogstack_client(self.config_obj)

            if self.config_obj.testing and not self.config_obj.testing_elastic:
                self.cohort_searcher_with_terms_and_search = (
                    cohort_searcher_with_terms_and_search_dummy
                )
                logging.info(
                    "Initialized cohort_searcher_with_terms_and_search_dummy function.",
                )
            else:
                if self.config_obj.verbosity > 0:
                    logging.info(
                        "Initialized cohort_searcher_with_terms_and_search function.",
                    )
                self.cohort_searcher_with_terms_and_search = (
                    cohort_searcher_with_terms_and_search
                )
        else:
            if self.config_obj.verbosity > 0:
                logging.warning("cohort_searcher_with_terms_and_search is disabled.")
            self.cohort_searcher_with_terms_and_search = None

        logging.debug(
            f"DEBUG: Final self.cohort_searcher_with_terms_and_search = {self.cohort_searcher_with_terms_and_search}",
        )
        # Respect all_patient_list if explicitly provided in config
        if (
            hasattr(self.config_obj, "all_patient_list")
            and self.config_obj.all_patient_list is not None
            and len(self.config_obj.all_patient_list) > 0
        ):
            self.all_patient_list = self.config_obj.all_patient_list.copy()
        else:
            self.all_patient_list = get_all_patients_list(self.config_obj)
        self.current_pat_lines_path = config_obj.current_pat_lines_path
        self.sftp_client = config_obj.sftp_obj

        if self.verbosity > 0:
            logging.info("Pre-annotation path: %s", self.pre_annotation_path)
            logging.info("Pre-annotation path MRC: %s", self.pre_annotation_path_mrc)

        # Using a medcat CUI filter for annotations data.
        self.use_filter = use_filter

        if self.use_filter:
            self.json_filter_path = json_filter_path
            import json

            with open(self.json_filter_path) as f:
                json_data = json.load(f)

            len(json_data["projects"][0])
            json_cuis = json_data["projects"][0]["cuis"].split(",")
            self.cat.cdb.filter_by_cui(json_cuis)

        set_best_gpu(config_obj.gpu_mem_threshold)

        random.seed(self.config_obj.random_seed_val)
        if config_obj.shuffle_pat_list:
            random.shuffle(self.all_patient_list)

        if self.config_obj.verbosity > 0:
            logging.info(f"remote_dump: {self.remote_dump}")
            logging.info("Pre-annotation path: %s", self.pre_annotation_path)
            logging.info("Pre-annotation path MRC: %s", self.pre_annotation_path_mrc)

        if self.config_obj.storage_backend == "file":
            self.stripped_list_start = [
                x.replace(".csv", "")
                for x in list_dir_wrapper(
                    path=self.current_pat_lines_path,
                    config_obj=config_obj,
                )
            ]

            (
                logging.info(
                    f"Length of stripped_list_start: {len(self.stripped_list_start)}",
                )
                if self.config_obj.verbosity > 0
                else None
            )
        elif self.config_obj.storage_backend == "database":
            # Early fetch completed patients from database for progress bar filtering
            try:
                engine = config_obj.db_engine
                if not engine:
                    logging.warning(
                        "Database engine not initialized. Cannot fetch existing patients.",
                    )
                    self.stripped_list_start = []
                else:
                    inspector = inspect(engine)
                    t_feat = (
                        "features_features" if engine.name == "sqlite" else "features"
                    )
                    s_feat = None if engine.name == "sqlite" else "features"

                    if inspector.has_table(t_feat, schema=s_feat):
                        with engine.connect() as connection:
                            full_t = (
                                f'"{t_feat}"'
                                if engine.name == "sqlite"
                                else '"features"."features"'
                            )
                            id_col = config_obj.patient_id_column_name
                            result = connection.execute(
                                text(f'SELECT DISTINCT "{id_col}" FROM {full_t}'),
                            )
                            self.stripped_list_start = [str(row[0]) for row in result]
                            logging.info(
                                f"Found {len(self.stripped_list_start)} existing patients in database.",
                            )
                    else:
                        self.stripped_list_start = []
            except Exception as e:
                logging.warning(f"Could not fetch existing patients from DB: {e}")
                self.stripped_list_start = []
        else:
            self.stripped_list_start = []

        # Filter out already-processed patients before creating progress bar
        if len(self.stripped_list_start) > 0:
            original_count = len(self.all_patient_list)
            self.all_patient_list = [
                p
                for p in self.all_patient_list
                if str(p) not in self.stripped_list_start
            ]
            logging.info(
                f"Filtering {original_count - len(self.all_patient_list)} already-processed patients from progress bar",
            )

        self.t = trange(
            len(self.all_patient_list),
            desc="Bar desc",
            leave=True,
            colour="GREEN",
            position=0,
            total=len(self.all_patient_list),
        )

        self.cat = get_cat(config_obj)

        # Only check/remove filters if we actually have a MedCAT model
        if self.cat is not None and not self.use_filter:
            removed_filters = []

            # Check and remove linking filters
            if hasattr(self.cat.config, "linking") and hasattr(
                self.cat.config.linking,
                "filters",
            ):
                if self.cat.config.linking.filters:
                    removed_filters.append(
                        f"linking.filters: {self.cat.config.linking.filters}",
                    )
                    self.cat.config.linking.filters = {}

            # Check and remove cuis_exclude
            if hasattr(self.cat.config, "linking") and hasattr(
                self.cat.config.linking,
                "filters",
            ):
                if hasattr(
                    self.cat.config.linking.filters,
                    "cuis",
                ) and self.cat.config.linking.filters.get("cuis"):
                    removed_filters.append(
                        f"cuis_exclude: {self.cat.config.linking.filters.get('cuis')}",
                    )
                    self.cat.config.linking.filters["cuis"] = set()

            # Check and remove filter_before_disamb
            if hasattr(self.cat.config, "linking") and hasattr(
                self.cat.config.linking,
                "filter_before_disamb",
            ):
                if self.cat.config.linking.filter_before_disamb:
                    removed_filters.append(
                        f"filter_before_disamb: {self.cat.config.linking.filter_before_disamb}",
                    )
                    self.cat.config.linking.filter_before_disamb = False

            # Alternative locations for CUI filters (depending on MedCAT version)
            if (
                hasattr(self.cat, "cdb")
                and hasattr(self.cat.cdb, "config")
                and hasattr(self.cat.cdb.config, "linking")
            ):
                if hasattr(
                    self.cat.cdb.config.linking,
                    "filters",
                ) and self.cat.cdb.config.linking.filters.get("cuis"):
                    removed_filters.append(
                        f"cdb.linking.filters.cuis: {self.cat.cdb.config.linking.filters.get('cuis')}",
                    )
                    self.cat.cdb.config.linking.filters["cuis"] = set()

            if removed_filters:
                logging.warning(
                    "Model has pre-existing filters. Since use_filter=False, the following filters are being removed:\n"
                    + "\n".join(f"  - {f}" for f in removed_filters),
                )
            else:
                logging.info(
                    "No pre-existing filters found in model. Processing all entities.",
                )

        self.n_pat_lines = config_obj.n_pat_lines

        if self.config_obj.prefetch_pat_batches:
            if self.config_obj.verbosity > 0:
                logging.info("Prefetching patient batches...")

            prefetch_batches(pat2vec_obj=self)

    def get_raw_drugs(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw drug data for a specific patient.

        Fetches medication records from the configured storage backend (database or file).

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing drug records for the patient, with columns such as
            drug name, dosage, administration time, and other relevant clinical information.

        """
        return retrieve_patient_data(
            patient_id,
            "drugs",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_bloods(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw blood test data for a specific patient.

        Fetches laboratory results including blood counts, chemistry panels, and other
        blood-related tests from the configured storage backend.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing blood test records with columns such as test name,
            result value, reference range, and collection time.

        """
        return retrieve_patient_data(
            patient_id,
            "bloods",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_epr_docs(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw EPR (Electronic Patient Record) documents for a specific patient.

        Fetches clinical notes and documents stored in the EPR system.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing EPR document records with columns such as document
            text, creation/update time, author, and document type.

        """
        return retrieve_patient_data(
            patient_id,
            "epr_docs",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_demographics(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw demographic data for a specific patient.

        Fetches patient identification information including name, date of birth,
        gender, and contact details.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing demographic records with columns such as patient ID,
            name, DOB, gender, address, and other identifying information.

        """
        return retrieve_patient_data(
            patient_id,
            "demographics",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_mct_docs(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw MCT (MedCAT-annotated) documents for a specific patient.

        Fetches clinical notes that have been processed and annotated using the
        MedCAT (Medical Concept Annotation Tool).

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing MCT document records with annotation data including
            identified concepts, CUIs, and entity context.

        """
        return retrieve_patient_data(
            patient_id,
            "mct_docs",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_textual_obs(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw textual observation data for a specific patient.

        Fetches textual observations from clinical records including narrative notes
        and free-text fields.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing textual observation records with columns such as
            observation text, category, and timestamp.

        """
        return retrieve_patient_data(
            patient_id,
            "textual_obs",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_reports(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw report data for a specific patient.

        Fetches clinical reports such as imaging reports, pathology reports, and
        other diagnostic summaries.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing report records with columns such as report text,
            report type, author, and timestamp.

        """
        return retrieve_patient_data(
            patient_id,
            "reports",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_diagnostics(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw diagnostic data for a specific patient.

        Fetches诊断记录 including diagnosis codes, descriptions, timestamps,
        and associated metadata.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing diagnostic records with columns such as diagnosis
            code, description, onset date, and status.

        """
        return retrieve_patient_data(
            patient_id,
            "diagnostics",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_news(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw NEWS (National Early Warning Score) data for a specific patient.

        Fetches vital signs observations used to calculate the NEWS score including
        respiratory rate, oxygen saturation, blood pressure, and other parameters.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing NEWS observation records with columns such as
            parameter name, value, timestamp, and clinician ID.

        """
        return retrieve_patient_data(
            patient_id,
            "news",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_bmi(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw BMI (Body Mass Index) data for a specific patient.

        Fetches height and weight measurements used to calculate BMI values.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing BMI records with columns such as measurement time,
            height, weight, calculated BMI value, and measure type.

        """
        return retrieve_patient_data(
            patient_id,
            "bmi",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_appointments(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw appointment/scheduling data for a specific patient.

        Fetches scheduled appointments including clinic visits, consultations,
        and procedure bookings.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing appointment records with columns such as appointment
            datetime, appointment type, provider, location, and status.

        """
        return retrieve_patient_data(
            patient_id,
            "appointments",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_covid(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw COVID-19 testing data for a specific patient.

        Fetches SARS-CoV-2 test results including PCR tests and antibody tests.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing COVID test records with columns such as test type,
            result, collection date, and test site.

        """
        return retrieve_patient_data(
            patient_id,
            "covid",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_smoking(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw smoking status data for a specific patient.

        Fetches tobacco use records indicating smoking status such as non-smoker,
        current smoker, or ex-smoker.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing smoking status records with columns such as
            smoking category, documentation time, and source.

        """
        return retrieve_patient_data(
            patient_id,
            "smoking",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_core_02(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw CORE_SpO2 (oxygen saturation) data for a specific patient.

        Fetches pulse oximetry observations tracking SpO2 levels.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing CORE_SpO2 records with columns such as oxygen
            saturation value, measurement time, and device.

        """
        return retrieve_patient_data(
            patient_id,
            "core_02",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_bed(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw bed assignment data for a specific patient.

        Fetches ward and bed location records tracking patient placements.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing bed assignment records with columns such as
            bed number, ward, admission time, and discharge time.

        """
        return retrieve_patient_data(
            patient_id,
            "bed",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_vte(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw VTE (Venous Thromboembolism) risk assessment data
        for a specific patient.

        Fetches VTE risk score records and prophylaxis recommendations.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing VTE assessment records with columns such as
            risk category, assessment time, and recommendation.

        """
        return retrieve_patient_data(
            patient_id,
            "vte_status",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_hospsite(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw hospital site/organization data for a specific patient.

        Fetches records indicating which hospital or site provided care.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing hospital site records with columns such as
            site name, site code, and time period.

        """
        return retrieve_patient_data(
            patient_id,
            "hosp_site",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_resus(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw resuscitation status data for a specific patient.

        Fetches advance care directive and resuscitation order records.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing resuscitation status records with columns such as
            code status, documentation time, and responsible clinician.

        """
        return retrieve_patient_data(
            patient_id,
            "core_resus",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_obs(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw general observation data for a specific patient.

        Fetches all observations not categorized into specific domains (bloods,
        drugs, diagnostics, etc.).

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing general observation records with various clinical
            measurements and assessment data.

        """
        return retrieve_patient_data(
            patient_id,
            "obs",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_epic_encounters(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw Epic encounters (admissions/visits) data for a specific patient.

        Fetches encounter records from the Epic electronic health record system
        including inpatient admissions, outpatient visits, and ER visits.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing encounter records with columns such as encounter
            type, admit/discharge time, location, and encounter number.

        """
        return retrieve_patient_data(
            patient_id,
            "epic_encounters",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_epic_encounters(self, patient_id: str) -> pd.DataFrame:
        """Retrieves Epic encounters (admissions/visits) data for a specific patient using direct search.

        Fetches encounter records from the Epic system via the search function.
        This method is useful when bypassing the database/file backend or when
        testing mode is enabled.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing encounter records with columns such as encounter
            type, admit/discharge time, location, and encounter number.
            Returns an empty DataFrame with proper columns if in testing mode and
            no data is found.

        """
        try:
            return search_epic_encounters(
                cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                patient_durable_keys=patient_id,
                id_field_name="activity_PatientDurableKey",
                config_obj=self.config_obj,
                t=self.t if hasattr(self, "t") else None,
            )
        except Exception:
            if self.config_obj.testing and not self.config_obj.testing_elastic:
                empty_df = pd.DataFrame(
                    columns=[
                        "activity_PatientDurableKey",
                        "activity_AdmissionDate",
                        "activity_DischargeDate",
                        "activity_Department",
                        "activity_Type",
                        "activity_VisitClass",
                        "activity_HospitalService",
                        "id",
                    ],
                )
                empty_df["activity_PatientDurableKey"] = [patient_id]
                return empty_df
            return pd.DataFrame()

    def get_raw_epic_clinical_notes(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw Epic clinical notes data for a specific patient.

        Fetches structured clinical notes from the Epic system including progress
        notes, consultation notes, and procedure notes.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing clinical note records with columns such as note
            text, note type, creation time, and author. If not found in database,
            attempts to fetch from Elasticsearch if available.

        """
        return retrieve_patient_data(
            patient_id,
            "epic_clinical_notes",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_epic_medical_history(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw Epic medical history data for a specific patient.

        Fetches structured medical history records from the Epic system including
        past diagnoses, procedures, and medications.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing medical history records with columns such as
            condition/procedure description, onset date, and source. If not found in
            database, attempts to fetch from Elasticsearch if available.

        """
        return retrieve_patient_data(
            patient_id,
            "epic_medical_history",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_epic_orders(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw Epic orders data for a specific patient.

        Fetches order records from the Epic system including medication orders,
        lab orders, and procedure orders.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing order records with columns such as order type,
            ordered item, order time, and status. If not found in database,
            attempts to fetch from Elasticsearch if available.

        """
        return retrieve_patient_data(
            patient_id,
            "epic_orders",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_epic_lab_results(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw Epic laboratory results data for a specific patient.

        Fetches lab result records from the Epic system including blood tests,
        urine tests, and other diagnostic laboratory results.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing lab result records with columns such as test name,
            result value, reference range, and collection time. If not found in
            database, attempts to fetch from Elasticsearch if available.

        """
        return retrieve_patient_data(
            patient_id,
            "epic_lab_results",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_epic_lab_results(self, patient_id: str) -> pd.DataFrame:
        """Retrieves Epic laboratory results data for a specific patient using direct search.

        Fetches lab result records from the Epic system via the search function.
        This method is useful when bypassing the database/file backend or when
        testing mode is enabled.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing lab result records with columns such as test name,
            result value, reference range, and collection time.
            Returns an empty DataFrame with proper columns if in testing mode and
            no data is found.

        """
        try:
            return search_epic_lab_results(
                cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                patient_durable_keys=patient_id,
                id_field_name="document_PatientDurableKey",
                config_obj=self.config_obj,
                t=self.t if hasattr(self, "t") else None,
            )
        except Exception:
            if self.config_obj.testing and not self.config_obj.testing_elastic:
                empty_df = pd.DataFrame(
                    columns=[
                        "document_PatientDurableKey",
                        "document_CreatedWhen",
                        "document_CollectedDate",
                        "document_UpdatedWhen",
                        "document_Name",
                        "document_Content",
                        "document_AbnormalLevel",
                        "document_LabResultEpicId",
                        "document_Fields.valueText",
                        "id",
                    ],
                )
                empty_df["document_PatientDurableKey"] = [patient_id]
                return empty_df
            return pd.DataFrame()

    def get_raw_epic_imaging_reports(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw Epic imaging reports data for a specific patient.

        Fetches imaging report records from the Epic system including radiology and
        other diagnostic imaging reports.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing imaging report records with columns such as report
            text, report type, creation time, and author. If not found in database,
            attempts to fetch from Elasticsearch if available.

        """
        return retrieve_patient_data(
            patient_id,
            "epic_imaging_reports",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_epic_clinical_notes_appointments(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw Epic clinical notes appointments data for a specific patient.

        Fetches clinical notes appointments records from the Epic system including
        scheduled appointments and related clinical notes.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing clinical notes appointments records with columns such as
            appointment datetime, appointment type, provider, location, and status.
            If not found in database, attempts to fetch from Elasticsearch if available.

        """
        return retrieve_patient_data(
            patient_id,
            "epic_clinical_notes_appointments",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_raw_epic_patients(self, patient_id: str) -> pd.DataFrame:
        """Retrieves raw Epic patients master data for a specific patient.

        Fetches patient identification and administrative information from the
        Epic system includingDemographics, contact information, and insurance details.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing patient master data with columns such as full name,
            date of birth, gender, address, phone number, and primary language.
            If not found in database, attempts to fetch from Elasticsearch if available.

        """
        return retrieve_patient_data(
            patient_id,
            "epic_patients",
            self.config_obj,
            cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
        )

    def get_epic_patients(self, patient_id: str) -> pd.DataFrame:
        """Retrieves Epic patients master data for a specific patient using direct search.

        Fetches patient identification and administrative information from the
        Epic system via the search function. This method is useful when bypassing
        the database/file backend or when testing mode is enabled.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing patient master data with columns such as full name,
            date of birth, gender, address, phone number, and primary language.
            Returns an empty DataFrame with proper columns if in testing mode and
            no data is found.

        """
        try:
            return search_epic_patients(
                cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                patient_durable_keys=patient_id,
                id_field_name="patient_DurableKey",
                config_obj=self.config_obj,
                t=self.t if hasattr(self, "t") else None,
            )
        except Exception:
            if self.config_obj.testing and not self.config_obj.testing_elastic:
                empty_df = pd.DataFrame(
                    columns=[
                        "patient_DurableKey",
                        "patient_BirthDate",
                        "patient_Age",
                        "patient_Gender",
                        "patient_Ethnicity",
                        "patient_SmokingStatus",
                        "patient_MaritalStatus",
                        "patient_IsCancer",
                        "patient_IsFetus",
                        "patient_DateOfDeath",
                        "id",
                    ],
                )
                empty_df["patient_DurableKey"] = [patient_id]
                return empty_df
            return pd.DataFrame()

    def get_all_features(self) -> pd.DataFrame:
        """Retrieves all patient features from the configured storage backend.

        This method loads the complete set of feature vectors that have been
        generated by the pat2vec pipeline for all patients in the cohort.

        If using database backend, reads the entire 'features' table.
        If using file backend, reads and concatenates all individual patient
        CSV files from the current_pat_lines_path directory.

        Returns:
            A DataFrame containing feature vectors for all patients with columns
            representing extracted features across the specified time windows.

        """
        return helper_functions.get_all_features(self.config_obj)

    def get_features(self, patient_id: str) -> pd.DataFrame:
        """Retrieves all feature vectors for a specific patient.

        This method loads feature vectors that have been generated by the pat2vec
        pipeline for the specified patient across their defined time windows.

        Args:
            patient_id: The unique identifier for the patient.

        Returns:
            A DataFrame containing feature vectors for the patient with columns
            representing extracted features across all time windows. May be empty
            if no features have been generated for this patient.

        """
        return helper_functions.get_df_from_db(
            self.config_obj,
            "features",
            (
                "features_features"
                if self.config_obj.storage_backend == "database"
                and hasattr(self.config_obj, "db_engine")
                and self.config_obj.db_engine.name == "sqlite"
                else "features"
            ),
            patient_ids=[patient_id],
            patient_id_column=self.config_obj.patient_id_column_name,
        )

    def _get_patient_data_batches(
        self,
        current_pat_client_id_code: str,
    ) -> dict[str, pd.DataFrame]:
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

        Debug logging: Logs epic_clinical_notes_annotations fetch status,
        row counts, columns, and MedCAT feature presence.

        """
        print("\n=== DEBUG _get_patient_data_batches START ===")
        print(f"Patient: {current_pat_client_id_code}")
        empty_return = pd.DataFrame()
        empty_return_epr = pd.DataFrame(columns=["updatetime", "body_analysed"])
        empty_return_mct = pd.DataFrame(
            columns=[
                "observationdocument_recordeddtm",
                "observation_valuetext_analysed",
            ],
        )
        empty_return_textual_obs = pd.DataFrame(
            columns=["basicobs_entered", "textualObs"],
        )
        empty_return_reports = pd.DataFrame(
            columns=["updatetime", "observation_valuetext_analysed"],
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
                "option": "covid",
                "var": "batch_covid",
                "func": get_pat_batch_obs,
                "args": {"search_term": SEARCH_TERM_ES},
                "empty": empty_return,
            },
            {
                "option": "news",
                "var": "batch_news",
                "func": get_pat_batch_news,
                "args": {},
                "empty": empty_return,
            },
            {
                "option": "bmi",
                "var": "batch_bmi",
                "func": get_pat_batch_bmi,
                "args": {},
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
            {
                "option": "epic_encounters",
                "var": "batch_epic_encounters",
                "func": search_epic_encounters,
                "args": {"id_field_name": "activity_PatientDurableKey"},
                "empty": empty_return,
                "id_arg": "patient_durable_keys",
            },
            {
                "option": "epic_lab_results",
                "var": "batch_epic_lab_results",
                "func": search_epic_lab_results,
                "args": {},
                "empty": empty_return,
                "id_arg": "patient_durable_keys",
            },
            {
                "option": "epic_patients",
                "var": "batch_epic_patients",
                "func": search_epic_patients,
                "args": {"id_field_name": "patient_DurableKey"},
                "empty": empty_return,
                "id_arg": "patient_durable_keys",
            },
            {
                "option": "epic_clinical_notes_appointments",
                "var": "batch_epic_clinical_notes_appointments",
                "func": search_epic_clinical_notes_appointments,
                "args": {},
                "empty": empty_return,
                "id_arg": "patient_durable_keys",
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
            {
                "option": "epic_clinical_notes_annotations",
                "var": "batch_epic_clinical_notes_annotations",
                "func": get_pat_batch_epic_clinical_notes_annotations,
                "empty": empty_return,  # Empty DataFrame for raw data table
            },
            {
                "option": "epic_medical_history_annotations",
                "var": "batch_epic_medical_history_annotations",
                "func": get_pat_batch_epic_medical_history_annotations,
                "empty": empty_return,  # Empty DataFrame for raw data table
            },
            {
                "option": "epic_imaging_reports_annotations",
                "var": "batch_epic_imaging_reports_annotations",
                "func": get_pat_batch_epic_imaging_reports_annotations,
                "empty": empty_return,  # Empty DataFrame for raw data table
            },
            {
                "option": "epic_orders_annotations",
                "var": "batch_epic_orders_annotations",
                "func": get_pat_batch_epic_orders_annotations,
                "empty": empty_return,  # Empty DataFrame for raw data table
            },
            {
                "option": "epic_clinical_notes_appointments_annotations",
                "var": "batch_epic_clinical_notes_appointments_annotations",
                "func": get_pat_batch_epic_clinical_notes_appointments_annotations,
                "empty": empty_return,  # Empty DataFrame for raw data table
            },
        ]

        batches = {}

        print(f"DEBUG: About to fetch {len(batch_configs)} standard batches")

        # Fetch standard batches
        for config in batch_configs:
            if self.config_obj.main_options.get(config["option"], True):
                id_arg_name = config.get("id_arg", "current_pat_client_id_code")
                call_kwargs = {
                    id_arg_name: (
                        [current_pat_client_id_code]
                        if id_arg_name == "patient_durable_keys"
                        else current_pat_client_id_code
                    ),
                    "config_obj": self.config_obj,
                    "cohort_searcher_with_terms_and_search": self.cohort_searcher_with_terms_and_search,
                    **config["args"],
                }
                if id_arg_name == "patient_durable_keys":
                    call_kwargs["output_filename"] = None

                res = config["func"](**call_kwargs)

                # Add debug logging for epic clinical notes
                if config["var"] in (
                    "batch_epic_clinical_notes",
                    "batch_epic_clinical_notes_annotations",
                ):
                    print(f"\n=== DEBUG {config['var']} ===")
                    print(f"Result type: {type(res)}")
                    print(f"Row count: {len(res) if res is not None else 'None'}")
                    if res is not None and not res.empty:
                        print(f"Columns: {res.columns.tolist()}")
                        print(f"Has pretty_name: {'pretty_name' in res.columns}")
                        print(f"Has cui: {'cui' in res.columns}")
                        if "pretty_name" in res.columns:
                            print(
                                f"Unique pretty_names: {res['pretty_name'].nunique()}",
                            )
                    else:
                        print("WARNING: Batch is None or empty!")
                    print("==========================================\n")

                id_col = (
                    config["args"].get("id_field_name", "document_PatientDurableKey")
                    if id_arg_name == "patient_durable_keys"
                    else "client_idcode"
                )
                if (
                    not res.empty
                    and id_col in res.columns
                    and id_col != "client_idcode"
                ):
                    res = res.rename(columns={id_col: "client_idcode"})
                batches[config["var"]] = res
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
                    cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                )
                # Handle cases where annotation functions might return None
                if batch_result is None:
                    if self.config_obj.verbosity > 2:
                        logging.debug(f"{config['var']} is empty")
                    batches[config["var"]] = config["empty"]
                else:
                    batches[config["var"]] = batch_result
            else:
                batches[config["var"]] = config["empty"]

        print("\n=== DEBUG _get_patient_data_batches END ===")

        return batches

    def _save_batches_to_db(
        self,
        patient_id: str,
        batches: dict[str, pd.DataFrame],
    ) -> None:
        """Saves fetched batches to the database if backend is enabled."""
        if self.config_obj.storage_backend != "database":
            return

        batch_to_table = {
            "batch_epr": ("raw_epr_docs", "client_idcode"),
            "batch_mct": ("raw_mct_docs", "client_idcode"),
            "batch_textual_obs_docs": ("raw_textual_obs", "client_idcode"),
            "batch_reports": ("raw_reports", "client_idcode"),
            "batch_bloods": ("raw_bloods", "client_idcode"),
            "batch_drugs": ("raw_drugs", "client_idcode"),
            "batch_diagnostics": ("raw_diagnostics", "client_idcode"),
            "batch_news": ("raw_news", "client_idcode"),
            "batch_bmi": ("raw_bmi", "client_idcode"),
            "batch_demo": ("raw_demographics", "client_idcode"),
            "batch_appointments": ("raw_appointments", "HospitalID"),
            "batch_covid": ("raw_covid", "client_idcode"),
            "batch_smoking": ("raw_smoking", "client_idcode"),
            "batch_core_02": ("raw_core_02", "client_idcode"),
            "batch_bednumber": ("raw_bed", "client_idcode"),
            "batch_vte": ("raw_vte", "client_idcode"),
            "batch_hospsite": ("raw_hospsite", "client_idcode"),
            "batch_resus": ("raw_resus", "client_idcode"),
            "batch_obs": ("raw_obs", "client_idcode"),
            "batch_epic_encounters": ("raw_epic_encounters", "client_idcode"),
            "batch_epic_clinical_notes": ("raw_epic_clinical_notes", "client_idcode"),
            "batch_epic_medical_history": ("raw_epic_medical_history", "client_idcode"),
            "batch_epic_orders": ("raw_epic_orders", "client_idcode"),
            "batch_epic_lab_results": ("raw_epic_lab_results", "client_idcode"),
            "batch_epic_patients": ("raw_epic_patients", "client_idcode"),
            "batch_epic_imaging_reports": ("raw_epic_imaging_reports", "client_idcode"),
            "batch_epic_clinical_notes_appointments": (
                "raw_epic_clinical_notes_appointments",
                "client_idcode",
            ),
            "batch_epic_clinical_notes_annotations": (
                "ann_epic_clinical_notes",
                "client_idcode",
            ),
            "batch_epic_medical_history_annotations": (
                "ann_epic_medical_history",
                "client_idcode",
            ),
            "batch_epic_orders_annotations": ("ann_epic_orders", "client_idcode"),
            "batch_epic_imaging_reports_annotations": (
                "ann_epic_imaging_reports",
                "client_idcode",
            ),
            "batch_epic_clinical_notes_appointments_annotations": (
                "ann_epic_clinical_notes_appointments",
                "client_idcode",
            ),
        }

        batch_to_option = {
            "batch_epr": "annotations",
            "batch_mct": "annotations_mrc",
            "batch_textual_obs_docs": "textual_obs",
            "batch_reports": "annotations_reports",
            "batch_bloods": "bloods",
            "batch_drugs": "drugs",
            "batch_diagnostics": "diagnostics",
            "batch_news": "news",
            "batch_bmi": "bmi",
            "batch_demo": "demo",
            "batch_appointments": "appointments",
            "batch_covid": "covid",
            "batch_smoking": "smoking",
            "batch_core_02": "core_02",
            "batch_bednumber": "bed",
            "batch_vte": "vte_status",
            "batch_hospsite": "hosp_site",
            "batch_resus": "core_resus",
            "batch_obs": "obs",
            "batch_epic_encounters": "epic_encounters",
            "batch_epic_clinical_notes": "epic_clinical_notes",
            "batch_epic_medical_history": "epic_medical_history",
            "batch_epic_orders": "epic_orders",
            "batch_epic_lab_results": "epic_lab_results",
            "batch_epic_patients": "epic_patients",
            "batch_epic_imaging_reports": "epic_imaging_reports",
            "batch_epic_clinical_notes_appointments": "epic_clinical_notes_appointments",
            "batch_epic_clinical_notes_annotations": "epic_clinical_notes_annotations",
            "batch_epic_medical_history_annotations": "epic_medical_history_annotations",
            "batch_epic_orders_annotations": "epic_orders_annotations",
            "batch_epic_imaging_reports_annotations": "epic_imaging_reports_annotations",
            "batch_epic_clinical_notes_appointments_annotations": "epic_clinical_notes_appointments_annotations",
        }

        for batch_key, (table_name, id_col) in batch_to_table.items():
            if batch_key not in batches:
                continue

            # Determine if the source is enabled via config
            option = batch_to_option.get(batch_key)
            is_enabled = (
                self.config_obj.main_options.get(option, True) if option else True
            )

            # For database backend, we need to handle two cases:
            # 1. Non-empty batch: Always save regardless of enabled status (has data)
            # 2. Empty batch: Only save if source is ENABLED (for table creation in testing mode)
            # Disabled sources should never have tables created.

            # The logic: save if (not empty) OR (enabled AND empty)
            # Simplifying: if empty and not enabled → skip, otherwise save
            if batches[batch_key].empty and not is_enabled:
                continue  # Skip disabled sources with empty data

            # All other cases: non-empty or enabled empty
            save_raw_patient_batch(
                batches[batch_key],
                patient_id,
                table_name,
                self.config_obj,
                id_column=id_col,
            )

    def _save_annotation_batches_to_db(
        self,
        patient_id: str,
        batches: dict[str, pd.DataFrame],
    ) -> None:
        """Saves annotation batches to the database if backend is enabled.

        This ensures that annotation tables are created with proper schema
        even when no annotations are generated (e.g., in testing mode).
        """
        if self.config_obj.storage_backend != "database":
            return

        from pat2vec.util.helper_functions import save_annotations_to_db
        from pat2vec.util.post_processing_annotations import EMPTY_ANNOT_COLS

        # Define table names and their corresponding option keys for ALL annotation sources
        annotation_configs = [
            {
                "table": "ann_epr_docs",
                "option": "annotations",
            },
            {
                "table": "ann_mct_docs",
                "option": "annotations_mrc",
            },
            {
                "table": "ann_textual_obs",
                "option": "textual_obs",
            },
            {
                "table": "ann_reports",
                "option": "annotations_reports",
            },
            {
                "table": "ann_epic_clinical_notes",
                "option": "epic_clinical_notes_annotations",
            },
            {
                "table": "ann_epic_medical_history",
                "option": "epic_medical_history_annotations",
            },
            {
                "table": "ann_epic_imaging_reports",
                "option": "epic_imaging_reports_annotations",
            },
            {
                "table": "ann_epic_orders",
                "option": "epic_orders_annotations",
            },
            {
                "table": "ann_epic_clinical_notes_appointments",
                "option": "epic_clinical_notes_appointments_annotations",
            },
        ]

        for config in annotation_configs:
            table_name = config.get("table")
            option = config.get("option")

            if not table_name or option is None:
                continue

            # Check if source option is enabled (default to True)
            is_enabled = self.config_obj.main_options.get(option, True)

            # Skip disabled sources
            if not is_enabled:
                continue

            empty_df = pd.DataFrame(columns=EMPTY_ANNOT_COLS)
            empty_df["client_idcode"] = patient_id

            try:
                save_annotations_to_db(
                    empty_df,
                    patient_id,
                    table_name,
                    self.config_obj,
                    id_column="client_idcode",
                )
            except Exception as e:
                logging.error(f"Failed to create annotation table {table_name}: {e}")

    def _setup_patient_time_window(
        self,
        current_pat_client_id_code: str,
    ) -> list[tuple] | None:
        """Sets up and returns the date list for a patient, handling IPW logic.

        If `individual_patient_window` is enabled, this method calculates a
        patient-specific date list. Otherwise, it returns the global date list.

        Args:
            current_pat_client_id_code: The patient's unique identifier.

        Returns:
            A list of date tuples, or None if the time window cannot be set up.

        """
        if self.config_obj.verbosity >= 4:
            logging.debug(
                "main_pat2vec>self.config_obj.individual_patient_window: %s",
                self.config_obj.individual_patient_window,
            )

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
                    logging.debug(
                        f"Control pat full {current_pat_client_id_code} ipw dates set:",
                    )
                    logging.debug("Start Date: %s", current_pat_start_date)
                    logging.debug("End Date: %s", current_pat_end_date)

            elif self.config_obj.individual_patient_window_controls_method == "random":
                # Select a random treatment's time window for application.
                patient_ids = list(self.config_obj.patient_dict.keys())
                if not patient_ids:
                    logging.warning(
                        "Warning: Cannot use 'random' control method with an empty patient_dict. Skipping.",
                    )
                    return None
                random_pat_id = random.choice(patient_ids)
                pat_dates = self.config_obj.patient_dict.get(random_pat_id)
                current_pat_start_date, current_pat_end_date = pat_dates
            else:
                logging.error(
                    f"Unknown control method: {self.config_obj.individual_patient_window_controls_method}",
                )
                return None
        else:  # It's a treatment patient
            if len(pat_dates) != 2:
                logging.warning(
                    f"Warning: Invalid dates for patient {current_pat_client_id_code}. Skipping.",
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
            logging.warning(
                f"Warning: Dates for patient {current_pat_client_id_code} are invalid. Skipping.",
            )
            return None

        # Determine anchor date for generation and clamping boundaries
        p_real_start, p_real_end = (
            min(
                current_pat_start_date,
                current_pat_end_date,
            ),
            max(current_pat_start_date, current_pat_end_date),
        )
        date_for_generate = p_real_end if self.config_obj.lookback else p_real_start

        # Override global dates as a workaround for generate_date_list
        self.config_obj.global_start_year = str(p_real_start.year).zfill(4)
        self.config_obj.global_start_month = str(p_real_start.month).zfill(2)
        self.config_obj.global_start_day = str(p_real_start.day).zfill(2)
        self.config_obj.global_end_year = str(p_real_end.year).zfill(4)
        self.config_obj.global_end_month = str(p_real_end.month).zfill(2)
        self.config_obj.global_end_day = str(p_real_end.day).zfill(2)
        # Update datetime objects to match the year/month/day components
        from datetime import datetime as dt_class

        self.config_obj.global_start_date = dt_class(
            int(self.config_obj.global_start_year),
            int(self.config_obj.global_start_month),
            int(self.config_obj.global_start_day),
        )
        self.config_obj.global_end_date = dt_class(
            int(self.config_obj.global_end_year),
            int(self.config_obj.global_end_month),
            int(self.config_obj.global_end_day),
            0,
            0,
            0,
        )
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
            logging.debug("ipw, datelist for %s", current_pat_client_id_code)
            logging.debug(date_list[0:5] if date_list else "date_list is empty")

        self.n_pat_lines = len(date_list)
        return date_list

    def _clean_document_batches(
        self,
        batches: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
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
                "key": "batch_textual_obs_annotations",
                "time_col": "basicobs_entered",
                "text_col": None,
                "option": "textual_obs",
            },
            {
                "key": "batch_reports_docs_annotations",
                "time_col": "updatetime",
                "text_col": None,
                "option": "annotations_reports",
            },
            {
                "key": "batch_epic_clinical_notes_annotations",
                "time_col": "updatetime",
                "text_col": None,
                "option": "epic_clinical_notes_annotations",
            },
            {
                "key": "batch_epic_orders_annotations",
                "time_col": "updatetime",
                "text_col": None,
                "option": "epic_orders_annotations",
            },
        ]

        for config in doc_configs:
            if self.config_obj.main_options.get(config["option"], True):
                batch = batches.get(config["key"])
                if batch is not None and not batch.empty:
                    time_col = config["time_col"]
                    text_col = config["text_col"]

                    if time_col not in batch.columns:
                        logging.warning(
                            f"Cleaning skipped for {config['key']}: column '{time_col}' missing.",
                        )
                        continue

                    try:
                        batch[time_col] = pd.to_datetime(
                            batch[time_col],
                            errors="coerce",
                            utc=True,
                        )
                        batch = batch.dropna(subset=[time_col])

                        if text_col and text_col in batch.columns:
                            batch = batch.dropna(subset=[text_col])
                            batch = batch[
                                batch[text_col].apply(lambda x: isinstance(x, str))
                            ]

                        batches[config["key"]] = batch
                    except Exception as e:
                        logging.error(f"Error cleaning batch {config['key']}: {e}")
                        logging.error(f"Batch type: {type(batch)}")
                        logging.error(f"Batch columns: {batch.columns}")

        if self.config_obj.verbosity > 3:
            logging.debug("Post-batch timestamp NaN drop counts:")
            logging.debug("EPR: %d", len(batches["batch_epr"]))
            logging.debug("MCT: %d", len(batches["batch_mct"]))
            logging.debug(
                "EPR annotations: %d",
                len(batches["batch_epr_docs_annotations"]),
            )
            logging.debug(
                "EPR annotations mct: %d",
                len(batches["batch_epr_docs_annotations_mct"]),
            )
            logging.debug(
                "textual obs docs: %d",
                len(batches["batch_textual_obs_docs"]),
            )
            logging.debug(
                "textual obs annotations: %d",
                len(batches["batch_textual_obs_annotations"]),
            )
            logging.debug(
                "batch_report_docs_annotations: %d",
                len(batches["batch_reports_docs_annotations"]),
            )

        return batches

    def _process_patient_slices(
        self,
        current_pat_client_id_code: str,
        date_list: list[tuple],
        batches: dict[str, pd.DataFrame],
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
                logging.info(
                    f"Patient {current_pat_client_id_code} already processed, skipping slice processing.",
                )
            return

        # The only_check_last logic from the original function is implicitly handled by this loop.
        for date_slice in date_list:
            try:
                if self.config_obj.verbosity > 5:
                    logging.debug(
                        f"Processing date {date_slice} for patient {current_pat_client_id_code}...",
                    )
                logging.debug(
                    f"DEBUG: _process_patient_slices: cohort_searcher_with_terms_and_search = {self.cohort_searcher_with_terms_and_search}",
                )

                if self.config_obj.calculate_vectors:
                    self.config_obj.last_lines = main_batch(
                        current_pat_client_id_code,
                        date_slice,
                        batches=batches,
                        config_obj=self.config_obj,
                        stripped_list_start=self.stripped_list_start,
                        t=self.t,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                        cat=self.cat,
                    )

                if self.config_obj.calculate_vectors:
                    if (
                        hasattr(self.config_obj, "last_lines")
                        and self.config_obj.last_lines is not None
                    ):
                        save_patient_features(
                            features_df=self.config_obj.last_lines,
                            patient_id=current_pat_client_id_code,
                            config_obj=self.config_obj,
                            overwrite=False,
                        )

            except Exception as e:
                logging.error(e)
                logging.error(
                    f"Exception in patmaker on {current_pat_client_id_code, date_slice}",
                )
                logging.error(traceback.format_exc())
                raise

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
        if i >= len(self.all_patient_list):
            logging.warning(
                f"Patient index {i} out of bounds (list size: {len(self.all_patient_list)}). Cannot process.",
            )
            return

        if self.config_obj.verbosity > 3:
            logging.debug(f"Processing patient {i} at {self.all_patient_list[i]}...")

        current_pat_client_id_code = str(self.all_patient_list[i])

        # Check if patient has already been processed
        if current_pat_client_id_code in self.stripped_list_start:
            if self.config_obj.verbosity >= 4:
                logging.debug(f"Patient {i} in stripped_list_start")
            if self.config_obj.multi_process is False:
                self.config_obj.skipped_counter += 1
            else:
                with self.config_obj.skipped_counter.get_lock():  # type: ignore
                    self.config_obj.skipped_counter.value += 1  # type: ignore
            if self.config_obj.verbosity > 0:
                logging.info(
                    f"Patient {current_pat_client_id_code} already processed, skipping.",
                )
            self.t.update(1)
            return

        if self.config_obj.storage_backend == "file":
            create_folders_for_pat(current_pat_client_id_code, self.config_obj)

        start_time = time.time()

        # 1. Set up time window for the patient
        date_list = self._setup_patient_time_window(current_pat_client_id_code)
        if date_list is None:
            self.t.update(1)
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

        # Create annotation tables before fetching batches to ensure they exist for get_df_from_db
        if self.config_obj.storage_backend == "database":
            self._save_annotation_batches_to_db(current_pat_client_id_code, {})

        batches = self._get_patient_data_batches(current_pat_client_id_code)

        # Save raw batches to DB if applicable
        if self.config_obj.storage_backend == "database":
            self._save_batches_to_db(current_pat_client_id_code, batches)

        update_pbar(
            current_pat_client_id_code,
            start_time,
            0,
            f"Done batches in {time.time() - start_time}",
            self.t,
            self.config_obj,
            self.config_obj.skipped_counter,
        )

        # 3. Clean document batches if required
        if self.config_obj.dropna_doc_timestamps:
            batches = self._clean_document_batches(batches)

        # Clear existing data for patient if using database backend
        if self.config_obj.storage_backend == "database":
            clear_patient_features(current_pat_client_id_code, self.config_obj)

        logging.info(
            f"Processing {len(date_list)} time slices for patient {current_pat_client_id_code}",
        )

        # 4. Process patient data in time slices
        self._process_patient_slices(current_pat_client_id_code, date_list, batches)

        # 5. Finalize
        if self.config_obj.remote_dump:
            if self.sftp_client:
                self.sftp_client.close()
