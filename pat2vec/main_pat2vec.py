import logging
import os
import random
import sys
import time
import traceback
from datetime import datetime
from multiprocessing import Pool
import pandas as pd
from pat2vec.util.cogstack_v8_lite import *
from colorama import Back, Fore, Style
from credentials import *
from tqdm import trange

from pat2vec.pat2vec_get_methods.current_pat_annotations_to_file import (
    get_current_pat_annotations_batch_to_file,
    get_current_pat_annotations_mct_batch_to_file,
)
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
    create_folders,
    create_folders_for_pat,
    filter_stripped_list,
    generate_date_list,
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
    def __init__(
        self,
        cogstack=True,
        use_filter=False,
        json_filter_path=None,
        random_seed_val=42,
        hostname=None,
        config_obj=None,
    ):

        # Additional parameters
        self.aliencat = config_obj.aliencat
        self.dgx = config_obj.dgx
        self.dhcap = config_obj.dhcap
        self.dhcap02 = config_obj.dhcap02
        self.batch_mode = config_obj.batch_mode
        self.remote_dump = config_obj.remote_dump
        self.negated_presence_annotations = config_obj.negated_presence_annotations
        self.store_annot = config_obj.store_annot
        self.share_sftp = config_obj.share_sftp
        self.multi_process = config_obj.multi_process
        self.annot_first = config_obj.annot_first
        self.strip_list = config_obj.strip_list
        self.verbosity = config_obj.verbosity
        self.random_seed_val = config_obj.random_seed_val
        # self.treatment_client_id_list = config_obj.treatment_client_id_list
        self.hostname = config_obj.hostname

        self.config_obj = config_obj

        self.cs = cs  # make cogstack v8 cs object available

        individual_patient_window_controls_method = (
            config_obj.individual_patient_window_controls_method
        )

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
        self.gpu_mem_threshold = config_obj.gpu_mem_threshold

        self.all_patient_list = get_all_patients_list(self.config_obj)

        # create_folders(self.all_patient_list, self.config_obj)

        self.current_pat_lines_path = config_obj.current_pat_lines_path
        self.sftp_client = config_obj.sftp_obj

        if cogstack == True:

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

        self.use_filter = use_filter

        if self.use_filter:
            self.json_filter_path = json_filter_path
            import json

            with open(self.json_filter_path, "r") as f:
                json_data = json.load(f)

            len(json_data["projects"][0])
            json_cuis = json_data["projects"][0]["cuis"].split(",")
            self.cat.cdb.filter_by_cui(json_cuis)

        if not (self.dhcap) and not (self.dhcap02):

            gpu_index, free_mem = get_free_gpu()

        else:
            gpu_index, free_mem = -1, self.gpu_mem_threshold - 1

        if int(free_mem) > self.gpu_mem_threshold:
            os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_index)
            print(f"Setting gpu with {free_mem} free")
        else:
            print(f"Setting NO gpu, most free memory: {free_mem} !")
            os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

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

        # random.shuffle(self.all_patient_list)

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

        self.stripped_list, self.stripped_list_start = filter_stripped_list(
            self.stripped_list, config_obj=self.config_obj
        )

        # self.date_list = config_obj.date_list

        self.n_pat_lines = config_obj.n_pat_lines

    # ------------------------------------begin main----------------------------------

    def pat_maker(self, i):

        if self.config_obj.verbosity > 3:
            print(f"Processing patient {i} at {self.all_patient_list[i]}...")

        skipped_counter = self.config_obj.skipped_counter
        stripped_list = self.stripped_list
        all_patient_list = self.all_patient_list
        skipped_counter = self.config_obj.skipped_counter

        remote_dump = self.config_obj.remote_dump
        hostname = self.config_obj.hostname
        username = self.config_obj.username
        password = self.config_obj.password
        annot_first = self.config_obj.annot_first

        stripped_list_start = self.stripped_list_start

        date_list = self.config_obj.date_list
        if self.config_obj.verbosity > 3:
            print("Date list> pat_maker")
            print(date_list[0:5])

        multi_process = self.config_obj.multi_process

        if skipped_counter == None:
            skipped_counter = 0

        current_pat_client_id_code = str(all_patient_list[i])

        # create_folders(list(current_pat_client_id_code), self.config_obj)

        create_folders_for_pat(current_pat_client_id_code, self.config_obj)

        p_bar_entry = current_pat_client_id_code

        if self.config_obj.individual_patient_window:

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

            else:

                # Pat is in patient_dict.keys...

                current_pat_start_date = self.config_obj.patient_dict.get(
                    all_patient_list[i]
                )[0]

                if (
                    type(self.config_obj.patient_dict.get(all_patient_list[i])[1])
                    != "datetime64[ns]"
                ):
                    try:
                        # print("forcing end date datetime")
                        current_pat_end_date = pd.to_datetime(
                            self.config_obj.patient_dict.get(all_patient_list[i])[1],
                            # infer_datetime_format=True,
                            # format="%d/%m/%y %H.%M.%S",
                            #    errors="coerce",
                        )

                        # current_pat_end_date = pd.to_datetime(
                        #     self.config_obj.patient_dict.get(all_patient_list[i])[1],
                        #     errors="coerce",
                        #     utc=True,
                        # )
                    except Exception as e:
                        print("failed force end date dt")
                        print(e)

                if (
                    type(self.config_obj.patient_dict.get(all_patient_list[i])[0])
                    != "datetime64[ns]"
                ):
                    try:
                        # print("forcing start date datetime")
                        current_pat_start_date = pd.to_datetime(
                            self.config_obj.patient_dict.get(all_patient_list[i])[0],
                            # infer_datetime_format=True,
                            # format="%d/%m/%y %H.%M.%S",
                            #    errors="coerce",
                        )

                        # current_pat_end_date = pd.to_datetime(
                        #     self.config_obj.patient_dict.get(all_patient_list[i])[1],
                        #     errors="coerce",
                        #     utc=True,
                        # )
                    except Exception as e:
                        print("failed force start date dt")
                        print(e)

                else:
                    # print("pats datetime end is is not datetime64[ns]")
                    current_pat_end_date = self.config_obj.patient_dict.get(
                        all_patient_list[i]
                    )[1]

                # print("Debug date routine:")
                # print("current_pat_start_date", current_pat_start_date)
                # print("current_pat_end_date", current_pat_end_date)
                # print(self.config_obj.patient_dict.get(all_patient_list[i])[0])
                # print(self.config_obj.patient_dict.get(all_patient_list[i])[1])

            self.config_obj.global_start_month = current_pat_start_date.month

            self.config_obj.global_start_year = current_pat_start_date.year

            self.config_obj.global_end_month = current_pat_end_date.month

            self.config_obj.global_end_year = current_pat_end_date.year

            self.config_obj.global_start_day = current_pat_start_date.day

            self.config_obj.global_end_day = current_pat_end_date.day

            self.config_obj.start_date = current_pat_start_date

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

            if self.config_obj.lookback:
                date_for_generate = current_pat_end_date
            else:
                date_for_generate = current_pat_start_date
            if self.config_obj.verbosity >= 4:

                print("date for generate", date_for_generate)

            self.config_obj.date_list = generate_date_list(
                date_for_generate,
                self.config_obj.years,
                self.config_obj.months,
                self.config_obj.days,
                interval_window_delta,
                config_obj=self.config_obj,
                # verbose=bool(self.config_obj.verbosity>0)
            )
            if self.config_obj.verbosity >= 4:
                print("overwriting temp datetime with ipw")
            date_list = self.config_obj.date_list

            if self.config_obj.verbosity >= 4:
                print(self.config_obj.date_list)

            self.n_pat_lines = len(self.config_obj.date_list)

            if self.config_obj.verbosity >= 4:
                print("ipw, datelist", current_pat_client_id_code)
                print(self.config_obj.date_list[0:5])

        start_time = time.time()

        if self.config_obj.verbosity >= 4:
            print("pat maker called: opts: ", self.config_obj.main_options)

        update_pbar(
            p_bar_entry,
            start_time,
            0,
            f"Pat_maker called on {i}...",
            self.t,
            self.config_obj,
            skipped_counter,
        )

        sftp_obj = self.config_obj.sftp_obj

        # get_pat batches

        stripped_list = stripped_list_start.copy()

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

            empty_return = pd.DataFrame()

            empty_return_epr = pd.DataFrame(columns=["updatetime", "body_analysed"])

            empty_return_mct = pd.DataFrame(
                columns=[
                    "observationdocument_recordeddtm",
                    "observation_valuetext_analysed",
                ]
            )
            empty_return_textual_obs = pd.DataFrame(
                columns=[
                    "basicobs_entered",
                    "textualObs",
                ]
            )

            empty_return_reports = pd.DataFrame(
                columns=["updatetime", "observation_valuetext_analysed"]
            )

            if self.config_obj.main_options.get("annotations", True):
                search_term = None  # inside function
                batch_epr = get_pat_batch_epr_docs(
                    current_pat_client_id_code=current_pat_client_id_code,
                    search_term=search_term,
                    config_obj=self.config_obj,
                    cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                )
            else:
                batch_epr = empty_return_epr

            if self.config_obj.main_options.get("annotations_mrc", True):
                search_term = None  # inside function
                batch_mct = get_pat_batch_mct_docs(
                    current_pat_client_id_code,
                    search_term,
                    config_obj=self.config_obj,
                    cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                )
            else:
                batch_mct = empty_return_mct

            if self.config_obj.main_options.get("textual_obs", True):
                search_term = None  # inside function
                batch_textual_obs_docs = get_pat_batch_textual_obs_docs(
                    current_pat_client_id_code,
                    search_term,
                    config_obj=self.config_obj,
                    cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                )
            else:
                batch_textual_obs_docs = empty_return_textual_obs

            if self.config_obj.main_options.get("annotations_reports", True):
                search_term = None  # inside function
                batch_reports = get_pat_batch_reports(
                    current_pat_client_id_code=current_pat_client_id_code,
                    search_term=search_term,
                    config_obj=self.config_obj,
                    cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                )
            else:
                batch_reports = empty_return_reports

            if not annot_first:

                if self.config_obj.main_options.get("smoking", True):
                    search_term = "CORE_SmokingStatus"
                    batch_smoking = get_pat_batch_obs(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_smoking = empty_return

                if self.config_obj.main_options.get("core_02", True):

                    search_term = "CORE_SpO2"
                    batch_core_02 = get_pat_batch_obs(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_core_02 = empty_return

                if self.config_obj.main_options.get("bed", True):
                    search_term = "CORE_BedNumber3"
                    batch_bednumber = get_pat_batch_obs(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_bednumber = empty_return

                if self.config_obj.main_options.get("vte_status", True):
                    search_term = "CORE_VTE_STATUS"
                    batch_vte = get_pat_batch_obs(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_vte = empty_return

                if self.config_obj.main_options.get("hosp_site", True):
                    search_term = "CORE_HospitalSite"
                    batch_hospsite = get_pat_batch_obs(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_hospsite = empty_return

                if self.config_obj.main_options.get("core_resus", True):
                    search_term = "CORE_RESUS_STATUS"
                    batch_resus = get_pat_batch_obs(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_resus = empty_return

                if self.config_obj.main_options.get("news", True):
                    search_term = None  # inside function
                    batch_news = get_pat_batch_news(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_news = empty_return

                if self.config_obj.main_options.get("bmi", True):
                    search_term = None  # inside function
                    batch_bmi = get_pat_batch_bmi(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )

                else:
                    batch_bmi = empty_return

                if self.config_obj.main_options.get("diagnostics", True):
                    search_term = None  # inside function
                    batch_diagnostics = get_pat_batch_diagnostics(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_diagnostics = empty_return

                if self.config_obj.main_options.get("drugs", True):
                    search_term = None  # inside function
                    batch_drugs = get_pat_batch_drugs(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_drugs = empty_return

                if self.config_obj.main_options.get("demo", True):
                    search_term = None  # inside function
                    batch_demo = get_pat_batch_demo(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_demo = empty_return

                if self.config_obj.main_options.get("bloods", True):
                    search_term = None  # inside function
                    batch_bloods = get_pat_batch_bloods(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_bloods = empty_return

                if self.config_obj.main_options.get("appointments", True):

                    batch_appointments = get_pat_batch_appointments(
                        current_pat_client_id_code,
                        search_term,
                        config_obj=self.config_obj,
                        cohort_searcher_with_terms_and_search=self.cohort_searcher_with_terms_and_search,
                    )
                else:
                    batch_appointments = empty_return

            if self.config_obj.main_options.get("annotations", True):

                batch_epr_docs_annotations = get_pat_batch_epr_docs_annotations(
                    current_pat_client_id_code,
                    config_obj=self.config_obj,
                    cat=self.cat,
                    t=self.t,
                )

                if type(batch_epr_docs_annotations) == None:
                    if self.config_obj.verbosity > 2:
                        print(f"batch_epr_docs_annotations empty")
                    batch_epr_docs_annotations = (
                        empty_return_epr  # catch, was empty_return
                    )
            else:
                batch_epr_docs_annotations = empty_return_epr

            if self.config_obj.main_options.get("annotations_mrc", True):

                batch_epr_docs_annotations_mct = get_pat_batch_mct_docs_annotations(
                    current_pat_client_id_code,
                    config_obj=self.config_obj,
                    cat=self.cat,
                    t=self.t,
                )

                if type(batch_epr_docs_annotations_mct) == None:
                    if self.config_obj.verbosity > 2:
                        print(f"batch_epr_docs_annotations_mct empty")

            else:
                batch_epr_docs_annotations_mct = empty_return_mct

            if self.config_obj.main_options.get("textual_obs", True):

                batch_textual_obs_annotations = get_pat_batch_textual_obs_annotations(
                    current_pat_client_id_code,
                    config_obj=self.config_obj,
                    cat=self.cat,
                    t=self.t,
                )
            else:
                batch_textual_obs_annotations = empty_return_textual_obs

            if self.config_obj.main_options.get("annotations_reports", True):

                batch_reports_docs_annotations = get_pat_batch_reports_docs_annotations(
                    current_pat_client_id_code,
                    config_obj=self.config_obj,
                    cat=self.cat,
                    t=self.t,
                )

                if type(batch_reports_docs_annotations) == None:
                    if self.config_obj.verbosity > 2:
                        print(f"batch_reports_docs_annotations empty")
                    batch_reports_docs_annotations = empty_return
            else:
                batch_reports_docs_annotations = empty_return_reports

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
                # ... existing code ...

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
                # batch_epr_docs_annotations.dropna(subset=['body_analysed'], inplace=True)

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
                        if annot_first:

                            get_current_pat_annotations_batch_to_file(
                                all_patient_list[i],
                                date_list[j],
                                batch_epr,
                                sftp_obj,
                                skip_check=skip_check,
                            )

                            get_current_pat_annotations_mct_batch_to_file(
                                all_patient_list[i],
                                date_list[j],
                                batch_mct,
                                sftp_obj,
                                skip_check=skip_check,
                            )

                        else:
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
                                    batch_textual_obs_annotations=batch_textual_obs_annotations,
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
