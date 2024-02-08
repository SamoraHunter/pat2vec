from pat2vec.pat2vec_get_methods.current_pat_annotations_batch_to_file import get_current_pat_annotations_batch_to_file
from pat2vec.pat2vec_get_methods.current_pat_annotations_mrc_batch_to_file import get_current_pat_annotations_mct_batch_to_file
from pat2vec.pat2vec_get_methods.current_pat_annotations_to_file import get_current_pat_annotations_to_file
from pat2vec.pat2vec_get_methods.current_pat_annotations_to_file import get_current_pat_annotations_mct_batch_to_file
from pat2vec.pat2vec_get_methods.current_pat_annotations_to_file import get_current_pat_annotations_batch_to_file
from pat2vec.pat2vec_get_methods.get_method_bed import get_bed
from pat2vec.pat2vec_get_methods.get_method_bloods import get_current_pat_bloods
from pat2vec.pat2vec_get_methods.get_method_bmi import get_bmi_features
from pat2vec.pat2vec_get_methods.get_method_core02 import get_core_02
from pat2vec.pat2vec_get_methods.get_method_core_resus import get_core_resus
from pat2vec.pat2vec_get_methods.get_method_current_pat_annotations_mrc_cs import get_current_pat_annotations_mrc_cs
from pat2vec.pat2vec_get_methods.get_method_demo import get_demographics3
from pat2vec.pat2vec_get_methods.get_method_demographics import get_demo
from pat2vec.pat2vec_get_methods.get_method_diagnostics import get_current_pat_diagnostics
from pat2vec.pat2vec_get_methods.get_method_drugs import get_current_pat_drugs
from pat2vec.pat2vec_get_methods.get_method_hosp_site import get_hosp_site
from pat2vec.pat2vec_get_methods.get_method_news import get_news
from pat2vec.pat2vec_get_methods.get_method_pat_annotations import get_current_pat_annotations
from pat2vec.pat2vec_get_methods.get_method_smoking import get_smoking
from pat2vec.pat2vec_get_methods.get_method_vte_status import get_vte_status
from pat2vec.pat2vec_main_methods.main_annotate_only import main_annotate_only
from pat2vec.pat2vec_main_methods.main_batch import main_batch
from pat2vec.pat2vec_main_methods.main_main import main
from pat2vec.pat2vec_main_methods.main_multi import main_multi
from pat2vec.pat2vec_main_methods.main_single_pat import get_single_pat
from pat2vec.pat2vec_pat_list.get_patient_treatment_list import extract_treatment_id_list_from_docs
from pat2vec.pat2vec_pat_list.get_patient_treatment_list import generate_control_list
from pat2vec.pat2vec_pat_list.get_patient_treatment_list import get_all_patients_list
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_obs
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_news
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_bmi
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_bloods
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_drugs
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_diagnostics
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_epr_docs
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_epr_docs_annotations
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_mct_docs_annotations
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_mct_docs
from pat2vec.patvec_get_batch_methods.main import get_pat_batch_demo
# from pat2vec.util.cogstack_v8_lite import __init__
# from pat2vec.util.cogstack_v8_lite import check_api_auth_details
# from pat2vec.util.cogstack_v8_lite import _check_auth_details
# from pat2vec.util.cogstack_v8_lite import get_docs_generator
# from pat2vec.util.cogstack_v8_lite import cogstack2df
# from pat2vec.util.cogstack_v8_lite import DataFrame
# from pat2vec.util.cogstack_v8_lite import list_chunker
# from pat2vec.util.cogstack_v8_lite import cohort_searcher_with_terms_and_search
# from pat2vec.util.cogstack_v8_lite import catch
# from pat2vec.util.cogstack_v8_lite import cohort_searcher_with_terms_no_search
# from pat2vec.util.cogstack_v8_lite import set_index_safe_wrapper
# from pat2vec.util.cogstack_v8_lite import cohort_searcher_no_terms_fuzzy
# from pat2vec.util.cogstack_v8_lite import cohort_searcher_with_terms_no_search
# from pat2vec.util.cogstack_v8_lite import cohort_searcher_no_terms
# from pat2vec.util.cogstack_v8_lite import nearest
# from pat2vec.util.cogstack_v8_lite import matcher
# from pat2vec.util.cogstack_v8_lite import stringlist2searchlist
# from pat2vec.util.cogstack_v8_lite import pylist2searchlist
# from pat2vec.util.cogstack_v8_lite import stringlist2pylist
# from pat2vec.util.cogstack_v8_lite import date_cleaner
# from pat2vec.util.cogstack_v8_lite import bulk_str_findall
# from pat2vec.util.cogstack_v8_lite import bulk_str_extract
# from pat2vec.util.cogstack_v8_lite import without_keys
# from pat2vec.util.cogstack_v8_lite import bulk_str_extract_round_robin
# from pat2vec.util.cogstack_v8_lite import appendAge
# from pat2vec.util.cogstack_v8_lite import age
# from pat2vec.util.cogstack_v8_lite import appendAgeAtRecord
# from pat2vec.util.cogstack_v8_lite import ageAtRecord
# from pat2vec.util.cogstack_v8_lite import append_age_at_record_series
# from pat2vec.util.cogstack_v8_lite import age_at_record
# from pat2vec.util.cogstack_v8_lite import append_age_at_record_series
# from pat2vec.util.cogstack_v8_lite import age_at_record
# from pat2vec.util.cogstack_v8_lite import age_at_record
# from pat2vec.util.cogstack_v8_lite import df_column_uniquify
# from pat2vec.util.cogstack_v8_lite import find_date
# from pat2vec.util.cogstack_v8_lite import split_clinical_notes
# from pat2vec.util.cogstack_v8_lite import get_demographics
# from pat2vec.util.cogstack_v8_lite import get_demographics2
# from pat2vec.util.cogstack_v8_lite import pull_and_write
# from pat2vec.util.cogstack_v8_lite import cohort_searcher_with_terms_and_search_multi
# from pat2vec.util.cogstack_v8_lite import iterative_multi_term_cohort_searcher_no_terms_fuzzy
from pat2vec.util.config_pat2vec import __init__
from pat2vec.util.config_pat2vec import calculate_interval
from pat2vec.util.config_pat2vec import update_global_start_date
from pat2vec.util.current_pat_batch_path_methods import __init__
# from pat2vec.util.current_pat_batch_path_methods import _create_directories
# from pat2vec.util.current_pat_batch_path_methods import _print_paths
from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch
# from pat2vec.util.ethnicity_abstractor import abstractEthnicity
# from pat2vec.util.evaluation_methods import compare_ipw_annotation_rows
# from pat2vec.util.evaluation_methods import create_profile_reports
# from pat2vec.util.evaluation_methods_ploting import generate_pie_charts
from pat2vec.util.get_dummy_data_cohort_searcher import generate_epr_documents_data
from pat2vec.util.get_dummy_data_cohort_searcher import generate_epr_documents_personal_data
from pat2vec.util.get_dummy_data_cohort_searcher import generate_observations_data
from pat2vec.util.get_dummy_data_cohort_searcher import generate_basic_observations_data
from pat2vec.util.get_dummy_data_cohort_searcher import extract_date_range
from pat2vec.util.get_dummy_data_cohort_searcher import cohort_searcher_with_terms_and_search_dummy
from pat2vec.util.methods_annotation import check_pat_document_annotation_complete
from pat2vec.util.methods_annotation import parse_meta_anns
from pat2vec.util.methods_annotation import get_pat_document_annotation_batch
from pat2vec.util.methods_annotation import get_pat_document_annotation_batch_mct
from pat2vec.util.methods_annotation import annot_pat_batch_docs
from pat2vec.util.methods_annotation import multi_annots_to_df
from pat2vec.util.methods_annotation import multi_annots_to_df_mct
from pat2vec.util.methods_annotation import json_to_dataframe
from pat2vec.util.methods_annotation import filter_annot_dataframe
from pat2vec.util.methods_annotation import calculate_pretty_name_count_features
from pat2vec.util.methods_get import list_dir_wrapper
from pat2vec.util.methods_get import get_start_end_year_month
from pat2vec.util.methods_get import get_empty_date_vector
from pat2vec.util.methods_get import convert_timestamp_to_tuple
from pat2vec.util.methods_get import enum_target_date_vector
from pat2vec.util.methods_get import generate_date_list
from pat2vec.util.methods_get import filter_dataframe_by_timestamp
from pat2vec.util.methods_get import dump_results
from pat2vec.util.methods_get import update_pbar
from pat2vec.util.methods_get import get_demographics3_batch
from pat2vec.util.methods_get import list_dir_wrapper
from pat2vec.util.methods_get import get_free_gpu
from pat2vec.util.methods_get import method1
from pat2vec.util.methods_get import method2
from pat2vec.util.methods_get import __str__
from pat2vec.util.methods_get import list_dir_wrapper
from pat2vec.util.methods_get import convert_date
from pat2vec.util.methods_get import get_start_end_year_month
from pat2vec.util.methods_get import get_empty_date_vector
from pat2vec.util.methods_get import get_demographics3_batch
from pat2vec.util.methods_get import sftp_exists
from pat2vec.util.methods_get import list_dir_wrapper
from pat2vec.util.methods_get import exist_check
from pat2vec.util.methods_get import check_sftp_connection
from pat2vec.util.methods_get import get_free_gpu
from pat2vec.util.methods_get import method1
from pat2vec.util.methods_get import method2
from pat2vec.util.methods_get import __str__
from pat2vec.util.methods_get import write_remote
from pat2vec.util.methods_get import write_csv_wrapper
from pat2vec.util.methods_get import read_remote
from pat2vec.util.methods_get import read_csv_wrapper
from pat2vec.util.methods_get import create_local_folders
from pat2vec.util.methods_get import create_remote_folders
from pat2vec.util.methods_get import create_folders_annot_csv_wrapper
from pat2vec.util.methods_get import filter_stripped_list
from pat2vec.util.methods_get import create_folders
from pat2vec.util.methods_get import create_folders
from pat2vec.util.methods_get import create_folders_for_pat
from pat2vec.util.methods_get import convert_date
from pat2vec.util.methods_get import add_offset_column
# from pat2vec.util.methods_get import apply_offset
from pat2vec.util.methods_get import build_patient_dict
from pat2vec.util.methods_get_medcat import get_cat
from pat2vec.util.methods_post_get import retrieve_pat_annotations
from pat2vec.util.methods_post_get import copy_project_folders_with_substring_match
from pat2vec.util.methods_post_get import check_csv_integrity
from pat2vec.util.methods_post_get import check_csv_files_in_directory
from pat2vec.util.post_processing import count_files
from pat2vec.util.post_processing import process_csv_files
from pat2vec.util.post_processing import extract_datetime_to_column
from pat2vec.util.post_processing import filter_annot_dataframe2
from pat2vec.util.post_processing import produce_filtered_annotation_dataframe
from pat2vec.util.post_processing import extract_types_from_csv
from pat2vec.util.post_processing import remove_file_from_paths
from pat2vec.util.post_processing import process_chunk
from pat2vec.util.post_processing import process_csv_files_multi
from pat2vec.util.post_processing import join_icd10_codes_to_annot
from pat2vec.util.post_processing import join_icd10_OPC4S_codes_to_annot
from pat2vec.util.post_processing import filter_and_select_rows
from pat2vec.util.post_processing import filter_dataframe_by_cui
from pat2vec.util.post_processing import copy_files_and_dirs
from pat2vec.util.post_processing import get_pat_ipw_record
from pat2vec.util.post_processing import filter_and_update_csv
from pat2vec.util.post_processing import build_ipw_dataframe
from pat2vec.util.post_processing import retrieve_pat_annots_mct_epr
from pat2vec.util.post_processing import retrieve_pat_docs_mct_epr
from pat2vec.util.post_processing import check_list_presence
from pat2vec.util.post_processing import filter_dataframe_n_lists
from pat2vec.util.post_processing import get_all_target_annots
from pat2vec.util.post_processing import build_merged_epr_mct_annot_df
from pat2vec.util.post_processing_build_methods import filter_annot_dataframe
# from pat2vec.util.post_processing_build_methods import filter_float_column
from pat2vec.util.post_processing_build_methods import build_merged_epr_mct_annot_df
from pat2vec.util.post_processing_build_methods import build_merged_epr_mct_doc_df
from pat2vec.util.post_processing_build_methods import join_docs_to_annots
from pat2vec.util.post_processing_build_methods import get_annots_joined_to_docs
from pat2vec.util.presentation_methods import group_images_by_suffix
from pat2vec.util.presentation_methods import create_powerpoint_slides_client_idcode_groups
from pat2vec.util.presentation_methods import create_powerpoint_from_images_group
from pat2vec.util.presentation_methods import create_powerpoint_slides
from pat2vec.util.presentation_methods import create_powerpoint_from_images


class pat2vec_methods():
    def __init__(self):

        self.get_current_pat_annotations_batch_to_file = get_current_pat_annotations_batch_to_file
        self.get_current_pat_annotations_mct_batch_to_file = get_current_pat_annotations_mct_batch_to_file
        self.get_current_pat_annotations_to_file = get_current_pat_annotations_to_file
        self.get_current_pat_annotations_mct_batch_to_file = get_current_pat_annotations_mct_batch_to_file
        self.get_current_pat_annotations_batch_to_file = get_current_pat_annotations_batch_to_file
        self.get_bed = get_bed
        self.get_current_pat_bloods = get_current_pat_bloods
        self.get_bmi_features = get_bmi_features
        self.get_core_02 = get_core_02
        self.get_core_resus = get_core_resus
        self.get_current_pat_annotations_mrc_cs = get_current_pat_annotations_mrc_cs
        self.get_demographics3 = get_demographics3
        self.get_demo = get_demo
        self.get_current_pat_diagnostics = get_current_pat_diagnostics
        self.get_current_pat_drugs = get_current_pat_drugs
        self.get_hosp_site = get_hosp_site
        self.get_news = get_news
        self.get_current_pat_annotations = get_current_pat_annotations
        self.get_smoking = get_smoking
        self.get_vte_status = get_vte_status
        self.main_annotate_only = main_annotate_only
        self.main_batch = main_batch
        self.main = main
        self.main_multi = main_multi
        self.get_single_pat = get_single_pat
        self.extract_treatment_id_list_from_docs = extract_treatment_id_list_from_docs
        self.generate_control_list = generate_control_list
        self.get_all_patients_list = get_all_patients_list
        self.get_pat_batch_obs = get_pat_batch_obs
        self.get_pat_batch_news = get_pat_batch_news
        self.get_pat_batch_bmi = get_pat_batch_bmi
        self.get_pat_batch_bloods = get_pat_batch_bloods
        self.get_pat_batch_drugs = get_pat_batch_drugs
        self.get_pat_batch_diagnostics = get_pat_batch_diagnostics
        self.get_pat_batch_epr_docs = get_pat_batch_epr_docs
        self.get_pat_batch_epr_docs_annotations = get_pat_batch_epr_docs_annotations
        self.get_pat_batch_mct_docs_annotations = get_pat_batch_mct_docs_annotations
        self.get_pat_batch_mct_docs = get_pat_batch_mct_docs
        self.get_pat_batch_demo = get_pat_batch_demo
        # self.__init__ = __init__
        # self._check_api_auth_details = _check_api_auth_details
        # self._check_auth_details = _check_auth_details
        # self.get_docs_generator = get_docs_generator
        # self.cogstack2df = cogstack2df
        # self.DataFrame = DataFrame
        # self.list_chunker = list_chunker
        # self.cohort_searcher_with_terms_and_search = cohort_searcher_with_terms_and_search
        # self.catch = catch
        # self.cohort_searcher_with_terms_no_search = cohort_searcher_with_terms_no_search
        # self.set_index_safe_wrapper = set_index_safe_wrapper
        # self.cohort_searcher_no_terms_fuzzy = cohort_searcher_no_terms_fuzzy
        # self.cohort_searcher_with_terms_no_search = cohort_searcher_with_terms_no_search
        # self.cohort_searcher_no_terms = cohort_searcher_no_terms
        # self.nearest = nearest
        # self.matcher = matcher
        # self.stringlist2searchlist = stringlist2searchlist
        # self.pylist2searchlist = pylist2searchlist
        # self.stringlist2pylist = stringlist2pylist
        # self.date_cleaner = date_cleaner
        # self.bulk_str_findall = bulk_str_findall
        # self.bulk_str_extract = bulk_str_extract
        # self.without_keys = without_keys
        # self.bulk_str_extract_round_robin = bulk_str_extract_round_robin
        # self.appendAge = appendAge
        # self.age = age
        # self.appendAgeAtRecord = appendAgeAtRecord
        # self.ageAtRecord = ageAtRecord
        # self.append_age_at_record_series = append_age_at_record_series
        # self.age_at_record = age_at_record
        # self.append_age_at_record_series = append_age_at_record_series
        # self.age_at_record = age_at_record
        # self.age_at_record = age_at_record
        # self.df_column_uniquify = df_column_uniquify
        # self.find_date = find_date
        # self.split_clinical_notes = split_clinical_notes
        # self.get_demographics = get_demographics
        # self.get_demographics2 = get_demographics2
        # self.pull_and_write = pull_and_write
        # self.cohort_searcher_with_terms_and_search_multi = cohort_searcher_with_terms_and_search_multi
        # self.iterative_multi_term_cohort_searcher_no_terms_fuzzy = iterative_multi_term_cohort_searcher_no_terms_fuzzy
        # self.__init__ = __init__
        self.calculate_interval = calculate_interval
        self.update_global_start_date = update_global_start_date
        # self.__init__ = __init__
        # self._create_directories = _create_directories
        # self._print_paths = _print_paths
        self.ingest_data_to_elasticsearch = ingest_data_to_elasticsearch
        # self.abstractEthnicity = abstractEthnicity
        # self.compare_ipw_annotation_rows = compare_ipw_annotation_rows
        # self.create_profile_reports = create_profile_reports
        # self.generate_pie_charts = generate_pie_charts
        self.generate_epr_documents_data = generate_epr_documents_data
        self.generate_epr_documents_personal_data = generate_epr_documents_personal_data
        self.generate_observations_data = generate_observations_data
        self.generate_basic_observations_data = generate_basic_observations_data
        self.extract_date_range = extract_date_range
        self.cohort_searcher_with_terms_and_search_dummy = cohort_searcher_with_terms_and_search_dummy
        self.check_pat_document_annotation_complete = check_pat_document_annotation_complete
        self.parse_meta_anns = parse_meta_anns
        self.get_pat_document_annotation_batch = get_pat_document_annotation_batch
        self.get_pat_document_annotation_batch_mct = get_pat_document_annotation_batch_mct
        self.annot_pat_batch_docs = annot_pat_batch_docs
        self.multi_annots_to_df = multi_annots_to_df
        self.multi_annots_to_df_mct = multi_annots_to_df_mct
        self.json_to_dataframe = json_to_dataframe
        self.filter_annot_dataframe = filter_annot_dataframe
        self.calculate_pretty_name_count_features = calculate_pretty_name_count_features
        self.list_dir_wrapper = list_dir_wrapper
        self.get_start_end_year_month = get_start_end_year_month
        self.get_empty_date_vector = get_empty_date_vector
        self.convert_timestamp_to_tuple = convert_timestamp_to_tuple
        self.enum_target_date_vector = enum_target_date_vector
        self.generate_date_list = generate_date_list
        self.filter_dataframe_by_timestamp = filter_dataframe_by_timestamp
        self.dump_results = dump_results
        self.update_pbar = update_pbar
        self.get_demographics3_batch = get_demographics3_batch
        self.list_dir_wrapper = list_dir_wrapper
        self.get_free_gpu = get_free_gpu
        self.method1 = method1
        self.method2 = method2
        # self.__str__ = __str__
        self.list_dir_wrapper = list_dir_wrapper
        self.convert_date = convert_date
        self.get_start_end_year_month = get_start_end_year_month
        self.get_empty_date_vector = get_empty_date_vector
        self.get_demographics3_batch = get_demographics3_batch
        self.sftp_exists = sftp_exists
        self.list_dir_wrapper = list_dir_wrapper
        self.exist_check = exist_check
        self.check_sftp_connection = check_sftp_connection
        self.get_free_gpu = get_free_gpu
        self.method1 = method1
        self.method2 = method2
        # self.__str__ = __str__
        self.write_remote = write_remote
        self.write_csv_wrapper = write_csv_wrapper
        self.read_remote = read_remote
        self.read_csv_wrapper = read_csv_wrapper
        self.create_local_folders = create_local_folders
        self.create_remote_folders = create_remote_folders
        self.create_folders_annot_csv_wrapper = create_folders_annot_csv_wrapper
        self.filter_stripped_list = filter_stripped_list
        self.create_folders = create_folders
        self.create_folders = create_folders
        self.create_folders_for_pat = create_folders_for_pat
        self.convert_date = convert_date
        self.add_offset_column = add_offset_column
        # self.apply_offset = apply_offset
        self.build_patient_dict = build_patient_dict
        self.get_cat = get_cat
        self.retrieve_pat_annotations = retrieve_pat_annotations
        self.copy_project_folders_with_substring_match = copy_project_folders_with_substring_match
        self.check_csv_integrity = check_csv_integrity
        self.check_csv_files_in_directory = check_csv_files_in_directory
        self.count_files = count_files
        self.process_csv_files = process_csv_files
        self.extract_datetime_to_column = extract_datetime_to_column
        self.filter_annot_dataframe2 = filter_annot_dataframe2
        self.produce_filtered_annotation_dataframe = produce_filtered_annotation_dataframe
        self.extract_types_from_csv = extract_types_from_csv
        self.remove_file_from_paths = remove_file_from_paths
        self.process_chunk = process_chunk
        self.process_csv_files_multi = process_csv_files_multi
        self.join_icd10_codes_to_annot = join_icd10_codes_to_annot
        self.join_icd10_OPC4S_codes_to_annot = join_icd10_OPC4S_codes_to_annot
        self.filter_and_select_rows = filter_and_select_rows
        self.filter_dataframe_by_cui = filter_dataframe_by_cui
        self.copy_files_and_dirs = copy_files_and_dirs
        self.get_pat_ipw_record = get_pat_ipw_record
        self.filter_and_update_csv = filter_and_update_csv
        self.build_ipw_dataframe = build_ipw_dataframe
        self.retrieve_pat_annots_mct_epr = retrieve_pat_annots_mct_epr
        self.retrieve_pat_docs_mct_epr = retrieve_pat_docs_mct_epr
        self.check_list_presence = check_list_presence
        self.filter_dataframe_n_lists = filter_dataframe_n_lists
        self.get_all_target_annots = get_all_target_annots
        self.build_merged_epr_mct_annot_df = build_merged_epr_mct_annot_df
        self.filter_annot_dataframe = filter_annot_dataframe
        # self.filter_float_column = filter_float_column
        self.build_merged_epr_mct_annot_df = build_merged_epr_mct_annot_df
        self.build_merged_epr_mct_doc_df = build_merged_epr_mct_doc_df
        self.join_docs_to_annots = join_docs_to_annots
        self.get_annots_joined_to_docs = get_annots_joined_to_docs
        self.group_images_by_suffix = group_images_by_suffix
        self.create_powerpoint_slides_client_idcode_groups = create_powerpoint_slides_client_idcode_groups
        self.create_powerpoint_from_images_group = create_powerpoint_from_images_group
        self.create_powerpoint_slides = create_powerpoint_slides
        self.create_powerpoint_from_images = create_powerpoint_from_images
