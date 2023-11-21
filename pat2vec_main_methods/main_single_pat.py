from datetime import datetime
import pandas as pd

from util.methods_get import filter_dataframe_by_timestamp, get_start_end_year_month

def get_single_pat(current_pat_client_id_code, target_date_range, pat_batch=None, cohort_searcher_with_terms_and_search = None, batch_mode=None, config_obj=None):
    
    
    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(target_date_range)
    batch_mode = config_obj.batch_mode

    
    if batch_mode:
        current_pat_docs = filter_dataframe_by_timestamp(pat_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'observationdocument_recordeddtm')
    else:
        date_range_str = f'{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}'
        obs_search_string = f'obscatalogmasteritem_displayname:("AoMRC_ClinicalSummary_FT") AND observationdocument_recordeddtm:[{date_range_str}]'
        
        current_pat_docs = cohort_searcher_with_terms_and_search(
            index_name="observations",
            fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname observation_valuetext_analysed observationdocument_recordeddtm clientvisit_visitidcode""".split(),
            term_name="client_idcode.keyword",
            entered_list=[current_pat_client_id_code],
            search_string=obs_search_string
        )
    
    return current_pat_docs

