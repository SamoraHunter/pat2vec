from pat2vec.patvec_get_batch_methods.get_merged_batches import (
    get_merged_pat_batch_bloods,
    get_merged_pat_batch_drugs,
    split_and_save_csv,
)


def prefetch_batches(pat2vec_obj=None):

    if pat2vec_obj.config_obj.main_options.get("bloods", True):

        dfb = get_merged_pat_batch_bloods(
            client_idcode_list=pat2vec_obj.all_patient_list,
            search_term=None,
            config_obj=pat2vec_obj.config_obj,
            cohort_searcher_with_terms_and_search=pat2vec_obj.cohort_searcher_with_terms_and_search,
        )

        split_and_save_csv(
            df=dfb,
            client_idcode_column="client_idcode",
            save_folder=pat2vec_obj.config_obj.pre_bloods_batch_path,
            num_processes=None,
        )
    if pat2vec_obj.config_obj.main_options.get("drugs", True):

        dfdr = get_merged_pat_batch_drugs(
            client_idcode_list=pat2vec_obj.all_patient_list,
            #    search_term=None,
            config_obj=pat2vec_obj.config_obj,
            cohort_searcher_with_terms_and_search=pat2vec_obj.cohort_searcher_with_terms_and_search,
        )

        split_and_save_csv(
            df=dfdr,
            client_idcode_column="client_idcode",
            save_folder=pat2vec_obj.config_obj.pre_drugs_batch_path,
            num_processes=None,
        )
