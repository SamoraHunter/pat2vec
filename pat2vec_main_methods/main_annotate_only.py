

import random

from tqdm import trange

from pat2vec_get_methods.current_pat_annotations_batch_to_file import get_current_pat_annotations_batch_to_file
from pat2vec_get_methods.current_pat_annotations_mrc_batch_to_file import get_current_pat_annotations_mct_batch_to_file


def main_annotate_only(all_patient_list, annotate_only=False, cohort_searcher_with_terms_and_search=None):

    if annotate_only:
        random.seed()
        random.shuffle(all_patient_list)

        skipped_counter = 0
        t = trange(len(all_patient_list), desc='Bar desc', leave=True,
                   colour="GREEN", position=0, total=len(all_patient_list))

        global_start_year, global_start_month, global_end_year, global_end_month = '1995', '01', '2023', '11'

        for i in t:
            current_pat_doc_batch = cohort_searcher_with_terms_and_search(
                index_name="epr_documents",
                fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                term_name="client_idcode.keyword",
                entered_list=[all_patient_list[i]],
                search_string=f'updatetime:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
            )

            current_pat_doc_mct_batch = cohort_searcher_with_terms_and_search(
                index_name="observations",
                fields_list="""observation_guid client_idcode obscatalogmasteritem_displayname observation_valuetext_analysed observationdocument_recordeddtm clientvisit_visitidcode""".split(),
                term_name="client_idcode.keyword",
                entered_list=[all_patient_list[i]],
                search_string="obscatalogmasteritem_displayname:(\"AoMRC_ClinicalSummary_FT\") AND " +
                f'observationdocument_recordeddtm:[{global_start_year}-{global_start_month} TO {global_end_year}-{global_end_month}]'
            )

            for j in range(0, len(combinations)):
                try:
                    if all_patient_list[i] not in stripped_list:
                        get_current_pat_annotations_batch_to_file(
                            all_patient_list[i], combinations[j], current_pat_doc_batch)
                        get_current_pat_annotations_mct_batch_to_file(
                            all_patient_list[i], combinations[j], current_pat_doc_mct_batch)
                except Exception as e:
                    print(e)
                    print(all_patient_list[i], combinations[j])

# Example usage:
# process_patients(all_patient_list, annotate_only=True)
