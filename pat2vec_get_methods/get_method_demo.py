import pandas as pd
from IPython.display import display

# from your_module import cohort_searcher_with_terms_and_search  # replace 'your_module' with the actual module name containing cohort_searcher_with_terms_and_search
# replace 'your_module' with the actual module name containing get_start_end_year_month
from pat2vec.util.methods_get import get_start_end_year_month

# cogstack object pass?


def get_demographics3(patlist, target_date_range, cohort_searcher_with_terms_and_search):
    """
    Get demographics information for a list of patients within a specified date range.

    Parameters:
    - patlist (list): List of patient IDs.
    - target_date_range (str): Date range in the format "(YYYY,MM,DD)".

    Returns:
    - pd.DataFrame: Demographics information for the specified patients.
    """
    print(type(cohort_searcher_with_terms_and_search))

    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(
        target_date_range, config_obj=config_obj)

    demo = cohort_searcher_with_terms_and_search(
        index_name="epr_documents",
        fields_list=["client_idcode", "client_firstname", "client_lastname", "client_dob",
                     "client_gendercode", "client_racecode", "client_deceaseddtm", "updatetime"],
        term_name="client_idcode.keyword",
        entered_list=patlist,
        search_string=f'updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]'
    )

    demo["updatetime"] = pd.to_datetime(demo["updatetime"], utc=True)
    demo = demo.sort_values(["client_idcode", "updatetime"])

    if len(demo) > 1:
        try:
            return demo.iloc[-1].to_frame()
        except Exception as e:
            print(e)
    elif len(demo) == 1:
        return demo
    else:
        demo = pd.DataFrame(data=None, columns=None)
        demo['client_idcode'] = patlist

        return demo

# # Example use:
# patlist_example = ["patient_id1", "patient_id2"]
# date_range_example = "2023-01-01 to 2023-12-31"
# result = get_demographics3(patlist_example, date_range_example)
# print(result)
