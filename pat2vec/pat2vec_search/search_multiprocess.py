import pandas as pd
from tqdm.notebook import tqdm


import csv
import multiprocessing
from multiprocessing import Pool

from pat2vec.pat2vec_search.cogstack_search_methods import (
    cohort_searcher_with_terms_and_search,
)


def pull_and_write(index_name, fields_list, term_name, entered_list, search_string):
    """
    Pull data from elasticsearch and write to a file.

    Parameters
    ----------
    index_name : str
        The name of the index to search.
    fields_list : list
        The list of fields to retrieve.
    term_name : str
        The name of the term to search.
    entered_list : list
        The list of values to search for.
    search_string : str
        The search string to use.

    Notes
    -----
    The file is written in append mode, so if the file already exists, data will be appended to it.
    The header is not written to the file, so if you want a header, you need to add it manually.
    """
    print(f"running...{len(entered_list)}")
    file_name = "temp_search_store.csv"

    df_write = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_list,
        term_name=term_name,
        entered_list=entered_list,
        search_string=search_string,
    )

    df_write.to_csv(file_name, mode="a", index=False, header=False)


def cohort_searcher_with_terms_and_search_multi(
    index_name, fields_list, term_name, entered_list, search_string
):
    """
    Search a cohort with terms and search string, in parallel.

    Parameters
    ----------
    index_name : str
        The name of the index to search.
    fields_list : list
        The list of fields to retrieve.
    term_name : str
        The name of the term to search.
    entered_list : list
        The list of values to search for.
    search_string : str
        The search string to use.

    Returns
    -------
    pd.DataFrame
        The DataFrame containing the results of the search.
    """
    file_name = "temp_search_store.csv"
    file = open(file_name, "w", newline="")

    with open(file_name, "w", newline="") as outcsv:
        writer = csv.writer(outcsv)
        writer.writerow(fields_list)

    print(pd.read_csv(file_name))
    n = multiprocessing.cpu_count()
    l = entered_list

    # using list comprehension
    pat_list_master = [l[i : i + n] for i in range(0, len(l), n)]
    print(len(pat_list_master))

    pool = Pool()

    if __name__ == "__main__":
        print(f"starting..")
        for _ in tqdm(
            pool.imap(pull_and_write, pat_list_master), total=len(pat_list_master)
        ):
            pass

        pool.close()

    return pd.read_csv("temp_search_store.csv")
