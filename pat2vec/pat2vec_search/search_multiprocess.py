import csv
import multiprocessing
from multiprocessing import Pool
from typing import List

import pandas as pd
from tqdm.notebook import tqdm

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
    index_name: str,
    fields_list: List[str],
    term_name: str,
    entered_list: List[str],
    search_string: str,
) -> pd.DataFrame:
    """Searches a cohort in parallel using multiple processes.

    This function splits a large list of search terms (`entered_list`) into
    chunks and distributes the search queries across multiple processes. The
    results from each process are written to a temporary file and then read
    back into a single DataFrame.

    Args:
        index_name: The name of the index to search.
        fields_list: The list of fields to retrieve.
        term_name: The name of the term to filter on.
        entered_list: The list of values to search for.
        search_string: The search string to use.

    Returns:
        A DataFrame containing the combined results of the parallel search.
    """
    file_name = "temp_search_store.csv"

    with open(file_name, "w", newline="") as outcsv:
        writer = csv.writer(outcsv)
        writer.writerow(fields_list)

    n = multiprocessing.cpu_count()
    l = entered_list

    # Split the list of terms into chunks for each process
    pat_list_master = [l[i : i + n] for i in range(0, len(l), n)]
    print(
        f"Splitting {len(l)} items into {len(pat_list_master)} chunks for parallel processing."
    )

    # Prepare arguments for each process
    args_list = [
        (index_name, fields_list, term_name, chunk, search_string)
        for chunk in pat_list_master
    ]

    with Pool() as pool:
        print(f"Starting parallel search with {pool._processes} processes...")
        for _ in tqdm(
            pool.imap_unordered(pull_and_write, args_list), total=len(args_list)
        ):
            pass

    return pd.read_csv("temp_search_store.csv")
