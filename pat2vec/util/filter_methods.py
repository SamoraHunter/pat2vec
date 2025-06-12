from fuzzywuzzy import process
from pat2vec.util.methods_annotation_regex import append_regex_term_counts
from IPython.display import display


def filter_dataframe_by_fuzzy_terms(
    df, filter_term_list, column_name="document_description", verbose=0
):
    """
    Filter DataFrame by fuzzy matching terms in the specified column.

    Args:
        df (pd.DataFrame): DataFrame to filter.
        filter_term_list (list): List of terms to filter by.
        column_name (str): Name of the column to perform fuzzy matching.
        verbose (int): Verbosity level (0: no verbose, 1: moderate verbose, 2: high verbose).

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    if verbose >= 1:
        print("Filtering DataFrame by fuzzy terms...")

    matched_indices = set()
    for term in filter_term_list:
        if verbose >= 1:
            print(f"Processing term: {term}")
        matches = process.extractBests(term, df[column_name], score_cutoff=80)
        for match, score, idx in matches:
            if verbose >= 2:
                print(f"Found match: {match} with similarity score: {score}")
            matched_indices.add(idx)

    if verbose >= 1:
        print("Filtering complete.")

    filtered_df = df[df.index.isin(matched_indices)]
    return filtered_df


def apply_data_type_epr_docs_filters(config_obj, batch_target):
    """
    Apply data type filters specific to EPR documents to the batch target based on the configuration object.

    Args:
        config_obj (object): Configuration object containing data type filter information.
        batch_target (DataFrame): Batch target DataFrame to be filtered.

    Returns:
        DataFrame: Filtered batch target DataFrame.

    """
    if config_obj.data_type_filter_dict is not None and not batch_target.empty:
        if (
            config_obj.data_type_filter_dict.get("filter_term_lists").get("epr_docs")
            is not None
        ):
            if config_obj.verbosity >= 1:
                print(
                    "Applying document type filter to EPR documents",
                    config_obj.data_type_filter_dict,
                )
            filter_term_list = config_obj.data_type_filter_dict.get(
                "filter_term_lists"
            ).get("epr_docs")
            batch_target = filter_dataframe_by_fuzzy_terms(
                batch_target,
                filter_term_list,
                column_name="document_description",
                verbose=config_obj.verbosity,
            )

        if (
            config_obj.data_type_filter_dict.get("filter_term_lists").get(
                "epr_docs_term_regex"
            )
            is not None
        ):
            if config_obj.verbosity > 1:
                print("Appending regex term counts...")
                if config_obj.verbosity > 5:
                    display(batch_target)
            batch_target = append_regex_term_counts(
                df=batch_target,
                terms=config_obj.data_type_filter_dict.get("filter_term_lists").get(
                    "epr_docs_term_regex"
                ),
                text_column="body_analysed",
                debug=config_obj.verbosity > 5,
            )
    else:
        if config_obj.verbosity >= 1:
            print(
                "Data type filter dictionary is None or batch target is empty. No filtering applied."
            )
    return batch_target


def apply_bloods_data_type_filter(config_obj, batch_target):
    """
    Apply data type filter specific to bloods to the batch target based on the configuration object.

    Args:
        config_obj (object): Configuration object containing data type filter information.
        batch_target (DataFrame): Batch target DataFrame to be filtered.

    Returns:
        DataFrame: Filtered batch target DataFrame.

    """
    if config_obj.data_type_filter_dict is not None:
        if (
            config_obj.data_type_filter_dict.get("filter_term_lists").get("bloods")
            is not None
        ):
            if config_obj.verbosity >= 1:
                print(
                    "Applying document type filter to bloods",
                    config_obj.data_type_filter_dict,
                )
            filter_term_list = config_obj.data_type_filter_dict.get(
                "filter_term_lists"
            ).get("bloods")
            batch_target = filter_dataframe_by_fuzzy_terms(
                batch_target,
                filter_term_list,
                column_name="basicobs_itemname_analysed",
                verbose=config_obj.verbosity,
            )
    else:
        if config_obj.verbosity >= 1:
            print("Data type filter dictionary is None. No filtering applied.")
    return batch_target


def apply_data_type_mct_docs_filters(config_obj, batch_target):
    """
    Apply data type filters specific to MCT documents to the batch target based on the configuration object.

    Args:
        config_obj (object): Configuration object containing data type filter information.
        batch_target (DataFrame): Batch target DataFrame to be filtered.

    Returns:
        DataFrame: Filtered batch target DataFrame.

    """
    if config_obj.data_type_filter_dict is not None and not batch_target.empty:
        if (
            config_obj.data_type_filter_dict.get("filter_term_lists").get("mct_docs")
            is not None
        ):
            if config_obj.verbosity >= 1:
                print(
                    "Applying document type filter to MCT documents",
                    config_obj.data_type_filter_dict,
                )
            filter_term_list = config_obj.data_type_filter_dict.get(
                "filter_term_lists"
            ).get("mct_docs")
            batch_target = filter_dataframe_by_fuzzy_terms(
                batch_target,
                filter_term_list,
                column_name="document_description",
                verbose=config_obj.verbosity,
            )

        if (
            config_obj.data_type_filter_dict.get("filter_term_lists").get(
                "mct_docs_term_regex"
            )
            is not None
        ):
            if config_obj.verbosity > 1:
                print("Appending regex term counts...")
                if config_obj.verbosity > 5:
                    display(batch_target)
            batch_target = append_regex_term_counts(
                df=batch_target,
                terms=config_obj.data_type_filter_dict.get("filter_term_lists").get(
                    "mct_docs_term_regex"
                ),
                text_column="body_analysed",
                debug=config_obj.verbosity > 5,
            )
    else:
        if config_obj.verbosity >= 1:
            print(
                "Data type filter dictionary is None or batch target is empty. No filtering applied."
            )
    return batch_target
