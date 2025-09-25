import pandas as pd
from typing import Any, List
import logging
from fuzzywuzzy import process
from pat2vec.util.methods_annotation_regex import append_regex_term_counts
from IPython.display import display

logger = logging.getLogger(__name__)

def filter_dataframe_by_fuzzy_terms(
    df: pd.DataFrame,
    filter_term_list: List[str],
    column_name: str = "document_description",
    verbose: int = 0,
) -> pd.DataFrame:
    """Filters a DataFrame by fuzzy matching terms in a specified column.

    This function iterates through a list of terms and finds the best fuzzy
    matches in a DataFrame column. It returns a new DataFrame containing only
    the rows that have a match score above a certain threshold (80).

    Args:
        df: The DataFrame to filter.
        filter_term_list: A list of terms to search for.
        column_name: The name of the column to perform the fuzzy match on.
        verbose: Verbosity level for logging.

    Returns:
        A new DataFrame containing only the rows with fuzzy-matched terms.
    """
    if verbose >= 1:
        logger.info("Filtering DataFrame by fuzzy terms...")

    matched_indices = set()
    for term in filter_term_list:
        if verbose >= 1:
            logger.info(f"Processing term: {term}")
        matches = process.extractBests(term, df[column_name], score_cutoff=80)
        for match, score, idx in matches:
            if verbose >= 2:
                logger.debug(f"Found match: {match} with similarity score: {score}")
            matched_indices.add(idx)

    if verbose >= 1:
        logger.info("Filtering complete.")

    filtered_df = df[df.index.isin(matched_indices)]
    return filtered_df


def apply_data_type_epr_docs_filters(
    config_obj: Any, batch_target: pd.DataFrame
) -> pd.DataFrame:
    """Applies data type filters to a DataFrame of EPR documents.

    This function filters a DataFrame based on rules defined in the `config_obj`.
    It can apply fuzzy term matching on the 'document_description' column and
    also count occurrences of regex patterns in the 'body_analysed' column,
    adding the counts as new columns.

    Args:
        config_obj: A configuration object containing filter settings.
        batch_target: The DataFrame of EPR documents to be filtered.

    Returns:
        The filtered DataFrame.
    """
    if config_obj.data_type_filter_dict is not None and not batch_target.empty:
        if (
            config_obj.data_type_filter_dict.get("filter_term_lists").get("epr_docs")
            is not None
        ):
            if config_obj.verbosity >= 1:
                logger.info(
                    f"Applying document type filter to EPR documents: {config_obj.data_type_filter_dict}"
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
                logger.info("Appending regex term counts...")
                if config_obj.verbosity > 5 and not batch_target.empty:
                    logger.debug(f"DataFrame before regex term counts:\n{batch_target.head().to_string()}")
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
            logger.info(
                "Data type filter dictionary is None or batch target is empty. No filtering applied."
            )
    return batch_target


def apply_bloods_data_type_filter(
    config_obj: Any, batch_target: pd.DataFrame
) -> pd.DataFrame:
    """Applies data type filters to a DataFrame of bloods data.

    This function filters a DataFrame based on fuzzy term matching against the
    'basicobs_itemname_analysed' column, using filter terms defined in the
    `config_obj`.

    Args:
        config_obj: A configuration object containing filter settings.
        batch_target: The DataFrame of bloods data to be filtered.

    Returns:
        The filtered DataFrame.
    """
    if config_obj.data_type_filter_dict is not None:
        if (
            config_obj.data_type_filter_dict.get("filter_term_lists").get("bloods")
            is not None
        ):
            if config_obj.verbosity >= 1:
                logger.info(
                    f"Applying document type filter to bloods: {config_obj.data_type_filter_dict}"
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
            logger.info("Data type filter dictionary is None. No filtering applied.")
    return batch_target


def apply_data_type_mct_docs_filters(
    config_obj: Any, batch_target: pd.DataFrame
) -> pd.DataFrame:
    """Applies data type filters to a DataFrame of MCT documents.

    This function filters a DataFrame based on rules defined in the `config_obj`.
    It can apply fuzzy term matching on the 'document_description' column and
    also count occurrences of regex patterns in the 'body_analysed' column,
    adding the counts as new columns.

    Args:
        config_obj: A configuration object containing filter settings.
        batch_target: The DataFrame of MCT documents to be filtered.

    Returns:
        The filtered DataFrame.
    """
    if config_obj.data_type_filter_dict is not None and not batch_target.empty:
        if (
            config_obj.data_type_filter_dict.get("filter_term_lists").get("mct_docs")
            is not None
        ):
            if config_obj.verbosity >= 1:
                logger.info(
                    f"Applying document type filter to MCT documents: {config_obj.data_type_filter_dict}"
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
                logger.info("Appending regex term counts...")
                if config_obj.verbosity > 5 and not batch_target.empty:
                    logger.debug(f"DataFrame before regex term counts:\n{batch_target.head().to_string()}")
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
            logger.info(
                "Data type filter dictionary is None or batch target is empty. No filtering applied."
            )
    return batch_target
