from pat2vec.util.clinical_note_splitter import split_and_append_chunks
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.filter_methods import filter_dataframe_by_fuzzy_terms
from pat2vec.util.helper_functions import get_df_from_db
from pat2vec.util.methods_annotation_regex import append_regex_term_counts
from pat2vec.util.methods_get import exist_check


import pandas as pd
from IPython.display import display
from sqlalchemy import text


import logging
import os
from typing import Any


def get_pat_batch_epr_docs(
    current_pat_client_id_code: str,
    search_term: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Retrieves a batch of EPR documents for a patient.

    Args:
        current_pat_client_id_code: The patient's unique identifier.
        search_term: The term to search for (currently unused).
        config_obj: The main configuration object.
        cohort_searcher_with_terms_and_search: The search function to use.

    Returns:
        A DataFrame containing the batch of EPR documents.
    """
    if config_obj is None or not all(
        hasattr(config_obj, attr)
        for attr in [
            "global_start_year",
            "global_start_month",
            "global_end_year",
            "global_end_month",
        ]
    ):
        raise ValueError("Invalid or missing configuration object.")

    overwrite_stored_pat_docs = config_obj.overwrite_stored_pat_docs

    global_start_year = config_obj.global_start_year
    global_start_month = config_obj.global_start_month
    global_end_year = config_obj.global_end_year
    global_end_month = config_obj.global_end_month
    global_start_day = config_obj.global_start_day
    global_end_day = config_obj.global_end_day

    global_start_year = str(global_start_year).zfill(4)
    global_start_month = str(global_start_month).zfill(2)
    global_end_year = str(global_end_year).zfill(4)
    global_end_month = str(global_end_month).zfill(2)

    split_clinical_notes_bool = config_obj.split_clinical_notes

    batch_epr_target_path = os.path.join(
        config_obj.pre_document_batch_path, str(current_pat_client_id_code) + ".csv"
    )

    batch_target = pd.DataFrame()

    if config_obj.storage_backend == "database":
        try:
            table_name = "raw_epr_docs"
            schema_name = "raw_data"

            if not overwrite_stored_pat_docs:
                df = get_df_from_db(
                    config_obj,
                    schema_name,
                    table_name,
                    patient_ids=[current_pat_client_id_code],
                )
                if not df.empty:
                    return df
        except Exception as e:
            logging.error(
                f"Error with database backend for EPR docs for patient {current_pat_client_id_code}: {e}"
            )
            return pd.DataFrame()

    global_start_day = str(global_start_day).zfill(2)
    global_end_day = str(global_end_day).zfill(2)

    if config_obj.verbosity >= 6:
        logging.debug("batch_epr_target_path: %s", batch_epr_target_path)
        logging.debug("global_start_year: %s", global_start_year)
        logging.debug("global_start_month: %s", global_start_month)
        logging.debug("global_end_year: %s", global_end_year)
        logging.debug("global_end_month: %s", global_end_month)
        logging.debug("global_start_day: %s", global_start_day)
        logging.debug("global_end_day: %s", global_end_day)

    existence_check = exist_check(batch_epr_target_path, config_obj)

    should_fetch = False
    if config_obj.storage_backend == "database":
        should_fetch = True
    elif overwrite_stored_pat_docs or not existence_check:
        should_fetch = True

    try:

        if should_fetch:

            batch_target = cohort_searcher_with_terms_and_search(
                index_name="epr_documents",
                fields_list="""client_idcode document_guid document_description body_analysed updatetime clientvisit_visitidcode""".split(),
                term_name=config_obj.client_idcode_term_name,
                entered_list=[current_pat_client_id_code],
                search_string=f"updatetime:[{global_start_year}-{global_start_month}-{global_start_day} TO {global_end_year}-{global_end_month}-{global_end_day}]",
            )

            if config_obj.data_type_filter_dict is not None and not batch_target.empty:
                if (
                    config_obj.data_type_filter_dict.get("filter_term_lists").get(
                        "epr_docs"
                    )
                    is not None
                ):

                    if config_obj.verbosity >= 1:
                        logging.info(
                            "applying doc type filter to EPR docs",
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
                        logging.debug("append_regex_term_counts...")
                        display(batch_target)
                    batch_target = append_regex_term_counts(
                        df=batch_target,
                        terms=config_obj.data_type_filter_dict.get(
                            "filter_term_lists"
                        ).get("epr_docs_term_regex"),
                        text_column="body_analysed",
                        debug=config_obj.verbosity > 5,
                    )
            # display(batch_target)

            if config_obj.store_pat_batch_docs or overwrite_stored_pat_docs:
                # batch_target.dropna(subset='body_analysed', inplace=True)

                if config_obj.verbosity >= 3:
                    logging.debug("get_epr_docs_predropna: %d", len(batch_target))

                col_list_drop_nan = ["body_analysed", "updatetime", "client_idcode"]

                rows_with_nan = batch_target[
                    batch_target[col_list_drop_nan].isna().any(axis=1)
                ]

                # Drop rows with NaN values
                batch_target = batch_target.drop(rows_with_nan.index).copy()

                if config_obj.verbosity >= 3:
                    logging.debug("get_epr_docs_postdropna: %d", len(batch_target))

                if split_clinical_notes_bool:

                    batch_target = split_and_append_chunks(batch_target, epr=True)

                    # if drop out of range notes, filter batch_target by global date. before writing.

                    if config_obj.filter_split_notes:

                        pre_filter_split_notes_len = len(batch_target)

                        # reuse dataframe filter
                        batch_target = filter_dataframe_by_timestamp(
                            df=batch_target,
                            start_year=int(global_start_year),
                            start_month=int(global_start_month),
                            end_year=int(global_end_year),
                            end_month=int(global_end_month),
                            start_day=int(global_start_day),
                            end_day=int(global_end_day),
                            timestamp_string="updatetime",
                            dropna=False,
                        )
                        if config_obj.verbosity > 2:
                            logging.debug(
                                f"pre_filter_split_notes_len: {pre_filter_split_notes_len}"
                            )
                            logging.debug(
                                f"post_filter_split_notes_len: {len(batch_target)}"
                            )

                if config_obj.storage_backend == "database":
                    try:
                        engine = config_obj.db_engine
                        if engine:
                            with engine.begin() as connection:
                                table_name = "raw_epr_docs"
                                schema_name = "raw_data"
                                db_table = (
                                    f"{schema_name}_{table_name}"
                                    if engine.name == "sqlite"
                                    else table_name
                                )
                                db_schema = (
                                    None if engine.name == "sqlite" else schema_name
                                )
                                if overwrite_stored_pat_docs:
                                    del_query = text(
                                        f"DELETE FROM {db_table if engine.name == 'sqlite' else f'{schema_name}.{table_name}'} WHERE client_idcode = :pat_id"
                                    )
                                    connection.execute(
                                        del_query,
                                        {"pat_id": current_pat_client_id_code},
                                    )
                                batch_target.to_sql(
                                    name=db_table,
                                    con=connection,
                                    schema=db_schema,
                                    if_exists="append",
                                    index=False,
                                )
                    except Exception as e:
                        logging.error(f"Failed to save EPR docs batch to DB: {e}")
                else:
                    batch_target.to_csv(batch_epr_target_path)

        else:
            batch_target = pd.read_csv(batch_epr_target_path)

        return batch_target
    except Exception as e:
        """"""
        logging.error(f"Error retrieving batch EPR documents: {e}")
        raise UnboundLocalError("Error retrieving batch EPR documents.")
