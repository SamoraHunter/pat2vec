from pat2vec.util.post_processing_get_pat_ipw_record import get_pat_ipw_record
import pandas as pd
import os
from typing import Any, Dict, List, Optional
from sqlalchemy import text, inspect
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


def build_ipw_dataframe(
    annot_filter_arguments: Optional[Dict[str, Any]] = None,
    filter_codes: Optional[List[int]] = None,
    config_obj: Optional[Any] = None,
    mode: str = "earliest",
    include_mct: bool = True,
    include_textual_obs: bool = True,
    custom_pat_list: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Builds a DataFrame of Individual Patient Window (IPW) records.

    This function iterates through a list of patients, finds the relevant "index"
    record for each one based on specified filters, and compiles these records
    into a single DataFrame. The index record is typically the first or last
    occurrence of a specific clinical event (e.g., a diagnosis CUI).

    Args:
        annot_filter_arguments: A dictionary of filters to apply
            to the annotations before selecting the IPW record. Defaults to None.
        filter_codes: A list of CUI codes to identify
            the relevant clinical events. Defaults to None.
        config_obj: The configuration object containing paths
            and settings. Defaults to None.
        mode: Determines whether to find the 'earliest' or 'latest'
            record for each patient. Defaults to "earliest".
        include_mct: If True, includes annotations from MCT
            (MRC clinical notes) in the search. Defaults to True.
        include_textual_obs: If True, includes annotations from
            textual observations. Defaults to True.
        custom_pat_list: A specific list of patient IDs to
            process. If empty, the patient list is derived from the database or files in
            the `pre_document_batch_path`. Defaults to an empty list.

    Returns:
        pd.DataFrame: A DataFrame where each row represents the IPW record for a
            patient, containing details of the index event.
    """

    df = pd.DataFrame()

    if custom_pat_list is not None:
        logger.info(f"Using custom pat list, len: {len(custom_pat_list)}")
        pat_list_stripped = custom_pat_list
    else:
        if config_obj and getattr(config_obj, "storage_backend", "file") == "database":
            pat_list_stripped = []
            try:
                engine = config_obj.db_engine
                if not engine:
                    logger.error("Database engine not initialized in config_obj.")
                    return pd.DataFrame()

                inspector = inspect(engine)

                # Try to get patient list from raw documents first, as it's the most comprehensive source
                t_docs = (
                    "raw_data_raw_epr_docs"
                    if engine.name == "sqlite"
                    else "raw_epr_docs"
                )
                s_docs = None if engine.name == "sqlite" else "raw_data"
                if inspector.has_table(t_docs, schema=s_docs):
                    with engine.connect() as connection:
                        full_t = (
                            f'"{t_docs}"'
                            if engine.name == "sqlite"
                            else f'"{s_docs}"."{t_docs}"'
                        )
                        result = connection.execute(
                            text(f'SELECT DISTINCT "client_idcode" FROM {full_t}')
                        )
                        pat_list_stripped = [
                            str(row[0]) for row in result if row[0] is not None
                        ]

                # If no patients found in docs, try demographics
                if not pat_list_stripped:
                    logger.info(
                        "No patients found in raw_epr_docs, trying raw_demographics."
                    )
                    t_demo = (
                        "raw_data_raw_demographics"
                        if engine.name == "sqlite"
                        else "raw_demographics"
                    )
                    s_demo = None if engine.name == "sqlite" else "raw_data"
                    if inspector.has_table(t_demo, schema=s_demo):
                        with engine.connect() as connection:
                            full_t = (
                                f'"{t_demo}"'
                                if engine.name == "sqlite"
                                else f'"{s_demo}"."{t_demo}"'
                            )
                            result = connection.execute(
                                text(f'SELECT DISTINCT "client_idcode" FROM {full_t}')
                            )
                            pat_list_stripped = [
                                str(row[0]) for row in result if row[0] is not None
                            ]

                # As a last resort, if raw data tables are empty, check the features table
                if not pat_list_stripped:
                    logger.warning(
                        "Could not find patients in raw data tables, trying features table."
                    )
                    t_feat = (
                        "features_features" if engine.name == "sqlite" else "features"
                    )
                    s_feat = None if engine.name == "sqlite" else "features"
                    if inspector.has_table(t_feat, schema=s_feat):
                        with engine.connect() as connection:
                            full_t = (
                                f'"{t_feat}"'
                                if engine.name == "sqlite"
                                else f'"{s_feat}"."{t_feat}"'
                            )
                            result = connection.execute(
                                text(f'SELECT DISTINCT "client_idcode" FROM {full_t}')
                            )
                            pat_list_stripped = [
                                str(row[0]) for row in result if row[0] is not None
                            ]

            except Exception as e:
                logger.error(f"Could not fetch patient list from database: {e}")
        else:  # file-based
            pat_list = os.listdir(config_obj.pre_document_batch_path)
            pat_list_stripped = [
                os.path.splitext(file)[0] for file in pat_list if file.endswith(".csv")
            ]

    results_list = []
    for pat in tqdm(pat_list_stripped, desc="Building IPW DataFrame"):

        res = get_pat_ipw_record(
            current_pat_idcode=pat,
            annot_filter_arguments=annot_filter_arguments,
            filter_codes=filter_codes,
            config_obj=config_obj,
            mode=mode,
            include_mct=include_mct,  # Boolean argument to include MCT
            include_textual_obs=include_textual_obs,  # Boolean argument to include textual_obs
        )

        if not res.empty:
            results_list.append(res)

    df = pd.concat(results_list, ignore_index=True) if results_list else pd.DataFrame()

    if "observationdocument_recordeddtm" in df.columns:
        df["updatetime"] = df["updatetime"].fillna(
            df["observationdocument_recordeddtm"]
        )

        if "updatetime" in df.columns:
            # Fill missing values in 'observationdocument_recordeddtm' column with values from 'updatetime'
            df["observationdocument_recordeddtm"] = df[
                "observationdocument_recordeddtm"
            ].fillna(df["updatetime"])

    return df
