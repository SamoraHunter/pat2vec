from pat2vec.util.post_processing_get_pat_ipw_record import get_pat_ipw_record
import pandas as pd
import os
from typing import Any, Dict, List, Optional

import logging

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
            process. If empty, the patient list is derived from the files in
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
        pat_list = os.listdir(config_obj.pre_document_batch_path)
        pat_list_stripped = [
            os.path.splitext(file)[0] for file in pat_list if file.endswith(".csv")
        ]
    for pat in pat_list_stripped:

        res = get_pat_ipw_record(
            current_pat_idcode=pat,
            annot_filter_arguments=annot_filter_arguments,
            filter_codes=filter_codes,
            config_obj=config_obj,
            mode=mode,
            include_mct=include_mct,  # Boolean argument to include MCT
            include_textual_obs=include_textual_obs,  # Boolean argument to include textual_obs
        )

        df = pd.concat([df, res], ignore_index=True)

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
