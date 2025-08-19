from pat2vec.util.post_processing_get_pat_ipw_record import get_pat_ipw_record
import pandas as pd
import os


def build_ipw_dataframe(
    annot_filter_arguments=None,
    filter_codes=None,
    config_obj=None,
    mode="earliest",
    include_mct=True,
    include_textual_obs=True,
    custom_pat_list=[],
):

    df = pd.DataFrame()

    pat_list = os.listdir(config_obj.pre_document_batch_path)

    pat_list_stripped = [
        os.path.splitext(file)[0] for file in pat_list if file.endswith(".csv")
    ]

    if custom_pat_list:
        print(f"Using custom pat list, len:", len(custom_pat_list))
        pat_list_stripped = custom_pat_list

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
