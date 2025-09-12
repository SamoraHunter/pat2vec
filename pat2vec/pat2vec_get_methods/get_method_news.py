import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month


def compute_feature_stats(data: pd.DataFrame, column: str, feature_name: str, config_obj):
    """
    Compute summary statistics for a given feature column in the NEWS dataset.

    Parameters:
    - data (pd.DataFrame): Subset of patient data for the feature.
    - column (str): Column to compute stats from (usually 'observation_valuetext_analysed').
    - feature_name (str): Name for the output columns.
    - config_obj: Configuration with negate_biochem.

    Returns:
    - dict: Dictionary of feature statistics.
    """
    stats = {}
    if len(data) > 0:
        values = data[column].dropna().astype(float)
        if len(values) > 0:
            stats[f"{feature_name}_mean"] = values.mean()
            stats[f"{feature_name}_median"] = values.median()
            stats[f"{feature_name}_std"] = values.std()
            stats[f"{feature_name}_max"] = values.max()
            stats[f"{feature_name}_min"] = values.min()
            stats[f"{feature_name}_n"] = values.shape[0]
            return stats

    if config_obj.negate_biochem:
        for suffix in ["mean", "median", "std", "max", "min", "n"]:
            stats[f"{feature_name}_{suffix}"] = np.nan
    return stats


def get_news(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """
    Retrieves NEWS2 features for a given patient within a specified date range.
    """

    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(
        target_date_range, config_obj=config_obj
    )

    if config_obj.batch_mode:
        current_pat_raw_news = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            "observationdocument_recordeddtm",
        )
    else:
        current_pat_raw_news = cohort_searcher_with_terms_and_search(
            index_name="observations",
            fields_list=[
                "observation_guid",
                "client_idcode",
                "obscatalogmasteritem_displayname",
                "observation_valuetext_analysed",
                "observationdocument_recordeddtm",
                "clientvisit_visitidcode",
            ],
            term_name=config_obj.client_idcode_term_name,
            entered_list=[current_pat_client_id_code],
            search_string=(
                'obscatalogmasteritem_displayname:("NEWS" OR "NEWS2") AND '
                f"observationdocument_recordeddtm:[{start_year}-{start_month}-{start_day} "
                f"TO {end_year}-{end_month}-{end_day}]"
            ),
        )

    # Always start with client_idcode
    news_features = {"client_idcode": current_pat_client_id_code}

    # Define mappings between display names and feature names
    feature_map = {
        "NEWS2_Score": "news_score",
        "NEWS_Systolic_BP": "news_systolic_bp",
        "NEWS_Diastolic_BP": "news_diastolic_bp",
        "NEWS_Respiration_Rate": "news_respiration_rate",
        "NEWS_Heart_Rate": "news_heart_rate",
        "NEWS_Oxygen_Saturation": "news_oxygen_saturation",
        "NEWS Temperature": "news_temperature",
        "NEWS_AVPU": "news_avpu",
        "NEWS_Supplemental_Oxygen": "news_supplemental_oxygen",
        "NEWS2_Sp02_Target": "news_sp02_target",
        "NEWS2_Sp02_Scale": "news_sp02_scale",
        "NEWS_Pulse_Type": "news_pulse_type",
        "NEWS_Pain_Score": "news_pain_score",
        "NEWS Oxygen Litres": "news_oxygen_litres",
        "NEWS Oxygen Delivery": "news_oxygen_delivery",
    }

    for display_name, feature_name in feature_map.items():
        subset = current_pat_raw_news[
            current_pat_raw_news["obscatalogmasteritem_displayname"] == display_name
        ].copy()

        subset.dropna(subset=["observation_valuetext_analysed"], inplace=True)

        # special case: cap NEWS2 score at [-20, 20]
        if feature_name == "news_score" and len(subset) > 0:
            subset = subset[
                (subset["observation_valuetext_analysed"].astype(float) < 20)
                & (subset["observation_valuetext_analysed"].astype(float) > -20)
            ]

        stats = compute_feature_stats(subset, "observation_valuetext_analysed", feature_name, config_obj)
        news_features.update(stats)

    news_features_df = pd.DataFrame([news_features])

    if config_obj.verbosity >= 6:
        display(news_features_df)

    return news_features_df
