import os
from typing import Callable, Dict, Optional, Tuple, List

import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates


def compute_feature_stats(
    data: pd.DataFrame, column: str, feature_name: str, config_obj: object
) -> Dict:
    """Computes summary statistics for a feature column in the NEWS dataset.

    Args:
        data (pd.DataFrame): Subset of patient data for the feature.
        column (str): Column to compute stats from (e.g.,
            'observation_valuetext_analysed').
        feature_name (str): Base name for the output feature columns.
        config_obj (object): Configuration object with `negate_biochem` attribute.

    Returns:
        Dict: A dictionary of calculated feature statistics (mean, median, std,
            max, min, n).
    """
    stats = {}
    if len(data) > 0:
        values = pd.to_numeric(data[column], errors="coerce").dropna()
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


def search_news_observations(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    observations_time_field="observationdocument_recordeddtm",
    fields_override: Optional[List[str]] = None,
    start_year="1995",
    start_month="01",
    start_day="01",
    end_year="2025",
    end_month="12",
    end_day="12",
    additional_custom_search_string=None,
    index_name: str = "observations",
    output_filename: Optional[str] = "news_search_results.csv",
    overwrite: bool = False,
    config_obj: Optional[object] = None,
):
    """Searches for NEWS/NEWS2 observation data within a date range.

    Args:
        cohort_searcher_with_terms_and_search: The cohort searcher function for
            executing the search query. Cannot be None.
        client_id_codes: Client ID codes (string or list of strings) to search.
            Cannot be None.
        observations_time_field (str): Field name for observation timestamp for
            date range filtering. Defaults to "observationdocument_recordeddtm".
        fields_override (Optional[List[str]]): Custom list of fields to return.
            Uses default observation fields if None.
        start_year (str): Start year for date range. Defaults to "1995".
        start_month (str): Start month for date range. Defaults to "01".
        start_day (str): Start day for date range. Defaults to "01".
        end_year (str): End year for date range. Defaults to "2025".
        end_month (str): End month for date range. Defaults to "12".
        end_day (str): End day for date range. Defaults to "12".
        additional_custom_search_string: Additional custom search string to append
            to the query.
        index_name (str): Elasticsearch index name to search. Defaults to
            "observations".
        output_filename (Optional[str]): Path to save results as CSV. Set to None
            to skip saving. Defaults to "news_search_results.csv".
        overwrite (bool): If True, re-run search even if output file exists.
            Defaults to False.
        config_obj (Optional[object]): Configuration object with root_path and
            proj_name attributes for path construction. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame containing the searched NEWS observation records.

    Raises:
        ValueError: If cohort_searcher_with_terms_and_search or client_id_codes is
            None.
    """
    if (
        output_filename
        and config_obj
        and hasattr(config_obj, "root_path")
        and hasattr(config_obj, "proj_name")
    ):
        output_filename = os.path.join(
            config_obj.root_path, config_obj.proj_name, output_filename
        )

    if output_filename and os.path.exists(output_filename) and not overwrite:
        print(f"Loading existing news data from {output_filename}")
        return pd.read_csv(output_filename)

    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")

    # Ensure client_id_codes is a list for the search function
    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = (
        validate_input_dates(
            start_year, start_month, start_day, end_year, end_month, end_day
        )
    )

    search_string = (
        "obscatalogmasteritem_displayname:(NEWS*) AND "
        f"{observations_time_field}:[{start_year}-{start_month}-{start_day} "
        f"TO {end_year}-{end_month}-{end_day}]"
    )

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    fields_to_use = [
        "observation_guid",
        "client_idcode",
        "obscatalogmasteritem_displayname",
        "observation_valuetext_analysed",
        "observationdocument_recordeddtm",
        "clientvisit_visitidcode",
    ]
    if fields_override:
        fields_to_use = fields_override

    client_idcode_term_name = "client_idcode.keyword"
    if config_obj and hasattr(config_obj, "client_idcode_term_name"):
        client_idcode_term_name = config_obj.client_idcode_term_name

    results = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_to_use,
        term_name=client_idcode_term_name,
        entered_list=client_id_codes,
        search_string=search_string,
    )

    if output_filename:
        if os.path.dirname(output_filename):
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        print(f"Saving news data to {output_filename}")
        results.to_csv(output_filename, index=False)

    return results


def get_news(
    current_pat_client_id_code: str,
    target_date_range: Tuple,
    pat_batch: pd.DataFrame,
    config_obj: Optional[object] = None,
    cohort_searcher_with_terms_and_search: Optional[Callable] = None,
    fields_override: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Retrieves NEWS/NEWS2 features for a patient within a date range.

    This function fetches NEWS (National Early Warning Score) observation data,
    either from a pre-loaded batch or by searching. It then calculates summary
    statistics (mean, median, std, etc.) for each component of the NEWS score.
    Feature columns include: news_score, news_systolic_bp, news_diastolic_bp,
    news_respiration_rate, news_heart_rate, news_oxygen_saturation, news_temperature,
    news_avpu, and more.

    Args:
        current_pat_client_id_code (str): The client ID code of the patient to
            retrieve NEWS features for.
        target_date_range (Tuple): Date range tuple specifying the time period
            to search. Expected format is (start_date, end_date).
        pat_batch (pd.DataFrame): Patient batch DataFrame containing observation
            records for batch mode processing.
        config_obj (Optional[object]): Configuration object with settings like
            `batch_mode`, `negate_biochem`, and `verbosity`. Defaults to None.
        cohort_searcher_with_terms_and_search (Optional[Callable]): The cohort
            searcher function used when batch_mode is False. Cannot be None in
            non-batch mode. Defaults to None.
        fields_override (Optional[List[str]]): A list of field names to override
            the default observation search fields. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame containing calculated NEWS feature statistics for
            each patient, with columns like news_score_mean, news_systolic_bp_std,
            etc., plus client_idcode identifier column.

    Raises:
        ValueError: If config_obj is None when required.
    """

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    if pat_batch.empty:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

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
        current_pat_raw_news = search_news_observations(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            observations_time_field="observationdocument_recordeddtm",
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            fields_override=fields_override,
            output_filename=None,
            config_obj=config_obj,
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
            numeric_values = pd.to_numeric(
                subset["observation_valuetext_analysed"], errors="coerce"
            )
            subset = subset[(numeric_values < 20) & (numeric_values > -20)].copy()

        stats = compute_feature_stats(
            subset, "observation_valuetext_analysed", feature_name, config_obj
        )
        news_features.update(stats)

    news_features_df = pd.DataFrame([news_features])

    if config_obj.verbosity >= 6:
        display(news_features_df)

    return news_features_df
