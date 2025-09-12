from datetime import datetime, timezone
import pandas as pd
from IPython.display import display
from pat2vec.util.filter_dataframe_by_timestamp import filter_dataframe_by_timestamp
from pat2vec.util.get_start_end_year_month import get_start_end_year_month
from pat2vec.util.parse_date import validate_input_dates
from pat2vec.util.methods_get import convert_date


DRUG_FIELDS = [
    "client_idcode",
    "order_guid",
    "order_name",
    "order_summaryline",
    "order_holdreasontext",
    "order_entered",
    "clientvisit_visitidcode",
    "order_performeddtm",
    "order_createdwhen",
]

COLUMNS_TO_DROP = [
    "_index",
    "_id",
    "_score",
    "order_guid",
    "order_name",
    "order_summaryline",
    "order_holdreasontext",
    "order_entered",
    "clientvisit_visitidcode",
    "order_performeddtm",
    "order_createdwhen",
    "datetime",
    "index",
]


def search_drug_orders(
    cohort_searcher_with_terms_and_search=None,
    client_id_codes=None,
    drug_time_field="order_createdwhen",
    start_year="1995",
    start_month="01",
    start_day="01",
    end_year="2025",
    end_month="12",
    end_day="12",
    additional_custom_search_string=None,
):
    """Search for drug/medication orders within a date range."""
    if cohort_searcher_with_terms_and_search is None:
        raise ValueError("cohort_searcher_with_terms_and_search cannot be None.")
    if client_id_codes is None:
        raise ValueError("client_id_codes cannot be None.")
    if drug_time_field is None:
        raise ValueError("drug_time_field cannot be None.")

    if isinstance(client_id_codes, str):
        client_id_codes = [client_id_codes]

    start_year, start_month, start_day, end_year, end_month, end_day = validate_input_dates(
        start_year, start_month, start_day, end_year, end_month, end_day
    )

    search_string = (
        'order_typecode:"medication" AND '
        + f"{drug_time_field}:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
    )

    if additional_custom_search_string:
        search_string += f" {additional_custom_search_string}"

    return cohort_searcher_with_terms_and_search(
        index_name="order",
        fields_list=DRUG_FIELDS,
        term_name="client_idcode.keyword",
        entered_list=client_id_codes,
        search_string=search_string,
    )


def prepare_drug_datetime(drugs_data, drug_time_field, batch_mode=False):
    """Prepare datetime column for drug data."""
    data = drugs_data.copy()

    if batch_mode:
        data["datetime"] = data[drug_time_field].copy()
    else:
        data["datetime"] = (
            pd.Series(data[drug_time_field])
            .dropna()
            .apply(convert_date)
        )

    return data


def calculate_drug_features(order_name_df_dict, order_name_list, drugs_arg_dict, batch_mode=False):
    """Calculate drug features for each order type based on config flags."""
    if batch_mode:
        today = datetime.now(timezone.utc)
    else:
        today = datetime.today()

    features = {}

    for col_name in order_name_list:
        filtered_df = order_name_df_dict.get(col_name)
        if filtered_df is None or len(filtered_df) == 0:
            continue

        df_len = len(filtered_df)

        if df_len >= 1:
            if drugs_arg_dict.get("_num-drug-order"):
                features[f"{col_name}_num-drug-order"] = df_len

            if drugs_arg_dict.get("_days-since-last-drug-order"):
                try:
                    sorted_df = filtered_df.sort_values(by="datetime")
                    date_object = sorted_df.iloc[-1]["datetime"]
                    delta = today - date_object
                    features[f"{col_name}_days-since-last-drug-order"] = delta.days
                except Exception as e:
                    print(f"Error calculating days since last drug for {col_name}: {e}")
                    features[f"{col_name}_days-since-last-drug-order"] = None

        if df_len >= 2 and drugs_arg_dict.get("_days-between-first-last-drug"):
            try:
                sorted_df = filtered_df.sort_values(by="datetime")
                earliest = sorted_df.iloc[0]["datetime"]
                latest = sorted_df.iloc[-1]["datetime"]
                delta = latest - earliest
                features[f"{col_name}_days-between-first-last-drug"] = delta.days
            except Exception as e:
                print(f"Error calculating days between first-last drug for {col_name}: {e}")
                features[f"{col_name}_days-between-first-last-drug"] = None

    return features


def create_drug_features_dataframe(current_pat_client_id_code, drug_features, original_data):
    """Create final drug features DataFrame with patient info + calculated features."""
    base_df = pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    if len(original_data) > 0:
        sample_row = original_data.iloc[0:1].copy()
        columns_to_keep = [col for col in sample_row.columns
                           if col not in COLUMNS_TO_DROP and col not in drug_features.keys()]
        for col in columns_to_keep:
            if col != "client_idcode":
                base_df[col] = sample_row[col].iloc[0]

    for feature_name, feature_value in drug_features.items():
        base_df[feature_name] = feature_value

    return base_df


def get_current_pat_drugs(
    current_pat_client_id_code,
    target_date_range,
    pat_batch,
    config_obj=None,
    cohort_searcher_with_terms_and_search=None,
):
    """Retrieve drug/medication features for a patient within a specified date range."""
    if config_obj is None:
        raise ValueError("config_obj cannot be None.")

    batch_mode = config_obj.batch_mode
    drug_time_field = config_obj.drug_time_field
    drugs_arg_dict = config_obj.feature_engineering_arg_dict.get("drugs", {})

    start_year, start_month, end_year, end_month, start_day, end_day = (
        get_start_end_year_month(target_date_range, config_obj=config_obj)
    )

    # Get drug orders
    if batch_mode:
        drugs = filter_dataframe_by_timestamp(
            pat_batch,
            start_year,
            start_month,
            end_year,
            end_month,
            start_day,
            end_day,
            drug_time_field,
        )
    else:
        drugs = search_drug_orders(
            cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search,
            client_id_codes=current_pat_client_id_code,
            drug_time_field=drug_time_field,
            start_year=start_year,
            start_month=start_month,
            start_day=start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
        )

    if len(drugs) == 0:
        return pd.DataFrame({"client_idcode": [current_pat_client_id_code]})

    # Prepare datetime
    current_pat_drugs = prepare_drug_datetime(drugs, drug_time_field, batch_mode)

    # Group by order_name
    order_name_list = list(current_pat_drugs["order_name"].unique())
    order_name_df_dict = {
        elem: current_pat_drugs[current_pat_drugs.order_name == elem]
        for elem in order_name_list
    }

    # Calculate features
    drug_features = calculate_drug_features(
        order_name_df_dict, order_name_list, drugs_arg_dict, batch_mode
    )

    # Create final features dataframe
    result_df = create_drug_features_dataframe(
        current_pat_client_id_code, drug_features, current_pat_drugs
    )

    if config_obj.verbosity >= 6:
        display(result_df)

    return result_df
