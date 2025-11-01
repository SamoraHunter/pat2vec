from collections import defaultdict
from datetime import timedelta

import pandas as pd

from pat2vec.pat2vec_search.nearest import nearest


def matcher(
    data_template_df: pd.DataFrame,
    lab_results_df: pd.DataFrame,
    source_patid_colname: str,
    source_date_colname: str,
    result_date_colname: str,
    result_testname: str,
    result_resultname: str,
    before: int,
    after: int,
) -> pd.DataFrame:
    """Matches lab results to a template DataFrame based on the nearest date.

    For each row in the `data_template_df`, this function finds the closest
    lab test result from `lab_results_df` for the same patient. The search is
    constrained to a time window defined by `before` and `after` days relative
    to the date in the template row. This is done for each unique lab test name
    found in `lab_results_df`. The matched results are then added as new
    columns to the template DataFrame.

    Args:
        data_template_df: Template DataFrame with patient IDs and target dates.
        lab_results_df: DataFrame with lab results, including patient IDs,
            dates, test names, and results.
        source_patid_colname: Column name for patient IDs in the template DataFrame.
        source_date_colname: Column name for dates in the template DataFrame.
        result_date_colname: Column name for dates in the lab results DataFrame.
        result_testname: Column name for test names in the lab results DataFrame.
        result_resultname: Column name for test results in the lab results DataFrame.
        before: Number of days before the target date to include in the search window.
        after: Number of days after the target date to include in the search window.

    Returns:
        The template DataFrame with added columns for each unique lab test,
        populated with the nearest result value.
    """
    data_template = data_template_df.copy()
    lab_results = lab_results_df.copy()

    # Prepare template dataframe
    data_template = data_template.dropna(subset=[source_date_colname]).reset_index(
        drop=True
    )
    data_template[source_date_colname] = pd.to_datetime(
        data_template[source_date_colname], utc=True
    )

    # Prepare lab results dataframe
    lab_results[result_date_colname] = pd.to_datetime(
        lab_results[result_date_colname], utc=True
    )

    bloods_filter = list(lab_results[result_testname].unique())
    bloods_values = defaultdict(list)

    for _, row in data_template.iterrows():
        h_id = row[source_patid_colname]
        target_time = row[source_date_colname]
        vals = {}
        max_time_before = timedelta(days=before)
        max_time_after = timedelta(days=after)

        h_id_bloods = lab_results[lab_results[source_patid_colname] == h_id]
        for blood_code_type, sub_df in h_id_bloods[
            h_id_bloods[result_testname].isin(bloods_filter)
        ].groupby(result_testname):
            vals[blood_code_type] = nearest(
                target_time,
                sub_df,
                result_date_colname,
                result_resultname,
                max_time_before,
                max_time_after,
            )
        missing_blood_types = [k for k in bloods_filter if k not in vals.keys()]
        for k in missing_blood_types:
            vals[k] = None
        for k, v in vals.items():
            bloods_values[k].append(v)

    out_file = pd.concat([data_template, pd.DataFrame(bloods_values)], axis=1)
    return out_file
