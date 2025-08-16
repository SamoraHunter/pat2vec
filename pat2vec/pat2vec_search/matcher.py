from pat2vec.pat2vec_search.nearest import nearest


import pandas as pd


from datetime import timedelta


def matcher(
    data_template_df,
    lab_results_df,
    source_patid_colname,
    source_date_colname,
    result_date_colname,
    result_testname,
    result_resultname,
    before,
    after,
):
    """
    Function to match a patient template dataframe with a lab results dataframe.
    For each patient in the template dataframe, it finds the closest lab test result
    in the specified time range (before and after) for each unique lab test name.
    The results are then added as new columns to the template dataframe.

    Parameters:
    - data_template_df (pd.DataFrame): Template dataframe containing patient IDs and dates.
    - lab_results_df (pd.DataFrame): Lab results dataframe containing patient IDs, dates, test names, and test results.
    - source_patid_colname (str): Column name for patient IDs in the template dataframe.
    - source_date_colname (str): Column name for dates in the template dataframe.
    - result_date_colname (str): Column name for dates in the lab results dataframe.
    - result_testname (str): Column name for test names in the lab results dataframe.
    - result_resultname (str): Column name for test results in the lab results dataframe.
    - before (int): Number of days before the target date to consider.
    - after (int): Number of days after the target date to consider.

    Returns:
    - pd.DataFrame: The template dataframe with the added lab test results as new columns.
    """
    data_template = data_template_df  # Upload the template and inspect then rename cols, remove nulls and reset index
    data_template = data_template.dropna(subset=[source_date_colname]).reset_index(
        drop=True
    )
    data_template[source_date_colname] = pd.to_datetime(
        data_template[source_date_colname], utc=True
    )
    lab_results = lab_results_df  # Import the test results
    lab_results[result_date_colname] = pd.to_datetime(
        lab_results[result_date_colname], utc=True
    )

    bloods_filter = list(lab_results[result_testname].unique())
    bloods_values = defaultdict(
        list
    )  # Function that searches for all tests per patient and then returns the closest result to the date range of each patient in the template file
    for indx, row in data_template.iterrows():
        h_id = row[source_patid_colname]  # Patient ID from the template
        target_time = row[source_date_colname]  # Date from the template
        vals = {}
        max_time_before = timedelta(days=before)  # Time before
        max_time_after = timedelta(days=after)  # Time after
        h_id_bloods = lab_results[
            lab_results[source_patid_colname] == h_id
        ]  # Patient ID in results table
        for blood_code_type, sub_df in h_id_bloods[
            h_id_bloods[result_testname].isin(bloods_filter)
        ].groupby(
            result_testname
        ):  # Groups the results by blood test name
            date_val_idx = sub_df.columns.tolist().index(
                result_date_colname
            )  # Organizes them by blood test date
            vals[blood_code_type] = nearest(
                target_time,
                sub_df,
                result_date_colname,
                result_resultname,  # Selects the nearest blood test to display
                max_time_before,
                max_time_after,
            )
        missing_blood_types = [k for k in bloods_filter if k not in vals.keys()]
        for k in missing_blood_types:
            vals[k] = None
        for k, v in vals.items():
            bloods_values[k].append(v)
    out_file = pd.concat([data_template, pd.DataFrame(bloods_values)], axis=1)
    # globals()[output_name] = out_file
    return out_file
