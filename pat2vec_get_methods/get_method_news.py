import numpy as np
import pandas as pd
from IPython.display import display

from pat2vec.util.methods_get import (filter_dataframe_by_timestamp,
                                      get_start_end_year_month)


def get_news(current_pat_client_id_code, target_date_range, pat_batch, config_obj=None, cohort_searcher_with_terms_and_search=None):
    """
    Retrieves NEWS2 scores for a given patient within a specified date range.

    Parameters:
    - current_pat_client_id_code (str): The client ID code of the patient.
    - target_date_range (tuple): The date range in the format "(YYYY,MM,DD)".
    - pat_batch (pd.DataFrame): DataFrame containing patient data.
    - config_obj (Config): Configuration object with batch_mode and negate_biochem attributes. Defaults to None.
    - cohort_searcher_with_terms_and_search (callable, optional): Function for searching cohort data. Defaults to None.

    Returns:
    - pd.DataFrame: DataFrame containing NEWS2 scores and statistics for the given patient.
      Columns include 'client_idcode', 'news_score_mean', 'news_score_median', 'news_score_std',
      'news_score_max', 'news_score_min', and 'news_score_n'.

    Notes:
    - If batch_mode is True in config_obj, it filters pat_batch by timestamp.
    - If no NEWS2 scores are found, returns a DataFrame with NaN values.
    - If negate_biochem is True in config_obj, returns a DataFrame with NaN values even if scores are present.
    """

    start_year, start_month, end_year, end_month, start_day, end_day = get_start_end_year_month(
        target_date_range, config_obj = config_obj)

    batch_mode = config_obj.batch_mode

    if (batch_mode):
        current_pat_raw_news = filter_dataframe_by_timestamp(
            pat_batch, start_year, start_month, end_year, end_month, start_day, end_day, 'observationdocument_recordeddtm')

    else:
        current_pat_raw_news = cohort_searcher_with_terms_and_search(index_name="observations",
                                                                     fields_list="""observation_guid client_idcode	obscatalogmasteritem_displayname	observation_valuetext_analysed	observationdocument_recordeddtm clientvisit_visitidcode""".split(),
                                                                     term_name="client_idcode.keyword",
                                                                     entered_list=[
                                                                         current_pat_client_id_code],
                                                                     search_string='obscatalogmasteritem_displayname:(\"NEWS\" OR \"NEWS2\") AND ' + f'observationdocument_recordeddtm:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]')

    # if(len(current_pat_raw_news)==0):

    news_features = pd.DataFrame(
        data=[current_pat_client_id_code], columns=['client_idcode'])

    news_features_data = current_pat_raw_news[current_pat_raw_news['obscatalogmasteritem_displayname'] == 'NEWS2_Score'].copy(
    )

    # screen and purge dud values
    news_features_data = news_features_data[(news_features_data['observation_valuetext_analysed'].astype(
        float) < 20) & (news_features_data['observation_valuetext_analysed'].astype(float) > -20)].copy()

    if (len(news_features_data) > 0):
        news_features = pd.DataFrame(data=[current_pat_client_id_code], columns=[
                                     'client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features['news_score_mean'] = value_array.mean()
        news_features['news_score_median'] = value_array.median()
        news_features['news_score_std'] = value_array.std()
        news_features['news_score_max'] = max(value_array)
        news_features['news_score_min'] = min(value_array)
        news_features['news_score_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):
        news_features['news_score_mean'] = np.nan
        news_features['news_score_median'] = np.nan
        news_features['news_score_std'] = np.nan
        news_features['news_score_max'] = np.nan
        news_features['news_score_min'] = np.nan
        news_features['news_score_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------
    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS_Systolic_BP'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).dropna().astype(float)
        news_features['news_systolic_bp_mean'] = value_array.mean()
        news_features['news_systolic_bp_median'] = value_array.median()
        news_features['news_systolic_bp_std'] = value_array.std()
        news_features['news_systolic_bp_max'] = max(value_array)
        news_features['news_systolic_bp_min'] = min(value_array)
        news_features['news_systolic_bp_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):

        news_features['news_systolic_bp_mean'] = np.nan
        news_features['news_systolic_bp_median'] = np.nan
        news_features['news_systolic_bp_std'] = np.nan
        news_features['news_systolic_bp_max'] = np.nan
        news_features['news_systolic_bp_min'] = np.nan
        news_features['news_systolic_bp_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------
    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS_Diastolic_BP'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features['news_diastolic_bp_mean'] = value_array.mean()
        news_features['news_diastolic_bp_median'] = value_array.median()
        news_features['news_diastolic_bp_std'] = value_array.std()
        news_features['news_diastolic_bp_max'] = max(value_array)
        news_features['news_diastolic_bp_min'] = min(value_array)
        news_features['news_diastolic_bp_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):
        news_features['news_diastolic_bp_mean'] = np.nan
        news_features['news_diastolic_bp_median'] = np.nan
        news_features['news_diastolic_bp_std'] = np.nan
        news_features['news_diastolic_bp_max'] = np.nan
        news_features['news_diastolic_bp_min'] = np.nan
        news_features['news_diastolic_bp_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------
    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS_Respiration_Rate'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features['news_respiration_rate_mean'] = value_array.mean()
        news_features['news_respiration_rate_median'] = value_array.median()
        news_features['news_respiration_rate_std'] = value_array.std()
        news_features['news_respiration_rate_max'] = max(value_array)
        news_features['news_respiration_rate_min'] = min(value_array)
        news_features['news_respiration_rate_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):
        news_features['news_respiration_rate_mean'] = np.nan
        news_features['news_respiration_rate_median'] = np.nan
        news_features['news_respiration_rate_std'] = np.nan
        news_features['news_respiration_rate_max'] = np.nan
        news_features['news_respiration_rate_min'] = np.nan
        news_features['news_respiration_rate_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS_Heart_Rate'].copy()

    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    if (len(news_features_data) > 0):

        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features['news_heart_rate_mean'] = value_array.mean()
        news_features['news_heart_rate_median'] = value_array.median()
        news_features['news_heart_rate_std'] = value_array.std()
        news_features['news_heart_rate_max'] = max(value_array)
        news_features['news_heart_rate_min'] = min(value_array)
        news_features['news_heart_rate_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):
        news_features['news_heart_rate_mean'] = np.nan
        news_features['news_heart_rate_median'] = np.nan
        news_features['news_heart_rate_std'] = np.nan
        news_features['news_heart_rate_max'] = np.nan
        news_features['news_heart_rate_min'] = np.nan
        news_features['news_heart_rate_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS_Oxygen_Saturation'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    if (len(news_features_data) > 0):

        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features['news_oxygen_saturation_mean'] = value_array.mean()
        news_features['news_oxygen_saturation_median'] = value_array.median()
        news_features['news_oxygen_saturation_std'] = value_array.std()
        news_features['news_oxygen_saturation_max'] = max(value_array)
        news_features['news_oxygen_saturation_min'] = min(value_array)
        news_features['news_oxygen_saturation_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):
        news_features['news_oxygen_saturation_mean'] = np.nan
        news_features['news_oxygen_saturation_median'] = np.nan
        news_features['news_oxygen_saturation_std'] = np.nan
        news_features['news_oxygen_saturation_max'] = np.nan
        news_features['news_oxygen_saturation_min'] = np.nan
        news_features['news_oxygen_saturation_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS Temperature'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features['news_temperature_mean'] = value_array.mean()
        news_features['news_temperature_median'] = value_array.median()
        news_features['news_temperature_std'] = value_array.std()
        news_features['news_temperature_max'] = max(value_array)
        news_features['news_temperature_min'] = min(value_array)
        news_features['news_temperature_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):
        news_features['news_temperature_mean'] = np.nan
        news_features['news_temperature_median'] = np.nan
        news_features['news_temperature_std'] = np.nan
        news_features['news_temperature_max'] = np.nan
        news_features['news_temperature_min'] = np.nan
        news_features['news_temperature_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news['obscatalogmasteritem_displayname'] == 'NEWS_AVPU'].copy(
    )
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features['news_avpu_mean'] = value_array.mean()
        news_features['news_avpu_median'] = value_array.median()
        news_features['news_avpu_std'] = value_array.std()
        news_features['news_avpu_max'] = max(value_array)
        news_features['news_avpu_min'] = min(value_array)
        news_features['news_avpu_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):
        news_features['news_avpu_mean'] = np.nan
        news_features['news_avpu_median'] = np.nan
        news_features['news_avpu_std'] = np.nan
        news_features['news_avpu_max'] = np.nan
        news_features['news_avpu_min'] = np.nan
        news_features['news_avpu_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS_Supplemental_Oxygen'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    term = 'supplemental_oxygen'

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features[f'news_{term}_mean'] = value_array.mean()
        news_features[f'news_{term}_median'] = value_array.median()
        news_features[f'news_{term}_std'] = value_array.std()
        news_features[f'news_{term}_max'] = max(value_array)
        news_features[f'news_{term}_min'] = min(value_array)
        news_features[f'news_{term}_n'] = value_array.shape[0]

    elif (config_obj.negate_biochem):
        news_features[f'news_{term}_mean'] = np.nan
        news_features[f'news_{term}_median'] = np.nan
        news_features[f'news_{term}_std'] = np.nan
        news_features[f'news_{term}_max'] = np.nan
        news_features[f'news_{term}_min'] = np.nan
        news_features[f'news_{term}_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS2_Sp02_Target'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    term = 'Sp02_Target'.lower()

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features[f'news_{term}_mean'] = value_array.mean()
        news_features[f'news_{term}_median'] = value_array.median()
        news_features[f'news_{term}_std'] = value_array.std()
        news_features[f'news_{term}_max'] = max(value_array)
        news_features[f'news_{term}_min'] = min(value_array)
        news_features[f'news_{term}_n'] = value_array.shape[0]

    elif (config_obj.negate_biochem):
        news_features[f'news_{term}_mean'] = np.nan
        news_features[f'news_{term}_median'] = np.nan
        news_features[f'news_{term}_std'] = np.nan
        news_features[f'news_{term}_max'] = np.nan
        news_features[f'news_{term}_min'] = np.nan
        news_features[f'news_{term}_n'] = np.nan

    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS2_Sp02_Scale'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    term = 'Sp02_Scale'.lower()

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features[f'news_{term}_mean'] = value_array.mean()
        news_features[f'news_{term}_median'] = value_array.median()
        news_features[f'news_{term}_std'] = value_array.std()
        news_features[f'news_{term}_max'] = max(value_array)
        news_features[f'news_{term}_min'] = min(value_array)
        news_features[f'news_{term}_n'] = value_array.shape[0]

    elif (config_obj.negate_biochem):
        news_features[f'news_{term}_mean'] = np.nan
        news_features[f'news_{term}_median'] = np.nan
        news_features[f'news_{term}_std'] = np.nan
        news_features[f'news_{term}_max'] = np.nan
        news_features[f'news_{term}_min'] = np.nan
        news_features[f'news_{term}_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS_Pulse_Type'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    term = 'pulse_type'.lower()

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features[f'news_{term}_mean'] = value_array.mean()
        news_features[f'news_{term}_median'] = value_array.median()
        news_features[f'news_{term}_std'] = value_array.std()
        news_features[f'news_{term}_max'] = max(value_array)
        news_features[f'news_{term}_min'] = min(value_array)
        news_features[f'news_{term}_n'] = value_array.shape[0]

    elif (config_obj.negate_biochem):
        news_features[f'news_{term}_mean'] = np.nan
        news_features[f'news_{term}_median'] = np.nan
        news_features[f'news_{term}_std'] = np.nan
        news_features[f'news_{term}_max'] = np.nan
        news_features[f'news_{term}_min'] = np.nan
        news_features[f'news_{term}_n'] = np.nan

    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS_Pain_Score'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    term = 'Pain_Score'.lower()

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].astype(
            float)
        news_features[f'news_{term}_mean'] = value_array.mean()
        news_features[f'news_{term}_median'] = value_array.median()
        news_features[f'news_{term}_std'] = value_array.std()
        news_features[f'news_{term}_max'] = max(value_array)
        news_features[f'news_{term}_min'] = min(value_array)
        news_features[f'news_{term}_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):
        news_features[f'news_{term}_mean'] = np.nan
        news_features[f'news_{term}_median'] = np.nan
        news_features[f'news_{term}_std'] = np.nan
        news_features[f'news_{term}_max'] = np.nan
        news_features[f'news_{term}_min'] = np.nan
        news_features[f'news_{term}_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS Oxygen Litres'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    term = 'oxygen_litres'.lower()

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].dropna(
        ).astype(float)
        news_features[f'news_{term}_mean'] = value_array.mean()
        news_features[f'news_{term}_median'] = value_array.median()
        news_features[f'news_{term}_std'] = value_array.std()
        news_features[f'news_{term}_max'] = max(value_array)
        news_features[f'news_{term}_min'] = min(value_array)
        news_features[f'news_{term}_n'] = value_array.shape[0]

    elif (config_obj.negate_biochem):
        news_features[f'news_{term}_mean'] = np.nan
        news_features[f'news_{term}_median'] = np.nan
        news_features[f'news_{term}_std'] = np.nan
        news_features[f'news_{term}_max'] = np.nan
        news_features[f'news_{term}_min'] = np.nan
        news_features[f'news_{term}_n'] = np.nan
    else:
        pass

    # -----------------------------------------------------------------

    # news_features_data = current_pat_raw_news[current_pat_raw_news['obscatalogmasteritem_displayname']=='NEWS Oxygen Delivery'].copy()
    # #news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    # news_features_data.dropna(subset=['observation_valuetext_analysed'],inplace=True)

    # term = 'oxygen_delivery'.lower()

    # if(len(news_features_data) > 0):
    #     #news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
    #     value_array = news_features_data['observation_valuetext_analysed'].dropna().astype(float)
    #     news_features[f'news_{term}_mean'] = value_array.mean()
    #     news_features[f'news_{term}_median'] = value_array.median()
    #     news_features[f'news_{term}_std'] = value_array.std()
    #     news_features[f'news_{term}_max'] = max(value_array)
    #     news_features[f'news_{term}_min'] = min(value_array)
    #     news_features[f'news_{term}_n'] = value_array.shape[0]
    # else:
    #     news_features[f'news_{term}_mean'] = np.nan
    #     news_features[f'news_{term}_median'] = np.nan
    #     news_features[f'news_{term}_std'] = np.nan
    #     news_features[f'news_{term}_max'] = np.nan
    #     news_features[f'news_{term}_min'] = np.nan
    #     news_features[f'news_{term}_n'] = np.nan

    # -----------------------------------------------------------------

    news_features_data = current_pat_raw_news[current_pat_raw_news[
        'obscatalogmasteritem_displayname'] == 'NEWS Oxygen Delivery'].copy()
    # news_features_data =  news_features_data[(news_features_data['observation_valuetext_analysed'].astype(float)<20)& (news_features_data['observation_valuetext_analysed'].astype(float)>-20)].copy()
    news_features_data.dropna(
        subset=['observation_valuetext_analysed'], inplace=True)

    term = 'oxygen_delivery'.lower()

    if (len(news_features_data) > 0):
        # news_features = pd.DataFrame(data = [current_pat_client_id_code] , columns =['client_idcode']).copy()
        value_array = news_features_data['observation_valuetext_analysed'].astype(
            float)
        news_features[f'news_{term}_mean'] = value_array.mean()
        news_features[f'news_{term}_median'] = value_array.median()
        news_features[f'news_{term}_std'] = value_array.std()
        news_features[f'news_{term}_max'] = max(value_array)
        news_features[f'news_{term}_min'] = min(value_array)
        news_features[f'news_{term}_n'] = value_array.shape[0]
    elif (config_obj.negate_biochem):
        news_features[f'news_{term}_mean'] = np.nan
        news_features[f'news_{term}_median'] = np.nan
        news_features[f'news_{term}_std'] = np.nan
        news_features[f'news_{term}_max'] = np.nan
        news_features[f'news_{term}_min'] = np.nan
        news_features[f'news_{term}_n'] = np.nan

    else:
        pass

    if (config_obj.verbosity >= 6):
        display(news_features)

    return news_features
