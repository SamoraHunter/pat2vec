"""
This module provides the main entry point for generating dummy data based on
Elasticsearch queries, routing requests to appropriate data generators.
"""

from datetime import datetime
import logging
import random
from typing import List

import pandas as pd

from .generator_helpers import (
    extract_date_range,
    extract_search_term_obscatalogmasteritem_displayname,
)

logger = logging.getLogger(__name__)


def cohort_searcher_with_terms_and_search_dummy(
    index_name: str,
    fields_list: List[str],
    term_name: str,
    entered_list: List[str],
    search_string: str,
) -> pd.DataFrame:
    """Generates dummy data based on simulated Elasticsearch query parameters.

    This function acts as a stand-in for a real CogStack/Elasticsearch query,
    routing requests to different dummy data generator functions based on the
    `index_name` and `search_string`.

    Args:
        index_name: The name of the target index (e.g., 'epr_documents').
        fields_list: A list of fields to be returned in the DataFrame.
        term_name: The field name for the term-level query (e.g., 'client_idcode').
        entered_list: The list of values for the term-level query.
        search_string: A string simulating a query string search, used for
            routing to the correct data generator.

    Returns:
        A pandas DataFrame containing the generated dummy data.
    """
    use_GPT = False
    verbose = False

    if verbose:
        logger.debug(
            f"cohort_searcher_with_terms_and_search_dummy received index_name: {index_name}, fields_list: {fields_list}"
        )

    date_range_tuple = extract_date_range(search_string)

    if date_range_tuple:
        (
            global_start_year,
            global_start_month,
            _,
            global_end_year,
            global_end_month,
            _,
        ) = date_range_tuple
    else:
        global_start_year, global_start_month = 1995, 1
        global_end_year, global_end_month = 2023, 12

    if verbose:
        logger.debug(f"cohort_searcher_with_terms_and_search_dummy: {search_string}")

    df = pd.DataFrame(columns=fields_list)

    # Import generators locally to avoid circular imports
    from .epr_documents import (
        generate_epr_documents_data,
        generate_epr_documents_personal_data,
    )
    from .basic_observations import (
        generate_basic_observations_data,
        generate_basic_observations_textual_obs_data,
        generate_observations_data,
    )
    from .observations import (
        generate_bmi_data,
        generate_bed_data,
        generate_core_o2_data,
        generate_core_resus_data,
        generate_hospital_site_data,
        generate_news_data,
        generate_smoking_data,
        generate_observations_MRC_text_data,
        generate_observations_Reports_text_data,
        generate_vte_data,
    )
    from .orders import generate_diagnostic_orders_data, generate_drug_orders_data
    from .appointments import generate_appointments_data
    from .covid import generate_covid_observations_data
    from .epic import (
        generate_epic_encounters_data,
        generate_epic_clinical_notes_data,
        generate_epic_medical_history_data,
        generate_epic_orders_data,
        generate_epic_lab_results_data,
        generate_epic_patients_data,
        generate_epic_imaging_reports_data,
        generate_epic_clinical_notes_appointments_data,
    )

    if index_name == "epr_documents":
        if "client_firstname" in fields_list:
            if verbose:
                logger.debug("Generating personal data for 'epr_documents'")
            num_rows = random.randint(1, 10)
            df = generate_epr_documents_personal_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        else:
            if verbose:
                logger.debug("Generating general document data for 'epr_documents'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_epr_documents_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                use_GPT=use_GPT,
                fields_list=fields_list,
            )
    elif index_name == "basic_observations":
        if "SARS CoV-2" in search_string and "COVID-19" in search_string:
            if verbose:
                logger.debug("Generating data for 'covid'")
            num_rows = random.randint(1, 5)
            df = generate_covid_observations_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        elif "basicobs_itemname_analysed:report" in search_string:
            if verbose:
                logger.debug("Generating text data for 'basic_observations, reports'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_observations_Reports_text_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                use_GPT=use_GPT,
                fields_list=fields_list,
            )
        elif "textualObs" in fields_list:
            if verbose:
                logger.debug("Generating data for 'basic_observations textualObs'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_basic_observations_textual_obs_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        elif "basicobs_value_numeric" in search_string:
            num_rows = 1
            base_date = datetime(2023, 6, 15)
            df = generate_basic_observations_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
                base_date=base_date,
            )
        else:
            if verbose:
                logger.debug("Generating data for 'basicobs_value_numeric'")
            num_rows = random.randint(1, 10)
            df = generate_basic_observations_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
    elif index_name == "observations":
        if any(
            term in search_string for term in ["OBS BMI", "OBS Weight", "OBS Height"]
        ):
            if verbose:
                logger.debug("Generating data for 'bmi'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            base_date = datetime(2023, 6, 14)
            df = generate_bmi_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
                base_date=base_date,
            )
        elif "NEWS" in search_string:
            if verbose:
                logger.debug("Generating data for 'news'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_news_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        elif '"CORE_SpO2"' in search_string:
            if verbose:
                logger.debug("Generating data for 'core_o2'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_core_o2_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        elif '"CORE_RESUS_STATUS"' in search_string:
            if verbose:
                logger.debug("Generating data for 'core_resus_status'")
            probabilities = [0.7, 0.25, 0.05]
            num_rows = random.choices(range(1, 4), probabilities)[0]
            df = generate_core_resus_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        elif "CORE_BedNumber3" in search_string:
            if verbose:
                logger.debug("Generating data for 'bed'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_bed_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        elif "CORE_VTE_STATUS" in search_string:
            if verbose:
                logger.debug("Generating data for 'vte_status'")
            probabilities = [0.1, 0.2, 0.4, 0.2, 0.1]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_vte_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        elif "CORE_SmokingStatus" in search_string:
            if verbose:
                logger.debug("Generating data for 'smoking'")
            probabilities = [0.8, 0.1, 0.05, 0.03, 0.02]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_smoking_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        elif "CORE_HospitalSite" in search_string:
            if verbose:
                logger.debug("Generating data for 'hospital_site'")
            probabilities = [0.8, 0.1, 0.05, 0.03, 0.02]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_hospital_site_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
        elif "AoMRC_ClinicalSummary_FT" in search_string:
            if verbose:
                logger.debug("Generating mrc text data for 'observations'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            df = generate_observations_MRC_text_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                use_GPT=use_GPT,
                fields_list=fields_list,
            )
        else:
            if verbose:
                logger.debug("Generating data for generic 'observations'")
            probabilities = [0.7, 0.1, 0.05, 0.05, 0.05]
            num_rows = random.choices(range(1, 6), probabilities)[0]
            search_term = str(
                extract_search_term_obscatalogmasteritem_displayname(search_string)
            )
            df = generate_observations_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                search_term,
                fields_list=fields_list,
            )
    elif index_name == "order":
        if "medication" in search_string:
            if verbose:
                logger.debug("Generating data for 'orders' with medication")
            num_rows = random.randint(1, 10)
            df = generate_drug_orders_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
            return df
        elif "diagnostic" in search_string:
            if verbose:
                logger.debug("Generating data for 'orders' with diagnostic")
            num_rows = random.randint(1, 10)
            df = generate_diagnostic_orders_data(
                num_rows,
                entered_list,
                global_start_year,
                global_start_month,
                global_end_year,
                global_end_month,
                fields_list=fields_list,
            )
            return df
    elif index_name == "pims_apps*":
        if verbose:
            logger.debug("Generating data for 'pims_apps'")
        num_rows = random.randint(1, 10)
        df = generate_appointments_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            fields_list=fields_list,
        )
    elif index_name == "epic_encounters":
        if verbose:
            logger.debug("Generating data for 'epic_encounters'")
        num_rows = random.randint(1, 5)
        df = generate_epic_encounters_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            fields_list=fields_list,
        )
    elif index_name == "epic_clinical_notes":
        if verbose:
            logger.debug("Generating data for 'epic_clinical_notes'")
        num_rows = random.randint(1, 5)
        df = generate_epic_clinical_notes_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            use_GPT=use_GPT,
            fields_list=fields_list,
        )
    elif index_name == "epic_medical_history":
        if term_name != "document_PatientDurableKey":
            logger.warning(
                f"Searching epic_medical_history with term_name '{term_name}'. Expected 'document_PatientDurableKey'."
            )
        if verbose:
            logger.debug("Generating data for 'epic_medical_history'")
        num_rows = random.randint(1, 5)
        df = generate_epic_medical_history_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            fields_list=fields_list,
        )
    elif index_name == "epic_orders":
        if term_name != "document_PatientDurableKey":
            logger.warning(
                f"Searching epic_orders with term_name '{term_name}'. Expected 'document_PatientDurableKey'."
            )
        if verbose:
            logger.debug("Generating data for 'epic_orders'")
        num_rows = random.randint(1, 5)
        df = generate_epic_orders_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            fields_list=fields_list,
        )
    elif index_name == "epic_lab_results":
        if verbose:
            logger.debug("Generating data for 'epic_lab_results'")
        num_rows = random.randint(1, 5)
        df = generate_epic_lab_results_data(
            num_rows,
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            fields_list=fields_list,
        )
    elif index_name == "epic_patients":
        if term_name != "patient_DurableKey":
            logger.warning(
                f"Searching epic_patients with term_name '{term_name}'. Expected 'patient_DurableKey'."
            )
        if verbose:
            logger.debug("Generating data for 'epic_patients'")
        df = generate_epic_patients_data(
            random.randint(1, 5),
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            fields_list=fields_list,
        )
    elif index_name == "epic_imaging_reports":
        if term_name != "document_PatientDurableKey":
            logger.warning(
                f"Searching epic_imaging_reports with term_name '{term_name}'. Expected 'document_PatientDurableKey'."
            )
        if verbose:
            logger.debug("Generating data for 'epic_imaging_reports'")
        df = generate_epic_imaging_reports_data(
            random.randint(1, 5),
            entered_list,
            global_start_year,
            global_start_month,
            global_end_year,
            global_end_month,
            fields_list=fields_list,
        )
    elif index_name == "epic_clinical_notes_appointments":
        if term_name != "document_PatientDurableKey":
            logger.warning(
                f"Searching epic_clinical_notes_appointments with term_name '{term_name}'. Expected 'document_PatientDurableKey'."
            )
        if verbose:
            logger.debug("Generating data for 'epic_clinical_notes_appointments'")
        df = generate_epic_clinical_notes_appointments_data(
            num_rows=random.randint(1, 5),
            entered_list=entered_list,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            fields_list=fields_list,
        )
        return df
    else:
        if verbose:
            logger.warning(
                f"No specific dummy data generator for index '{index_name}' with search string '{search_string}'."
            )

    df = df.loc[:, ~df.columns.duplicated()]

    if search_string:
        df["search_term"] = search_string

    return df
