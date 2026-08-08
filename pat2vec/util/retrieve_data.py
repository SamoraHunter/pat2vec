import logging
from typing import Any

import pandas as pd

from pat2vec.util.helper_functions import get_df_from_db

logger = logging.getLogger(__name__)

# Configuration mapping for different data types
# Keys correspond to the 'data_type' argument in retrieve_patient_data
DATA_TYPE_CONFIG: dict[str, dict[str, str]] = {
    "epr_docs": {
        "db_table": "raw_epr_docs",
        "db_schema": "raw_data",
        "path_attr": "pre_document_batch_path",
        "id_column": "client_idcode",
    },
    "mct_docs": {
        "db_table": "raw_mct_docs",
        "db_schema": "raw_data",
        "path_attr": "pre_document_batch_path_mct",
        "id_column": "client_idcode",
    },
    "bloods": {
        "db_table": "raw_bloods",
        "db_schema": "raw_data",
        "path_attr": "pre_bloods_batch_path",
        "id_column": "client_idcode",
    },
    "drugs": {
        "db_table": "raw_drugs",
        "db_schema": "raw_data",
        "path_attr": "pre_drugs_batch_path",
        "id_column": "client_idcode",
    },
    "diagnostics": {
        "db_table": "raw_diagnostics",
        "db_schema": "raw_data",
        "path_attr": "pre_diagnostics_batch_path",
        "id_column": "client_idcode",
    },
    "news": {
        "db_table": "raw_news",
        "db_schema": "raw_data",
        "path_attr": "pre_news_batch_path",
        "id_column": "client_idcode",
    },
    "bmi": {
        "db_table": "raw_bmi",
        "db_schema": "raw_data",
        "path_attr": "pre_bmi_batch_path",
        "id_column": "client_idcode",
    },
    "demographics": {
        "db_table": "raw_demographics",
        "db_schema": "raw_data",
        "path_attr": "pre_demo_batch_path",
        "id_column": "client_idcode",
    },
    "obs": {
        "db_table": "raw_obs",
        "db_schema": "raw_data",
        "path_attr": "pre_obs_batch_path",
        "id_column": "client_idcode",
    },
    "smoking": {
        "db_table": "raw_smoking",
        "db_schema": "raw_data",
        "path_attr": "pre_obs_batch_path",
        "id_column": "client_idcode",
        "display_name_filter": "CORE_SmokingStatus",
    },
    "vte_status": {
        "db_table": "raw_vte",
        "db_schema": "raw_data",
        "path_attr": "pre_obs_batch_path",
        "id_column": "client_idcode",
        "display_name_filter": "CORE_VTE_STATUS",
    },
    "hosp_site": {
        "db_table": "raw_hospsite",
        "db_schema": "raw_data",
        "path_attr": "pre_obs_batch_path",
        "id_column": "client_idcode",
        "display_name_filter": "CORE_HospitalSite",
    },
    "core_resus": {
        "db_table": "raw_resus",
        "db_schema": "raw_data",
        "path_attr": "pre_obs_batch_path",
        "id_column": "client_idcode",
        "display_name_filter": "CORE_RESUS_STATUS",
    },
    "core_02": {
        "db_table": "raw_core_02",
        "db_schema": "raw_data",
        "path_attr": "pre_obs_batch_path",
        "id_column": "client_idcode",
        "display_name_filter": "CORE_SpO2",
    },
    "bed": {
        "db_table": "raw_bed",
        "db_schema": "raw_data",
        "path_attr": "pre_obs_batch_path",
        "id_column": "client_idcode",
        "display_name_filter": "CORE_BedNumber3",
    },
    "covid": {
        "db_table": "raw_covid",
        "db_schema": "raw_data",
        "path_attr": "pre_misc_batch_path",
        "id_column": "client_idcode",
    },
    "textual_obs": {
        "db_table": "raw_textual_obs",
        "db_schema": "raw_data",
        "path_attr": "pre_textual_obs_document_batch_path",
        "id_column": "client_idcode",
    },
    "reports": {
        "db_table": "raw_reports",
        "db_schema": "raw_data",
        "path_attr": "pre_document_batch_path_reports",
        "id_column": "client_idcode",
    },
    "appointments": {
        "db_table": "raw_appointments",
        "db_schema": "raw_data",
        "path_attr": "pre_appointments_batch_path",
        "id_column": "HospitalID",
    },
    "epr_annotations": {
        "db_table": "ann_epr_docs",
        "db_schema": "annotations",
        "path_attr": "pre_document_annotation_batch_path",
        "id_column": "client_idcode",
    },
    "mct_annotations": {
        "db_table": "ann_mct_docs",
        "db_schema": "annotations",
        "path_attr": "pre_document_annotation_batch_path_mct",
        "id_column": "client_idcode",
    },
    "textual_obs_annotations": {
        "db_table": "ann_textual_obs",
        "db_schema": "annotations",
        "path_attr": "pre_textual_obs_annotation_batch_path",
        "id_column": "client_idcode",
    },
    "reports_annotations": {  # Added missing entry for reports_annotations
        "db_table": "ann_reports",
        "db_schema": "annotations",
        "path_attr": "pre_document_annotation_batch_path_reports",
        "id_column": "client_idcode",
    },
    "epic_orders_annotations": {
        "db_table": "ann_epic_orders",
        "db_schema": "annotations",
        "path_attr": "pre_epic_orders_annotation_batch_path",
        "id_column": "client_idcode",
    },
    "epic_clinical_notes_annotations": {
        "db_table": "ann_epic_clinical_notes",
        "db_schema": "annotations",
        "path_attr": "pre_epic_clinical_notes_annotation_batch_path",
        "id_column": "client_idcode",
    },
    "epic_clinical_notes_appointments_annotations": {
        "db_table": "ann_epic_clinical_notes_appointments",
        "db_schema": "annotations",
        "path_attr": "pre_epic_clinical_notes_appointments_annotation_batch_path",
        "id_column": "client_idcode",
    },
    "epic_imaging_reports_annotations": {
        "db_table": "ann_epic_imaging_reports",
        "db_schema": "annotations",
        "path_attr": "pre_epic_imaging_reports_annotation_batch_path",
        "id_column": "client_idcode",
    },
    "epic_medical_history_annotations": {
        "db_table": "ann_epic_medical_history",
        "db_schema": "annotations",
        "path_attr": "pre_epic_medical_history_annotation_batch_path",
        "id_column": "client_idcode",
    },
    "epic_encounters": {
        "db_table": "raw_epic_encounters",
        "db_schema": "raw_data",
        "path_attr": "pre_epic_encounters_batch_path",
        "id_column": "client_idcode",
    },
    "epic_clinical_notes": {
        "db_table": "raw_epic_clinical_notes",
        "db_schema": "raw_data",
        "path_attr": "pre_epic_clinical_notes_batch_path",
        "id_column": "client_idcode",
        "time_field": "document_CreatedWhen",  # Added time_field
    },
    "epic_medical_history": {
        "db_table": "raw_epic_medical_history",
        "db_schema": "raw_data",
        "path_attr": "pre_epic_medical_history_batch_path",
        "id_column": "client_idcode",
    },
    "epic_orders": {
        "db_table": "raw_epic_orders",
        "db_schema": "raw_data",
        "path_attr": "pre_epic_orders_batch_path",
        "id_column": "client_idcode",
    },
    "epic_lab_results": {
        "db_table": "raw_epic_lab_results",
        "db_schema": "raw_data",
        "path_attr": "pre_epic_lab_results_batch_path",
        "id_column": "client_idcode",
    },
    "epic_patients": {
        "db_table": "raw_epic_patients",
        "db_schema": "raw_data",
        "path_attr": "pre_epic_patients_batch_path",
        "id_column": "client_idcode",
    },
    "epic_imaging_reports": {
        "db_table": "raw_epic_imaging_reports",
        "db_schema": "raw_data",
        "path_attr": "pre_epic_imaging_reports_batch_path",
        "id_column": "client_idcode",
    },
    "epic_clinical_notes_appointments": {
        "db_table": "raw_epic_clinical_notes_appointments",
        "db_schema": "raw_data",
        "path_attr": "pre_epic_clinical_notes_appointments_batch_path",
        "id_column": "client_idcode",
    },
}


def retrieve_patient_data(
    client_idcode: str,
    data_type: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
) -> pd.DataFrame:
    """Retrieves patient data based on data type and storage backend configuration.

    Args:
        client_idcode: The unique identifier for the patient.
        data_type: The type of data to retrieve (e.g., 'epr_docs', 'bloods', 'drugs').
        config_obj: The configuration object containing backend settings and paths.
        cohort_searcher_with_terms_and_search: Optional search function to fetch
            data from Elasticsearch if not found in database/CSV.

    Returns:
        pd.DataFrame: A DataFrame containing the requested data, or an empty DataFrame
        if not found or if data_type is invalid.
    """
    if data_type not in DATA_TYPE_CONFIG:
        logger.error(
            f"Unknown data type: '{data_type}'. Supported types: {list(DATA_TYPE_CONFIG.keys())}"
        )
        return pd.DataFrame()

    config = DATA_TYPE_CONFIG[data_type]

    # In testing mode with dummy generator, use the search function directly
    if (
        config_obj.testing
        and not config_obj.testing_elastic
        and cohort_searcher_with_terms_and_search is not None
    ):
        # Import for potential future use (currently not required)
        try:
            df = _fetch_data_from_dummy_generator(
                client_idcode,
                data_type,
                config_obj,
                cohort_searcher_with_terms_and_search,
            )
            return df
        except Exception as e:
            logger.error(f"Error fetching {data_type} from dummy generator: {e}")
            return pd.DataFrame()

    if config_obj.storage_backend == "database":
        df = get_df_from_db(
            config_obj,
            config["db_schema"],
            config["db_table"],
            patient_ids=[client_idcode],
            patient_id_column=config["id_column"],
        )
        # For Epic types, try ES fallback if database is empty
        if df.empty and data_type.startswith("epic_"):
            df = _fetch_epic_data_from_es(
                client_idcode,
                data_type.replace("_annotations", ""),
                config_obj,
                cohort_searcher_with_terms_and_search,
            )
        return df
    else:
        # File-based backend
        path_attr = config["path_attr"]
        if not hasattr(config_obj, path_attr):
            logger.error(f"Config object missing required attribute: {path_attr}")
            return pd.DataFrame()

        base_path = getattr(config_obj, path_attr)
        file_path = f"{base_path}/{client_idcode}.csv"

        try:
            df = pd.read_csv(file_path)
            # Apply concept-specific filtering for mixed observation directories
            filter_val = config.get("display_name_filter")
            if filter_val and not df.empty:
                col = "obscatalogmasteritem_displayname"
                if col in df.columns:
                    df = df[df[col] == filter_val]
            return df
        except FileNotFoundError:
            # For Epic types, try ES fallback if file not found
            if (
                data_type.startswith("epic_")
                and cohort_searcher_with_terms_and_search is not None
            ):
                df = _fetch_epic_data_from_es(
                    client_idcode,
                    data_type.replace("_annotations", ""),
                    config_obj,
                    cohort_searcher_with_terms_and_search,
                )
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return pd.DataFrame()


def _fetch_epic_data_from_es(
    client_idcode: str,
    data_type: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any | None = None,
) -> pd.DataFrame:
    """Fetches Epic data from Elasticsearch if not found in database/CSV.

    Args:
        client_idcode: The unique identifier for the patient.
        data_type: The type of Epic data (e.g., 'epic_imaging_reports').
        config_obj: Configuration object with search function and date settings.
        cohort_searcher_with_terms_and_search: Search function to use.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched data, or empty if not found.
    """
    try:
        from pat2vec.util.helper_functions import save_raw_patient_batch

        start_year = config_obj.global_start_year
        start_month = config_obj.global_start_month
        start_day = config_obj.global_start_day
        end_year = config_obj.global_end_year
        end_month = config_obj.global_end_month
        end_day = config_obj.global_end_day

        search_string = f"document_UpdatedWhen:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

        # Map data_type to appropriate term_name
        term_map = {
            "epic_clinical_notes": "document_PatientDurableKey",
            "epic_medical_history": "document_PatientDurableKey",
            "epic_orders": "document_PatientDurableKey",
            "epic_lab_results": "document_PatientDurableKey",
            "epic_patients": "patient_DurableKey",
            "epic_encounters": "activity_PatientDurableKey",
            "epic_clinical_notes_appointments": "document_PatientDurableKey",
            "epic_imaging_reports": "document_PatientDurableKey",
        }
        term_name = term_map.get(data_type, "client_idcode")

        # The search function is expected to return data with appropriate columns
        # Define appropriate fields based on data type - include primary key, type-specific fields,
        # and metadata columns for ES indexing (_index, _id, _score)
        field_map = {
            "epic_clinical_notes": [
                "document_PatientDurableKey",
                "document_CreatedWhen",
                "document_text",
                "updatetime",
                "_index",
                "_id",
                "_score",
            ],
            "epic_medical_history": [
                "document_PatientDurableKey",
                "document_Category",
                "document_Name",
                "updatetime",
                "_index",
                "_id",
                "_score",
            ],
            "epic_orders": [
                "document_PatientDurableKey",
                "order_Id",
                "order_Type",
                "updatetime",
                "_index",
                "_id",
                "_score",
            ],
            "epic_lab_results": [
                "document_PatientDurableKey",
                "lab_ResultId",
                "lab_TestName",
                "updatetime",
                "_index",
                "_id",
                "_score",
            ],
            "epic_patients": [
                "patient_DurableKey",
                "patient_FirstName",
                "patient_LastName",
                "patient_DoB",
                "updatetime",
                "_index",
                "_id",
                "_score",
            ],
            "epic_encounters": [
                "activity_PatientDurableKey",
                "activity_AdmissionDate",
                "activity_DischargeDate",
                "activity_Department",
                "activity_Type",
                "updatetime",
                "_index",
                "_id",
                "_score",
            ],
            "epic_clinical_notes_appointments": [
                "document_PatientDurableKey",
                "appointment_Id",
                "appointment_Time",
                "updatetime",
                "_index",
                "_id",
                "_score",
            ],
            "epic_imaging_reports": [
                "document_PatientDurableKey",
                "report_Type",
                "report_Text",
                "updatetime",
                "_index",
                "_id",
                "_score",
            ],
        }

        fields_list = field_map.get(data_type, ["client_idcode", "updatetime"])

        results = cohort_searcher_with_terms_and_search(
            index_name=data_type,
            fields_list=fields_list,
            term_name=term_name,
            entered_list=[client_idcode],
            search_string=search_string,
        )

        if results is not None and not results.empty:
            # Rename ES columns to match database schema
            # Handle document_PatientDurableKey -> client_idcode
            if "document_PatientDurableKey" in results.columns:
                results.rename(
                    columns={"document_PatientDurableKey": "client_idcode"},
                    inplace=True,
                )
            # Handle patient_DurableKey -> client_idcode (for epic_patients)
            elif "patient_DurableKey" in results.columns:
                results.rename(
                    columns={"patient_DurableKey": "client_idcode"},
                    inplace=True,
                )
            # Handle activity_PatientDurableKey -> client_idcode (for epic_encounters)
            elif "activity_PatientDurableKey" in results.columns:
                results.rename(
                    columns={"activity_PatientDurableKey": "client_idcode"},
                    inplace=True,
                )

            # Rename time fields to updatetime
            if "document_CreatedWhen" in results.columns:
                results.rename(
                    columns={"document_CreatedWhen": "updatetime"}, inplace=True
                )
            elif "activity_AdmissionDate" in results.columns:
                results.rename(
                    columns={"activity_AdmissionDate": "updatetime"}, inplace=True
                )

            # Ensure client_idcode is present for db storage
            if "client_idcode" not in results.columns:
                results["client_idcode"] = client_idcode

            # Save to database if using database backend
            if config_obj.storage_backend == "database":
                try:
                    save_raw_patient_batch(
                        results,
                        client_idcode,
                        (
                            data_type.replace("raw_", "")
                            if data_type.startswith("raw_")
                            else data_type
                        ),
                        config_obj,
                    )
                except Exception as e:
                    logger.debug(f"Failed to save ES data for {data_type}: {e}")

            return results

        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error fetching {data_type} from ES for {client_idcode}: {e}")
        return pd.DataFrame()


def _fetch_data_from_dummy_generator(
    client_idcode: str,
    data_type: str,
    config_obj: Any,
    cohort_searcher_with_terms_and_search: Any,
) -> pd.DataFrame:
    """Fetch patient data using the dummy generator when in testing mode.

    Args:
        client_idcode: The unique identifier for the patient.
        data_type: The type of data to retrieve.
        config_obj: Configuration object with date settings and other params.
        cohort_searcher_with_terms_and_search: The dummy search function to call.

    Returns:
        pd.DataFrame: A DataFrame containing the generated dummy data.
    """
    start_year = config_obj.global_start_year
    start_month = config_obj.global_start_month
    start_day = config_obj.global_start_day
    end_year = config_obj.global_end_year
    end_month = config_obj.global_end_month
    end_day = config_obj.global_end_day

    # Use specific search patterns that the dummy generator recognizes
    if data_type == "drugs":
        search_string = f"medication updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
    elif data_type == "diagnostics":
        search_string = f"diagnostic updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"
    else:
        search_string = f"updatetime:[{start_year}-{start_month}-{start_day} TO {end_year}-{end_month}-{end_day}]"

    # Map data types to indices that dummy_generator recognizes
    index_name_map = {
        "drugs": "order",  # Use 'order' not 'drug_orders'
        "bloods": "basic_observations",
        "epr_docs": "epr_documents",
        "mct_docs": "observations",
        "demographics": "epr_documents",
        "diagnostics": "order",  # diagnostics are under 'order' with 'diagnostic' in search_string
        "news": "basic_observations",
        "bmi": "basic_observations",
        "obs": "basic_observations",
        "smoking": "basic_observations",
        "vte_status": "basic_observations",
        "hosp_site": "basic_observations",
        "core_resus": "basic_observations",
        "core_02": "basic_observations",
        "bed": "basic_observations",
        "covid": "basic_observations",
        "textual_obs": "basic_observations",
        "reports": "basic_observations",
        "appointments": "pims_apps*",
    }

    index_name = index_name_map.get(data_type, data_type)

    field_map = {
        "drugs": [
            "order_guid",
            "client_idcode",
            "order_name",
            "order_summaryline",
            "order_holdreasontext",
            "order_entered",
            "order_createdwhen",
            "_id",
            "_index",
            "_score",
        ],
        "bloods": [
            "basicobs_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_numeric",
            "basicobs_entered",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "epr_docs": [
            "client_idcode",
            "document_guid",
            "document_description",
            "body_analysed",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "mct_docs": [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "_id",
            "_index",
            "_score",
        ],
        "demographics": [
            "client_idcode",
            "client_firstname",
            "client_lastname",
            "client_dob",
            "client_gendercode",
            "updatetime",
        ],
        "diagnostics": [
            "order_guid",
            "client_idcode",
            "order_name",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "news": [
            "basicobs_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_numeric",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "bmi": [
            "basicobs_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_numeric",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "obs": [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "_id",
            "_index",
            "_score",
        ],
        "smoking": [
            "basicobs_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "vte_status": [
            "basicobs_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "hosp_site": [
            "basicobs_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "core_resus": [
            "basicobs_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "core_02": [
            "basicobs_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "basicobs_value_numeric",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "bed": [
            "basicobs_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "covid": [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "_id",
            "_index",
            "_score",
        ],
        "textual_obs": [
            "basicobs_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "basicobs_value_numeric",
            "updatetime",
            "textualObs",
            "_id",
            "_index",
            "_score",
        ],
        "reports": [
            "basicobs_guid",
            "client_idcode",
            "basicobs_itemname_analysed",
            "textualObs",
            "updatetime",
            "_id",
            "_index",
            "_score",
        ],
        "appointments": [
            "HospitalID",
            "AppointmentDateTime",
            "AppointmentType",
            "_id",
            "_index",
            "_score",
        ],
    }

    fields_list = field_map.get(data_type, ["client_idcode", "updatetime"])

    results = cohort_searcher_with_terms_and_search(
        index_name=index_name,
        fields_list=fields_list,
        term_name="client_idcode",
        entered_list=[client_idcode],
        search_string=search_string,
    )

    if results is not None and not results.empty:
        return results

    return pd.DataFrame()
