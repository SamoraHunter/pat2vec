import pandas as pd
import logging
from typing import Any, Dict
from pat2vec.util.helper_functions import get_df_from_db

logger = logging.getLogger(__name__)

# Configuration mapping for different data types
# Keys correspond to the 'data_type' argument in retrieve_patient_data
DATA_TYPE_CONFIG: Dict[str, Dict[str, str]] = {
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
    client_idcode: str, data_type: str, config_obj: Any
) -> pd.DataFrame:
    """Retrieves patient data based on data type and storage backend configuration.

    Args:
        client_idcode: The unique identifier for the patient.
        data_type: The type of data to retrieve (e.g., 'epr_docs', 'bloods', 'drugs').
        config_obj: The configuration object containing backend settings and paths.

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

    if config_obj.storage_backend == "database":
        return get_df_from_db(
            config_obj,
            config["db_schema"],
            config["db_table"],
            patient_ids=[client_idcode],
            patient_id_column=config["id_column"],
        )
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
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return pd.DataFrame()
