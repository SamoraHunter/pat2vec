"""Centralized Elasticsearch index configuration for pat2vec.

This module defines the authoritative mapping between get methods, Elasticsearch
indices, fields, and time/date field configurations. This ensures consistency
between source clusters and what get methods expect.

Usage:
    from pat2vec.util.elasticsearch_index_config import (
        BLOODS_FIELDS,
        APPOINTMENT_FIELDS,
        get_all_index_configs,
        IndexConfig,
    )
"""

from typing import Any

BLOODS_FIELDS = [
    "client_idcode",
    "basicobs_itemname_analysed",
    "basicobs_value_numeric",
    "basicobs_entered",
    "clientvisit_serviceguid",
    "updatetime",
]

APPOINTMENT_FIELDS = [
    "Popular",
    "AppointmentType",
    "AttendanceReference",
    "ClinicCode",
    "ClinicDesc",
    "Consultant",
    "DateModified",
    "DNA",
    "HospitalID",
    "PatNHSNo",
    "Specialty",
    "AppointmentDateTime",
    "Attended",
    "CancDesc",
    "CancRefNo",
    "ConsultantCode",
    "DateCreated",
    "Ethnicity",
    "Gender",
    "NHSNoStatusCode",
    "NotSpec",
    "PatDateOfBirth",
    "PatForename",
    "PatPostCode",
    "PatSurname",
    "PiMsPatRefNo",
    "Primarykeyfieldname",
    "Primarykeyfieldvalue",
    "SessionCode",
    "SpecialtyCode",
]

BED_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]

BMI_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]

NEWS_FIELDS = BMI_FIELDS.copy()

CORE_O2_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]

CORE_RESUS_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]

OBS_FIELDS = CORE_RESUS_FIELDS.copy()

DEMOGRAPHICS_FIELDS = [
    "client_idcode",
    "client_firstname",
    "client_lastname",
    "client_dob",
    "client_gendercode",
    "client_racecode",
    "client_deceaseddtm",
    "updatetime",
]

DIAGNOSTICS_FIELDS = [
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

EPIC_CLINICAL_NOTES_APPOINTMENTS_FIELDS = [
    "document_PatientDurableKey",
    "document_CreatedWhen",
    "id",
]

EPIC_ENCOUNTER_FIELDS = [
    "activity_PatientDurableKey",
    "activity_AdmissionDate",
    "activity_DischargeDate",
    "activity_Department",
    "activity_Type",
    "activity_VisitClass",
    "activity_HospitalService",
    "id",
]

EPIC_LAB_RESULTS_FIELDS = [
    "document_PatientDurableKey",
    "document_CreatedWhen",
    "document_CollectedDate",
    "document_UpdatedWhen",
    "document_Name",
    "document_Content",
    "document_AbnormalLevel",
    "document_LabResultEpicId",
    "document_Fields.valueText",
    "id",
]

EPIC_PATIENTS_FIELDS = [
    "patient_DurableKey",
    "patient_CreatedWhen",
    "patient_BirthDate",
    "patient_Age",
    "patient_Gender",
    "patient_Ethnicity",
    "patient_SmokingStatus",
    "patient_MaritalStatus",
    "patient_IsCancer",
    "patient_IsFetus",
    "patient_DateOfDeath",
    "id",
]

HOSP_SITE_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]

SMOKING_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]

VTE_FIELDS = [
    "observation_guid",
    "client_idcode",
    "obscatalogmasteritem_displayname",
    "observation_valuetext_analysed",
    "observationdocument_recordeddtm",
    "clientvisit_visitidcode",
]

REPORTS_FIELDS = [
    "client_idcode",
    "updatetime",
    "textualObs",
    "basicobs_guid",
    "basicobs_value_analysed",
    "basicobs_itemname_analysed",
]

TEXTUAL_OBS_FIELDS = [
    "client_idcode",
    "basicobs_itemname_analysed",
    "basicobs_value_numeric",
    "basicobs_value_analysed",
    "basicobs_entered",
    "clientvisit_serviceguid",
    "basicobs_guid",
    "updatetime",
    "textualObs",
]

EPR_DOCS_FIELDS = [
    "client_idcode",
    "document_guid",
    "document_description",
    "body_analysed",
    "updatetime",
    "clientvisit_visitidcode",
]
EMPTY_ANNOT_COLS = []

COVID_FIELDS = [
    "observation_guid",
    "client_idcode",
    "basicobs_itemname_analysed",
    "basicobs_value_analysed",
    "basicobs_entered",
    "clientvisit_visitidcode",
]

OBS_COVID_FIELDS = COVID_FIELDS.copy()

EPIC_ORDERS_FIELDS = [
    "document_PatientDurableKey",
    "document_CreatedWhen",
    "document_UpdatedWhen",
    "document_Name",
    "document_Content",
    "document_OrderClass",
    "document_OrderDate",
    "document_OrderStatus",
    "id",
]

EPIC_CLINICAL_NOTES_FIELDS = [
    "document_PatientDurableKey",
    "document_CreatedWhen",
    "document_UpdatedWhen",
    "document_Name",
    "document_Content",
    "id",
]

EPIC_MEDICAL_HISTORY_FIELDS = [
    "document_PatientDurableKey",
    "document_CreatedWhen",
    "document_UpdatedWhen",
    "document_Name",
    "document_Content",
    "id",
]

EPIC_IMAGING_REPORTS_FIELDS = [
    "document_PatientDurableKey",
    "document_CreatedWhen",
    "document_UpdatedWhen",
    "document_Name",
    "document_Content",
    "id",
]


class IndexConfig:
    """Configuration for a single Elasticsearch index."""

    def __init__(
        self,
        index_pattern: str,
        fields: list[str],
        time_field: str | None = None,
        date_fields: list[str] | None = None,
        description: str = "",
    ) -> None:
        """Initialize index configuration.

        Args:
        ----
            index_pattern: The Elasticsearch index pattern (e.g., "pims_apps*", "observations").
            fields: List of fields queried by the get method.
            time_field: Primary time/date field used for filtering (e.g., "document_UpdatedWhen").
            date_fields: Additional date/time fields that may be present.
            description: Documentation about this index configuration.

        """
        self.index_pattern = index_pattern
        self.fields = fields
        self.time_field = time_field
        self.date_fields = date_fields or []
        self.description = description

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary for serialization."""
        return {
            "index_pattern": self.index_pattern,
            "fields": self.fields,
            "time_field": self.time_field,
            "date_fields": self.date_fields,
            "description": self.description,
        }


def get_all_index_configs() -> dict[str, IndexConfig]:
    """Get all Elasticsearch index configurations for pat2vec get methods.

    Returns
    -------
        Dictionary mapping get method names to their IndexConfig objects.
        This can be used for:
        - Manual review of index/field mappings
        - Validation that source clusters match expected configurations
        - Documentation generation

    """
    configs: dict[str, IndexConfig] = {}

    configs["get_appointments"] = IndexConfig(
        index_pattern="pims_apps*",
        fields=APPOINTMENT_FIELDS,
        time_field="AppointmentDateTime",
        date_fields=["DateModified"],
        description="PIMS appointments data with appointment scheduling info",
    )

    configs["get_bed"] = IndexConfig(
        index_pattern="observations",
        fields=BED_FIELDS,
        time_field=None,
        description="Bed occupancy and location observations",
    )

    configs["get_current_pat_bloods"] = IndexConfig(
        index_pattern="basic_observations",
        fields=BLOODS_FIELDS,
        time_field="basicobs_entered",
        description="Laboratory blood test results",
    )

    configs["get_bmi_features"] = IndexConfig(
        index_pattern="observations",
        fields=BMI_FIELDS,
        time_field=None,
        description="Body Mass Index measurements",
    )

    configs["get_core_02"] = IndexConfig(
        index_pattern="observations",
        fields=CORE_O2_FIELDS,
        time_field=None,
        description="Core 02 (oxygen saturation) observations",
    )

    configs["get_core_resus"] = IndexConfig(
        index_pattern="observations",
        fields=CORE_RESUS_FIELDS,
        time_field=None,
        description="Core resuscitation observations",
    )

    configs["get_demographics3"] = IndexConfig(
        index_pattern="epr_documents",
        fields=DEMOGRAPHICS_FIELDS,
        time_field=None,
        description="Patient demographic information from EPR documents",
    )

    configs["get_demo"] = IndexConfig(
        index_pattern="epr_documents",
        fields=DEMOGRAPHICS_FIELDS,
        time_field=None,
        description="Patient demographic information (alias of demographics3)",
    )

    configs["get_current_pat_diagnostics"] = IndexConfig(
        index_pattern="order",
        fields=DIAGNOSTICS_FIELDS,
        time_field="order_createdwhen",
        description="Diagnostic order/test results",
    )

    configs["get_current_pat_drugs"] = IndexConfig(
        index_pattern="order",
        fields=DRUG_FIELDS,
        time_field="order_createdwhen",
        description="Medication drug orders",
    )

    configs["get_hosp_site"] = IndexConfig(
        index_pattern="observations",
        fields=HOSP_SITE_FIELDS,
        time_field=None,
        description="Hospital site/location information",
    )

    configs["get_news"] = IndexConfig(
        index_pattern="observations",
        fields=[],
        time_field=None,
        description="National Early Warning Score observations (fields not defined)",
    )

    configs["get_smoking"] = IndexConfig(
        index_pattern="observations",
        fields=SMOKING_FIELDS,
        time_field=None,
        description="Smoking status observations",
    )

    configs["get_vte_status"] = IndexConfig(
        index_pattern="observations",
        fields=VTE_FIELDS,
        time_field=None,
        description="Venous thromboembolism (VTE) risk assessment",
    )

    configs["get_current_pat_annotations"] = IndexConfig(
        index_pattern="epr_documents",
        fields=EMPTY_ANNOT_COLS,
        time_field=None,
        description="MedCAT annotations on EPR documents",
    )

    configs["get_current_pat_annotations_mrc_cs"] = IndexConfig(
        index_pattern="observations",
        fields=EMPTY_ANNOT_COLS,
        time_field=None,
        description="MedCAT annotations on MRC CS observations",
    )

    configs["get_current_pat_textual_obs_annotations"] = IndexConfig(
        index_pattern="basic_observations",
        fields=EMPTY_ANNOT_COLS,
        time_field=None,
        description="MedCAT annotations on textual observations",
    )

    configs["get_current_pat_epic_orders_annotations"] = IndexConfig(
        index_pattern="epic_orders",
        fields=EMPTY_ANNOT_COLS,
        time_field=None,
        description="MedCAT annotations on Epic orders",
    )

    configs["get_covid"] = IndexConfig(
        index_pattern="basic_observations",
        fields=[],
        time_field=None,
        description="COVID-related observations (fields not defined)",
    )

    configs["get_epic_encounters"] = IndexConfig(
        index_pattern="epic_encounters",
        fields=EPIC_ENCOUNTER_FIELDS,
        time_field=None,
        description="Epic hospital encounters/admissions",
    )

    configs["get_epic_imaging_reports"] = IndexConfig(
        index_pattern="epic_imaging_reports",
        fields=EPIC_IMAGING_REPORTS_FIELDS,
        time_field=None,
        description="Epic imaging reports (now only via annotations)",
    )

    configs["get_epic_patients"] = IndexConfig(
        index_pattern="epic_patients",
        fields=EPIC_PATIENTS_FIELDS,
        time_field=None,
        description="Epic patient master directory",
    )

    configs["get_epic_lab_results"] = IndexConfig(
        index_pattern="epic_lab_results",
        fields=EPIC_LAB_RESULTS_FIELDS,
        time_field=None,
        description="Epic laboratory test results",
    )

    configs["get_epic_orders"] = IndexConfig(
        index_pattern="epic_orders",
        fields=[],
        time_field=None,
        description="Epic orders (now only via annotations)",
    )

    configs["get_epic_medical_history"] = IndexConfig(
        index_pattern="epic_medical_history",
        fields=EPIC_MEDICAL_HISTORY_FIELDS,
        time_field=None,
        description="Epic medical history (now only via annotations)",
    )

    configs["get_epic_clinical_notes"] = IndexConfig(
        index_pattern="epic_clinical_notes",
        fields=EPIC_CLINICAL_NOTES_FIELDS,
        time_field=None,
        description="Epic clinical notes (now only via annotations)",
    )

    configs["get_epic_clinical_notes_appointments"] = IndexConfig(
        index_pattern="epic_clinical_notes_appointments",
        fields=EPIC_CLINICAL_NOTES_APPOINTMENTS_FIELDS,
        time_field=None,
        description="Epic clinical notes appointments metadata",
    )

    configs["get_current_pat_report_annotations"] = IndexConfig(
        index_pattern="reports",
        fields=EMPTY_ANNOT_COLS,
        time_field=None,
        description="MedCAT annotations on reports",
    )

    return configs


def get_index_config_for_method(method_name: str) -> IndexConfig | None:
    """Get the index configuration for a specific get method.

    Args:
    ----
        method_name: The name of the get method (e.g., 'get_current_pat_bloods').

    Returns:
    -------
        IndexConfig object if found, None otherwise.

    """
    configs = get_all_index_configs()
    return configs.get(method_name)


def print_index_config_summary() -> None:
    """Print a human-readable summary of all index configurations."""
    configs = get_all_index_configs()

    print("=" * 80)
    print("ELASTICSEARCH INDEX CONFIGURATION SUMMARY")
    print("=" * 80)
    print()

    for method_name, config in sorted(configs.items()):
        print(f"Method: {method_name}")
        print(f"  Index Pattern: {config.index_pattern}")
        print(f"  Time Field: {config.time_field or 'None'}")
        if config.date_fields:
            print(f"  Date Fields: {', '.join(config.date_fields)}")
        print(f"  Field Count: {len(config.fields)}")
        if config.description:
            print(f"  Description: {config.description}")
        print()

    print("=" * 80)
    print(f"Total methods configured: {len(configs)}")
    print("=" * 80)


def validate_configs_against_cluster(index_map: dict[str, str]) -> list[dict[str, Any]]:
    """Validate current configurations against a provided index mapping.

    Args:
    ----
        index_map: Dictionary mapping method names to index patterns.

    Returns:
    -------
        List of validation issues found.

    """
    configs = get_all_index_configs()
    issues: list[dict[str, Any]] = []

    for method_name, expected_index in index_map.items():
        if method_name not in configs:
            issues.append(
                {
                    "type": "missing_config",
                    "method": method_name,
                    "issue": f"No configuration found for method '{method_name}'",
                },
            )
            continue

        config = configs[method_name]
        if config.index_pattern != expected_index:
            issues.append(
                {
                    "type": "index_mismatch",
                    "method": method_name,
                    "expected": config.index_pattern,
                    "found": expected_index,
                },
            )

    return issues


__all__ = [
    "APPOINTMENT_FIELDS",
    "BED_FIELDS",
    "BLOODS_FIELDS",
    "BMI_FIELDS",
    "CORE_O2_FIELDS",
    "CORE_RESUS_FIELDS",
    "COVID_FIELDS",
    "DEMOGRAPHICS_FIELDS",
    "DIAGNOSTICS_FIELDS",
    "DRUG_FIELDS",
    "EMPTY_ANNOT_COLS",
    "EPIC_CLINICAL_NOTES_APPOINTMENTS_FIELDS",
    "EPIC_CLINICAL_NOTES_FIELDS",
    "EPIC_ENCOUNTER_FIELDS",
    "EPIC_IMAGING_REPORTS_FIELDS",
    "EPIC_LAB_RESULTS_FIELDS",
    "EPIC_MEDICAL_HISTORY_FIELDS",
    "EPIC_ORDERS_FIELDS",
    "EPIC_PATIENTS_FIELDS",
    "HOSP_SITE_FIELDS",
    "SMOKING_FIELDS",
    "VTE_FIELDS",
    "IndexConfig",
    "get_all_index_configs",
    "get_index_config_for_method",
    "print_index_config_summary",
    "validate_configs_against_cluster",
]


if __name__ == "__main__":
    print_index_config_summary()
