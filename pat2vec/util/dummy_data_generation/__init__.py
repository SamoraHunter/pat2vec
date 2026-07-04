"""Dummy data generation package.

This package provides functions for generating synthetic dummy data for testing
and development purposes. It includes generators for various Elasticsearch indices.
"""

# Import generator helpers
from .generator_helpers import (
    create_random_date_from_globals,
    extract_date_range,
    extract_search_term_obscatalogmasteritem_displayname,
    generate_uuid,
    generate_uuid_list,
    is_safe_host,
    maybe_nan,
    random_state,
)

# Import sequence generators
from .sequence_generators import (
    generate_patient_timeline,
    generate_patient_timeline_faker,
    get_patient_timeline_dummy,
    run_generate_patient_timeline_and_append,
)

# Import EPR documents
from .epr_documents import (
    generate_epr_documents_data,
    generate_epr_documents_personal_data,
)

# Import orders
from .orders import (
    generate_diagnostic_orders_data,
    generate_drug_orders_data,
)

# Import basic observations
from .basic_observations import (
    generate_basic_observations_data,
    generate_basic_observations_textual_obs_data,
)

# Import observation modules
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

# Import appointments and COVID (used via __all__)
from .appointments import generate_appointments_data  # noqa: F401
from .covid import generate_covid_observations_data

# Import problem list (used via __all__)
from .problem_list import generate_problem_list_data  # noqa: F401

# Import observation router (used via __all__)
from .observation_router import (
    cohort_searcher_with_terms_and_search_dummy,  # noqa: F401
)

# Import epic modules for backward compatibility (used via __all__)
from .epic import (
    generate_epic_clinical_notes_appointments_data,  # noqa: F401
    generate_epic_clinical_notes_data,  # noqa: F401
    generate_epic_medical_history_data,  # noqa: F401
    generate_epic_encounters_data,  # noqa: F401
    generate_epic_imaging_reports_data,  # noqa: F401
    generate_epic_lab_results_data,  # noqa: F401
    generate_epic_orders_data,  # noqa: F401
    generate_epic_patients_data,  # noqa: F401
)

# Import elasticsearch population (used via __all__)
from .elasticsearch_population import populate_elastic_with_dummy_data  # noqa: F401

__all__ = [
    # Generator helpers
    "create_random_date_from_globals",
    "extract_date_range",
    "extract_search_term_obscatalogmasteritem_displayname",
    "generate_uuid",
    "generate_uuid_list",
    "is_safe_host",
    "maybe_nan",
    "random_state",
    # Sequence generators
    "generate_patient_timeline",
    "generate_patient_timeline_faker",
    "get_patient_timeline_dummy",
    "run_generate_patient_timeline_and_append",
    # EPR documents
    "generate_epr_documents_data",
    "generate_epr_documents_personal_data",
    # Orders
    "generate_diagnostic_orders_data",
    "generate_drug_orders_data",
    # Basic observations
    "generate_basic_observations_data",
    "generate_basic_observations_textual_obs_data",
    # Observation types
    "generate_bmi_data",
    "generate_bed_data",
    "generate_core_o2_data",
    "generate_core_resus_data",
    "generate_hospital_site_data",
    "generate_news_data",
    "generate_smoking_data",
    "generate_observations_MRC_text_data",
    "generate_observations_Reports_text_data",
    "generate_vte_data",
]

# Backward compatibility aliases
generate_core_02_data = generate_core_o2_data
generate_covid_data = generate_covid_observations_data
generate_demographics_data = generate_epr_documents_personal_data
generate_diagnostics_data = generate_diagnostic_orders_data
generate_drugs_data = generate_drug_orders_data
generate_reports_data = generate_observations_Reports_text_data
generate_vte_status_data = generate_vte_data

# Re-export all for backward compatibility via __all__
__all__.extend(
    [
        "generate_core_02_data",
        "generate_covid_data",
        "generate_demographics_data",
        "generate_diagnostics_data",
        "generate_drugs_data",
        "generate_reports_data",
        "generate_vte_status_data",
        # Epic modules
        "generate_epic_clinical_notes_appointments_data",
        "generate_epic_clinical_notes_data",
        "generate_epic_medical_history_data",
        "generate_epic_encounters_data",
        "generate_epic_imaging_reports_data",
        "generate_epic_lab_results_data",
        "generate_epic_orders_data",
        "generate_epic_patients_data",
        # Appointments
        "generate_appointments_data",
        # COVID
        "generate_covid_observations_data",
        # Problem list
        "generate_problem_list_data",
        # Router and population
        "cohort_searcher_with_terms_and_search_dummy",
        "populate_elastic_with_dummy_data",
    ]
)
