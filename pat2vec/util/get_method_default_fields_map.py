"""
Provides a mapping from pat2vec's `get` methods to the default fields
they query. This helps users understand the data sources for each
feature extraction function.
"""

from typing import Dict, List, Optional

from pat2vec.pat2vec_get_methods.get_method_appointments import APPOINTMENT_FIELDS
from pat2vec.pat2vec_get_methods.get_method_bed import BED_FIELDS
from pat2vec.pat2vec_get_methods.get_method_bloods import BLOODS_FIELDS
from pat2vec.pat2vec_get_methods.get_method_bmi import BMI_FIELDS
from pat2vec.pat2vec_get_methods.get_method_core02 import CORE_O2_FIELDS
from pat2vec.pat2vec_get_methods.get_method_core_resus import CORE_RESUS_FIELDS
from pat2vec.pat2vec_get_methods.get_method_demo import DEMOGRAPHICS_FIELDS
from pat2vec.pat2vec_get_methods.get_method_diagnostics import DIAGNOSTICS_FIELDS
from pat2vec.pat2vec_get_methods.get_method_drugs import DRUG_FIELDS
from pat2vec.pat2vec_get_methods.get_method_hosp_site import HOSP_SITE_FIELDS
from pat2vec.pat2vec_get_methods.get_method_smoking import SMOKING_FIELDS
from pat2vec.pat2vec_get_methods.get_method_vte_status import VTE_FIELDS

# This dictionary maps the name of the 'get' function to the default
# list of fields it queries.
GET_METHOD_DEFAULT_FIELDS_MAP: Dict[str, List[str]] = {
    "get_appointments": APPOINTMENT_FIELDS,
    "get_bed": BED_FIELDS,
    "get_current_pat_bloods": BLOODS_FIELDS,
    "get_bmi_features": BMI_FIELDS,
    "get_core_02": CORE_O2_FIELDS,
    "get_core_resus": CORE_RESUS_FIELDS,
    "get_demographics3": DEMOGRAPHICS_FIELDS,
    "get_demo": DEMOGRAPHICS_FIELDS,
    "get_current_pat_diagnostics": DIAGNOSTICS_FIELDS,
    "get_current_pat_drugs": DRUG_FIELDS,
    "get_hosp_site": HOSP_SITE_FIELDS,
    "get_smoking": SMOKING_FIELDS,
    "get_vte_status": VTE_FIELDS,
}


def get_default_fields_for_method(method_name: str) -> Optional[List[str]]:
    """
    Retrieves the default list of fields for a given `get` method.

    Args:
        method_name: The name of the `get` method (e.g., 'get_current_pat_bloods').

    Returns:
        A list of default fields, or None if the method is not found.
    """
    return GET_METHOD_DEFAULT_FIELDS_MAP.get(method_name)


def get_all_method_default_fields() -> Dict[str, List[str]]:
    """
    Retrieves a dictionary of all `get` methods and their default fields.

    Returns:
        A dictionary mapping method names to their default list of fields.
    """
    return GET_METHOD_DEFAULT_FIELDS_MAP.copy()
