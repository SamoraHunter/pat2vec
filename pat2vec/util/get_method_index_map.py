"""
Provides a mapping from pat2vec's `get` methods to the default Elasticsearch
indices they query. This helps users understand the data sources for each
feature extraction function.
"""

from typing import Dict, Optional

# This dictionary maps the name of the 'get' function to the default
# Elasticsearch index it queries. For methods that operate on pre-fetched
# data (like annotations), the index is the one from which the raw data
# was originally sourced.
GET_METHOD_INDEX_MAP: Dict[str, str] = {
    "get_appointments": "pims_apps*",
    "get_bed": "observations",
    "get_current_pat_bloods": "basic_observations",
    "get_bmi_features": "observations",
    "get_core_02": "observations",
    "get_core_resus": "observations",
    "get_demographics3": "epr_documents",
    "get_demo": "epr_documents",
    "get_current_pat_diagnostics": "order",
    "get_current_pat_drugs": "order",
    "get_hosp_site": "observations",
    "get_news": "observations",
    "get_smoking": "observations",
    "get_vte_status": "observations",
    "get_current_pat_annotations": "epr_documents",
    "get_current_pat_annotations_mrc_cs": "observations",
    "get_current_pat_textual_obs_annotations": "basic_observations",
    "get_current_pat_report_annotations": "observations",
}


def get_index_for_method(method_name: str) -> Optional[str]:
    """
    Retrieves the default Elasticsearch index for a given `get` method.

    Args:
        method_name: The name of the `get` method (e.g., 'get_current_pat_bloods').

    Returns:
        The name of the default index as a string, or None if the method
        is not found in the map.
    """
    return GET_METHOD_INDEX_MAP.get(method_name)


def get_all_method_indices() -> Dict[str, str]:
    """
    Retrieves a dictionary of all `get` methods and their default indices.

    Returns:
        A dictionary mapping method names to their default index names.
    """
    return GET_METHOD_INDEX_MAP.copy()
