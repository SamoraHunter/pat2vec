import json
import logging
from typing import Any, Dict, List, Optional

# Attempt to import cs, but don't fail if it's not initialized yet
# (it will be initialized by the user or main script)
try:
    from pat2vec.pat2vec_search.cogstack_search_methods import (
        cs,
        initialize_cogstack_client,
    )
except ImportError:
    cs = None
    initialize_cogstack_client = None

logger = logging.getLogger(__name__)


def generate_schema_from_cluster(
    indices: Optional[List[str]] = None, output_file: str = "elastic_schemas.json"
) -> Dict[str, Any]:
    """
    Generates index schemas (mappings and settings) from the connected Elasticsearch cluster.

    This function retrieves the mappings and settings for specified indices from the
    live Elasticsearch instance connected via `pat2vec.cs`. It cleans the settings
    to make them suitable for creating new indices in a test environment (removing
    UUIDs, creation dates, etc.).

    Args:
        indices: List of index names or patterns to export. If None, defaults to
                 the standard pat2vec indices: ["epr_documents", "basic_observations",
                 "observations", "order", "pims_apps*"].
        output_file: Path to save the generated schema JSON.

    Returns:
        A dictionary where keys are the simplified index names (e.g., 'pims_apps'
        instead of 'pims_apps*') and values are dictionaries containing "mappings"
        and "settings".
    """
    if indices is None:
        indices = [
            "epr_documents",
            "basic_observations",
            "observations",
            "order",
            "pims_apps*",
        ]

    schemas = {}

    # Ensure client is available
    global cs
    if cs is None:
        if initialize_cogstack_client:
            logger.info("Initializing CogStack client...")
            cs = initialize_cogstack_client()

    if cs is None:
        logger.error("CogStack client (cs) could not be initialized.")
        return {}

    for index_pattern in indices:
        try:
            logger.info(f"Fetching schema for index pattern: {index_pattern}")

            # Get mapping and settings
            mappings_response = cs.elastic.indices.get_mapping(index=index_pattern)
            settings_response = cs.elastic.indices.get_settings(index=index_pattern)

            if not mappings_response:
                logger.warning(f"No indices found matching {index_pattern}")
                continue

            # Pick the first concrete index found for this pattern
            concrete_index = list(mappings_response.keys())[0]
            logger.info(
                f"Using concrete index '{concrete_index}' as template for '{index_pattern}'"
            )

            mapping = mappings_response[concrete_index].get("mappings", {})
            settings = settings_response[concrete_index].get("settings", {})

            # Clean settings to remove cluster-specific metadata
            if "index" in settings:
                # Remove read-only or internal settings that prevent creation or are unique to the source index
                keys_to_remove = [
                    "uuid",
                    "creation_date",
                    "version",
                    "provided_name",
                    "routing",
                    # We might want to keep shards/replicas or override them later,
                    # but removing them is safer for single-node test instances.
                    "number_of_shards",
                    "number_of_replicas",
                    "resize",
                    "blocks",
                ]
                for key in keys_to_remove:
                    settings["index"].pop(key, None)

            # Map the index pattern to the canonical name used in pat2vec tests
            # e.g., "pims_apps*" -> "pims_apps"
            clean_name = index_pattern.rstrip("*")

            schemas[clean_name] = {"mappings": mapping, "settings": settings}

        except Exception as e:
            logger.error(f"Error exporting schema for {index_pattern}: {e}")

    if output_file:
        try:
            with open(output_file, "w") as f:
                json.dump(schemas, f, indent=2)
            logger.info(f"Schemas successfully saved to {output_file}")
        except Exception as e:
            logger.error(f"Failed to write output file {output_file}: {e}")

    return schemas


if __name__ == "__main__":
    # Allow running directly if credentials are set up
    logging.basicConfig(level=logging.INFO)
    generate_schema_from_cluster()
