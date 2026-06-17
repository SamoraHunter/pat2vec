import json
import logging
import pandas as pd
import os
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


def generate_mapping_for_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """Infers Elasticsearch field mappings from a pandas DataFrame."""
    mapping = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_datetime64_any_dtype(dtype):
            mapping[col] = {"type": "date"}
        elif pd.api.types.is_bool_dtype(dtype):
            mapping[col] = {"type": "boolean"}
        elif pd.api.types.is_integer_dtype(dtype):
            mapping[col] = {"type": "long"}
        elif pd.api.types.is_float_dtype(dtype):
            mapping[col] = {"type": "double"}
        elif pd.api.types.is_string_dtype(dtype):
            # Check if it looks like a JSON string to infer nested
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else ""
            if (
                isinstance(sample, str)
                and sample.startswith("{")
                and sample.endswith("}")
            ):
                try:
                    # Attempt to infer sub-properties from all rows to handle mixed keys
                    props = {}
                    for val in df[col].dropna():
                        obj = json.loads(val)
                        for k, v in obj.items():
                            if k not in props:
                                # Simple inference for nested keys
                                if isinstance(v, bool):
                                    t = "boolean"
                                elif isinstance(v, int):
                                    t = "long"
                                elif isinstance(v, float):
                                    t = "double"
                                else:
                                    t = "keyword"
                                props[k] = {"type": t}
                            elif props[k]["type"] != "keyword":
                                # If we see mixed types for the same key across rows, default to keyword
                                if (
                                    isinstance(v, int) and props[k]["type"] == "double"
                                ) or (
                                    isinstance(v, float) and props[k]["type"] == "long"
                                ):
                                    props[k]["type"] = "double"
                                elif not isinstance(v, (int, float, bool)):
                                    props[k]["type"] = "keyword"
                    mapping[col] = {"type": "nested", "properties": props}
                except Exception:
                    mapping[col] = {"type": "keyword"}
            else:
                mapping[col] = {"type": "keyword"}
        else:
            mapping[col] = {"type": "keyword"}
    return mapping


def generate_elastic_schema(df: pd.DataFrame, index_name: str) -> Dict[str, Any]:
    """Generates a complete Elasticsearch schema dictionary for an index."""
    return {
        index_name: {"mappings": {"properties": generate_mapping_for_dataframe(df)}}
    }


def create_schema_from_dataframe(
    df: pd.DataFrame, index_name: str, config: Any
) -> None:
    """Creates or updates an Elasticsearch schema file based on a DataFrame."""
    new_schema = generate_elastic_schema(df, index_name)
    schema_path = config.test_schema_path

    existing_schemas = {}
    if os.path.exists(schema_path):
        try:
            with open(schema_path, "r") as f:
                existing_schemas = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load existing schema at {schema_path}: {e}")

    # Merge new index schema into existing ones
    existing_schemas.update(new_schema)

    try:
        os.makedirs(os.path.dirname(schema_path), exist_ok=True)
        with open(schema_path, "w") as f:
            json.dump(existing_schemas, f, indent=2)
        if config.verbosity > 0:
            logger.info(f"Schema for index '{index_name}' saved to {schema_path}")
    except Exception as e:
        logger.error(f"Failed to write schema file {schema_path}: {e}")


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
            "epic_encounters",
            "epic_clinical_notes",
            "epic_medical_history",
            "epic_orders",
            "epic_lab_results",
            "epic_patients",
            "epic_imaging_reports",
            "epic_clinical_notes_appointments",
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
