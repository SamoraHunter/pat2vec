"""Elasticsearch population helper.

This module contains functions for populating Elasticsearch with dummy data.
"""

import json
import logging
import os
import random
from typing import Any, List

import pandas as pd

from .epr_documents import (
    generate_epr_documents_data,
    generate_epr_documents_personal_data,
)
from .basic_observations import (
    generate_basic_observations_data,
    generate_basic_observations_textual_obs_data,
)
from .observations import (
    generate_bmi_data,
    generate_news_data,
)
from .orders import generate_diagnostic_orders_data, generate_drug_orders_data
from .appointments import generate_appointments_data
from .epic import (
    generate_epic_clinical_notes_data,
    generate_epic_encounters_data,
    generate_epic_imaging_reports_data,
    generate_epic_lab_results_data,
    generate_epic_medical_history_data,
    generate_epic_orders_data,
    generate_epic_patients_data,
)
from .generator_helpers import (
    create_random_date_from_globals,
    generate_uuid,
    is_safe_host,
)

logger = logging.getLogger(__name__)


def populate_elastic_with_dummy_data(
    config_obj: Any, n_patients: int = 10
) -> List[str]:
    """Generates dummy data and ingests it into Elasticsearch.

    This function generates random patient IDs and creates dummy data for
    several indices (epr_documents, observations, basic_observations,
    order, pims_apps). It then uses `ingest_data_to_elasticsearch` to
    load this data into the configured Elasticsearch instance.

    Args:
        config_obj: The configuration object containing date ranges.
        n_patients: The number of dummy patients to generate. Defaults to 10.

    Returns:
        A list of the generated dummy patient IDs.
    """
    from .sequence_generators import generate_uuid_list
    from pat2vec.pat2vec_search.cogstack_search_methods import CogStack

    # Safeguard: Ensure testing flags are enabled in config
    if not getattr(config_obj, "testing", False) or not getattr(
        config_obj, "testing_elastic", False
    ):
        logger.error(
            "Safety Block: 'testing' and 'testing_elastic' must both be True to populate dummy data. Aborting."
        )
        return []

    # Initialize CogStack client to interact with Elastic
    creds_filename = "test_elastic_credentials.py"
    creds_path = os.path.abspath(creds_filename)
    if not os.path.exists(creds_path):
        logger.error(
            f"Safety Block: Test credentials file '{creds_filename}' not found at {creds_path}. Aborting dummy data population."
        )
        return []

    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("test_creds_module", creds_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load specs from {creds_path}")
        test_creds = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(test_creds)
        hosts = getattr(test_creds, "hosts", [])
        username = getattr(test_creds, "username", None)
        password = getattr(test_creds, "password", None)
        api_key = getattr(test_creds, "api_key", None)
        if api_key:
            cs = CogStack(hosts=hosts, api_key=api_key, api=True)
        else:
            cs = CogStack(hosts=hosts, username=username, password=password, api=False)
        logger.info(f"Loaded isolated test credentials from {creds_path}")
    except Exception as e:
        logger.error(
            f"Failed to initialize CogStack client from credentials file {creds_path}: {e}"
        )
        return []

    if cs:
        try:
            # Safeguard: Check if connecting to a safe/test environment
            nodes = cs.elastic.transport.node_pool.all()
            hosts = [node.host for node in nodes]
            if not all(is_safe_host(h) for h in hosts):
                logger.error(
                    f"Unsafe operation: Attempting to populate dummy data on non-local host(s): {hosts}. Aborting."
                )
                return []

            # Safeguard: Check username
            safe_users = ["elastic", "test_user", "dummy_user"]
            current_user = getattr(config_obj, "username", None)
            if current_user and current_user not in safe_users:
                logger.error(
                    f"Unsafe operation: Attempting to populate dummy data with non-test user '{current_user}'. Aborting."
                )
                return []

            # Log cluster info
            cluster_info = cs.elastic.info()
            cluster_name = cluster_info.get("cluster_name")
            logger.info(
                f"Populating dummy data on cluster: {cluster_name} (version {cluster_info.get('version', {}).get('number')})"
            )

            # Safeguard: Verify cluster is empty or allowed to proceed
            indices = cs.elastic.cat.indices(format="json")
            user_indices = [
                i["index"] for i in indices if not i["index"].startswith(".")
            ]
            if user_indices and not getattr(config_obj, "testing_elastic", False):
                logger.error(
                    f"Unsafe operation: Target cluster is not empty. Found indices: {user_indices}. Aborting."
                )
                return []
        except Exception as e:
            logger.error(f"Failed to verify cluster safety: {e}. Aborting.")
            return []
    else:
        logger.error("Failed to initialize CogStack client. Aborting population.")
        return []

    global_start_year = int(config_obj.global_start_year)
    global_start_month = int(config_obj.global_start_month)

    global_end_year = int(config_obj.global_end_year)
    global_end_month = int(config_obj.global_end_month)

    # Load schema and create indices if schema file exists
    schema_path = getattr(config_obj, "test_schema_path", None) or os.path.join(
        "test_files", "elastic_schemas.json"
    )

    if os.path.exists(schema_path):
        try:
            with open(schema_path, "r") as f:
                schemas = json.load(f)
            logger.info(f"Applying schemas from {schema_path}...")
            for index_name, schema_data in schemas.items():
                # Delete index if it exists to ensure clean state with correct mapping
                if cs.elastic.indices.exists(index=index_name):
                    cs.elastic.indices.delete(index=index_name)
                    logger.info(f"Deleted existing index: {index_name}")
                mappings = schema_data.get("mappings", {})
                settings = schema_data.get("settings", {})
                # Force dynamic mapping to True to ensure dummy fields are indexed
                mappings["dynamic"] = True

                # Create index
                cs.elastic.indices.create(
                    index=index_name, mappings=mappings, settings=settings
                )
                logger.info(f"Created index: {index_name} with custom schema")
        except Exception as e:
            logger.error(f"Failed to apply Elastic schemas: {e}")

    # 1. Generate Dummy Patient IDs.
    config_patient_list = getattr(config_obj, "all_patient_list", None)
    if config_patient_list is not None and len(config_patient_list) > 0:
        patient_ids = list(config_patient_list)
        logger.info(
            f"Using {len(patient_ids)} patients from config_obj.all_patient_list"
        )
    elif getattr(config_obj, "testing_elastic", False):
        patient_ids = generate_uuid_list(n_patients, "P")
        logger.info(
            f"Generated {n_patients} dummy patient IDs for testing_elastic: {patient_ids[:5]}..."
        )
    else:
        try:
            from pat2vec.pat2vec_pat_list.get_patient_treatment_list import (
                extract_treatment_id_list_from_docs,
            )

            patient_ids = extract_treatment_id_list_from_docs(config_obj)
        except Exception as e:
            logger.debug(f"Could not load existing patient list: {e}")
        if patient_ids:
            logger.info(
                f"Using {len(patient_ids)} existing patient IDs from treatment doc: {patient_ids[:5]}..."
            )
            if len(patient_ids) > n_patients:
                patient_ids = patient_ids[:n_patients]
        else:
            patient_ids = generate_uuid_list(n_patients, "P")
            logger.info(
                f"Generated {n_patients} dummy patient IDs (fallback): {patient_ids[:5]}..."
            )

    # 2. Generate and Ingest Data for Each Index

    # EPR documents
    df_epr = generate_epr_documents_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        use_GPT=False,
    )
    df_epr_personal = generate_epr_documents_personal_data(
        num_rows=1,
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )

    df_epr_merged = pd.merge(
        df_epr,
        df_epr_personal.drop(columns=["updatetime"]),
        on="client_idcode",
        how="left",
    )
    df_epr_merged = df_epr_merged.where(pd.notnull(df_epr_merged), None)

    # Import ingest_data_to_elasticsearch
    from pat2vec.util.elasticsearch_methods import ingest_data_to_elasticsearch

    ingest_data_to_elasticsearch(df_epr_merged, "epr_documents", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="epr_documents")

    # Save the generated cohort to treatment_docs.csv for testing_elastic workflow
    if getattr(config_obj, "testing_elastic", False):
        try:
            filename = getattr(
                config_obj, "treatment_doc_filename", "treatment_docs.csv"
            )
            root_path = getattr(config_obj, "root_path", "")
            if root_path:
                os.makedirs(root_path, exist_ok=True)
                output_path = os.path.join(root_path, filename)
            else:
                output_path = filename
            logger.info(
                f"Saving generated cohort to {output_path} for testing_elastic workflow."
            )
            df_epr.to_csv(output_path, index=False)
        except Exception as e:
            logger.error(f"Failed to save generated treatment docs: {e}")

    # basic_observations
    df_basic_obs = generate_basic_observations_data(
        num_rows=random.randint(1, 10),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )

    df_basic_textual = generate_basic_observations_textual_obs_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )

    df_basic_all = pd.concat([df_basic_obs, df_basic_textual], ignore_index=True)
    df_basic_all = df_basic_all.where(pd.notnull(df_basic_all), None)

    ingest_data_to_elasticsearch(
        df_basic_all, "basic_observations", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="basic_observations")

    # observations (BMI, NEWS, MRC Text, Bed, etc.)
    obs_dfs = []

    obs_dfs.append(
        generate_bmi_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
        )
    )

    obs_dfs.append(
        generate_news_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
        )
    )

    obs_dfs.append(
        generate_observations_MRC_text_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            use_GPT=False,
        )
    )

    obs_dfs.append(
        generate_observations_data_generic(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
            search_term="Generic Observation",
        )
    )

    df_obs = pd.concat(obs_dfs, ignore_index=True)
    df_obs = df_obs.where(pd.notnull(df_obs), None)

    ingest_data_to_elasticsearch(df_obs, "observations", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="observations")

    # order (Drugs and Diagnostics)
    order_dfs = []
    order_dfs.append(
        generate_drug_orders_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
        )
    )
    order_dfs.append(
        generate_diagnostic_orders_data(
            num_rows=random.randint(1, 5),
            entered_list=patient_ids,
            global_start_year=global_start_year,
            global_start_month=global_start_month,
            global_end_year=global_end_year,
            global_end_month=global_end_month,
        )
    )

    df_orders = pd.concat(order_dfs, ignore_index=True)
    df_orders = df_orders.where(pd.notnull(df_orders), None)

    ingest_data_to_elasticsearch(df_orders, "order", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="order")

    # pims_apps (Appointments)
    df_apps = generate_appointments_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )

    df_apps = df_apps.where(pd.notnull(df_apps), None)
    ingest_data_to_elasticsearch(df_apps, "pims_apps", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="pims_apps")

    # Epic modules
    df_epic_imaging_reports = generate_epic_imaging_reports_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )
    df_epic_imaging_reports = df_epic_imaging_reports.where(
        pd.notnull(df_epic_imaging_reports), None
    )

    ingest_data_to_elasticsearch(
        df_epic_imaging_reports, "epic_imaging_reports", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_imaging_reports")

    df_epic_orders = generate_epic_orders_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )
    df_epic_orders = df_epic_orders.where(pd.notnull(df_epic_orders), None)

    ingest_data_to_elasticsearch(df_epic_orders, "epic_orders", es_client=cs.elastic)
    cs.elastic.indices.refresh(index="epic_orders")

    df_epic_patients = generate_epic_patients_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )
    df_epic_patients = df_epic_patients.where(pd.notnull(df_epic_patients), None)

    ingest_data_to_elasticsearch(
        df_epic_patients, "epic_patients", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_patients")

    df_epic_encounters = generate_epic_encounters_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )
    df_epic_encounters = df_epic_encounters.where(pd.notnull(df_epic_encounters), None)

    ingest_data_to_elasticsearch(
        df_epic_encounters, "epic_encounters", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_encounters")

    df_epic_clinical_notes = generate_epic_clinical_notes_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
        use_GPT=False,
    )
    df_epic_clinical_notes = df_epic_clinical_notes.where(
        pd.notnull(df_epic_clinical_notes), None
    )

    ingest_data_to_elasticsearch(
        df_epic_clinical_notes, "epic_clinical_notes", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_clinical_notes")

    df_epic_medical_history = generate_epic_medical_history_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )
    df_epic_medical_history = df_epic_medical_history.where(
        pd.notnull(df_epic_medical_history), None
    )

    ingest_data_to_elasticsearch(
        df_epic_medical_history, "epic_medical_history", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_medical_history")

    df_epic_lab_results = generate_epic_lab_results_data(
        num_rows=random.randint(1, 5),
        entered_list=patient_ids,
        global_start_year=global_start_year,
        global_start_month=global_start_month,
        global_end_year=global_end_year,
        global_end_month=global_end_month,
    )
    df_epic_lab_results = df_epic_lab_results.where(
        pd.notnull(df_epic_lab_results), None
    )

    ingest_data_to_elasticsearch(
        df_epic_lab_results, "epic_lab_results", es_client=cs.elastic
    )
    cs.elastic.indices.refresh(index="epic_lab_results")

    logger.info("Successfully populated Elasticsearch with dummy data.")
    return patient_ids


def generate_observations_MRC_text_data(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    use_GPT: bool = False,
    fields_list=None,
) -> pd.DataFrame:
    """Generates dummy data for the 'observations' index (MRC clinical notes)."""
    from .sequence_generators import (
        generate_patient_timeline,
        get_patient_timeline_dummy,
    )

    if fields_list is None:
        fields_list = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
            "_id",
            "_index",
            "_score",
        ]

    df_holder_list = []

    for i in range(0, len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        timeline_generator = (
            generate_patient_timeline if use_GPT else get_patient_timeline_dummy
        )

        data = {
            "observation_guid": [generate_uuid("O") for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": ["AoMRC_ClinicalSummary_FT"],
            "observation_valuetext_analysed": [
                (
                    timeline_generator(current_pat_client_id_code)
                    if use_GPT
                    else get_patient_timeline_dummy(current_pat_client_id_code) or ""
                )
                for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    df = pd.concat(df_holder_list, ignore_index=True)

    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    target_col = "observation_valuetext_analysed"
    if target_col in df.columns and target_col not in unique_fields:
        unique_fields.append(target_col)

    for field in unique_fields:
        if field not in df.columns:
            df[field] = None

    df = df[unique_fields]
    df.reset_index(drop=True, inplace=True)
    return df


def generate_observations_data_generic(
    num_rows: int,
    entered_list: List[str],
    global_start_year: int,
    global_start_month: int,
    global_end_year: int,
    global_end_month: int,
    global_start_day: int = 1,
    global_end_day: int = 31,
    search_term: str = "Test",
    fields_list=None,
) -> pd.DataFrame:
    """Generates dummy data for the 'observations' index (generic fallback)."""

    if fields_list is None:
        fields_list = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordeddtm",
            "clientvisit_visitidcode",
            "_id",
            "_index",
            "_score",
        ]

    df_holder_list = []

    for i in range(0, len(entered_list)):
        current_pat_client_id_code = entered_list[i]

        data = {
            "observation_guid": [generate_uuid("O") for _ in range(num_rows)],
            "client_idcode": [current_pat_client_id_code for _ in range(num_rows)],
            "obscatalogmasteritem_displayname": [search_term for _ in range(num_rows)],
            "observation_valuetext_analysed": [
                str(random.uniform(0, 100)) for _ in range(num_rows)
            ],
            "observationdocument_recordeddtm": [
                create_random_date_from_globals(
                    global_start_year,
                    global_start_month,
                    global_end_year,
                    global_end_month,
                    global_start_day,
                    global_end_day,
                ).strftime("%Y-%m-%dT%H:%M:%S")
                for _ in range(num_rows)
            ],
            "clientvisit_visitidcode": [f"visit_{i}" for i in range(num_rows)],
            "_id": [f"{i}" for i in range(num_rows)],
            "_index": [None for _ in range(num_rows)],
            "_score": [None for _ in range(num_rows)],
        }

        df = pd.DataFrame(data)
        df_holder_list.append(df)

    if not df_holder_list:
        return pd.DataFrame(columns=fields_list)

    final_df = pd.concat(df_holder_list, ignore_index=True)

    unique_fields = list(dict.fromkeys(fields_list + ["_id", "_index", "_score"]))

    for field in unique_fields:
        if field not in final_df.columns:
            final_df[field] = None

    df = final_df[unique_fields]
    df.reset_index(drop=True, inplace=True)
    return df
