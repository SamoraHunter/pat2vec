from pat2vec.patvec_get_batch_methods.get_merged_batches import (
    get_merged_pat_batch_appointments,
    get_merged_pat_batch_bloods,
    get_merged_pat_batch_demo,
    get_merged_pat_batch_diagnostics,
    get_merged_pat_batch_drugs,
    get_merged_pat_batch_epr_docs,
    get_merged_pat_batch_mct_docs,
    get_merged_pat_batch_news,
    get_merged_pat_batch_obs,
    get_merged_pat_batch_reports,
    get_merged_pat_batch_textual_obs_docs,
    split_and_save_csv,
)
import tqdm
from typing import Optional, List, Any
from dataclasses import dataclass


@dataclass
class BatchConfig:
    """Configuration for a single data type batch processing operation.

    Attributes:
        name: A human-readable name for the batch type (e.g., "bloods").
        enabled_option: The key in the main options config that enables/disables
            this batch type.
        get_function: The function used to retrieve the merged batch of data.
        save_path_attr: The attribute name in the main config object that holds
            the save path for the split patient files.
        search_term: An optional search term required by the `get_function`.
    """

    name: str
    enabled_option: str
    get_function: callable
    save_path_attr: str
    search_term: Optional[str] = None


def prefetch_batches(pat2vec_obj: Any) -> List[BatchConfig]:
    """Prefetches and processes patient data batches with progress tracking.

    This function orchestrates the pre-fetching of data for multiple data types
    (e.g., bloods, drugs, documents) in bulk. For each enabled data type, it
    calls the appropriate `get_merged_pat_batch_*` function to retrieve data for
    all patients at once. It then splits these large, merged DataFrames into
    individual patient files and saves them to their respective directories.

    This approach is often more efficient than fetching data patient-by-patient,
    especially when dealing with a large cohort.

    Args:
        pat2vec_obj: The patient vector object containing configuration and patient data.

    Returns:
        A list of the `BatchConfig` objects that were processed.
    """
    if pat2vec_obj is None:
        print("[ERROR] pat2vec_obj cannot be None")
        return

    # Check verbosity setting
    verbose = getattr(pat2vec_obj.config_obj, "verbose", 0)

    # Define batch configurations
    batch_configs = [
        BatchConfig(
            name="bloods",
            enabled_option="bloods",
            get_function=get_merged_pat_batch_bloods,
            save_path_attr="pre_bloods_batch_path",
            search_term="",  # Search term is not used by the function
        ),
        BatchConfig(
            name="diagnostics",
            enabled_option="diagnostics",
            get_function=get_merged_pat_batch_diagnostics,
            save_path_attr="pre_diagnostics_batch_path",
        ),
        BatchConfig(
            name="drugs",
            enabled_option="drugs",
            get_function=get_merged_pat_batch_drugs,
            save_path_attr="pre_drugs_batch_path",
        ),
        BatchConfig(
            name="EPR documents",
            enabled_option="annotations",
            get_function=get_merged_pat_batch_epr_docs,
            save_path_attr="pre_document_batch_path",
            search_term="",  # Search term is not used by the function
        ),
        BatchConfig(
            name="MCT documents",
            enabled_option="annotations_mrc",
            get_function=get_merged_pat_batch_mct_docs,
            save_path_attr="pre_document_batch_path_mct",
            search_term="",  # Search term is not used by the function
        ),
        BatchConfig(
            name="textual_obs",
            enabled_option="textual_obs",
            get_function=get_merged_pat_batch_textual_obs_docs,
            save_path_attr="pre_textual_obs_document_batch_path",
            search_term="",  # Search term is not used by the function
        ),
        BatchConfig(
            name="CORE_SmokingStatus",
            enabled_option="smoking_status",
            get_function=get_merged_pat_batch_obs,
            save_path_attr="pre_misc_batch_path",
            search_term="CORE_SmokingStatus",
        ),
        BatchConfig(
            name="CORE_SpO2",
            enabled_option="core_02",
            get_function=get_merged_pat_batch_obs,
            save_path_attr="pre_misc_batch_path",
            search_term="CORE_SpO2",
        ),
        BatchConfig(
            name="CORE_BedNumber3",
            enabled_option="bed",
            get_function=get_merged_pat_batch_obs,
            save_path_attr="pre_misc_batch_path",
            search_term="CORE_BedNumber3",
        ),
        BatchConfig(
            name="CORE_VTE_STATUS",
            enabled_option="vte_status",
            get_function=get_merged_pat_batch_obs,
            save_path_attr="pre_misc_batch_path",
            search_term="CORE_VTE_STATUS",
        ),
        BatchConfig(
            name="CORE_HospitalSite",
            enabled_option="hosp_site",
            get_function=get_merged_pat_batch_obs,
            save_path_attr="pre_misc_batch_path",
            search_term="CORE_HospitalSite",
        ),
        BatchConfig(
            name="CORE_RESUS_STATUS",
            enabled_option="core_resus",
            get_function=get_merged_pat_batch_obs,
            save_path_attr="pre_misc_batch_path",
            search_term="CORE_RESUS_STATUS",
        ),
        BatchConfig(
            name="news",
            enabled_option="news",
            get_function=get_merged_pat_batch_news,
            save_path_attr="pre_news_batch_path",
            search_term="",  # Search term is not used by the function
        ),
        BatchConfig(
            name="annotations_reports",
            enabled_option="annotations_reports",
            get_function=get_merged_pat_batch_reports,
            save_path_attr="pre_report_batch_path",
            search_term="report",  # Assuming 'report' is the intended search term
        ),
        BatchConfig(
            name="appointments",
            enabled_option="appointments",
            get_function=get_merged_pat_batch_appointments,
            save_path_attr="pre_appointments_batch_path",
            search_term="",  # Search term is not used by the function
        ),
        BatchConfig(
            name="demo",
            enabled_option="demo",
            get_function=get_merged_pat_batch_demo,
            save_path_attr="pre_demo_batch_path",
            search_term="",  # Search term is not used by the function
        ),
    ]

    # Get enabled batch configs
    enabled_configs = [
        config
        for config in batch_configs
        if pat2vec_obj.config_obj.main_options.get(config.enabled_option, True)
    ]

    # Process each enabled batch with progress bar
    for config in tqdm.tqdm(enabled_configs, desc="Processing batch types"):
        try:
            if verbose > 0:
                print(f"[INFO] Processing {config.name} batch")

            # Prepare function arguments
            func_kwargs = {
                "client_idcode_list": pat2vec_obj.all_patient_list,
                "config_obj": pat2vec_obj.config_obj,
                "cohort_searcher_with_terms_and_search": pat2vec_obj.cohort_searcher_with_terms_and_search,
            }

            # Add search_term if required
            if config.search_term is not None:
                func_kwargs["search_term"] = config.search_term

            # Get batch data
            df = config.get_function(**func_kwargs)

            # Get save path from config
            save_path = getattr(pat2vec_obj.config_obj, config.save_path_attr)

            # Save batch data
            split_and_save_csv(
                df=df,
                client_idcode_column="client_idcode",
                save_folder=save_path,
                num_processes=None,
            )

            if verbose > 0:
                print(f"[INFO] Successfully processed and saved {config.name} batch")

        except Exception as e:
            # Always print errors regardless of verbosity
            print(f"[ERROR] Error processing {config.name} batch: {str(e)}")

    return enabled_configs
