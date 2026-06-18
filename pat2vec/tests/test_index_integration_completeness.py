import unittest
import inspect
import importlib
import pat2vec.util.config_pat2vec as config_mod
import pat2vec.util.retrieve_data as retrieve_mod
import pat2vec.util.migrate_to_db as migrate_mod
import pat2vec.util.post_processing_build_methods as build_mod
import pat2vec.pat2vec_search.cogstack_search_methods as search_mod
import pat2vec.util.methods_annotation as annot_mod
import pat2vec.util.post_processing_get_pat_ipw_record as ipw_mod
import pat2vec.util.pre_processing as pre_mod
import pat2vec.util.methods_annotation_multi_annots_to_df as multi_annots_to_df_mod
import pat2vec.util.get_method_index_map as index_map_mod
import pat2vec.util.get_method_default_fields_map as fields_map_mod
import pat2vec.util.post_processing_utils as post_utils_mod
import pat2vec.util.filter_methods as filter_mod


class TestIndexIntegrationCompleteness(unittest.TestCase):
    """System-wide test to ensure all indices are fully integrated across all modules."""

    def setUp(self):
        self.indices = [
            "epr_docs",
            "mct_docs",
            "bloods",
            "drugs",
            "diagnostics",
            "news",
            "bmi",
            "demographics",
            "textual_obs",
            "reports",
            "appointments",
            "obs",
            "covid",
            "smoking",
            "vte_status",
            "hosp_site",
            "core_resus",
            "core_02",
            "bed",
            "epic_encounters",
            "epic_clinical_notes",
            "epic_medical_history",
            "epic_orders",
            "epic_lab_results",
            "epic_patients",
            "epic_imaging_reports",
            "epic_clinical_notes_appointments",
        ]

        self.annotatable_indices = [
            "epr_docs",
            "mct_docs",
            "textual_obs",
            "reports",
            "epic_clinical_notes",
            "epic_medical_history",
            "epic_orders",
            "epic_imaging_reports",
            "epic_clinical_notes_appointments",
        ]

    def test_retrieve_data_configs(self):
        """Ensure every index has a retrieval config for both file and DB backends."""
        for idx in self.indices:
            with self.subTest(index=idx):
                self.assertIn(
                    idx,
                    retrieve_mod.DATA_TYPE_CONFIG,
                    f"{idx} missing from DATA_TYPE_CONFIG",
                )

                # Check for corresponding annotation config if annotatable
                if idx in self.annotatable_indices:
                    ann_key = f"{idx.replace('_docs', '')}_annotations"
                    if idx.startswith("epic"):
                        ann_key = f"{idx}_annotations"
                    self.assertIn(
                        ann_key,
                        retrieve_mod.DATA_TYPE_CONFIG,
                        f"Annotation config {ann_key} missing for {idx}",
                    )

    def test_database_migration_mappings(self):
        """Ensure every index is mapped for CSV-to-DB migration."""
        mapped_tables = [m[2] for m in migrate_mod.MAPPINGS]
        for idx in self.indices:
            with self.subTest(index=idx):
                # Table names in migration usually match raw_<name>
                expected_table = f"raw_{idx}"
                if idx == "vte_status":
                    expected_table = "raw_vte"
                if idx == "hosp_site":
                    expected_table = "raw_hospsite"
                if idx == "core_resus":
                    expected_table = "raw_resus"

                self.assertIn(
                    expected_table,
                    mapped_tables,
                    f"Table {expected_table} missing from migrate_to_db.MAPPINGS",
                )

    def test_post_processing_mergers(self):
        """Ensure every index has a merge builder function."""
        for idx in self.indices:
            with self.subTest(index=idx):
                if idx in ["epr_docs", "mct_docs"]:
                    func_name = "build_merged_epr_mct_doc_df"
                elif idx == "obs":
                    continue  # Generic obs folder doesn't have a direct merger
                else:
                    func_name = f"merge_{idx}_csv"

                self.assertTrue(
                    hasattr(build_mod, func_name),
                    f"Merge function {func_name} missing in post_processing_build_methods.py",
                )

    def test_search_methods_presence(self):
        """Ensure every index has a dedicated iterative fuzzy searcher."""
        for idx in self.indices:

            if idx in [
                "bloods",
                "bmi",
                "demographics",
                "appointments",
                "epic_patients",
                "covid",
                "smoking",
                "vte_status",
                "hosp_site",
                "core_resus",
                "core_02",
                "bed",
                "drugs",
                "diagnostics",
                "news",
            ]:
                continue

            with self.subTest(index=idx):
                searcher_name = f"iterative_multi_term_cohort_searcher_no_terms_fuzzy_{idx.replace('_docs', '')}"
                if idx == "epr_docs":
                    searcher_name = (
                        "iterative_multi_term_cohort_searcher_no_terms_fuzzy"
                    )

                self.assertTrue(
                    hasattr(search_mod, searcher_name),
                    f"Searcher {searcher_name} missing in cogstack_search_methods.py",
                )

    def test_annotation_processing_functions(self):
        """Ensure every annotatable index has a multi_annots_to_df_* function."""
        for idx in self.annotatable_indices:
            with self.subTest(index=idx):
                func_name = f"multi_annots_to_df_{idx.replace('_docs', '')}"
                if idx == "epr_docs":
                    func_name = "multi_annots_to_df"  # Legacy name
                if idx == "reports":
                    func_name = "multi_annots_to_df_reports"
                if idx == "epic_orders":
                    func_name = "multi_annots_to_df_epic_orders"

                # Note: some are in methods_annotation.py or methods_annotation_multi_annots_to_df.py
                has_func = (
                    hasattr(annot_mod, func_name)
                    or hasattr(build_mod, func_name)
                    or hasattr(multi_annots_to_df_mod, func_name)
                )
                self.assertTrue(has_func, f"Annotation processor {func_name} missing")

    def test_ipw_record_integration(self):
        """Ensure get_pat_ipw_record searches across all relevant annotatable sources."""
        source_code = inspect.getsource(ipw_mod.get_pat_ipw_record)
        for idx in self.annotatable_indices:
            with self.subTest(index=idx):
                # Check for source record retrieval call
                search_term = idx  # Use the full index name for config option check
                self.assertIn(
                    search_term,
                    source_code,
                    f"IPW record retrieval likely missing search logic for {idx}",
                )

    def test_pre_processing_search_routing(self):
        """Ensure all searchers are registered in the main cohort extraction routing."""
        source_code = inspect.getsource(
            pre_mod.get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy
        )
        for idx in self.indices:  # Iterate over all indices, not just annotatable
            # Exclude annotation-specific options or structured data that don't use this generic search routing
            if idx in [
                "epic_orders",
                "bloods",
                "drugs",
                "diagnostics",
                "news",
                "bmi",
                "demographics",
                "appointments",
                "epic_patients",
                "covid",
                "smoking",
                "vte_status",
                "hosp_site",
                "core_resus",
                "core_02",
                "bed",
            ]:
                continue
            with self.subTest(index=idx):
                # Check if the source is included in the search_configs list
                search_term = idx.replace("_docs", "")
                self.assertIn(
                    search_term,
                    source_code,
                    f"Pre-processing search routing missing for {idx}",
                )

    def test_vectorization_method_mappings(self):
        """Ensure every index has a corresponding vectorization method mapped."""
        mapped_indices = list(index_map_mod.GET_METHOD_INDEX_MAP.values())
        for idx in self.indices:
            with self.subTest(index=idx):
                # Handle pattern matching vs explicit index names in mapping
                idx_search = idx
                if idx == "appointments":
                    idx_search = "pims_apps*"
                if idx == "epr_docs":
                    idx_search = "epr_documents"
                if idx == "mct_docs":
                    idx_search = "observations"
                if idx == "textual_obs":
                    idx_search = "basic_observations"
                if idx == "reports":
                    idx_search = "reports"
                if idx == "news":
                    idx_search = "observations"
                if idx == "bmi":
                    idx_search = "observations"
                if idx == "bloods":
                    idx_search = "basic_observations"
                if idx == "drugs":
                    idx_search = "order"
                if idx == "diagnostics":
                    idx_search = "order"
                if idx == "demographics":
                    idx_search = "epr_documents"
                if idx in [
                    "covid",
                    "smoking",
                    "vte_status",
                    "hosp_site",
                    "core_resus",
                    "core_02",
                    "bed",
                    "news",
                    "obs",
                ]:
                    idx_search = (
                        "observations" if idx != "covid" else "basic_observations"
                    )

                self.assertIn(
                    idx_search,
                    mapped_indices,
                    f"Index {idx_search} (from {idx}) has no mapped vectorization method in GET_METHOD_INDEX_MAP",
                )

    def test_get_method_functions_exist(self):
        """Exhaustive check: Verify that every mapped feature extraction function actually exists in the package."""
        # Map of method names to their module suffix convention
        exceptions = {
            "get_demographics3": "demo",  # get_demographics3 is implemented in get_method_demo.py
            "get_demo": "demographics",  # get_demo is implemented in get_method_demographics.py
            "get_core_02": "core02",  # get_core_02 is implemented in get_method_core02.py
            "get_current_pat_annotations": "pat_annotations",  # in get_method_pat_annotations.py
            "get_current_pat_annotations_mrc_cs": "current_pat_annotations_mrc_cs",  # in get_method_current_pat_annotations_mrc_cs.py
        }

        for method_name in index_map_mod.GET_METHOD_INDEX_MAP.keys():
            with self.subTest(method=method_name):
                if method_name in exceptions:
                    mod_suffix = exceptions[method_name]
                else:
                    # Default logic: strip common prefixes and suffixes
                    mod_suffix = (
                        method_name.replace("get_current_pat_", "")
                        .replace("get_", "")
                        .replace("_features", "")
                    )

                module_path = f"pat2vec.pat2vec_get_methods.get_method_{mod_suffix}"
                try:
                    module = importlib.import_module(module_path)
                    self.assertTrue(
                        hasattr(module, method_name),
                        f"Function '{method_name}' not found in '{module_path}'",
                    )
                except ImportError as e:
                    self.fail(
                        f"Module '{module_path}' for method '{method_name}' is missing. Integration is broken. Error: {e}"
                    )

    def test_default_fields_mappings(self):
        """Ensure every vectorization method has default fields defined for feature extraction."""
        for method_name in index_map_mod.GET_METHOD_INDEX_MAP.keys():
            with self.subTest(method=method_name):
                self.assertIn(
                    method_name,
                    fields_map_mod.GET_METHOD_DEFAULT_FIELDS_MAP,
                    f"Method {method_name} missing from GET_METHOD_DEFAULT_FIELDS_MAP",
                )

    def test_post_processing_utils_comprehensiveness(self):
        """Check if utility functions handle all index-related paths and columns."""
        # Check copy_files_and_dirs for backup support
        copy_source = inspect.getsource(post_utils_mod.copy_files_and_dirs)
        for idx in self.indices:
            search_str = f"current_pat_{idx}_batches"
            if idx == "epr_docs":
                search_str = "current_pat_document_batches"
            if idx == "mct_docs":
                search_str = "current_pat_document_batches_mct"
            if idx == "demographics":
                search_str = "current_pat_demo_batches"
            if idx == "textual_obs":
                search_str = "current_pat_textual_obs_document_batches"
            if idx == "reports":
                search_str = "current_pat_document_batches_reports"
            if idx == "covid":
                search_str = "current_pat_misc_batches"
            if idx == "obs":
                search_str = "current_pat_obs_batches"
            if idx == "epic_orders":
                search_str = "current_pat_epic_orders_annotations_batches"
            if idx in [
                "smoking",
                "vte_status",
                "hosp_site",
                "core_resus",
                "core_02",
                "bed",
            ]:
                search_str = "current_pat_obs_batches"

            with self.subTest(check="copy_files_and_dirs", index=idx):
                self.assertIn(
                    search_str,
                    copy_source,
                    f"Backup utility might skip {search_str} for index {idx}",
                )

        # Check annotation folders for annotatable indices
        for idx in self.annotatable_indices:
            search_str = f"current_pat_{idx}_annotations_batches"
            if idx == "epr_docs":
                search_str = "current_pat_documents_annotations_batches"
            if idx == "mct_docs":
                search_str = "current_pat_documents_annotations_batches_mct"
            if idx == "reports":
                search_str = "current_pat_documents_annotations_batches_reports"
            if idx == "epic_orders_annotations":
                search_str = "current_pat_epic_orders_annotations_batches"

            with self.subTest(check="copy_files_and_dirs_annotations", index=idx):
                self.assertIn(
                    search_str,
                    copy_source,
                    f"Backup utility might skip annotation folder {search_str} for index {idx}",
                )

        # Check filter_and_update_csv timestamp columns for IPW alignment
        filter_source = inspect.getsource(post_utils_mod.filter_and_update_csv)
        # Expected timestamp columns for Epic
        epic_ts_cols = [
            "activity_AdmissionDate",
            "document_CreatedWhen",
            "document_UpdatedWhen",
            "document_OrderDate",
            "document_ServiceDate",
            "document_CollectedDate",
            "patient_CreatedWhen",
        ]
        for col in epic_ts_cols:
            with self.subTest(check="filter_and_update_csv", column=col):
                self.assertIn(
                    col,
                    filter_source,
                    f"IPW filtering utility missing timestamp column {col}",
                )

    def test_config_main_options_presence(self):
        """Ensure every relevant index has a toggle in the config's main_options."""
        config = config_mod.config_class(testing=True)
        options = config.main_options

        # Mapping between index names used in tests and config keys
        mapping = {
            "epr_docs": "annotations",
            "mct_docs": "annotations_mrc",
            "bloods": "bloods",
            "drugs": "drugs",
            "diagnostics": "diagnostics",
            "news": "news",
            "bmi": "bmi",
            "demographics": "demo",
            "textual_obs": "textual_obs",
            "reports": "annotations_reports",
            "appointments": "appointments",
            "covid": "covid",
            "smoking": "smoking",
            "vte_status": "vte_status",
            "hosp_site": "hosp_site",
            "core_resus": "core_resus",
            "core_02": "core_02",
            "bed": "bed",
            "epic_encounters": "epic_encounters",
            "epic_clinical_notes": "epic_clinical_notes",
            "epic_medical_history": "epic_medical_history",
            "epic_orders": "epic_orders",
            "epic_orders_annotations": "epic_orders_annotations",
            "epic_lab_results": "epic_lab_results",
            "epic_patients": "epic_patients",
            "epic_imaging_reports": "epic_imaging_reports",
            "epic_clinical_notes_appointments": "epic_clinical_notes_appointments",
        }

        for idx, key in mapping.items():
            with self.subTest(index=idx, key=key):
                self.assertIn(
                    key,
                    options,
                    f"Config main_options missing key '{key}' for index '{idx}'",
                )

    def test_filter_methods_presence(self):
        """Ensure every relevant index has a data type filter applicator."""
        for idx in self.indices:
            # Structured data that typically doesn't need explicit fuzzy/regex applicators
            if idx in [
                "bmi",
                "demographics",
                "appointments",
                "epic_encounters",
                "epic_patients",
                "covid",
                "smoking",
                "vte_status",
                "hosp_site",
                "core_resus",
                "core_02",
                "bed",
                "epic_clinical_notes",
                "epic_medical_history",
                "epic_imaging_reports",
                "epic_clinical_notes_appointments",
                "epic_orders",
                "epic_lab_results",
                "epic_orders_annotations",
            ]:
                continue

            with self.subTest(index=idx):
                func_name = f"apply_data_type_{idx}_filters"
                if idx == "bloods":
                    func_name = "apply_bloods_data_type_filter"
                self.assertTrue(
                    hasattr(filter_mod, func_name),
                    f"Filter applicator {func_name} missing in filter_methods.py",
                )


if __name__ == "__main__":
    unittest.main()
