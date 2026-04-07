import unittest
import json
import os
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_epr_documents_data,
    generate_epr_documents_personal_data,
    generate_basic_observations_data,
    generate_basic_observations_textual_obs_data,
    generate_observations_data,
    generate_bmi_data,
    generate_news_data,
    generate_observations_MRC_text_data,
    generate_diagnostic_orders_data,
    generate_drug_orders_data,
    generate_appointments_data,
)


class TestSchemaConsistency(unittest.TestCase):
    def setUp(self):
        # Locate the schema file relative to this test file
        # pat2vec/tests/ -> ../../test_files/elastic_schemas.json
        self.schema_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "..",
                "test_files",
                "elastic_schemas.json",
            )
        )

        if not os.path.exists(self.schema_path):
            self.skipTest(f"Schema file not found at {self.schema_path}")

        with open(self.schema_path, "r") as f:
            self.schemas = json.load(f)

    def get_generated_columns(self, generator_func, **kwargs):
        """Helper to run a generator and return its column set (excluding metadata)."""
        args = {
            "num_rows": 1,
            "entered_list": ["P001"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2021,
            "global_end_month": 1,
        }
        args.update(kwargs)

        # Some generators require search_term
        if (
            "search_term" not in args
            and generator_func.__name__ == "generate_observations_data"
        ):
            args["search_term"] = "Test Observation"

        # Some generators require use_GPT
        if "use_GPT" not in args and "use_GPT" in generator_func.__code__.co_varnames:
            args["use_GPT"] = False

        df = generator_func(**args)
        cols = set(df.columns)

        # Remove metadata columns not typically defined in schema properties
        cols.discard("_id")
        cols.discard("_index")
        cols.discard("_score")
        cols.discard("order_typecode")
        return cols

    def test_epr_documents_schema(self):
        """Verify epr_documents schema matches generator outputs."""
        schema_cols = set(
            self.schemas["epr_documents"]["mappings"]["properties"].keys()
        )

        # epr_documents is populated by document data and personal data generators
        cols_docs = self.get_generated_columns(generate_epr_documents_data)
        cols_personal = self.get_generated_columns(generate_epr_documents_personal_data)

        generated_cols = cols_docs.union(cols_personal)
        self.assertEqual(schema_cols, generated_cols, "epr_documents schema mismatch")

    def test_basic_observations_schema(self):
        """Verify basic_observations schema matches generator outputs."""
        schema_cols = set(
            self.schemas["basic_observations"]["mappings"]["properties"].keys()
        )

        # basic_observations is populated by basic obs and textual obs generators
        cols_basic = self.get_generated_columns(generate_basic_observations_data)
        cols_textual = self.get_generated_columns(
            generate_basic_observations_textual_obs_data
        )

        generated_cols = cols_basic.union(cols_textual)
        self.assertEqual(
            schema_cols, generated_cols, "basic_observations schema mismatch"
        )

    def test_observations_schema(self):
        """Verify observations schema matches generator outputs."""
        schema_cols = set(self.schemas["observations"]["mappings"]["properties"].keys())

        # observations index is populated by multiple generators
        # We check a representative set that covers all fields
        cols_bmi = self.get_generated_columns(generate_bmi_data)
        cols_news = self.get_generated_columns(generate_news_data)
        cols_mrc = self.get_generated_columns(generate_observations_MRC_text_data)
        cols_generic = self.get_generated_columns(generate_observations_data)

        generated_cols = cols_bmi.union(cols_news).union(cols_mrc).union(cols_generic)
        self.assertEqual(schema_cols, generated_cols, "observations schema mismatch")

    def test_order_schema(self):
        """Verify order schema matches generator outputs."""
        schema_cols = set(self.schemas["order"]["mappings"]["properties"].keys())

        cols_diag = self.get_generated_columns(generate_diagnostic_orders_data)
        cols_drug = self.get_generated_columns(generate_drug_orders_data)

        generated_cols = cols_diag.union(cols_drug)
        self.assertEqual(schema_cols, generated_cols, "order schema mismatch")

    def test_pims_apps_schema(self):
        """Verify pims_apps schema matches generator outputs."""
        schema_cols = set(self.schemas["pims_apps"]["mappings"]["properties"].keys())

        cols = self.get_generated_columns(generate_appointments_data)

        self.assertEqual(schema_cols, cols, "pims_apps schema mismatch")
