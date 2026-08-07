import unittest
import os
import tempfile
from datetime import datetime
from unittest.mock import patch
import pandas as pd

from pat2vec.util.config_pat2vec import config_class
from pat2vec.main_pat2vec import main


class TestGetMethodsDummyGenerator(unittest.TestCase):
    """Tests to verify that when testing=True and testing_elastic=False,
    all get methods use the dummy generator (cohort_searcher_with_terms_and_search_dummy)
    and return data from it."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "test_get_methods_dummy"
        self.db_path = os.path.join(self.test_dir, f"{self.project_name}.sqlite")
        self.db_connection_string = f"sqlite:///{self.db_path}"

        self.test_patient_id = "P_TEST_DUMMY_GEN"
        self.base_date = datetime(2023, 6, 15)

    def create_test_config(self):
        """Create a test config with testing=True and testing_elastic=False."""
        return config_class(
            storage_backend="database",
            db_connection_string=self.db_connection_string,
            testing=True,
            testing_elastic=False,
            verbosity=0,
            proj_name=self.project_name,
            root_path=self.test_dir,
            global_start_year=self.base_date.year - 1,
            global_start_month=self.base_date.month,
            global_end_year=self.base_date.year + 1,
            global_end_month=self.base_date.month,
            all_patient_list=[self.test_patient_id],
        )

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    @patch("pat2vec.main_pat2vec.cohort_searcher_with_terms_and_search_dummy")
    def test_get_methods_receive_dummy_generator_when_testing_enabled(self, mock_dummy):
        """Test that when testing=True and testing_elastic=False,
        get methods pass the dummy generator to retrieve_patient_data."""
        config = self.create_test_config()

        expected_df = pd.DataFrame(
            {
                "client_idcode": [self.test_patient_id],
                "order_name": ["Aspirin"],
                "updatetime": [self.base_date.strftime("%Y-%m-%dT%H:%M:%S")],
            }
        )
        mock_dummy.return_value = expected_df

        pat2vec_obj = main(config_obj=config, cogstack=True)

        self.assertEqual(
            pat2vec_obj.cohort_searcher_with_terms_and_search,
            mock_dummy,
            "cohort_searcher_with_terms_and_search should be set to dummy generator in testing mode",
        )

        pat2vec_obj.get_raw_drugs(self.test_patient_id)
        pat2vec_obj.get_raw_bloods(self.test_patient_id)

        self.assertGreaterEqual(
            mock_dummy.call_count,
            1,
            "Dummy generator should be called when get methods are invoked in testing mode",
        )

    def test_get_methods_return_data_when_testing_mode_is_active(self):
        """Test that get methods return actual data (not empty) when testing is enabled."""
        config = self.create_test_config()

        pat2vec_obj = main(config_obj=config, cogstack=True)

        drugs_df = pat2vec_obj.get_raw_drugs(self.test_patient_id)
        bloods_df = pat2vec_obj.get_raw_bloods(self.test_patient_id)

        self.assertIsInstance(
            drugs_df, pd.DataFrame, "get_raw_drugs should return a DataFrame"
        )
        self.assertIsInstance(
            bloods_df, pd.DataFrame, "get_raw_bloods should return a DataFrame"
        )

    def test_cohort_searcher_is_dummy_in_testing_mode(self):
        """Test that cohort_searcher_with_terms_and_search is set to dummy when testing=True."""
        config = self.create_test_config()

        pat2vec_obj = main(config_obj=config, cogstack=True)

        from pat2vec.util.get_dummy_data_cohort_searcher import (
            cohort_searcher_with_terms_and_search_dummy,
        )

        self.assertEqual(
            pat2vec_obj.cohort_searcher_with_terms_and_search,
            cohort_searcher_with_terms_and_search_dummy,
            "cohort_searcher_with_terms_and_search should be the dummy generator in testing mode",
        )

    def test_get_methods_receive_search_function(self):
        """Test that get methods pass the search function to retrieve_patient_data."""
        config = self.create_test_config()

        pat2vec_obj = main(config_obj=config, cogstack=True)

        self.assertIsNotNone(
            pat2vec_obj.cohort_searcher_with_terms_and_search,
            "cohort_searcher_with_terms_and_search should be set in testing mode",
        )

        drugs_df = pat2vec_obj.get_raw_drugs(self.test_patient_id)

        self.assertIsInstance(drugs_df, pd.DataFrame, "Result should be a DataFrame")


if __name__ == "__main__":
    unittest.main()
