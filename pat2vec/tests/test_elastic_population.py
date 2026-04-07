import unittest
import os
from unittest.mock import MagicMock, patch
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_cohort_searcher import populate_elastic_with_dummy_data


class TestElasticPopulation(unittest.TestCase):
    def setUp(self):
        # Create dummy credentials file required by population logic
        self.creds_file = "test_elastic_credentials.py"
        with open(self.creds_file, "w") as f:
            f.write(
                'username="test"\npassword="pw"\nhosts=["http://localhost:19200"]\n'
            )

        self.config = config_class(
            storage_backend="database",
            db_connection_string="sqlite:///:memory:",
            testing=True,
            testing_elastic=True,
            verbosity=0,
            global_start_year="2020",
            global_start_month="01",
            global_start_day="01",
            global_end_year="2021",
            global_end_month="01",
            global_end_day="01",
        )

    def tearDown(self):
        if os.path.exists(self.creds_file):
            os.remove(self.creds_file)

    @patch("pat2vec.util.get_dummy_data_cohort_searcher.ingest_data_to_elasticsearch")
    @patch("pat2vec.pat2vec_search.cogstack_search_methods.CogStack")
    def test_populate_elastic_calls_ingest(self, mock_cogstack_cls, mock_ingest):
        """
        Verifies that populate_elastic_with_dummy_data generates dataframes and calls ingestion
        for the required indices:
        - epr_documents
        - basic_observations
        - observations
        - order
        - pims_apps
        """

        mock_cs = MagicMock()
        mock_cogstack_cls.return_value = mock_cs
        mock_cs.elastic = MagicMock()

        # Setup mock client to pass safeguards
        mock_node = MagicMock()
        mock_node.host = "localhost"
        mock_cs.elastic.transport.node_pool.all.return_value = [mock_node]
        mock_cs.elastic.info.return_value = {
            "cluster_name": "docker-cluster",
            "version": {"number": "8.0.0"},
        }
        # Mock cat.indices to return empty list so it looks like a clean cluster
        mock_cs.elastic.cat.indices.return_value = []

        patient_ids = populate_elastic_with_dummy_data(self.config, n_patients=5)

        self.assertEqual(len(patient_ids), 5)
        self.assertTrue(len(patient_ids[0]) > 0)

        # Verify ingest was called for expected indices
        expected_indices = {
            "epr_documents",
            "basic_observations",
            "observations",
            "order",
            "pims_apps",
        }

        called_indices = {call.args[1] for call in mock_ingest.call_args_list}

        for index in expected_indices:
            self.assertIn(
                index, called_indices, f"Failed to attempt ingestion for index: {index}"
            )

        # Verify dataframes are not empty
        for call in mock_ingest.call_args_list:
            df = call.args[0]
            self.assertFalse(
                df.empty, f"Ingested dataframe for {call.args[1]} was empty"
            )
