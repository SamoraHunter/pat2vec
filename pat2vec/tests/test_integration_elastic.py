import unittest
import os
import datetime
from pat2vec.util.docker_elastic import ElasticContainer
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_cohort_searcher import populate_elastic_with_dummy_data
from pat2vec.pat2vec_search.cogstack_search_methods import initialize_cogstack_client
import pat2vec.pat2vec_search.cogstack_search_methods as csm


class TestIntegrationElastic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Spin up container
        cls.container = ElasticContainer()
        if not cls.container.start():
            raise unittest.SkipTest("Docker not available or failed to start")

        cls.host, cls.username, cls.password = cls.container.get_credentials()

        # Create fixed credentials file required by population logic
        cls.creds_file_name = "test_elastic_credentials.py"
        with open(cls.creds_file_name, "w") as f:
            f.write(
                f"""
username = "{cls.username}"
password = "{cls.password}"
api_key = None
hosts = ["{cls.host}"]
"""
            )

    @classmethod
    def tearDownClass(cls):
        cls.container.stop()
        if os.path.exists(cls.creds_file_name):
            os.remove(cls.creds_file_name)

    def test_populate_and_search(self):
        # Locate the schema file relative to this test file
        schema_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), "../../test_files/elastic_schemas.json"
            )
        )

        # Configure to use the test credentials
        config = config_class(
            credentials_path=self.creds_file_name,
            test_schema_path=schema_path,
            testing=True,
            testing_elastic=True,
            global_start_year=2020,
            global_start_month=1,
            global_start_day=1,
            global_end_year=2021,
            global_end_month=1,
            global_end_day=1,
            start_date=datetime.datetime(2020, 1, 1),
            lookback=False,
        )

        # Force reset of global client to ensure it picks up new credentials
        csm.cs = None

        # Populate
        patient_ids = populate_elastic_with_dummy_data(config, n_patients=3)
        self.assertEqual(len(patient_ids), 3)

        # Verify indices created using the client
        client = initialize_cogstack_client(config)
        self.assertIsNotNone(client)

        # Force refresh to make documents visible to search
        client.elastic.indices.refresh(index="epr_documents")

        # Verify data exists in one of the core indices
        res = client.elastic.count(index="epr_documents")
        self.assertGreater(res["count"], 0)
