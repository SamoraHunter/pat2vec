import datetime
import os

import pat2vec.pat2vec_search.cogstack_search_methods as csm
from pat2vec.pat2vec_search.cogstack_search_methods import initialize_cogstack_client
from pat2vec.util.config_pat2vec import config_class
from pat2vec.util.get_dummy_data_cohort_searcher import populate_elastic_with_dummy_data


def test_populate_and_search(elastic_container):
    schema_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../test_files/elastic_schemas.json")
    )

    config = config_class(
        credentials_path=elastic_container,
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

    csm.cs = None

    patient_ids = populate_elastic_with_dummy_data(config, n_patients=3)
    assert len(patient_ids) == 3

    client = initialize_cogstack_client(config)
    assert client is not None

    client.elastic.indices.refresh(index="epr_documents")

    res = client.elastic.count(index="epr_documents")
    assert res["count"] > 0


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
