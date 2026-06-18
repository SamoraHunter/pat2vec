import pandas as pd
from datetime import timedelta
from pat2vec.pat2vec_get_methods.get_method_epic_patients import get_epic_patients
from pat2vec.util.get_dummy_data_cohort_searcher import (
    cohort_searcher_with_terms_and_search_dummy,
)


class MockConfig:
    def __init__(self):
        self.batch_mode = False
        self.verbosity = 0
        self.global_start_year = 1995
        self.global_start_month = 1
        self.global_start_day = 1
        self.global_end_year = 2025
        self.global_end_month = 12
        self.global_end_day = 31
        self.time_window_interval_delta = timedelta(days=1)


def test_get_epic_patients():
    """Test the get_epic_patients function with dummy data."""
    config = MockConfig()
    patient_id = "P123456"
    # target_date_range is typically a tuple of (start_dt, end_dt)
    start_date = pd.to_datetime("2020-01-01")
    end_date = pd.to_datetime("2020-12-31")
    target_date_range = (start_date, end_date)

    # In non-batch mode, pat_batch is not used by the search logic
    pat_batch = pd.DataFrame()

    df = get_epic_patients(
        current_pat_client_id_code=patient_id,
        target_date_range=target_date_range,
        pat_batch=pat_batch,
        config_obj=config,
        cohort_searcher_with_terms_and_search=cohort_searcher_with_terms_and_search_dummy,
    )

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "client_idcode" in df.columns
    assert df["client_idcode"].iloc[0] == patient_id
    # Verify that patient demographics features are extracted
    assert any(col.startswith("epic_pat_") for col in df.columns) or df.empty
