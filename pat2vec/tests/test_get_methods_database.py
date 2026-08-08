from unittest.mock import MagicMock, patch

import pandas as pd

from pat2vec.main_pat2vec import main


class MockConnection:
    """Mock database connection for testing."""

    def __init__(self):
        self.dialect = MagicMock()
        self.dialect.name = "sqlite"

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, *args, **kwargs):
        pass

    def begin(self):
        return self


class MockInspector:
    """Mock inspector for testing."""

    def has_table(self, table, schema=None):
        return True

    def get_columns(self, table, schema=None):
        return [{"name": "client_idcode"}]


class MockDatabaseEngine:
    """Mock database engine for testing."""

    def __init__(self):
        self.name = "sqlite"

    def connect(self):
        conn = MockConnection()
        conn.dialect.name = "sqlite"
        return conn

    def begin(self):
        return MockConnection()


class MockConfig:
    """Mock configuration for testing."""

    def __init__(self):
        self.storage_backend = "database"
        self.batch_mode = False
        self.verbosity = 0
        self.db_engine = MockDatabaseEngine()
        self.patient_id_column_name = "client_idcode"


def _create_pat2vec_obj():
    """Bypasses main.__init__ to avoid live DB connections while retaining all inherited mixin methods."""
    obj = main.__new__(main)
    config = MagicMock()
    config.storage_backend = "database"
    config.db_engine = MagicMock()
    obj.config_obj = config
    return obj


def test_get_raw_drugs():
    """Test get_raw_drugs returns a DataFrame."""
    with patch("pat2vec.util.retrieve_data.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "drug_name": ["Aspirin"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_drugs("P1")

        assert not result.empty
        mock_get_df.assert_called_once()


def test_get_raw_bloods():
    """Test get_raw_bloods returns a DataFrame."""
    with patch("pat2vec.util.retrieve_data.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "lab_name": ["WBC"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_bloods("P1")

        assert not result.empty


def test_get_raw_epr_docs():
    """Test get_raw_epr_docs returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "body_analysed": ["Note"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_epr_docs("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_demographics():
    """Test get_raw_demographics returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "updatetime": ["2020-01-01"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_demographics("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_mct_docs():
    """Test get_raw_mct_docs returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "text": ["MCT note"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_mct_docs("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_textual_obs():
    """Test get_raw_textual_obs returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "textualObs": ["Obs text"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_textual_obs("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_reports():
    """Test get_raw_reports returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "report": ["Report content"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_reports("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_diagnostics():
    """Test get_raw_diagnostics returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "diagnosis": ["Dx"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_diagnostics("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_news():
    """Test get_raw_news returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "news_score": [5]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_news("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_bmi():
    """Test get_raw_bmi returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "bmi_value": [25.5]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_bmi("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_appointments():
    """Test get_raw_appointments returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame(
            {"client_idcode": ["P1"], "appointment_date": ["2020-01-01"]}
        )
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_appointments("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_covid():
    """Test get_raw_covid returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "covid_result": ["Positive"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_covid("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_smoking():
    """Test get_raw_smoking returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame(
            {"client_idcode": ["P1"], "smoking_status": ["Non-smoker"]}
        )
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_smoking("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_core_02():
    """Test get_raw_core_02 returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "spo2": [98]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_core_02("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_bed():
    """Test get_raw_bed returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "bed_number": ["ICU-1"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_bed("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_vte():
    """Test get_raw_vte returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "vte_status": ["High"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_vte("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_hospsite():
    """Test get_raw_hospsite returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "hospital_site": ["Site A"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_hospsite("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_resus():
    """Test get_raw_resus returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "resus_status": ["Full code"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_resus("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_raw_obs():
    """Test get_raw_obs returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_df_from_db") as mock_get_df:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "observation": ["Value"]})
        mock_get_df.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_raw_obs("P1")

        assert isinstance(result, pd.DataFrame)


def test_get_all_features():
    """Test get_all_features returns a DataFrame."""
    with patch("pat2vec.util.helper_functions.get_all_features") as mock_get_all:
        mock_df = pd.DataFrame({"client_idcode": ["P1"], "feature1": [1]})
        mock_get_all.return_value = mock_df

        pat2vec_obj = _create_pat2vec_obj()
        result = pat2vec_obj.get_all_features()

        assert not result.empty
