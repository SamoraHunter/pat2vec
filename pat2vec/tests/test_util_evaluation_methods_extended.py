"""Extended tests for evaluation_methods.py."""

import tempfile
import shutil
import pandas as pd
from unittest.mock import patch
import pytest

try:
    import importlib.util

    HAS_YDATA = importlib.util.find_spec("pat2vec.util.evaluation_methods") is not None

    from pat2vec.util.evaluation_methods import compare_ipw_annotation_rows
except ImportError:
    HAS_YDATA = False


class TestEvaluationMethodsCompareIpwBasic:
    """Tests for compare_ipw_annotation_rows function."""

    def test_compare_ipw_empty_basic(self):
        """Test basic comparison with minimal columns."""
        df1 = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "text_sample": ["same text"],
            }
        )
        df1.name = "DF1"

        df2 = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "text_sample": ["same text"],
            }
        )
        df2.name = "DF2"

        with patch("pat2vec.util.evaluation_methods.logger"):
            compare_ipw_annotation_rows([df1, df2])

    def test_compare_ipw_custom_columns(self):
        """Test with custom column list."""
        df1 = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "text_sample": ["same text"],
                "custom_col": ["value1"],
            }
        )
        df1.name = "DF1"

        df2 = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "text_sample": ["same text"],
                "custom_col": ["value2"],
            }
        )
        df2.name = "DF2"

        with patch("pat2vec.util.evaluation_methods.logger"):
            compare_ipw_annotation_rows([df1, df2], columns_to_print=["custom_col"])


class TestEvaluationMethodsCsvProfilerBasic:
    """Tests for CsvProfiler that don't require ydata_profiling."""

    def test_csv_profiler_class_import(self):
        """Test CsvProfiler class can be imported."""
        from pat2vec.util.evaluation_methods import CsvProfiler

        assert CsvProfiler is not None

    def test_csv_profiler_has_create_profile_reports(self):
        """Test CsvProfiler has required methods."""
        from pat2vec.util.evaluation_methods import CsvProfiler

        assert hasattr(CsvProfiler, "create_profile_reports")


class TestEvaluationMethodsBasic:
    """Basic functionality tests."""

    def setup_method(self):
        """Set up test directory."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test directory."""
        shutil.rmtree(self.test_dir)

    @pytest.mark.skipif(not HAS_YDATA, reason="ydata_profiling not installed")
    def test_csv_profiler_function_exists(self):
        """Test CsvProfiler.create_profile_reports function exists."""
        from pat2vec.util.evaluation_methods import CsvProfiler

        assert callable(CsvProfiler.create_profile_reports)
