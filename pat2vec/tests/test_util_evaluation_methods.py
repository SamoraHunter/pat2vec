import unittest
import pandas as pd
import os
import shutil
import tempfile
import importlib.util
from unittest.mock import patch
from pat2vec.util.evaluation_methods import compare_ipw_annotation_rows, CsvProfiler

HAS_YDATA = importlib.util.find_spec("ydata_profiling") is not None


class TestEvaluationMethods(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch("pat2vec.util.evaluation_methods.input", return_value="")
    @patch("pat2vec.util.evaluation_methods.clear_output")
    def test_compare_ipw_annotation_rows(self, mock_clear, mock_input):
        """Test comparing annotation rows across dataframes."""
        df1 = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "text_sample": ["text A"],
                "pretty_name": ["Asthma"],
                "cui": [100],
                "types": ["['disorder']"],
                "acc": [0.95],
                "context_similarity": [0.9],
                "detected_name": ["asthma"],
                "source_value": ["asthma"],
                "Time_Value": ["Recent"],
                "Time_Confidence": [0.9],
                "Presence_Value": ["True"],
                "Presence_Confidence": [0.9],
                "Subject_Value": ["Patient"],
                "Subject_Confidence": [0.9],
                "updatetime": ["2023-01-01"],
            }
        )
        df1.name = "DF1"

        df2 = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "text_sample": ["text B"],
                "pretty_name": ["COPD"],
                "cui": [200],
                "types": ["['disorder']"],
                "acc": [0.85],
                "context_similarity": [0.8],
                "detected_name": ["copd"],
                "source_value": ["copd"],
                "Time_Value": ["Recent"],
                "Time_Confidence": [0.9],
                "Presence_Value": ["True"],
                "Presence_Confidence": [0.9],
                "Subject_Value": ["Patient"],
                "Subject_Confidence": [0.9],
                "updatetime": ["2023-01-01"],
            }
        )
        df2.name = "DF2"

        with patch("pat2vec.util.evaluation_methods.logger") as mock_logger:
            compare_ipw_annotation_rows([df1, df2], columns_to_print=None)
            self.assertTrue(mock_input.called)
            self.assertTrue(mock_logger.info.called)

    @unittest.skipUnless(HAS_YDATA, "ydata_profiling not installed")
    @patch("pat2vec.util.evaluation_methods.tqdm", lambda x, **kwargs: x)
    @patch("ydata_profiling.ProfileReport")
    def test_csv_profiler_basic(self, mock_profile_report):
        """Test CsvProfiler creates reports for CSV files."""
        df = pd.DataFrame(
            {"client_idcode": ["P1"], "updatetime": ["2023-01-01"], "cui": [100]}
        )
        df.to_csv(os.path.join(self.test_dir, "test.csv"), index=False)

        output_dir = os.path.join(self.test_dir, "reports")

        CsvProfiler.create_profile_reports(
            self.test_dir, output_dir=output_dir, icd10_opc4s=False
        )

        self.assertTrue(mock_profile_report.called)
        mock_instance = mock_profile_report.return_value
        self.assertTrue(mock_instance.to_file.called)

    @unittest.skipUnless(HAS_YDATA, "ydata_profiling not installed")
    @patch("pat2vec.util.evaluation_methods.tqdm", lambda x, **kwargs: x)
    @patch("ydata_profiling.ProfileReport")
    def test_csv_profiler_with_icd10_filter(self, mock_profile_report):
        """Test CsvProfiler with targetId filter logic."""
        df = pd.DataFrame({"client_idcode": ["P1", "P2"], "targetId": ["J45", None]})
        df.to_csv(os.path.join(self.test_dir, "test_filter.csv"), index=False)

        output_dir = os.path.join(self.test_dir, "reports_filter")

        CsvProfiler.create_profile_reports(
            self.test_dir, output_dir=output_dir, icd10_opc4s=True
        )

        args, _ = mock_profile_report.call_args
        passed_df = args[0]
        self.assertEqual(len(passed_df), 1)

    @unittest.skipUnless(HAS_YDATA, "ydata_profiling not installed")
    @patch("pat2vec.util.evaluation_methods.tqdm", lambda x, **kwargs: x)
    @patch("pat2vec.util.evaluation_methods.pd.read_csv")
    def test_csv_profiler_missing_columns(self, mock_read_csv, mock_profile_report):
        """Test CsvProfiler handles missing columns gracefully."""
        df_no_cols = pd.DataFrame({"client_idcode": ["P1"], "other_col": [123]})
        mock_read_csv.return_value = df_no_cols

        output_dir = os.path.join(self.test_dir, "reports_missing_cols")

        CsvProfiler.create_profile_reports(
            self.test_dir, cols=["updatetime", "targetId"], output_dir=output_dir
        )

        args, _ = mock_profile_report.call_args
        passed_df = args[0]
        self.assertIn("client_idcode", passed_df.columns)

    def test_compare_ipw_annotation_rows_same_text(self):
        """Test compare function when text_sample is the same across dataframes."""
        df1 = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "text_sample": ["same text"],
                "pretty_name": ["Asthma"],
                "cui": [100],
                "types": ["['disorder']"],
                "acc": [0.95],
                "context_similarity": [0.9],
                "detected_name": ["asthma"],
                "source_value": ["asthma"],
                "Time_Value": ["Recent"],
                "Time_Confidence": [0.9],
                "Presence_Value": ["True"],
                "Presence_Confidence": [0.9],
                "Subject_Value": ["Patient"],
                "Subject_Confidence": [0.9],
                "updatetime": ["2023-01-01"],
            }
        )
        df1.name = "DF1"

        df2 = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "text_sample": ["same text"],
                "pretty_name": ["Asthma"],
                "cui": [100],
                "types": ["['disorder']"],
                "acc": [0.95],
                "context_similarity": [0.9],
                "detected_name": ["asthma"],
                "source_value": ["asthma"],
                "Time_Value": ["Recent"],
                "Time_Confidence": [0.9],
                "Presence_Value": ["True"],
                "Presence_Confidence": [0.9],
                "Subject_Value": ["Patient"],
                "Subject_Confidence": [0.9],
                "updatetime": ["2023-01-01"],
            }
        )
        df2.name = "DF2"

        with patch("pat2vec.util.evaluation_methods.logger") as mock_logger:
            compare_ipw_annotation_rows([df1, df2], columns_to_print=None)
            self.assertFalse(mock_logger.info.called)

    def test_compare_ipw_annotation_rows_empty_dfs(self):
        """Test compare function with empty DataFrames."""
        df1 = pd.DataFrame(columns=["client_idcode", "text_sample"])
        df1.name = "DF1"

        df2 = pd.DataFrame(columns=["client_idcode", "text_sample"])
        df2.name = "DF2"

        compare_ipw_annotation_rows([df1, df2], columns_to_print=None)

    def test_compare_ipw_annotation_rows_custom_columns(self):
        """Test compare function with custom column list."""
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


if __name__ == "__main__":
    unittest.main()
