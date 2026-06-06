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
                "detected_name": ["asthma"],  # Added missing column
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
                "detected_name": ["copd"],  # Added missing column
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

        # Should trigger prints and input prompt due to text_sample difference
        with patch("pat2vec.util.evaluation_methods.logger") as mock_logger:
            compare_ipw_annotation_rows([df1, df2])
            self.assertTrue(mock_input.called)
            self.assertTrue(mock_logger.info.called)

    @unittest.skipUnless(HAS_YDATA, "ydata_profiling not installed")
    @patch("pat2vec.util.evaluation_methods.tqdm", lambda x, **kwargs: x)
    @patch("ydata_profiling.ProfileReport")
    def test_csv_profiler_basic(self, mock_profile_report):
        """Test CsvProfiler creates reports for CSV files."""
        # Create a dummy CSV
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

        # Verify the dataframe passed to ProfileReport was filtered (should have 1 row)
        args, _ = mock_profile_report.call_args
        passed_df = args[0]
        self.assertEqual(len(passed_df), 1)
