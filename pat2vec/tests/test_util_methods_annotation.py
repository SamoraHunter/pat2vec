import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import uuid
import os
import shutil
import tempfile
from datetime import datetime, timedelta
from pat2vec.util.methods_annotation import (
    check_pat_document_annotation_complete,
    annot_pat_batch_docs,
    multi_annots_to_df_textual_obs,
    calculate_pretty_name_count_features,
)


class TestMethodsAnnotation(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config = MagicMock()
        self.config.start_time = datetime.now()
        self.config.storage_backend = "file"
        self.config.remote_dump = False
        self.config.pre_document_annotation_batch_path = self.test_dir
        self.config.pre_textual_obs_annotation_batch_path = self.test_dir
        self.config.pre_document_annotation_batch_path_reports = self.test_dir
        self.config.pre_document_annotation_batch_path_mct = self.test_dir
        self.config.verbosity = 0
        self.config.add_icd10 = False
        self.config.add_opc4s = False
        self.config.main_options = {
            "annotations": True,
            "annotations_mrc": False,
            "annotations_reports": False,
            "textual_obs": False,
        }

        # Set thresholds to timedelta to avoid TypeError in update_pbar
        self.config.multi_process = False
        self.config.slow_execution_threshold_low = timedelta(seconds=10)
        self.config.slow_execution_threshold_high = timedelta(seconds=20)
        self.config.slow_execution_threshold_extreme = timedelta(seconds=30)

        self.pat_id = "P1"
        self.t = MagicMock()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_check_pat_document_annotation_complete_file(self):
        """Test existence check for file backend."""
        # Use a unique ID and subdirectory to ensure a clean state
        unique_test_dir = tempfile.mkdtemp(dir=self.test_dir)
        self.config.pre_document_annotation_batch_path = unique_test_dir
        self.config.pre_document_annotation_batch_path_mct = unique_test_dir
        self.config.pre_document_annotation_batch_path_reports = unique_test_dir
        self.config.pre_textual_obs_annotation_batch_path = unique_test_dir

        test_pid = f"P_{uuid.uuid4().hex}"  # This line was already correct in the provided context
        csv_path = os.path.join(unique_test_dir, f"{test_pid}.csv")
        if os.path.exists(csv_path):
            os.remove(csv_path)

        with patch("os.path.exists", return_value=False):
            self.assertFalse(
                check_pat_document_annotation_complete(test_pid, self.config)
            )
        open(csv_path, "w").close()
        self.assertTrue(check_pat_document_annotation_complete(test_pid, self.config))

    @patch("pat2vec.util.methods_annotation.text")
    def test_check_pat_document_annotation_complete_db(self, mock_text):
        """Test existence check for database backend."""
        self.config.storage_backend = "database"
        self.config.db_engine = MagicMock()
        self.config.db_engine.name = "sqlite"

        mock_conn = self.config.db_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.scalar.return_value = 1

        self.assertTrue(
            check_pat_document_annotation_complete(self.pat_id, self.config)
        )
        mock_conn.execute.return_value.scalar.return_value = None
        self.assertFalse(
            check_pat_document_annotation_complete(self.pat_id, self.config)
        )

    def test_annot_pat_batch_docs(self):
        """Verify MedCAT multi-text annotation call."""
        cat = MagicMock()
        df = pd.DataFrame({"body_analysed": ["text1", "text2"]})
        annot_pat_batch_docs(self.pat_id, df, cat, self.config, self.t)
        cat.get_entities_multi_texts.assert_called_once()

    @patch("pat2vec.util.methods_annotation.json_to_dataframe")
    def test_multi_annots_to_df_textual_obs(self, mock_json_to_df):
        """Test conversion and saving for textual observations."""
        mock_json_to_df.return_value = pd.DataFrame(
            {"client_idcode": [self.pat_id], "basicobs_entered": ["2023-01-01"]}
        )
        df = pd.DataFrame(
            {
                "textualObs": ["t"],
                "basicobs_guid": ["g1"],
                "basicobs_entered": ["2023-01-01"],
            }
        )
        multi_annots = [{"entities": {}}]

        res = multi_annots_to_df_textual_obs(
            self.pat_id, df, multi_annots, self.config, self.t
        )
        self.assertFalse(res.empty)
        self.assertTrue(
            os.path.exists(os.path.join(self.test_dir, f"{self.pat_id}.csv"))
        )

    def test_calculate_pretty_name_count_features(self):
        """Verify horizontal vector generation from pretty names."""
        df = pd.DataFrame({"pretty_name": ["Asthma", "Asthma", "Diabetes"]})
        res = calculate_pretty_name_count_features(df, suffix="test")
        self.assertEqual(res.at[0, "pretty_name_count_test_Asthma"], 2.0)
        self.assertEqual(res.at[0, "pretty_name_count_test_Diabetes"], 1.0)

        self.assertIsNone(calculate_pretty_name_count_features(pd.DataFrame()))
