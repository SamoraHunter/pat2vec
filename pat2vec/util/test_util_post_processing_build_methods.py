import unittest
import os
import pandas as pd
import tempfile
import shutil
from unittest.mock import MagicMock, patch
from pat2vec.util.post_processing_build_methods import (
    optimize_dtypes,
    build_merged_epr_mct_annot_df,
    build_merged_bloods,
    merge_demographics_csv,
    join_docs_to_annots,
    retrieve_pat_bloods,
)


class TestPostProcessingBuildMethods(unittest.TestCase):
    """Unit tests for the merged data building and post-processing utility module."""

    def setUp(self):
        """Set up temporary directory and mock configuration."""
        self.test_dir = tempfile.mkdtemp()
        self.config_obj = MagicMock()
        self.config_obj.root_path = self.test_dir
        self.config_obj.proj_name = "test_project"
        self.config_obj.storage_backend = "file"
        self.config_obj.include_text_sample_in_annots = False
        self.config_obj.pre_bloods_batch_path = os.path.join(self.test_dir, "bloods")
        self.config_obj.pre_bmi_batch_path = os.path.join(self.test_dir, "bmi")
        self.config_obj.pre_demo_batch_path = os.path.join(self.test_dir, "demo")
        self.config_obj.pre_document_batch_path = os.path.join(self.test_dir, "docs")
        os.makedirs(self.config_obj.pre_bloods_batch_path)
        os.makedirs(self.config_obj.pre_bmi_batch_path)
        os.makedirs(self.config_obj.pre_demo_batch_path)
        os.makedirs(self.config_obj.pre_document_batch_path)

    def tearDown(self):
        """Clean up temporary resources."""
        shutil.rmtree(self.test_dir)

    def test_optimize_dtypes(self):
        """Test memory optimization by downcasting numeric columns."""
        df = pd.DataFrame({"ints": [1, 2, 3], "floats": [1.0, 2.0, 3.0]})
        df["ints"] = df["ints"].astype("int64")
        df["floats"] = df["floats"].astype("float64")
        optimized = optimize_dtypes(df.copy())
        self.assertEqual(optimized["ints"].dtype, "int8")
        self.assertEqual(optimized["floats"].dtype, "float32")

    def test_retrieve_pat_bloods_file(self):
        """Test local file retrieval for patient bloods data."""
        pat_id = "P1"
        df = pd.DataFrame({"client_idcode": [pat_id], "val": [10]})
        df.to_csv(
            os.path.join(self.config_obj.pre_bloods_batch_path, f"{pat_id}.csv"),
            index=False,
        )
        res = retrieve_pat_bloods(pat_id, self.config_obj)
        self.assertEqual(res.iloc[0]["client_idcode"], pat_id)

    def test_join_docs_to_annots(self):
        """Test joining annotations with their source documents while handling duplicates."""
        annots = pd.DataFrame({"document_guid": ["G1", "G2"], "cui": [100, 200]})
        docs = pd.DataFrame(
            {
                "document_guid": ["G1", "G3"],
                "body": ["Text 1", "Text 3"],
                "cui": [100, 300],
            }
        )
        result = join_docs_to_annots(annots, docs)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["body"], "Text 1")
        self.assertTrue(pd.isna(result.iloc[1]["body"]))

    def test_build_merged_bloods(self):
        """Test full build cycle for merged bloods from multiple files."""
        pat_list = ["P1", "P2"]
        for p in pat_list:
            df = pd.DataFrame(
                {
                    "client_idcode": [p],
                    "basicobs_itemname_analysed": ["B1"],
                    "basicobs_value_numeric": [1.0],
                    "basicobs_entered": ["2023-01-01"],
                    "clientvisit_serviceguid": ["S1"],
                    "updatetime": ["2023-01-01"],
                }
            )
            df.to_csv(
                os.path.join(self.config_obj.pre_bloods_batch_path, f"{p}.csv"),
                index=False,
            )
        output_path = build_merged_bloods(pat_list, self.config_obj, overwrite=True)
        merged_df = pd.read_csv(output_path)
        self.assertEqual(len(merged_df), 2)

    def test_merge_demographics_csv(self):
        """Test merging of demographics data from files."""
        pat_list = ["P1"]
        df = pd.DataFrame(
            {
                "client_idcode": ["P1"],
                "client_firstname": ["John"],
                "client_lastname": ["Doe"],
                "client_dob": ["1990-01-01"],
                "client_gendercode": ["M"],
                "client_racecode": ["W"],
                "client_deceaseddtm": [""],
                "updatetime": ["2023-01-01"],
            }
        )
        df.to_csv(
            os.path.join(self.config_obj.pre_demo_batch_path, "P1.csv"), index=False
        )
        output_path = merge_demographics_csv(pat_list, self.config_obj, overwrite=True)
        merged_df = pd.read_csv(output_path)
        self.assertEqual(merged_df.iloc[0]["client_firstname"], "John")

    @patch("pat2vec.util.post_processing_build_methods.retrieve_pat_annots_mct_epr")
    def test_build_merged_epr_mct_annot_df(self, mock_retrieve):
        """Test annotation merging with aggressive RAM safety logic (dropping text blobs)."""
        pat_id = "P1"
        mock_retrieve.return_value = pd.DataFrame(
            {
                "client_idcode": [pat_id],
                "updatetime": ["2023-01-01"],
                "cui": [100],
                "body_analysed": ["large text blob to drop"],
                "acc": [0.9],
            }
        )
        output_path = build_merged_epr_mct_annot_df(
            [pat_id], self.config_obj, overwrite=True
        )
        merged_df = pd.read_csv(output_path)
        self.assertNotIn("body_analysed", merged_df.columns)
        self.assertEqual(len(merged_df), 1)
        self.assertEqual(merged_df.iloc[0]["client_idcode"], pat_id)


if __name__ == "__main__":
    unittest.main()
