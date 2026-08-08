"""Unit tests for ICD-10 and OPCS-4 code joining with actual mapping files."""

import os
import unittest

import pandas as pd

from pat2vec.util.post_processing_annotations import (
    join_icd10_codes_to_annot,
    join_icd10_OPC4S_codes_to_annot,
)


class TestJoinMappingCodes(unittest.TestCase):
    """Test cases for ICD-10 and OPCS-4 code joining functions."""

    def setUp(self):
        """Set up test fixtures with actual mapping file samples."""
        # Get project root directory (parent of pat2vec)
        self.test_files_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "test_files")
        )

        # Use the sample mapping files from test_files directory
        self.icd10_map_path = os.path.join(self.test_files_dir, "test_icd10_map.tsv")
        self.map_csv_path = os.path.join(self.test_files_dir, "test_map.csv")

        # Test annotation data with CUIs that match our sample files
        self.annotation_df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P2", "P3", "P4"],
                "cui": [
                    38341003,
                    8517006,
                    99999999,
                    38341003,
                ],  # Two match ICD10, one doesn't
                "pretty_name": ["Hypertension", "Smoking", "Unknown", "Hypertension"],
                "source_value": ["bp high", "smoker", "test", "bp high"],
            }
        )

    def test_join_icd10_codes_to_annot_inner_match(self):
        """Test inner join - should only return matching CUIs."""
        # Use the actual mapping file path
        result = join_icd10_codes_to_annot(
            self.annotation_df.copy(),
            inner=True,
            file_path=self.icd10_map_path,
        )

        # Should have 5 rows due to duplicates in map (CUI 38341003 has multiple ICD-10 codes, appears twice = 2*2 + 1 = 5)
        self.assertEqual(
            len(result),
            5,
            "Inner join should return matching CUIs with all duplicate mappings",
        )

        # Verify the matched CUIs (as strings due to astype conversion)
        result_cuis = [str(c) for c in result["cui"].tolist()]
        self.assertIn("38341003", result_cuis)
        self.assertIn("8517006", result_cuis)

        # Map rows don't have CUI 99999999 - that's fine, inner join only returns matches

    def test_join_icd10_codes_to_annot_left_match(self):
        """Test left join - should return all rows with NaN for non-matching."""
        result = join_icd10_codes_to_annot(
            self.annotation_df.copy(),
            inner=False,
            file_path=self.icd10_map_path,
        )

        # Should have 6 rows due to duplicates in map (CUI 38341003 has multiple ICD-10 codes, appears twice in annotations = 2*2 + 1 + 1 non-matching = 6)
        self.assertEqual(
            len(result), 6, "Left join should return all rows with duplicate mappings"
        )

        # Verify CUIs that matched have ICD-10 codes
        hypertension_rows = result[result["cui"] == "38341003"]
        smoking_row = result[result["cui"] == "8517006"]
        unknown_row = result[result["cui"] == "99999999"]

        # Should have ICD-10 codes for matched entries
        self.assertEqual(
            len(hypertension_rows),
            4,
            "CUI 38341003 should have 4 rows due to duplicates",
        )
        self.assertFalse(
            hypertension_rows["icd10"].isna().all(),
            "Matched CUI should have ICD-10 code",
        )
        self.assertIn(
            "Z87.891",
            smoking_row["icd10"].tolist(),
            "Smoking CUI 8517006 should map to Z87.891",
        )

        # Non-matching should have NaN for icd10
        self.assertTrue(
            unknown_row["icd10"].isna().all(),
            "Non-matching CUI 99999999 row should have NaN ICD-10",
        )

    def test_join_icd10_OPC4S_codes_to_annot_inner_match(self):
        """Test inner join for map.csv with conceptId column."""
        result = join_icd10_OPC4S_codes_to_annot(
            self.annotation_df.copy(),
            inner=True,
            file_path=self.map_csv_path,
        )

        # Should have 5 rows due to duplicates in map.csv (CUI 38341003 has multiple entries)
        self.assertEqual(
            len(result),
            5,
            "Inner join should return matching CUIs with all duplicate mappings",
        )

    def test_join_icd10_OPC4S_codes_to_annot_left_match(self):
        """Test left join for map.csv with conceptId column."""
        result = join_icd10_OPC4S_codes_to_annot(
            self.annotation_df.copy(),
            inner=False,
            file_path=self.map_csv_path,
        )

        # Should have 6 rows due to duplicates in map.csv + 1 non-matching row
        self.assertEqual(
            len(result), 6, "Left join should return all rows with duplicate mappings"
        )

        # Verify matched CUIs have codes from map.csv (now opcs4 since we renamed targetId)
        hypertension_rows = result[result["cui"] == "38341003"]
        smoking_row = result[result["cui"] == "8517006"]

        self.assertEqual(
            len(hypertension_rows),
            4,
            "CUI 38341003 appears twice in annotations and twice in map (2*2=4)",
        )
        self.assertFalse(
            hypertension_rows["opcs4"].isna().all(),
            "Matched CUI should have opcs4 (renamed from targetId) from map.csv",
        )
        self.assertIn(
            "Z86.4",
            smoking_row["opcs4"].tolist(),
            "Smoking CUI 8517006 should map to Z86.4 in map.csv",
        )

    def test_join_icd10_codes_column_names(self):
        """Test that correct columns are created after join."""
        result = join_icd10_codes_to_annot(
            self.annotation_df.copy(),
            inner=False,
            file_path=self.icd10_map_path,
        )

        # Should have icd10 column
        self.assertIn("icd10", result.columns, "Result should have icd10 column")

        # mapTargetName should be renamed to targetName (ICD-10 join)
        self.assertIn(
            "targetName",
            result.columns,
            "mapTargetName should be renamed to targetName",
        )

    def test_join_opcs4_codes_column_names(self):
        """Test that correct columns are created from map.csv after join."""
        result = join_icd10_OPC4S_codes_to_annot(
            self.annotation_df.copy(),
            inner=False,
            file_path=self.map_csv_path,
        )

        # Should have opcs4 column (renamed from targetId in map.csv)
        self.assertIn("opcs4", result.columns, "Result should have opcs4 column")


if __name__ == "__main__":
    unittest.main()
