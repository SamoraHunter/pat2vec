import unittest
import os
import tempfile
from unittest.mock import patch, MagicMock
import pandas as pd
from pat2vec.util.get_dummy_data_cohort_searcher import (
    generate_uuid,
    generate_uuid_list,
    extract_search_term_obscatalogmasteritem_displayname,
    run_generate_patient_timeline_and_append,
)


class TestDummyDataHelpers(unittest.TestCase):
    def test_generate_uuid(self):
        """Test UUID-like generation with prefixes."""
        pid = generate_uuid("P", length=5)
        self.assertTrue(pid.startswith("P"))
        self.assertEqual(len(pid), 6)

        with self.assertRaises(ValueError):
            generate_uuid("X")

    def test_generate_uuid_list(self):
        """Test list generation of UUIDs."""
        uuids = generate_uuid_list(3, "V")
        self.assertEqual(len(uuids), 3)
        self.assertTrue(all(u.startswith("V") for u in uuids))

    def test_extract_search_term(self):
        """Test extraction of terms from displayname queries."""
        s1 = 'obscatalogmasteritem_displayname:("Smoking")'
        self.assertEqual(
            extract_search_term_obscatalogmasteritem_displayname(s1), "Smoking"
        )

        s2 = 'obscatalogmasteritem_displayname:("Oxygen" AND "Liters")'
        self.assertEqual(
            extract_search_term_obscatalogmasteritem_displayname(s2), "Oxygen"
        )

        s3 = "simple string"
        self.assertEqual(extract_search_term_obscatalogmasteritem_displayname(s3), s3)

    def test_run_generate_patient_timeline_and_append(self):
        """Verify that the timeline generation tool expands a CSV file."""
        test_csv = os.path.join(tempfile.gettempdir(), "temp_timeline.csv")
        if os.path.exists(test_csv):
            os.remove(test_csv)

        # Mock the timeline generation to avoid network calls to Hugging Face
        dummy_df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P2"],
                "body_analysed": ["text1", "text2"],
                "updatetime": ["2023-01-01", "2023-01-02"],
            }
        )

        # Patch at the module level where the function is used
        # Mock the pipeline function directly to prevent actual model loading
        mock_pipeline_instance = MagicMock()
        mock_pipeline_instance.return_value = [{"generated_text": "mocked text"}]

        with (
            patch(
                "pat2vec.util.get_dummy_data_cohort_searcher.pipeline",
                return_value=mock_pipeline_instance,
            ),
            patch(
                "pat2vec.util.get_dummy_data_cohort_searcher.get_patient_timeline_dummy",
                return_value=dummy_df,
            ),
        ):  # Keep this patch if get_patient_timeline_dummy is also called
            with patch(
                "pat2vec.util.get_dummy_data_cohort_searcher.generate_patient_timeline_faker",
                return_value=dummy_df,
            ):
                try:
                    run_generate_patient_timeline_and_append(n=2, output_path=test_csv)
                    self.assertTrue(os.path.exists(test_csv))
                    df = pd.read_csv(test_csv)
                    self.assertEqual(len(df), 2)
                finally:
                    if os.path.exists(test_csv):
                        os.remove(test_csv)


if __name__ == "__main__":
    unittest.main()
