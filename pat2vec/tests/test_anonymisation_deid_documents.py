import unittest
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd
from pathlib import Path
import json

# Import the class and functions to test
from pat2vec.util.anonymisation_deid_documents import (
    DeIdAnonymizer,
    anonymize_single_text,
)


class TestDeIdAnonymizer(unittest.TestCase):
    """Unit tests for the DeIdAnonymizer functionality."""

    def setUp(self):
        """Set up common test fixtures and mocks."""
        # Patch MEDCAT_AVAILABLE first so DeIdAnonymizer doesn't raise ImportError
        self.patcher_medcat_available = patch(
            "pat2vec.util.anonymisation_deid_documents.MEDCAT_AVAILABLE",
            new=True,
        )
        self.mock_medcat_available = self.patcher_medcat_available.start()

        # Mock DeIdModel and spacy globally for these tests to avoid needing local model packs
        self.patcher_deid = patch("pat2vec.util.anonymisation_deid_documents.DeIdModel")
        self.mock_deid_class = self.patcher_deid.start()
        self.patcher_spacy = patch("pat2vec.util.anonymisation_deid_documents.spacy")
        self.mock_spacy = self.patcher_spacy.start()

        # Mock Path.exists globally
        self.patcher_path_exists = patch("pathlib.Path.exists", return_value=True)
        self.patcher_path_exists.start()

        self.model_path = Path("/fake/model/path")
        self.anonymizer = DeIdAnonymizer()

    def tearDown(self):
        """Clean up mocks after each test."""
        self.patcher_medcat_available.stop()
        self.patcher_deid.stop()
        self.patcher_spacy.stop()
        self.patcher_path_exists.stop()

    def test_init_without_model(self):
        """Verify that the anonymizer initializes correctly without an immediate model load."""
        anonymizer = DeIdAnonymizer()
        self.assertFalse(anonymizer.is_loaded)
        self.assertIsNone(anonymizer.model)

    def test_load_model_success(self):
        """Test successful loading of a DeIdModel and registration of spaCy extensions."""
        mock_model_instance = MagicMock()
        mock_model_instance.pii_labels = ["PERSON", "DATE"]
        self.mock_deid_class.load_model_pack.return_value = mock_model_instance

        # Mock spacy extension check to simulate a fresh environment
        self.mock_spacy.tokens.Span.has_extension.return_value = False

        success = self.anonymizer.load_model(self.model_path)

        self.assertTrue(success)
        self.assertTrue(self.anonymizer.is_loaded)
        self.assertEqual(self.anonymizer.pii_labels, ["PERSON", "DATE"])
        # Verify that the hotfix for link_candidates was applied
        self.mock_spacy.tokens.Span.set_extension.assert_called_with(
            "link_candidates", default=[]
        )

    def test_load_model_empty_labels_warning(self):
        """Ensure a warning is logged if the loaded model has no labels to redact."""
        mock_model_instance = MagicMock()
        mock_model_instance.pii_labels = []
        self.mock_deid_class.load_model_pack.return_value = mock_model_instance

        with self.assertLogs(
            "pat2vec.util.anonymisation_deid_documents", level="WARNING"
        ) as cm:
            self.anonymizer.load_model(self.model_path)
            self.assertTrue(
                any(
                    "Falling back to default PII label list" in line
                    for line in cm.output
                )
            )

    def test_anonymize_text_single(self):
        """Test anonymizing a single string of clinical text."""
        mock_model = MagicMock()
        mock_model.deid_text.return_value = "Patient *** visited clinic on ***"
        self.anonymizer.model = mock_model
        self.anonymizer.is_loaded = True

        result = self.anonymizer.anonymize_text(
            "Patient John visited clinic on Monday", redact=True
        )

        self.assertEqual(result, "Patient *** visited clinic on ***")
        mock_model.deid_text.assert_called_with(
            "Patient John visited clinic on Monday", redact=True
        )

    def test_anonymize_text_with_tags(self):
        """Test anonymization using type tags instead of redaction."""
        mock_model = MagicMock()
        mock_model.deid_text.return_value = "Patient <PERSON> visited"
        self.anonymizer.model = mock_model
        self.anonymizer.is_loaded = True

        result = self.anonymizer.anonymize_text("Patient John visited", redact=False)
        self.assertEqual(result, "Patient <PERSON> visited")

    def test_anonymize_texts_multi_batch(self):
        """Test batch processing of multiple texts."""
        mock_model = MagicMock()
        mock_model.deid_multi_texts.return_value = ["Anon 1", "Anon 2"]
        self.anonymizer.model = mock_model
        self.anonymizer.is_loaded = True

        result = self.anonymizer.anonymize_texts(["Text 1", "Text 2"], n_process=2)

        self.assertEqual(len(result), 2)
        mock_model.deid_multi_texts.assert_called()

    def test_anonymize_dataframe_with_nan_handling(self):
        """Verify that DataFrame column anonymization handles NaN values gracefully."""
        df = pd.DataFrame({"notes": ["Patient John", None]})
        mock_model = MagicMock()
        # MedCAT expected call: ["Patient John", ""]
        mock_model.deid_multi_texts.return_value = ["Patient ***", ""]
        self.anonymizer.model = mock_model
        self.anonymizer.is_loaded = True

        result_df = self.anonymizer.anonymize_dataframe(df, ["notes"])

        self.assertIn("notes_anonymized", result_df.columns)
        self.assertEqual(result_df["notes_anonymized"].tolist(), ["Patient ***", ""])

    def test_anonymize_dataframe_inplace(self):
        """Verify that inplace DataFrame modification works correctly."""
        df = pd.DataFrame({"notes": ["Sensitive"]})
        mock_model = MagicMock()
        mock_model.deid_multi_texts.return_value = ["Redacted"]
        self.anonymizer.model = mock_model
        self.anonymizer.is_loaded = True

        self.anonymizer.anonymize_dataframe(df, ["notes"], inplace=True)

        self.assertEqual(df["notes"].iloc[0], "Redacted")
        self.assertNotIn("notes_anonymized", df.columns)

    def test_inspect_text_pii_logging(self):
        """Test the inspection utility for identifying PII without modifying text."""
        mock_model = MagicMock()
        self.anonymizer.model = mock_model
        self.anonymizer.is_loaded = True

        # Mock get_structured_annotations directly to avoid MagicMock formatting errors in logger
        self.anonymizer.get_structured_annotations = MagicMock(
            return_value=[{"text": "1990-01-01", "label": "DATE", "confidence": 0.99}]
        )

        entities = self.anonymizer.inspect_text("Born on 1990-01-01")

        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0]["label"], "DATE")
        self.assertEqual(entities[0]["text"], "1990-01-01")

    def test_generate_report_stats(self):
        """Ensure the statistical report aggregates operation logs correctly."""
        self.anonymizer.anonymization_log = [
            {
                "operation": "single_text",
                "timestamp": pd.Timestamp.now(),
                "details": {"redact": True},
            },
            {
                "operation": "multiple_texts",
                "timestamp": pd.Timestamp.now(),
                "details": {"count": 10, "redact": False},
            },
        ]
        report = self.anonymizer.generate_report()
        self.assertEqual(report["total_operations"], 2)
        self.assertEqual(report["total_texts_processed"], 11)
        self.assertEqual(report["operation_breakdown"]["single_text"], 1)

    @patch("builtins.open", new_callable=mock_open)
    def test_save_log_serialization(self, mock_file):
        """Verify that operation logs are correctly serialized to JSON."""
        self.anonymizer.anonymization_log = [
            {
                "operation": "audit",
                "timestamp": pd.Timestamp("2026-06-04 13:30"),
                "details": {"user": "admin"},
            }
        ]
        self.anonymizer.save_log("audit.json")
        mock_file.assert_called_with(Path("audit.json"), "w")

        # Verify JSON formatting
        handle = mock_file()
        written_content = "".join(call.args[0] for call in handle.write.call_args_list)
        log_json = json.loads(written_content)
        self.assertEqual(log_json[0]["timestamp"], "2026-06-04T13:30:00")

    def test_convenience_functions(self):
        """Test the standalone convenience functions."""
        mock_model_instance = MagicMock()
        mock_model_instance.deid_text.return_value = "Anon"
        self.mock_deid_class.load_model_pack.return_value = mock_model_instance

        result = anonymize_single_text("Text", self.model_path)
        self.assertEqual(result, "Anon")

    def test_check_model_loaded_raises_runtime_error(self):
        """Ensure operations fail if no model has been loaded."""
        anonymizer = DeIdAnonymizer()
        with self.assertRaises(RuntimeError) as cm:
            anonymizer.anonymize_text("Some text")
        self.assertIn("DeIdModel not loaded", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
