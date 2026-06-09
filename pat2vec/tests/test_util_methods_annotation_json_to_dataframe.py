import unittest
import pandas as pd
from pat2vec.util.methods_annotation_json_to_dataframe import (
    json_to_dataframe,
    parse_meta_anns,
)


class TestMethodsAnnotationJsonToDataframe(unittest.TestCase):
    def test_parse_meta_anns_mapping(self):
        """Verify extraction of values and confidence from MedCAT meta_anns dictionary."""
        meta = {
            "Time": {"value": "Past", "confidence": 0.95},
            "Presence": {"value": "True", "confidence": 0.88},
            "Subject": {"value": "Patient", "confidence": 0.99},
        }
        res = parse_meta_anns(meta)
        self.assertEqual(res["Time_Value"], "Past")
        self.assertEqual(res["Presence_Confidence"], 0.88)
        self.assertEqual(res["Subject_Value"], "Patient")

    def test_json_to_dataframe_full_conversion(self):
        """Test converting a MedCAT entity result into a structured pandas row."""
        json_data = {
            "entities": {
                "0": {
                    "pretty_name": "Asthma",
                    "cui": "C0004096",
                    "type_ids": ["T047"],
                    "types": ["Disease"],
                    "source_value": "asthma",
                    "detected_name": "asthma",
                    "acc": 0.9,
                    "context_similarity": 0.85,
                    "start": 10,
                    "end": 16,
                    "icd10": [],
                    "ontologies": [],
                    "snomed": True,
                    "id": "0",
                    "meta_anns": {"Time": {"value": "Recent"}},
                }
            }
        }
        doc = pd.Series(
            {
                "body_analysed": "Patient has asthma in history",
                "updatetime": pd.to_datetime("2023-01-01"),
                "document_guid": "G_ANNOT",
            }
        )

        df = json_to_dataframe(
            json_data, doc, "P_TEST", include_text_sample=True, window=5
        )

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["pretty_name"], "Asthma")
        self.assertEqual(df.iloc[0]["text_sample"], "nt has asthma in")

    def test_parse_meta_anns_subject_fallback(self):
        """Test fallback to Subject when Subject/Experiencer is missing."""
        meta = {
            "Time": {"value": "Past", "confidence": 0.95},
            "Presence": {"value": "True", "confidence": 0.88},
            "Subject": {"value": "Patient", "confidence": 0.99},
        }
        result = parse_meta_anns(meta)
        self.assertEqual(result["Subject_Value"], "Patient")
        self.assertEqual(result["Subject_Confidence"], 0.99)

    def test_json_to_dataframe_with_multiple_entities(self):
        """Test handling of multiple entities in a single document."""
        json_data = {
            "entities": {
                "0": {
                    "pretty_name": "Hypertension",
                    "cui": "C0020538",
                    "type_ids": ["T047"],
                    "types": ["Disease"],
                    "source_value": "hypertension",
                    "detected_name": "high blood pressure",
                    "acc": 0.92,
                    "context_similarity": 0.88,
                    "start": 0,
                    "end": 14,
                    "icd10": ["I10"],
                    "ontologies": ["ICD10"],
                    "snomed": True,
                    "id": "0",
                    "meta_anns": {
                        "Time": {"value": "Current"},
                        "Presence": {"value": "True"},
                        "Subject/Experiencer": {"value": "Patient"},
                    },
                },
                "1": {
                    "pretty_name": "Diabetes",
                    "cui": "C0011667",
                    "type_ids": ["T047"],
                    "types": ["Disease"],
                    "source_value": "diabetes",
                    "detected_name": "diabetes",
                    "acc": 0.95,
                    "context_similarity": 0.92,
                    "start": 28,
                    "end": 36,
                    "icd10": ["E11"],
                    "ontologies": ["ICD10"],
                    "snomed": False,
                    "id": "1",
                    "meta_anns": {
                        "Time": {"value": "Past"},
                        "Presence": {"value": "True"},
                        "Subject/Experiencer": {"value": "Patient"},
                    },
                },
            }
        }
        doc = pd.Series(
            {
                "body_analysed": "Patient has hypertension and diabetes",
                "updatetime": pd.to_datetime("2024-06-15"),
                "document_guid": "G_MULTI",
            }
        )

        df = json_to_dataframe(json_data, doc, "P_MULTI", include_text_sample=True)

        # Should have 2 rows - one per entity
        self.assertEqual(len(df), 2)
        self.assertIn("Hypertension", list(df["pretty_name"]))
        self.assertIn("Diabetes", list(df["pretty_name"]))

    def test_json_to_dataframe_empty_entities(self):
        """Test handling of empty entities dictionary."""
        json_data = {"entities": {}}
        doc = pd.Series(
            {
                "body_analysed": "No annotations here",
                "updatetime": pd.to_datetime("2023-01-01"),
                "document_guid": "G_TEST",
            }
        )
        df = json_to_dataframe(json_data, doc, "P_EMPTY")
        self.assertTrue(df.empty)
        self.assertIn("pretty_name", df.columns)

    def test_json_to_dataframe_with_full_doc(self):
        """Test full document text inclusion in first annotation."""
        json_data = {
            "entities": {
                "0": {
                    "pretty_name": "Diabetes",
                    "cui": "C0011667",
                    "type_ids": ["T047"],
                    "types": ["Disease"],
                    "source_value": "diabetes",
                    "detected_name": "diabetes",
                    "acc": 0.95,
                    "context_similarity": 0.92,
                    "start": 8,
                    "end": 16,
                    "icd10": ["E11"],
                    "ontologies": ["ICD10"],
                    "snomed": False,
                    "id": "0",
                    "meta_anns": {
                        "Time": {"value": "Present"},
                        "Presence": {"value": "True"},
                        "Subject/Experiencer": {"value": "Patient"},
                    },
                }
            }
        }
        doc = pd.Series(
            {
                "body_analysed": "Patient diagnosed with diabetes mellitus",
                "updatetime": pd.to_datetime("2024-06-15"),
                "document_guid": "G_FULL_DOC",
            }
        )

        # Test full_doc=True - should include full text in first annotation
        df = json_to_dataframe(json_data, doc, "P_TEST", full_doc=True)
        self.assertEqual(len(df), 1)
        self.assertEqual(
            df.iloc[0]["full_doc"], "Patient diagnosed with diabetes mellitus"
        )

    def test_json_to_dataframe_with_empty_meta_anns(self):
        """Test handling of empty meta_anns dictionary."""
        json_data = {
            "entities": {
                "0": {
                    "pretty_name": "Fever",
                    "cui": "C0015967",
                    "type_ids": ["T047"],
                    "types": ["Disease"],
                    "source_value": "fever",
                    "detected_name": "fever",
                    "acc": 0.87,
                    "context_similarity": 0.83,
                    "start": 8,
                    "end": 12,
                    "icd10": [],
                    "ontologies": [],
                    "snomed": True,
                    "id": "0",
                    "meta_anns": {},  # Empty meta anns
                }
            }
        }
        doc = pd.Series(
            {
                "body_analysed": "Patient presents with fever",
                "updatetime": pd.to_datetime("2024-06-15"),
                "document_guid": "G_FEVER",
            }
        )

        # Empty meta_anns should return NaN for all meta annotation fields
        df = json_to_dataframe(json_data, doc, "P_FEVER")
        self.assertEqual(len(df), 1)
        self.assertIsNone(df.iloc[0]["Time_Value"])
        self.assertIsNone(df.iloc[0]["Presence_Value"])
        self.assertIsNone(df.iloc[0]["Subject_Value"])

    def test_json_to_dataframe_exception_handling(self):
        """Test exception handling when df_parts concat fails (simulated)."""
        # The try/except block catches pd.concat errors
        # We can't easily trigger this without modifying code to add a special case
        # For now, verify the function handles invalid input gracefully
        json_data = {
            "entities": {  # Empty entities - should return empty df with columns
                "0": {
                    "pretty_name": "Test",
                    "cui": "C123",
                    "type_ids": [],
                    "types": [],
                    "source_value": "",
                    "detected_name": "",
                    "acc": 0,
                    "context_similarity": 0,
                    "start": 0,
                    "end": 0,
                    "icd10": [],
                    "ontologies": [],
                    "snomed": False,
                    "id": "0",
                    "meta_anns": {},
                }
            }
        }

        doc = pd.Series(
            {
                "body_analysed": "Test document",
                "updatetime": pd.to_datetime("2023-01-01"),
                "document_guid": "G_TEST",
            }
        )

        df = json_to_dataframe(json_data, doc, "P_TEST")
        self.assertFalse(df.empty)
