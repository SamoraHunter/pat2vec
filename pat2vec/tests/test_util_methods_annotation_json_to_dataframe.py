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
