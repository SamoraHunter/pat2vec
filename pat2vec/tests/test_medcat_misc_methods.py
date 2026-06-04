import unittest
import pandas as pd
import json
import os
import tempfile
from pat2vec.util.medcat_misc_methods import (
    medcat_trainer_export_to_df,
    extract_labels_from_medcat_annotation_export,
    recreate_json,
    create_ner_results_dataframe,
)


class TestMedcatMiscMethods(unittest.TestCase):
    def setUp(self):
        # Sample data matching the structure expected by the MedCATTrainer export
        self.test_json_data = {
            "projects": [
                {
                    "name": "Test Project",
                    "id": 1,
                    "documents": [
                        {
                            "id": 101,
                            "name": "Doc1",
                            "text": "The patient was diagnosed with asthma in the past.",
                            "annotations": [
                                {
                                    "id": 1001,
                                    "user": "tester",
                                    "cui": "C0004096",
                                    "value": "asthma",
                                    "start": 31,
                                    "end": 37,
                                    "validated": True,
                                    "correct": True,
                                    "deleted": False,
                                    "alternative": False,
                                    "killed": False,
                                    "irrelevant": False,
                                    "create_time": "2023-10-27T10:00:00",
                                    "last_modified": "2023-10-27T10:05:00",
                                    "comment": "Accurate",
                                    "manually_created": False,
                                    "meta_anns": {
                                        "Subject/Experiencer": {"value": "Patient"},
                                        "Presence": {"value": "True"},
                                        "Time": {"value": "Past"},
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        self.tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        with open(self.tmp_file.name, "w") as f:
            json.dump(self.test_json_data, f)

    def tearDown(self):
        if os.path.exists(self.tmp_file.name):
            os.remove(self.tmp_file.name)

    def test_medcat_trainer_export_to_df(self):
        df = medcat_trainer_export_to_df(self.tmp_file.name)
        self.assertFalse(df.empty)
        self.assertEqual(df.at[0, "value"], "asthma")
        # Meta annotations should be lowercase and underscored
        self.assertEqual(df.at[0, "subject_experiencer"], "Patient")
        self.assertEqual(df.at[0, "presence"], "True")

    def test_recreate_json(self):
        df = medcat_trainer_export_to_df(self.tmp_file.name)
        json_str = recreate_json(df)
        recreated = json.loads(json_str)
        self.assertIn("projects", recreated)
        self.assertEqual(
            recreated["projects"][0]["documents"][0]["annotations"][0]["value"],
            "asthma",
        )

    def test_create_ner_results_dataframe(self):
        fps, fns, tps = {"C1": 5}, {"C1": 2}, {"C1": 10}
        metrics = {"C1": 0.5}  # Simplified for test
        df = create_ner_results_dataframe(
            fps, fns, tps, metrics, metrics, metrics, metrics
        )
        self.assertEqual(df.loc["C1", "fps"], 5)
        self.assertEqual(df.loc["C1", "tps"], 10)

    def test_extract_labels_from_medcat_annotation_export(self):
        df = medcat_trainer_export_to_df(self.tmp_file.name)
        human_labels = pd.DataFrame(
            {
                "text_sample": ["The patient was diagnosed with asthma in the past."],
                "source_value": ["asthma"],
            }
        )

        result = extract_labels_from_medcat_annotation_export(
            df, human_labels, window=300
        )
        self.assertEqual(result.at[0, "extracted_label"], 1)


if __name__ == "__main__":
    unittest.main()
