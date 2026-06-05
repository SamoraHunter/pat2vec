import unittest
from unittest.mock import patch, mock_open
import pandas as pd
import json
from pat2vec.util.medcat_misc_methods import (
    medcat_trainer_export_to_df,
    extract_labels_from_medcat_annotation_export,
    recreate_json,
    create_ner_results_dataframe,
    parse_medcat_trainer_project_json,
)


class TestMedcatMiscMethods(unittest.TestCase):
    def test_medcat_trainer_export_to_df(self):
        mock_data = {
            "projects": [
                {
                    "name": "P1",
                    "id": 1,
                    "documents": [
                        {
                            "id": 10,
                            "name": "D1",
                            "text": "sample text",
                            "annotations": [
                                {
                                    "id": 100,
                                    "user": "U1",
                                    "cui": "C1",
                                    "value": "val",
                                    "start": 0,
                                    "end": 4,
                                    "validated": True,
                                    "correct": True,
                                    "deleted": False,
                                    "alternative": False,
                                    "killed": False,
                                    "irrelevant": False,
                                    "create_time": "",
                                    "last_modified": "",
                                    "comment": "",
                                    "manually_created": True,
                                    "meta_anns": {"Presence": {"value": "True"}},
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        with patch("builtins.open", mock_open(read_data=json.dumps(mock_data))):
            df = medcat_trainer_export_to_df("fake.json")
            self.assertEqual(len(df), 1)
            self.assertEqual(df.iloc[0]["project_name"], "P1")
            self.assertEqual(df.iloc[0]["presence"], "True")

    def test_recreate_json(self):
        df = pd.DataFrame(
            [
                {
                    "project_name": "P1",
                    "project_id": 1,
                    "document_id": 10,
                    "document_name": "D1",
                    "text": "txt",
                    "annotation_id": 100,
                    "user": "U1",
                    "cui": "C1",
                    "value": "val",
                    "start": 0,
                    "end": 3,
                    "validated": True,
                    "correct": True,
                    "deleted": False,
                    "alternative": False,
                    "killed": False,
                    "irrelevant": False,
                    "create_time": "",
                    "last_modified": "",
                    "comment": "",
                    "manually_created": True,
                    "subject_experiencer": "Patient",
                    "presence": "True",
                    "time": "Recent",
                }
            ]
        )
        json_str = recreate_json(df)
        data = json.loads(json_str)
        self.assertEqual(
            data["projects"][0]["documents"][0]["annotations"][0]["meta_anns"][
                "Presence"
            ]["value"],
            "True",
        )

    def test_create_ner_results_dataframe(self):
        fps, fns, tps = {"C1": 1}, {"C1": 0}, {"C1": 5}
        prec, rec, f1, counts = {"C1": 0.8}, {"C1": 1.0}, {"C1": 0.9}, {"C1": 6}
        df = create_ner_results_dataframe(fps, fns, tps, prec, rec, f1, counts)
        self.assertEqual(df.loc["C1", "tps"], 5)

    def test_parse_medcat_trainer_project_json_nested(self):
        mock_data = {"projects": [{"id": 1, "name": "Project", "documents": []}]}
        with patch("builtins.open", mock_open(read_data=json.dumps(mock_data))):
            # This tests the branch that handles dict with 'projects' key
            df = parse_medcat_trainer_project_json("fake.json")
            self.assertEqual(df.iloc[0]["project_name"], "Project")

    def test_extract_labels_from_medcat_annotation_export(self):
        """Test extraction and validation of labels from annotation export."""
        df = pd.DataFrame(
            [
                {
                    "text": "Patient has asthma",
                    "value": "asthma",
                    "start": 12,
                    "end": 18,
                    "subject_experiencer": "Patient",
                    "presence": "True",
                    "time": "Recent",
                }
            ]
        )
        human_labels = pd.DataFrame(
            [{"text_sample": "Patient has asthma", "source_value": "asthma"}]
        )

        result = extract_labels_from_medcat_annotation_export(df, human_labels)
        self.assertEqual(result.iloc[0]["extracted_label"], 1)
