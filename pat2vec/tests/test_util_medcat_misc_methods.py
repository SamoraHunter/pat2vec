import unittest
from unittest.mock import patch, mock_open
import pandas as pd
import json
import tempfile
import os
from pat2vec.util.medcat_misc_methods import (
    medcat_trainer_export_to_df,
    extract_labels_from_medcat_annotation_export,
    recreate_json,
    create_ner_results_dataframe,
    parse_medcat_trainer_project_json,
    manually_label_annotation_df,
    plot_ner_results,
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

    @patch("pat2vec.util.medcat_misc_methods.input", side_effect=["", "0", "quit"])
    @patch("pat2vec.util.medcat_misc_methods.clear_output")
    def test_manually_label_annotation_df(self, mock_clear, mock_input):
        """Test interactive labeling utility with mocked inputs."""
        df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P2", "P3"],
                "text_sample": ["text1", "text2", "text3"],
                "source_value": ["val1", "val2", "val3"],
                "cui": ["C1", "C2", "C3"],
                "human_label": [None, None, None],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp_path = tmp.name
            # Initialize temp file with headers to avoid EmptyDataError
            pd.DataFrame(
                columns=[
                    "client_idcode",
                    "text_sample",
                    "source_value",
                    "cui",
                    "human_label",
                ]
            ).to_csv(tmp_path, index=False)

        try:
            try:
                manually_label_annotation_df(df, file_path=tmp_path, verbose=False)
            except ValueError as e:
                self.assertEqual(str(e), "User ended the labeling process.")

            self.assertEqual(df.at[0, "human_label"], 1)  # Empty input maps to 1
            self.assertEqual(df.at[1, "human_label"], 0)  # "0" maps to 0
            self.assertTrue(pd.isna(df.at[2, "human_label"]))
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    @patch("pat2vec.util.medcat_misc_methods.plt.show")
    def test_plot_ner_results(self, mock_show):
        """Verify that the plotting function triggers matplotlib calls."""
        results_df = pd.DataFrame(
            {
                "cui_name": ["Concept A", "Concept B"],
                "cui_f1": [0.7, 0.9],
                "cui_prec": [0.6, 0.85],
                "cui_rec": [0.8, 0.95],
                "fps": [10, 5],
                "fns": [5, 2],
                "tps": [20, 45],
                "cui_counts": [35, 52],
            }
        )
        plot_ner_results(results_df)
        self.assertGreater(mock_show.call_count, 0)
