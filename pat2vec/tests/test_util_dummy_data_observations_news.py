import unittest

import pandas as pd

from pat2vec.util.dummy_data_generation.observations.news import (
    generate_clinically_coherent_value,
    generate_news_data,
)


class TestNEWSFunctions(unittest.TestCase):
    def setUp(self):
        self.base_kwargs = {
            "num_rows": 5,
            "entered_list": ["CLIENT001", "CLIENT002"],
            "global_start_year": 2020,
            "global_start_month": 1,
            "global_end_year": 2026,
            "global_end_month": 12,
        }

    def test_generate_news_data_returns_dataframe(self):
        result = generate_news_data(**self.base_kwargs)
        self.assertIsInstance(result, pd.DataFrame)

    def test_generate_news_data_correct_columns(self):
        result = generate_news_data(**self.base_kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordddtm",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_news_data_row_count(self):
        result = generate_news_data(**self.base_kwargs)
        expected_rows = 10
        self.assertEqual(len(result), expected_rows)

    def test_generate_news_data_single_client(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_news_data(**kwargs)
        self.assertEqual(len(result), 5)
        self.assertTrue(all(result["client_idcode"] == "CLIENT001"))

    def test_generate_news_data_multiple_clients(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = ["CLIENT001", "CLIENT002", "CLIENT003"]
        result = generate_news_data(**kwargs)
        self.assertEqual(len(result), 15)

    def test_generate_news_data_empty_entered_list(self):
        kwargs = self.base_kwargs.copy()
        kwargs["entered_list"] = []
        result = generate_news_data(**kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordddtm",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)
        self.assertEqual(len(result), 0)

    def test_generate_news_data_custom_fields_list(self):
        kwargs = self.base_kwargs.copy()
        custom_fields = ["client_idcode", "observation_valuetext_analysed"]
        kwargs["fields_list"] = custom_fields
        result = generate_news_data(**kwargs)
        self.assertEqual(list(result.columns), custom_fields)

    def test_generate_news_data_zero_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 0
        result = generate_news_data(**kwargs)
        expected_cols = [
            "observation_guid",
            "client_idcode",
            "obscatalogmasteritem_displayname",
            "observation_valuetext_analysed",
            "observationdocument_recordddtm",
            "clientvisit_visitidcode",
        ]
        self.assertEqual(list(result.columns), expected_cols)

    def test_generate_news_data_date_format(self):
        result = generate_news_data(**self.base_kwargs)
        date_col = result["observationdocument_recordddtm"]
        for val in date_col:
            if pd.notna(val) and val != "":
                self.assertRegex(val, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_generate_news_data_obsguid_format(self):
        result = generate_news_data(**self.base_kwargs)
        guid_col = result["observation_guid"]
        for val in guid_col:
            if pd.notna(val) and val != "":
                parts = val.split("-")
                self.assertEqual(len(parts), 5)

    def test_generate_news_data_clientvisit_visitidcode_format(self):
        result = generate_news_data(**self.base_kwargs)
        visit_col = result["clientvisit_visitidcode"]
        for val in visit_col:
            if pd.notna(val) and val != "":
                self.assertTrue(val.startswith("visit_"))

    def test_generate_news_data_different_num_rows(self):
        kwargs = self.base_kwargs.copy()
        kwargs["num_rows"] = 10
        kwargs["entered_list"] = ["CLIENT001"]
        result = generate_news_data(**kwargs)
        self.assertEqual(len(result), 10)

    def test_generate_news_data_output_shape_consistent(self):
        kwargs = self.base_kwargs.copy()
        result1 = generate_news_data(**kwargs)
        result2 = generate_news_data(**kwargs)
        self.assertEqual(result1.shape, result2.shape)


class TestClinicallyCoherentValue(unittest.TestCase):
    def setUp(self):
        self.base_params = {
            "respiration_rate": None,
            "heart_rate": None,
            "temp_celsius": None,
            "pain_score": None,
        }

    def test_generate_clinically_coherent_value_news_systolic_bp_normal(self):
        result = generate_clinically_coherent_value(
            "NEWS_Systolic_BP", **self.base_params
        )
        val = float(result)
        self.assertGreaterEqual(val, 70)
        self.assertLessEqual(val, 250)

    def test_generate_clinically_coherent_value_news_systolic_bp_high_resp(self):
        result = generate_clinically_coherent_value(
            "NEWS_Systolic_BP", respiration_rate=25
        )
        val = float(result)
        self.assertGreaterEqual(val, 130)
        self.assertLessEqual(val, 250)

    def test_generate_clinically_coherent_value_news_systolic_bp_high_hr(self):
        result = generate_clinically_coherent_value("NEWS_Systolic_BP", heart_rate=150)
        val = float(result)
        self.assertGreaterEqual(val, 140)
        self.assertLessEqual(val, 250)

    def test_generate_clinically_coherent_value_news_diastolic_bp_normal(self):
        result = generate_clinically_coherent_value(
            "NEWS_Diastolic_BP", **self.base_params
        )
        val = float(result)
        self.assertGreaterEqual(val, 40)
        self.assertLessEqual(val, 150)

    def test_generate_clinically_coherent_value_news_diastolic_bp_high_resp(self):
        result = generate_clinically_coherent_value(
            "NEWS_Diastolic_BP", respiration_rate=25
        )
        val = float(result)
        self.assertGreaterEqual(val, 85)
        self.assertLessEqual(val, 150)

    def test_generate_clinically_coherent_value_news_diastolic_bp_high_hr(self):
        result = generate_clinically_coherent_value("NEWS_Diastolic_BP", heart_rate=150)
        val = float(result)
        self.assertGreaterEqual(val, 90)
        self.assertLessEqual(val, 150)

    def test_generate_clinically_coherent_value_news_respiration_rate_normal(self):
        result = generate_clinically_coherent_value(
            "NEWS_Respiration_Rate", **self.base_params
        )
        val = float(result)
        self.assertGreaterEqual(val, 8)
        self.assertLessEqual(val, 50)

    def test_generate_clinically_coherent_value_news_respiration_rate_high_temp(self):
        result = generate_clinically_coherent_value(
            "NEWS_Respiration_Rate", temp_celsius=39.0
        )
        val = float(result)
        self.assertGreaterEqual(val, 20)
        self.assertLessEqual(val, 35)

    def test_generate_clinically_coherent_value_news_heart_rate_normal(self):
        result = generate_clinically_coherent_value(
            "NEWS_Heart_Rate", **self.base_params
        )
        val = float(result)
        self.assertGreaterEqual(val, 40)
        self.assertLessEqual(val, 200)

    def test_generate_clinically_coherent_value_news_heart_rate_high_resp(self):
        result = generate_clinically_coherent_value(
            "NEWS_Heart_Rate", respiration_rate=30
        )
        val = float(result)
        self.assertGreaterEqual(val, 100)
        self.assertLessEqual(val, 160)

    def test_generate_clinically_coherent_value_news_heart_rate_high_temp(self):
        result = generate_clinically_coherent_value(
            "NEWS_Heart_Rate", temp_celsius=39.5
        )
        val = float(result)
        self.assertGreaterEqual(val, 90)
        self.assertLessEqual(val, 150)

    def test_generate_clinically_coherent_value_news_oxygen_saturation_normal(self):
        result = generate_clinically_coherent_value(
            "NEWS_Oxygen_Saturation", **self.base_params
        )
        val = float(result)
        self.assertGreaterEqual(val, 85)
        self.assertLessEqual(val, 100)

    def test_generate_clinically_coherent_value_news_oxygen_saturation_high_resp(self):
        result = generate_clinically_coherent_value(
            "NEWS_Oxygen_Saturation", respiration_rate=25
        )
        val = float(result)
        self.assertGreaterEqual(val, 89)
        self.assertLessEqual(val, 96)

    def test_generate_clinically_coherent_value_news_oxygen_saturation_high_hr(self):
        result = generate_clinically_coherent_value(
            "NEWS_Oxygen_Saturation", heart_rate=120
        )
        val = float(result)
        self.assertGreaterEqual(val, 88)
        self.assertLessEqual(val, 97)

    def test_generate_clinically_coherent_value_news_temperature_normal(self):
        result = generate_clinically_coherent_value(
            "NEWS Temperature", **self.base_params
        )
        val = float(result)
        self.assertGreaterEqual(val, 35.0)
        self.assertLessEqual(val, 42.0)

    def test_generate_clinically_coherent_value_news_temperature_high_pain(self):
        result = generate_clinically_coherent_value("NEWS Temperature", pain_score=8)
        val = float(result)
        self.assertGreaterEqual(val, 36.5)
        self.assertLessEqual(val, 39.5)

    def test_generate_clinically_coherent_value_news_avpu(self):
        result = generate_clinically_coherent_value("NEWS_AVPU", **self.base_params)
        self.assertIn(result, ["A", "V", "P", "U"])

    def test_generate_clinically_coherent_value_news_supplemental_oxygen(self):
        result = generate_clinically_coherent_value(
            "NEWS_Supplemental_Oxygen", **self.base_params
        )
        self.assertIn(result, ["No", "Yes"])

    def test_generate_clinically_coherent_value_news2_sp02_target(self):
        result = generate_clinically_coherent_value(
            "NEWS2_Sp02_Target", **self.base_params
        )
        self.assertIn(result, ["94-98%", "92-94%"])

    def test_generate_clinically_coherent_value_news2_sp02_scale(self):
        result = generate_clinically_coherent_value(
            "NEWS2_Sp02_Scale", **self.base_params
        )
        self.assertIn(result, ["Standard", "High"])

    def test_generate_clinically_coherent_value_news_pulse_type(self):
        result = generate_clinically_coherent_value(
            "NEWS_Pulse_Type", **self.base_params
        )
        self.assertIn(result, ["Regular", "Irregular", "Strong", "Weak"])

    def test_generate_clinically_coherent_value_news_pain_score_normal(self):
        result = generate_clinically_coherent_value(
            "NEWS_Pain_Score", **self.base_params
        )
        val = float(result)
        self.assertGreaterEqual(val, 0)
        self.assertLessEqual(val, 10)

    def test_generate_clinically_coherent_value_news_pain_score_high_temp(self):
        result = generate_clinically_coherent_value(
            "NEWS_Pain_Score", temp_celsius=39.0
        )
        val = float(result)
        self.assertGreaterEqual(val, 4)
        self.assertLessEqual(val, 10)

    def test_generate_clinically_coherent_value_news_oxygen_litres_normal(self):
        result = generate_clinically_coherent_value(
            "NEWS Oxygen Litres", **self.base_params
        )
        val = float(result)
        self.assertGreaterEqual(val, 0)
        self.assertLessEqual(val, 15)

    def test_generate_clinically_coherent_value_news_oxygen_litres_high_pain(self):
        result = generate_clinically_coherent_value("NEWS Oxygen Litres", pain_score=8)
        val = float(result)
        self.assertGreaterEqual(val, 5)
        self.assertLessEqual(val, 15)

    def test_generate_clinically_coherent_value_news_oxygen_delivery(self):
        result = generate_clinically_coherent_value(
            "NEWS Oxygen Delivery", **self.base_params
        )
        self.assertIn(
            result,
            [
                "None",
                "Nasal Prongs",
                "Face Mask",
                "Venturi Mask",
                "High Flow Nasal Cannula",
                "Non-Rebreather Mask",
            ],
        )

    def test_generate_clinically_coherent_value_news2_score(self):
        result = generate_clinically_coherent_value("NEWS2_Score", **self.base_params)
        val = float(result)
        self.assertGreaterEqual(val, 0)
        self.assertLessEqual(val, 15)

    def test_generate_clinically_coherent_value_unknown_component(self):
        result = generate_clinically_coherent_value(
            "UNKNOWN_COMPONENT", **self.base_params
        )
        val = float(result)
        self.assertGreaterEqual(val, 0)
        self.assertLessEqual(val, 15)


class TestNEWSCorrelation(unittest.TestCase):
    def test_news_correlation_respiration_rate_high(self):
        params = {"respiration_rate": 25}
        result = generate_clinically_coherent_value("NEWS_Systolic_BP", **params)
        val = float(result)
        self.assertGreaterEqual(val, 130)

    def test_news_correlation_heart_rate_high(self):
        params = {"heart_rate": 150}
        result = generate_clinically_coherent_value("NEWS_Systolic_BP", **params)
        val = float(result)
        self.assertGreaterEqual(val, 140)

    def test_news_correlation_temperature_high_resp(self):
        params = {"temp_celsius": 39.0}
        result = generate_clinically_coherent_value("NEWS_Respiration_Rate", **params)
        val = float(result)
        self.assertGreaterEqual(val, 20)


if __name__ == "__main__":
    unittest.main()
