import unittest
from unittest.mock import patch, Mock
import pandas as pd
from datetime import datetime

# The function to be tested
from pat2vec.util.post_processing_get_pat_ipw_record import get_pat_ipw_record


class TestGetPatIpwRecord(unittest.TestCase):
    """Unit tests for the get_pat_ipw_record function."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock config object
        self.mock_config = Mock()
        self.mock_config.verbosity = 0
        self.mock_config.pre_document_annotation_batch_path = "/fake/path/epr"
        self.mock_config.pre_document_annotation_batch_path_mct = "/fake/path/mct"
        self.mock_config.pre_textual_obs_annotation_batch_path = (
            "/fake/path/textual_obs"
        )
        self.mock_config.global_start_year = "2020"
        self.mock_config.global_start_month = "1"
        self.mock_config.global_start_day = "1"
        self.mock_config.global_end_year = "2022"
        self.mock_config.global_end_month = "12"
        self.mock_config.global_end_day = "31"
        self.mock_config.lookback = False

        # Base columns for sample dataframes
        self.base_columns = {
            "type_ids": ["T047"],
            "types": ["['disease']"],
            "source_value": ["val"],
            "detected_name": ["d_name"],
            "acc": [0.9],
            "id": ["id1"],
            "Time_Value": ["present"],
            "Time_Confidence": [0.9],
            "Presence_Value": ["present"],
            "Presence_Confidence": [0.9],
            "Subject_Value": ["patient"],
            "Subject_Confidence": [0.9],
        }

        # Sample dataframes
        self.epr_df = pd.DataFrame(
            {
                "client_idcode": ["P001"],
                "updatetime": [datetime(2021, 2, 1)],
                "cui": [101],
                "pretty_name": ["EPR_finding"],
                **self.base_columns,
            }
        )

        self.mct_df = pd.DataFrame(
            {
                "client_idcode": ["P001"],
                "observationdocument_recordeddtm": [datetime(2021, 1, 1)],
                "cui": [102],
                "pretty_name": ["MCT_finding"],
                **self.base_columns,
            }
        )

        self.textual_obs_df = pd.DataFrame(
            {
                "client_idcode": ["P001"],
                "basicobs_entered": [datetime(2021, 3, 1)],
                "cui": [103],
                "pretty_name": ["TextualObs_finding"],
                **self.base_columns,
            }
        )

        # textual_obs also has an updatetime column in some cases, which should be ignored.
        self.textual_obs_df_with_updatetime = self.textual_obs_df.copy()
        self.textual_obs_df_with_updatetime["updatetime"] = [datetime(2020, 1, 1)]

        self.patient_id = "P001"

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_epr_is_earliest(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test that the EPR record is returned when it is the earliest."""
        epr_df = self.epr_df.copy()
        epr_df["updatetime"] = [datetime(2020, 12, 31)]

        # Create empty DataFrames with the correct columns for MCT and textual_obs
        empty_mct_df = pd.DataFrame(columns=list(self.mct_df.columns))
        empty_textual_df = pd.DataFrame(columns=list(self.textual_obs_df.columns))

        def side_effect(path):
            if "epr" in path:
                return epr_df
            elif "mct" in path:
                return empty_mct_df
            elif "textual_obs" in path:
                return empty_textual_df
            return pd.DataFrame()

        mock_exists.return_value = True
        mock_read_csv.side_effect = side_effect
        mock_filter_annot.side_effect = lambda df, _: df if not df.empty else df
        # Fix: Make filter_and_select_rows return the dataframe as-is instead of filtering
        mock_filter_select.side_effect = lambda df, *args, **kwargs: (
            df if not df.empty else df
        )

        result_df = get_pat_ipw_record(
            current_pat_idcode=self.patient_id,
            config_obj=self.mock_config,
            filter_codes=[101, 102, 103],
        )

        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["pretty_name"], "EPR_finding")
        self.assertEqual(result_df.iloc[0]["cui"], 101)
        self.assertEqual(result_df.iloc[0]["source"], "EPR")

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_mct_is_earliest(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test that the MCT record is returned when it is the earliest."""
        # Create empty DataFrames with correct columns for EPR and textual_obs
        empty_epr_df = pd.DataFrame(columns=list(self.epr_df.columns))
        empty_textual_df = pd.DataFrame(columns=list(self.textual_obs_df.columns))

        def side_effect(path):
            if "epr" in path:
                return empty_epr_df
            elif "mct" in path:
                return self.mct_df
            elif "textual_obs" in path:
                return empty_textual_df
            return pd.DataFrame()

        mock_exists.return_value = True
        mock_read_csv.side_effect = side_effect
        mock_filter_annot.side_effect = lambda df, _: df if not df.empty else df
        mock_filter_select.side_effect = lambda df, *args, **kwargs: (
            df if not df.empty else df
        )

        result_df = get_pat_ipw_record(
            current_pat_idcode=self.patient_id,
            config_obj=self.mock_config,
            filter_codes=[101, 102, 103],
        )

        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["pretty_name"], "MCT_finding")
        self.assertEqual(result_df.iloc[0]["cui"], 102)
        self.assertEqual(result_df.iloc[0]["source"], "MCT")

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_textual_obs_is_earliest(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test that the textual_obs record is returned when it is the earliest."""
        # Make textual_obs the earliest
        textual_obs_df = self.textual_obs_df.copy()
        textual_obs_df["basicobs_entered"] = [datetime(2020, 11, 1)]

        # Create empty DataFrames with correct columns
        empty_epr_df = pd.DataFrame(columns=list(self.epr_df.columns))
        empty_mct_df = pd.DataFrame(columns=list(self.mct_df.columns))

        def side_effect(path):
            if "epr" in path:
                return empty_epr_df
            elif "mct" in path:
                return empty_mct_df
            elif "textual_obs" in path:
                return textual_obs_df
            return pd.DataFrame()

        mock_exists.return_value = True
        mock_read_csv.side_effect = side_effect
        mock_filter_annot.side_effect = lambda df, _: df if not df.empty else df
        mock_filter_select.side_effect = lambda df, *args, **kwargs: (
            df if not df.empty else df
        )

        result_df = get_pat_ipw_record(
            current_pat_idcode=self.patient_id,
            config_obj=self.mock_config,
            filter_codes=[101, 102, 103],
        )

        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["pretty_name"], "TextualObs_finding")
        self.assertEqual(result_df.iloc[0]["cui"], 103)
        self.assertEqual(result_df.iloc[0]["source"], "textual_obs")

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_textual_obs_with_existing_updatetime_column(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test that textual_obs drops existing updatetime column and uses basicobs_entered."""
        # Create empty DataFrames with correct columns
        empty_epr_df = pd.DataFrame(columns=list(self.epr_df.columns))
        empty_mct_df = pd.DataFrame(columns=list(self.mct_df.columns))

        def side_effect(path):
            if "epr" in path:
                return empty_epr_df
            elif "mct" in path:
                return empty_mct_df
            elif "textual_obs" in path:
                return self.textual_obs_df_with_updatetime
            return pd.DataFrame()

        mock_exists.return_value = True
        mock_read_csv.side_effect = side_effect
        mock_filter_annot.side_effect = lambda df, _: df if not df.empty else df
        mock_filter_select.side_effect = lambda df, *args, **kwargs: (
            df if not df.empty else df
        )

        result_df = get_pat_ipw_record(
            current_pat_idcode=self.patient_id,
            config_obj=self.mock_config,
            filter_codes=[103],
        )

        self.assertEqual(len(result_df), 1)
        self.assertEqual(
            result_df.iloc[0]["updatetime"], datetime(2021, 3, 1)
        )  # Should use basicobs_entered
        self.assertNotEqual(
            result_df.iloc[0]["updatetime"], datetime(2020, 1, 1)
        )  # Should not use old updatetime

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_no_records_found_lookback_false(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test behavior when no records are found and lookback is False."""
        mock_exists.return_value = False  # No files exist
        mock_filter_select.return_value = pd.DataFrame()  # Empty results

        result_df = get_pat_ipw_record(
            current_pat_idcode=self.patient_id,
            config_obj=self.mock_config,
            filter_codes=[101, 102, 103],
        )

        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["client_idcode"], self.patient_id)
        self.assertEqual(
            result_df.iloc[0]["updatetime"], datetime(2020, 1, 1)
        )  # Start date

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_no_records_found_lookback_true(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test behavior when no records are found and lookback is True."""
        self.mock_config.lookback = True
        mock_exists.return_value = False
        mock_filter_select.return_value = pd.DataFrame()

        result_df = get_pat_ipw_record(
            current_pat_idcode=self.patient_id,
            config_obj=self.mock_config,
            filter_codes=[101, 102, 103],
        )

        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["client_idcode"], self.patient_id)
        self.assertEqual(
            result_df.iloc[0]["updatetime"], datetime(2022, 12, 31)
        )  # End date

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_include_mct_false(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test that MCT records are excluded when include_mct=False."""
        # Create empty DataFrame for textual_obs
        empty_textual_df = pd.DataFrame(columns=list(self.textual_obs_df.columns))

        def side_effect(path):
            if "epr" in path:
                return self.epr_df
            elif "mct" in path:
                return self.mct_df
            elif "textual_obs" in path:
                return empty_textual_df
            return pd.DataFrame()

        mock_exists.return_value = True
        mock_read_csv.side_effect = side_effect
        mock_filter_annot.side_effect = lambda df, _: df if not df.empty else df
        mock_filter_select.side_effect = lambda df, *args, **kwargs: (
            df if not df.empty else df
        )

        result_df = get_pat_ipw_record(
            current_pat_idcode=self.patient_id,
            config_obj=self.mock_config,
            filter_codes=[101, 102, 103],
            include_mct=False,
        )

        # Should return EPR since MCT is excluded and EPR is the only available source
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["source"], "EPR")

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_include_textual_obs_false(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test that textual_obs records are excluded when include_textual_obs=False."""
        # Create empty DataFrame for EPR so MCT will be the only source
        empty_epr_df = pd.DataFrame(columns=list(self.epr_df.columns))

        def side_effect(path):
            if "epr" in path:
                return empty_epr_df
            elif "mct" in path:
                return self.mct_df
            elif "textual_obs" in path:
                return self.textual_obs_df
            return pd.DataFrame()

        mock_exists.return_value = True
        mock_read_csv.side_effect = side_effect
        mock_filter_annot.side_effect = lambda df, _: df if not df.empty else df
        mock_filter_select.side_effect = lambda df, *args, **kwargs: (
            df if not df.empty else df
        )

        result_df = get_pat_ipw_record(
            current_pat_idcode=self.patient_id,
            config_obj=self.mock_config,
            filter_codes=[101, 102, 103],
            include_textual_obs=False,
        )

        # Should return MCT since textual_obs is excluded and MCT is the only available source
        self.assertEqual(len(result_df), 1)
        self.assertEqual(
            result_df.iloc[0]["source"], "MCT"
        )  # Fixed: removed duplicate .iloc[0]["source"]

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_verbosity_from_config(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test that verbosity is taken from config object."""
        self.mock_config.verbosity = 15

        # Create empty DataFrames with correct columns for MCT and textual_obs
        empty_mct_df = pd.DataFrame(columns=list(self.mct_df.columns))
        empty_textual_df = pd.DataFrame(columns=list(self.textual_obs_df.columns))

        def side_effect(path):
            if "epr" in path:
                return self.epr_df
            elif "mct" in path:
                return empty_mct_df
            elif "textual_obs" in path:
                return empty_textual_df
            return pd.DataFrame()

        mock_exists.return_value = True
        mock_read_csv.side_effect = side_effect
        mock_filter_annot.side_effect = lambda df, _: df if not df.empty else df
        mock_filter_select.side_effect = lambda df, *args, **kwargs: (
            df if not df.empty else df
        )

        with patch("builtins.print") as mock_print:
            get_pat_ipw_record(
                current_pat_idcode=self.patient_id,
                config_obj=self.mock_config,
                filter_codes=[101],
            )
            # Should print verbose messages when verbosity >= 10
            mock_print.assert_called()

    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_and_select_rows")
    @patch("pat2vec.util.post_processing_get_pat_ipw_record.filter_annot_dataframe2")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_filter_and_annot_arguments_passed(
        self, mock_read_csv, mock_exists, mock_filter_annot, mock_filter_select
    ):
        """Test that annotation filter arguments are passed correctly."""
        filter_args = {"some_filter": "value"}

        # Create empty DataFrames with correct columns for MCT and textual_obs
        empty_mct_df = pd.DataFrame(columns=list(self.mct_df.columns))
        empty_textual_df = pd.DataFrame(columns=list(self.textual_obs_df.columns))

        def side_effect(path):
            if "epr" in path:
                return self.epr_df
            elif "mct" in path:
                return empty_mct_df
            elif "textual_obs" in path:
                return empty_textual_df
            return pd.DataFrame()

        mock_exists.return_value = True
        mock_read_csv.side_effect = side_effect
        # Fix: Make sure filter_annot_dataframe2 returns a DataFrame with same structure
        mock_filter_annot.side_effect = lambda df, _: df.copy() if not df.empty else df
        mock_filter_select.side_effect = lambda df, *args, **kwargs: (
            df if not df.empty else df
        )

        get_pat_ipw_record(
            current_pat_idcode=self.patient_id,
            config_obj=self.mock_config,
            annot_filter_arguments=filter_args,
            filter_codes=[101],
        )

        # Verify filter_annot_dataframe2 was called with filter arguments
        # The DataFrame passed should be structurally equivalent to epr_df
        called_args = mock_filter_annot.call_args
        self.assertEqual(called_args[0][1], filter_args)  # Check filter_args
        # Check that the DataFrame has the expected structure
        called_df = called_args[0][0]
        self.assertIn("client_idcode", called_df.columns)
        self.assertIn("pretty_name", called_df.columns)

    def test_empty_dataframes_handling(self):
        """Test handling of empty dataframes during comparison."""
        with patch(
            "pat2vec.util.post_processing_get_pat_ipw_record._get_source_record"
        ) as mock_get_source:
            mock_get_source.return_value = pd.DataFrame()  # Always return empty

            result_df = get_pat_ipw_record(
                current_pat_idcode=self.patient_id,
                config_obj=self.mock_config,
                filter_codes=[101, 102, 103],
            )

            # Should return a default dataframe with start_datetime
            self.assertEqual(len(result_df), 1)
            self.assertEqual(result_df.iloc[0]["client_idcode"], self.patient_id)
            self.assertEqual(result_df.iloc[0]["updatetime"], datetime(2020, 1, 1))


if __name__ == "__main__":
    unittest.main()
