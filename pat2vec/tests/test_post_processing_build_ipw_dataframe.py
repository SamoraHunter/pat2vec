import unittest
from unittest.mock import patch, Mock
import pandas as pd

# The function to be tested
from pat2vec.util.post_processing_build_ipw_dataframe import build_ipw_dataframe


class TestBuildIpwDataframe(unittest.TestCase):
    """Unit tests for the build_ipw_dataframe function."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock config object with a fake path
        self.mock_config = Mock()
        self.mock_config.verbosity = 0
        self.mock_config.pre_document_batch_path = "/fake/path/docs"

        # Sample IPW record data that get_pat_ipw_record might return
        self.ipw_record_p1 = pd.DataFrame(
            {
                "client_idcode": ["P001"],
                "updatetime": [pd.to_datetime("2022-01-01")],
                "cui": [101],
                "pretty_name": ["Condition A"],
            }
        )
        self.ipw_record_p2 = pd.DataFrame(
            {
                "client_idcode": ["P002"],
                "updatetime": [pd.to_datetime("2022-02-01")],
                "cui": [102],
                "pretty_name": ["Condition B"],
            }
        )

    @patch("pat2vec.util.post_processing_build_ipw_dataframe.get_pat_ipw_record")
    @patch("os.listdir")
    def test_basic_functionality(self, mock_listdir, mock_get_ipw_record):
        """Test the basic functionality of building the IPW dataframe."""
        # Arrange
        mock_listdir.return_value = ["P001.csv", "P002.csv", "not_a_csv.txt"]
        mock_get_ipw_record.side_effect = [self.ipw_record_p1, self.ipw_record_p2]

        # Act
        result_df = build_ipw_dataframe(config_obj=self.mock_config)

        # Assert
        self.assertEqual(mock_listdir.call_count, 1)
        self.assertEqual(mock_get_ipw_record.call_count, 2)
        self.assertEqual(len(result_df), 2)
        self.assertIn("P001", result_df["client_idcode"].values)
        self.assertIn("P002", result_df["client_idcode"].values)
        pd.testing.assert_frame_equal(
            result_df,
            pd.concat([self.ipw_record_p1, self.ipw_record_p2], ignore_index=True),
        )

    @patch("pat2vec.util.post_processing_build_ipw_dataframe.get_pat_ipw_record")
    @patch("os.listdir")
    def test_ignores_non_csv_files(self, mock_listdir, mock_get_ipw_record):
        """Test that non-CSV files in the directory are ignored."""
        # Arrange
        mock_listdir.return_value = [
            "P001.csv",
            "P002.txt",
            ".DS_Store",
            "P003.csv.bak",
        ]
        mock_get_ipw_record.return_value = self.ipw_record_p1

        # Act
        build_ipw_dataframe(config_obj=self.mock_config)

        # Assert
        mock_get_ipw_record.assert_called_once_with(
            current_pat_idcode="P001",
            annot_filter_arguments=None,
            filter_codes=None,
            config_obj=self.mock_config,
            mode="earliest",
            include_mct=True,
            include_textual_obs=True,
        )

    @patch("pat2vec.util.post_processing_build_ipw_dataframe.get_pat_ipw_record")
    @patch("os.listdir")
    def test_with_custom_pat_list(self, mock_listdir, mock_get_ipw_record):
        """Test that a custom patient list is used instead of os.listdir."""
        # Arrange
        custom_list = ["P001"]
        mock_get_ipw_record.return_value = self.ipw_record_p1

        # Act
        result_df = build_ipw_dataframe(
            config_obj=self.mock_config, custom_pat_list=custom_list
        )

        # Assert
        mock_listdir.assert_not_called()  # Should not be called when custom list is provided
        mock_get_ipw_record.assert_called_once_with(
            current_pat_idcode="P001",
            annot_filter_arguments=None,
            filter_codes=None,
            config_obj=self.mock_config,
            mode="earliest",
            include_mct=True,
            include_textual_obs=True,
        )
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["client_idcode"], "P001")

    @patch("pat2vec.util.post_processing_build_ipw_dataframe.get_pat_ipw_record")
    @patch("os.listdir")
    def test_no_patients_found(self, mock_listdir, mock_get_ipw_record):
        """Test behavior when no patient CSV files are found."""
        # Arrange
        mock_listdir.return_value = ["readme.txt"]  # No CSVs

        # Act
        result_df = build_ipw_dataframe(config_obj=self.mock_config)

        # Assert
        self.assertTrue(result_df.empty)
        mock_get_ipw_record.assert_not_called()

    @patch("pat2vec.util.post_processing_build_ipw_dataframe.get_pat_ipw_record")
    @patch("os.listdir")
    def test_get_pat_ipw_record_returns_empty(self, mock_listdir, mock_get_ipw_record):
        """Test when get_pat_ipw_record returns an empty DataFrame for a patient."""
        # Arrange
        mock_listdir.return_value = ["P001.csv", "P002.csv"]
        mock_get_ipw_record.side_effect = [
            self.ipw_record_p1,
            pd.DataFrame(),
        ]  # P002 has no record

        # Act
        result_df = build_ipw_dataframe(config_obj=self.mock_config)

        # Assert
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.iloc[0]["client_idcode"], "P001")

    @patch("pat2vec.util.post_processing_build_ipw_dataframe.get_pat_ipw_record")
    @patch("os.listdir")
    def test_column_fillna_logic(self, mock_listdir, mock_get_ipw_record):
        """Test the fillna logic for updatetime and observationdocument_recordeddtm."""
        # Arrange
        record_with_nan = pd.DataFrame(
            {
                "client_idcode": ["P003"],
                "updatetime": [None],
                "observationdocument_recordeddtm": [pd.to_datetime("2022-03-01")],
                "cui": [103],
            }
        )
        mock_listdir.return_value = ["P003.csv"]
        mock_get_ipw_record.return_value = record_with_nan

        # Act
        result_df = build_ipw_dataframe(config_obj=self.mock_config)

        # Assert
        self.assertEqual(len(result_df), 1)
        # updatetime should be filled from observationdocument_recordeddtm
        self.assertEqual(result_df.iloc[0]["updatetime"], pd.to_datetime("2022-03-01"))
        # observationdocument_recordeddtm should also be filled from updatetime (now that it's filled)
        self.assertEqual(
            result_df.iloc[0]["observationdocument_recordeddtm"],
            pd.to_datetime("2022-03-01"),
        )

    @patch("pat2vec.util.post_processing_build_ipw_dataframe.get_pat_ipw_record")
    @patch("os.listdir")
    def test_column_fillna_logic_reverse(self, mock_listdir, mock_get_ipw_record):
        """Test the fillna logic when updatetime exists but observationdocument_recordeddtm is NaN."""
        # Arrange
        record_with_nan = pd.DataFrame(
            {
                "client_idcode": ["P004"],
                "updatetime": [pd.to_datetime("2022-04-01")],
                "observationdocument_recordeddtm": [None],
                "cui": [104],
            }
        )
        mock_listdir.return_value = ["P004.csv"]
        mock_get_ipw_record.return_value = record_with_nan

        # Act
        result_df = build_ipw_dataframe(config_obj=self.mock_config)

        # Assert
        self.assertEqual(len(result_df), 1)
        # observationdocument_recordeddtm should be filled from updatetime
        self.assertEqual(
            result_df.iloc[0]["observationdocument_recordeddtm"],
            pd.to_datetime("2022-04-01"),
        )
        # updatetime should remain unchanged
        self.assertEqual(result_df.iloc[0]["updatetime"], pd.to_datetime("2022-04-01"))

    @patch("pat2vec.util.post_processing_build_ipw_dataframe.get_pat_ipw_record")
    @patch("os.listdir")
    def test_all_passthrough_arguments(self, mock_listdir, mock_get_ipw_record):
        """Test that all passthrough arguments are correctly forwarded."""
        # Arrange
        mock_listdir.return_value = ["P001.csv"]
        mock_get_ipw_record.return_value = self.ipw_record_p1
        test_annot_filters = {"acc": 0.95}
        test_filter_codes = [999, 888]

        # Act
        build_ipw_dataframe(
            config_obj=self.mock_config,
            annot_filter_arguments=test_annot_filters,
            filter_codes=test_filter_codes,
            include_mct=False,
            include_textual_obs=False,
        )

        # Assert
        mock_get_ipw_record.assert_called_once_with(
            current_pat_idcode="P001",
            annot_filter_arguments=test_annot_filters,
            filter_codes=test_filter_codes,
            config_obj=self.mock_config,
            mode="earliest",  # Default mode
            include_mct=False,
            include_textual_obs=False,
        )

    @patch("pat2vec.util.post_processing_build_ipw_dataframe.get_pat_ipw_record")
    @patch("os.listdir")
    def test_mode_handling_with_empty_results(self, mock_listdir, mock_get_ipw_record):
        """
        Test 'earliest' and 'latest' modes, including when a patient has no valid records.
        """
        # Arrange
        mock_listdir.return_value = ["P001.csv", "P002.csv", "P003.csv"]

        # P001 has a record, P002 has no valid record, P003 has a record.
        # This simulates a patient (P002) not meeting the conditions inside get_pat_ipw_record.
        p3_record = pd.DataFrame(
            {
                "client_idcode": ["P003"],
                "updatetime": [pd.to_datetime("2022-03-01")],
                "cui": [103],
                "pretty_name": ["Condition C"],
            }
        )

        # --- Scenario 1: mode='earliest' ---
        mock_get_ipw_record.side_effect = [
            self.ipw_record_p1,
            pd.DataFrame(),
            p3_record,
        ]

        # Act
        result_earliest = build_ipw_dataframe(
            config_obj=self.mock_config, mode="earliest"
        )

        # Assert
        self.assertEqual(
            mock_get_ipw_record.call_count,
            3,
            "get_pat_ipw_record should be called for all 3 patients",
        )
        calls = mock_get_ipw_record.call_args_list
        for call in calls:
            self.assertEqual(call.kwargs["mode"], "earliest")
        self.assertEqual(
            len(result_earliest), 2, "Should only contain records for P001 and P003"
        )
        expected_earliest = pd.concat(
            [self.ipw_record_p1, p3_record], ignore_index=True
        )
        pd.testing.assert_frame_equal(result_earliest, expected_earliest)

        # --- Scenario 2: mode='latest' ---
        mock_get_ipw_record.reset_mock()
        p1_latest_record = self.ipw_record_p1.copy()
        p1_latest_record["updatetime"] = pd.to_datetime("2022-12-01")
        p3_latest_record = p3_record.copy()
        p3_latest_record["updatetime"] = pd.to_datetime("2022-12-15")
        mock_get_ipw_record.side_effect = [
            p1_latest_record,
            pd.DataFrame(),
            p3_latest_record,
        ]

        # Act
        result_latest = build_ipw_dataframe(config_obj=self.mock_config, mode="latest")

        # Assert
        self.assertEqual(
            mock_get_ipw_record.call_count,
            3,
            "get_pat_ipw_record should be called for all 3 patients in latest mode",
        )
        calls_latest = mock_get_ipw_record.call_args_list
        for call in calls_latest:
            self.assertEqual(call.kwargs["mode"], "latest")
        self.assertEqual(
            len(result_latest),
            2,
            "Should only contain records for P001 and P003 in latest mode",
        )
        expected_latest = pd.concat(
            [p1_latest_record, p3_latest_record], ignore_index=True
        )
        pd.testing.assert_frame_equal(result_latest, expected_latest)


if __name__ == "__main__":
    unittest.main()
