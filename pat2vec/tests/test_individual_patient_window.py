import unittest
from unittest.mock import patch, Mock
from datetime import datetime
import pandas as pd
import numpy as np

from pat2vec.util.config_pat2vec import config_class


class TestIndividualPatientWindow(unittest.TestCase):
    """Test suite for individual patient window functionality."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.base_start_date = datetime(2020, 1, 1)

        # Create patches for commonly used functions
        self.patcher_paths_class = patch(
            "pat2vec.util.current_pat_batch_path_methods.PathsClass"
        )

        # Start only the PathsClass patch globally since it's used in constructor
        self.mock_paths_class = self.patcher_paths_class.start()
        self.mock_paths_class.return_value = Mock()

    def tearDown(self):
        """Clean up patches after each test method."""
        self.patcher_paths_class.stop()

    def test_config_with_individual_patient_window_false(self):
        """Test config initialization with individual_patient_window=False."""
        with patch("builtins.print"):  # Suppress print output
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=False,
                testing=True,
            )

        self.assertFalse(config.individual_patient_window)
        self.assertIsNotNone(config.date_list)
        self.assertIsNotNone(config.n_pat_lines)
        # patient_dict should not exist or be None when individual_patient_window=False
        self.assertFalse(
            hasattr(config, "patient_dict") and config.patient_dict is not None
        )

    # FIX: Patched the functions where they are looked up (in config_pat2vec), not where they are defined.
    @patch("pat2vec.util.config_pat2vec.build_patient_dict")
    @patch("pat2vec.util.config_pat2vec.add_offset_column")
    def test_config_with_individual_patient_window_true(
        self, mock_add_offset, mock_build_dict
    ):
        """Test config initialization with individual_patient_window=True."""
        # Create mock dataframe
        mock_df = pd.DataFrame(
            {"patient_id": ["P001", "P002"], "start_date": ["2020-01-01", "2020-02-01"]}
        )

        # Make sure the returned DataFrame has the expected columns
        mock_df_with_offset = mock_df.copy()
        # The build_patient_dict function expects 'start_date_converted' column
        mock_df_with_offset["start_date_converted"] = pd.to_datetime(
            mock_df["start_date"]
        )
        mock_df_with_offset["start_date_offset"] = pd.to_datetime(
            ["2019-01-01", "2019-02-01"]
        )

        mock_add_offset.return_value = mock_df_with_offset
        mock_build_dict.return_value = {
            "P001": (datetime(2020, 1, 1), datetime(2021, 1, 1)),
            "P002": (datetime(2020, 2, 1), datetime(2021, 2, 1)),
        }

        with patch("builtins.print"):
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        self.assertTrue(config.individual_patient_window)
        self.assertIsNone(config.date_list)
        self.assertIsNone(config.n_pat_lines)
        self.assertIsNotNone(config.patient_dict)

    def test_lookback_with_individual_patient_window(self):
        """Test lookback behavior with individual patient window."""
        mock_df = pd.DataFrame(
            {"patient_id": ["P001", "P002"], "start_date": ["2020-01-01", "2020-02-01"]}
        )

        # FIX: Patched the functions where they are looked up (in config_pat2vec), not where they are defined.
        with patch(
            "pat2vec.util.config_pat2vec.add_offset_column"
        ) as mock_add_offset, patch(
            "pat2vec.util.config_pat2vec.build_patient_dict"
        ) as mock_build_dict, patch(
            "builtins.print"
        ):  # Suppress print output

            mock_df_with_offset = mock_df.copy()
            mock_df_with_offset["start_date_converted"] = pd.to_datetime(
                mock_df["start_date"]
            )
            mock_df_with_offset["start_date_offset"] = pd.to_datetime(
                ["2021-01-01", "2021-02-01"]
            )
            mock_add_offset.return_value = mock_df_with_offset

            original_dict = {
                "P001": (datetime(2020, 1, 1), datetime(2021, 1, 1)),
                "P002": (datetime(2020, 2, 1), datetime(2021, 2, 1)),
            }
            mock_build_dict.return_value = original_dict

            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                lookback=True,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

            # When lookback=True, patient_dict values should be reversed
            for key, value in config.patient_dict.items():
                original_value = original_dict[key]
                expected_reversed = tuple(reversed(original_value))
                self.assertEqual(value, expected_reversed)

    @patch("pat2vec.util.config_pat2vec.build_patient_dict")
    @patch("pat2vec.util.config_pat2vec.add_offset_column")
    def test_updatetime_nan_handling_with_individual_patient_window(
        self, mock_add_offset, mock_build_dict
    ):
        """Test handling of NaN values in updatetime field with individual patient window."""
        # Create mock dataframe with various updatetime scenarios
        mock_df = pd.DataFrame(
            {
                "patient_id": ["P001", "P002", "P003", "P004"],
                "start_date": ["2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01"],
                "updatetime": [
                    "2020-01-15 10:30:00",  # Valid datetime string
                    None,  # None value (should become NaN)
                    "",  # Empty string
                    "2020-04-15 14:20:00",  # Valid datetime string
                ],
            }
        )

        # Mock the add_offset_column function to simulate realistic behavior
        mock_df_with_offset = mock_df.copy()

        # Convert start_date to datetime
        mock_df_with_offset["start_date_converted"] = pd.to_datetime(
            mock_df["start_date"]
        )

        # Add offset dates
        mock_df_with_offset["start_date_offset"] = pd.to_datetime(
            ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01"]
        )

        # Process updatetime field - this is where NaNs might be introduced
        mock_df_with_offset["updatetime_converted"] = pd.to_datetime(
            mock_df["updatetime"],
            errors="coerce",  # This will create NaT for invalid dates
        )

        mock_add_offset.return_value = mock_df_with_offset

        # Mock build_patient_dict return
        mock_build_dict.return_value = {
            "P001": (datetime(2020, 1, 1), datetime(2021, 1, 1)),
            "P002": (datetime(2020, 2, 1), datetime(2021, 2, 1)),
            "P003": (datetime(2020, 3, 1), datetime(2021, 3, 1)),
            "P004": (datetime(2020, 4, 1), datetime(2021, 4, 1)),
        }

        with patch("builtins.print"):
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        # Verify the mocked functions were called
        mock_add_offset.assert_called_once()
        mock_build_dict.assert_called_once()

        # Check that the dataframe passed to add_offset_column had the expected structure
        called_df = mock_add_offset.call_args[0][0]  # First positional argument

        # Debug: Print the actual dataframe structure and values
        print(f"\nDataFrame passed to add_offset_column:")
        print(f"Columns: {called_df.columns.tolist()}")
        print(f"UpdateTime values: {called_df['updatetime'].tolist()}")
        print(f"UpdateTime dtypes: {called_df['updatetime'].dtype}")

        # Check for NaN/None values in updatetime
        updatetime_nulls = called_df["updatetime"].isnull().sum()
        print(f"Number of null/NaN updatetime values: {updatetime_nulls}")

        # Check the output dataframe from add_offset_column
        output_df = mock_add_offset.return_value
        if "updatetime_converted" in output_df.columns:
            updatetime_converted_nulls = (
                output_df["updatetime_converted"].isnull().sum()
            )
            print(
                f"Number of null/NaN updatetime_converted values: {updatetime_converted_nulls}"
            )

        # Assertions to catch the NaN issue
        self.assertTrue(config.individual_patient_window)
        self.assertIsNotNone(config.patient_dict)

        # Verify that None and empty string values are properly handled
        self.assertIn(None, called_df["updatetime"].values)  # Should contain None
        self.assertIn("", called_df["updatetime"].values)  # Should contain empty string

    @patch("pat2vec.util.config_pat2vec.build_patient_dict")
    @patch("pat2vec.util.config_pat2vec.add_offset_column")
    def test_debug_add_offset_column_signature(self, mock_add_offset, mock_build_dict):
        """Debug test to understand the exact signature and behavior of add_offset_column."""

        mock_df = pd.DataFrame(
            {
                "patient_id": ["P001", "P002"],
                "start_date": ["2020-01-01", "2020-02-01"],
                "updatetime": ["2020-01-15 10:30:00", "2020-02-15 11:45:00"],
            }
        )

        # Create a side effect that captures ALL arguments passed to add_offset_column
        captured_call_info = {}

        def capture_add_offset_call(*args, **kwargs):
            """Capture exactly what arguments are passed to add_offset_column."""
            captured_call_info["args"] = args
            captured_call_info["kwargs"] = kwargs
            captured_call_info["num_args"] = len(args)
            captured_call_info["num_kwargs"] = len(kwargs)

            print(f"\n=== ADD_OFFSET_COLUMN CALL CAPTURED ===")
            print(f"Number of positional args: {len(args)}")
            print(f"Number of keyword args: {len(kwargs)}")

            print(f"\nPositional arguments:")
            for i, arg in enumerate(args):
                if isinstance(arg, pd.DataFrame):
                    print(
                        f"  Arg {i}: DataFrame(shape={arg.shape}, columns={arg.columns.tolist()})"
                    )
                    if "updatetime" in arg.columns:
                        print(f"    UpdateTime values: {arg['updatetime'].tolist()}")
                        print(
                            f"    UpdateTime nulls: {arg['updatetime'].isnull().sum()}"
                        )
                else:
                    print(f"  Arg {i}: {repr(arg)} (type: {type(arg)})")

            print(f"\nKeyword arguments:")
            for key, value in kwargs.items():
                print(f"  {key}: {repr(value)} (type: {type(value)})")

            # Return a realistic dataframe
            if args and isinstance(args[0], pd.DataFrame):
                df = args[0].copy()

                # Add the expected output columns
                df["start_date_converted"] = pd.to_datetime(df["start_date"])
                df["start_date_offset"] = df["start_date_converted"] - pd.DateOffset(
                    years=1
                )

                print(f"\nReturning DataFrame with columns: {df.columns.tolist()}")
                if "updatetime" in df.columns:
                    print(f"UpdateTime preserved: {df['updatetime'].tolist()}")
                    print(
                        f"UpdateTime nulls in output: {df['updatetime'].isnull().sum()}"
                    )

                return df
            else:
                # Return empty dataframe if no valid input
                return pd.DataFrame()

        mock_add_offset.side_effect = capture_add_offset_call

        mock_build_dict.return_value = {
            "P001": (datetime(2020, 1, 1), datetime(2021, 1, 1)),
            "P002": (datetime(2020, 2, 1), datetime(2021, 2, 1)),
        }

        print("\n" + "=" * 60)
        print("DEBUGGING add_offset_column FUNCTION SIGNATURE")
        print("=" * 60)

        with patch("builtins.print"):
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        # Analyze the captured call
        print(f"\n=== CALL ANALYSIS ===")
        print(f"Function was called: {mock_add_offset.called}")
        print(f"Call count: {mock_add_offset.call_count}")

        if captured_call_info:
            print(f"Arguments captured: {len(captured_call_info.get('args', []))}")
            print(
                f"Keyword arguments captured: {len(captured_call_info.get('kwargs', {}))}"
            )

        # Verify basic functionality
        self.assertTrue(mock_add_offset.called, "add_offset_column should be called")
        self.assertTrue(config.individual_patient_window)
        self.assertIsNotNone(config.patient_dict)

    @patch("pat2vec.util.config_pat2vec.build_patient_dict")
    @patch("pat2vec.util.config_pat2vec.add_offset_column")
    def test_updatetime_nan_issue_focused_debug(self, mock_add_offset, mock_build_dict):
        """Focused test to debug the specific updatetime NaN issue."""

        # Create test data with potential problematic updatetime values
        mock_df = pd.DataFrame(
            {
                "patient_id": ["P001", "P002", "P003", "P004", "P005"],
                "start_date": [
                    "2020-01-01",
                    "2020-02-01",
                    "2020-03-01",
                    "2020-04-01",
                    "2020-05-01",
                ],
                "updatetime": [
                    "2020-01-15 10:30:00",  # Valid datetime
                    None,  # None
                    "",  # Empty string
                    "Invalid Date String",  # Invalid date
                    "2020-05-15 14:20:00",  # Valid datetime
                ],
            }
        )

        # Store what actually gets processed
        call_data = {}

        def debug_side_effect(*args, **kwargs):
            """Debug side effect that captures call details and processes data."""

            # Store call information
            call_data["args"] = args
            call_data["kwargs"] = kwargs

            print(f"\n=== add_offset_column DEBUG ===")
            print(f"Called with {len(args)} args and {len(kwargs)} kwargs")

            # Process the first argument as the dataframe
            if args and isinstance(args[0], pd.DataFrame):
                input_df = args[0]
                print(f"\nInput DataFrame:")
                print(f"  Shape: {input_df.shape}")
                print(f"  Columns: {input_df.columns.tolist()}")

                if "updatetime" in input_df.columns:
                    print(f"\nUpdateTime Analysis:")
                    print(f"  Data type: {input_df['updatetime'].dtype}")
                    print(f"  Null count: {input_df['updatetime'].isnull().sum()}")
                    print(f"  Values:")
                    for i, val in enumerate(input_df["updatetime"]):
                        is_null = pd.isnull(val)
                        print(
                            f"    [{i}] {repr(val)} (null: {is_null}, type: {type(val)})"
                        )

                # Create output dataframe
                output_df = input_df.copy()
                output_df["start_date_converted"] = pd.to_datetime(
                    input_df["start_date"]
                )
                output_df["start_date_offset"] = output_df[
                    "start_date_converted"
                ] - pd.DateOffset(years=1)

                # Test datetime conversion on updatetime (potential source of NaNs)
                if "updatetime" in input_df.columns:
                    print(f"\nTesting datetime conversion on updatetime...")

                    # This is likely what the real function does that might introduce NaNs
                    converted_updatetime = pd.to_datetime(
                        input_df["updatetime"], errors="coerce"
                    )

                    print(f"After pd.to_datetime(errors='coerce'):")
                    print(f"  Null count: {converted_updatetime.isnull().sum()}")
                    print(f"  Values:")
                    for i, val in enumerate(converted_updatetime):
                        is_null = pd.isnull(val)
                        print(f"    [{i}] {repr(val)} (null: {is_null})")

                    # Check which original values became NaN
                    original_not_null = input_df["updatetime"].notna()
                    converted_is_null = converted_updatetime.isna()
                    newly_null = original_not_null & converted_is_null

                    if newly_null.any():
                        print(f"\nWARNING: These values became NaN during conversion:")
                        for idx in newly_null[newly_null].index:
                            orig_val = input_df["updatetime"].iloc[idx]
                            print(f"  Row {idx}: {repr(orig_val)} -> NaT")

                    # Add the converted column to output (this might be what's happening)
                    output_df["updatetime_converted"] = converted_updatetime

                call_data["output_df"] = output_df
                return output_df

            return pd.DataFrame()  # Return empty if no valid input

        mock_add_offset.side_effect = debug_side_effect

        mock_build_dict.return_value = {
            "P001": (datetime(2020, 1, 1), datetime(2021, 1, 1)),
            "P002": (datetime(2020, 2, 1), datetime(2021, 2, 1)),
            "P003": (datetime(2020, 3, 1), datetime(2021, 3, 1)),
            "P004": (datetime(2020, 4, 1), datetime(2021, 4, 1)),
            "P005": (datetime(2020, 5, 1), datetime(2021, 5, 1)),
        }

        print("\n" + "=" * 60)
        print("FOCUSED UPDATETIME NaN DEBUG")
        print("=" * 60)

        with patch("builtins.print"):
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        # Analyze the results
        self.assertTrue(mock_add_offset.called, "add_offset_column should be called")
        self.assertTrue(config.individual_patient_window)

        if "output_df" in call_data:
            output_df = call_data["output_df"]

            print(f"\n=== FINAL ANALYSIS ===")
            print(f"Output DataFrame shape: {output_df.shape}")
            print(f"Output DataFrame columns: {output_df.columns.tolist()}")

            # Check if original updatetime was preserved
            if "updatetime" in output_df.columns:
                original_nulls = mock_df["updatetime"].isnull().sum()
                output_nulls = output_df["updatetime"].isnull().sum()

                print(f"Original updatetime nulls: {original_nulls}")
                print(f"Output updatetime nulls: {output_nulls}")

    @patch("pat2vec.util.config_pat2vec.build_patient_dict")
    @patch("pat2vec.util.config_pat2vec.add_offset_column")
    def test_non_iso_datetime_formats_causing_nans(
        self, mock_add_offset, mock_build_dict
    ):
        """Test to reproduce and debug NaN issues caused by non-ISO datetime formats."""

        # Create test data with various non-ISO datetime formats that might exist in your live data
        mock_df = pd.DataFrame(
            {
                "patient_id": [
                    "P001",
                    "P002",
                    "P003",
                    "P004",
                    "P005",
                    "P006",
                    "P007",
                    "P008",
                ],
                "start_date": [
                    "2020-01-01",
                    "2020-02-01",
                    "2020-03-01",
                    "2020-04-01",
                    "2020-05-01",
                    "2020-06-01",
                    "2020-07-01",
                    "2020-08-01",
                ],
                "updatetime": [
                    "2020-01-15 10:30:00",  # ISO-like format (should work)
                    "15/01/2020 10:30:00",  # DD/MM/YYYY format (might fail)
                    "01-15-2020 10:30:00",  # MM-DD-YYYY format (might fail)
                    "2020/01/15 10:30:00",  # YYYY/MM/DD format (might work)
                    "15-Jan-2020 10:30:00",  # DD-Mon-YYYY format (might fail)
                    "Jan 15, 2020 10:30:00",  # Month DD, YYYY format (might work)
                    "2020-01-15T10:30:00",  # ISO format (should work)
                    "15.01.2020 10:30:00",  # DD.MM.YYYY format (might fail)
                ],
            }
        )

        def test_datetime_conversion_side_effect(*args, **kwargs):
            """Test different datetime parsing strategies to identify problematic formats."""

            if args and isinstance(args[0], pd.DataFrame):
                input_df = args[0]
                output_df = input_df.copy()

                print(f"\n=== TESTING NON-ISO DATETIME FORMATS ===")
                print(f"Input DataFrame shape: {input_df.shape}")

                if "updatetime" in input_df.columns:
                    updatetime_values = input_df["updatetime"]

                    print(f"\nOriginal updatetime values:")
                    for i, val in enumerate(updatetime_values):
                        print(f"  [{i}] {repr(val)}")

                    # Test 1: Default pd.to_datetime (strict parsing)
                    print(f"\n--- Test 1: Default pd.to_datetime() ---")
                    try:
                        converted_strict = pd.to_datetime(updatetime_values)
                        strict_nulls = converted_strict.isnull().sum()
                        print(f"Strict parsing - NaN count: {strict_nulls}")
                        if strict_nulls > 0:
                            print("Values that became NaN with strict parsing:")
                            for i, (orig, conv) in enumerate(
                                zip(updatetime_values, converted_strict)
                            ):
                                if pd.isnull(conv) and pd.notnull(orig):
                                    print(f"  [{i}] {repr(orig)} -> NaT")
                    except Exception as e:
                        print(f"Strict parsing failed: {e}")
                        converted_strict = None

                    # Test 2: pd.to_datetime with errors='coerce' (lenient parsing)
                    print(f"\n--- Test 2: pd.to_datetime(errors='coerce') ---")
                    converted_coerce = pd.to_datetime(
                        updatetime_values, errors="coerce"
                    )
                    coerce_nulls = converted_coerce.isnull().sum()
                    print(f"Coerce parsing - NaN count: {coerce_nulls}")
                    if coerce_nulls > 0:
                        print("Values that became NaN with coerce parsing:")
                        for i, (orig, conv) in enumerate(
                            zip(updatetime_values, converted_coerce)
                        ):
                            if pd.isnull(conv) and pd.notnull(orig):
                                print(f"  [{i}] {repr(orig)} -> NaT")

                    # Test 3: pd.to_datetime with infer_datetime_format=True
                    print(
                        f"\n--- Test 3: pd.to_datetime(infer_datetime_format=True) ---"
                    )
                    try:
                        converted_infer = pd.to_datetime(
                            updatetime_values,
                            infer_datetime_format=True,
                            errors="coerce",
                        )
                        infer_nulls = converted_infer.isnull().sum()
                        print(f"Infer format parsing - NaN count: {infer_nulls}")
                        if infer_nulls > 0:
                            print("Values that became NaN with infer format parsing:")
                            for i, (orig, conv) in enumerate(
                                zip(updatetime_values, converted_infer)
                            ):
                                if pd.isnull(conv) and pd.notnull(orig):
                                    print(f"  [{i}] {repr(orig)} -> NaT")
                    except Exception as e:
                        print(f"Infer format parsing failed: {e}")
                        converted_infer = None

                    # Test 4: Multiple format attempts
                    print(f"\n--- Test 4: Multiple format attempts ---")
                    formats_to_try = [
                        "%Y-%m-%d %H:%M:%S",  # ISO-like
                        "%d/%m/%Y %H:%M:%S",  # DD/MM/YYYY
                        "%m-%d-%Y %H:%M:%S",  # MM-DD-YYYY
                        "%Y/%m/%d %H:%M:%S",  # YYYY/MM/DD
                        "%d-%b-%Y %H:%M:%S",  # DD-Mon-YYYY
                        "%b %d, %Y %H:%M:%S",  # Mon DD, YYYY
                        "%Y-%m-%dT%H:%M:%S",  # ISO format
                        "%d.%m.%Y %H:%M:%S",  # DD.MM.YYYY
                    ]

                    successful_formats = {}
                    for fmt in formats_to_try:
                        try:
                            converted_fmt = pd.to_datetime(
                                updatetime_values, format=fmt, errors="coerce"
                            )
                            fmt_nulls = converted_fmt.isnull().sum()
                            successful_formats[fmt] = fmt_nulls
                            print(
                                f"Format {fmt}: {len(updatetime_values) - fmt_nulls} successful, {fmt_nulls} NaN"
                            )
                        except Exception as e:
                            print(f"Format {fmt}: Failed - {e}")

                    # Find the best format
                    if successful_formats:
                        best_format = min(
                            successful_formats, key=successful_formats.get
                        )
                        print(
                            f"\nBest format: {best_format} (fewest NaNs: {successful_formats[best_format]})"
                        )

                # Add required columns for the mock
                output_df["start_date_converted"] = pd.to_datetime(
                    input_df["start_date"]
                )
                output_df["start_date_offset"] = output_df[
                    "start_date_converted"
                ] - pd.DateOffset(years=1)

                # Simulate what might be happening in the real function
                if "updatetime" in input_df.columns:
                    # This might be the problematic line in your real code
                    output_df["updatetime_converted"] = pd.to_datetime(
                        input_df["updatetime"], errors="coerce"
                    )

                return output_df

            return pd.DataFrame()

        mock_add_offset.side_effect = test_datetime_conversion_side_effect

        mock_build_dict.return_value = {
            f"P{str(i).zfill(3)}": (datetime(2020, i, 1), datetime(2021, i, 1))
            for i in range(1, 9)
        }

        print("\n" + "=" * 70)
        print("TESTING NON-ISO DATETIME FORMATS CAUSING NANs")
        print("=" * 70)

        with patch("builtins.print"):
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        # Verify the test completed
        self.assertTrue(mock_add_offset.called)
        self.assertTrue(config.individual_patient_window)

        print(f"\n=== TEST COMPLETED ===")
        print(
            "Check the output above to identify which datetime formats are causing NaNs"
        )

    @patch("pat2vec.util.config_pat2vec.build_patient_dict")
    @patch("pat2vec.util.config_pat2vec.add_offset_column")
    def test_updatetime_format_detection_and_solution(
        self, mock_add_offset, mock_build_dict
    ):
        """Test to detect problematic datetime formats and suggest solutions."""

        # Create realistic problematic datetime formats based on common database exports
        mock_df = pd.DataFrame(
            {
                "patient_id": ["P001", "P002", "P003", "P004", "P005"],
                "start_date": [
                    "2020-01-01",
                    "2020-02-01",
                    "2020-03-01",
                    "2020-04-01",
                    "2020-05-01",
                ],
                "updatetime": [
                    "15/01/2020 10:30",  # European format DD/MM/YYYY HH:MM
                    "2020-02-15 11:45:30.123456",  # Microseconds
                    "03/15/2020 09:15",  # US format MM/DD/YYYY HH:MM
                    "2020-04-15T14:20:00+00:00",  # ISO with timezone
                    "15-May-2020 16:30",  # DD-Mon-YYYY format
                ],
            }
        )

        def format_detection_side_effect(*args, **kwargs):
            """Detect and analyze problematic datetime formats."""

            if args and isinstance(args[0], pd.DataFrame):
                input_df = args[0]
                output_df = input_df.copy()

                print(f"\n=== DATETIME FORMAT ANALYSIS ===")

                if "updatetime" in input_df.columns:
                    updatetime_values = input_df["updatetime"]

                    print(f"Analyzing {len(updatetime_values)} updatetime values...")

                    # Analyze each value individually
                    problematic_indices = []
                    for i, val in enumerate(updatetime_values):
                        if pd.notnull(val):
                            print(f"\n[{i}] Testing: {repr(val)}")

                            # Test if this specific value causes issues
                            try:
                                # Test with default parsing
                                result_default = pd.to_datetime([val], errors="raise")
                                print(
                                    f"    Default parsing: SUCCESS -> {result_default[0]}"
                                )
                            except Exception as e:
                                print(f"    Default parsing: FAILED -> {e}")
                                problematic_indices.append(i)

                                # Try with coerce to see what happens
                                result_coerce = pd.to_datetime([val], errors="coerce")
                                if pd.isna(result_coerce[0]):
                                    print(
                                        f"    Coerce parsing: -> NaT (THIS CAUSES NaN!)"
                                    )
                                else:
                                    print(
                                        f"    Coerce parsing: SUCCESS -> {result_coerce[0]}"
                                    )

                                # Try to guess the format
                                possible_formats = [
                                    "%d/%m/%Y %H:%M",
                                    "%m/%d/%Y %H:%M",
                                    "%d-%b-%Y %H:%M",
                                    "%Y-%m-%d %H:%M:%S.%f",
                                    "%Y-%m-%dT%H:%M:%S%z",
                                ]

                                for fmt in possible_formats:
                                    try:
                                        result_fmt = pd.to_datetime([val], format=fmt)
                                        print(
                                            f"    Format {fmt}: SUCCESS -> {result_fmt[0]}"
                                        )
                                        break
                                    except:
                                        continue
                                else:
                                    print(
                                        f"    No standard format worked for this value"
                                    )

                    # Summary of problematic values
                    if problematic_indices:
                        print(f"\n=== PROBLEMATIC VALUES SUMMARY ===")
                        print(
                            f"Found {len(problematic_indices)} problematic datetime values:"
                        )
                        for idx in problematic_indices:
                            val = updatetime_values.iloc[idx]
                            print(f"  Row {idx}: {repr(val)}")

                    # Test the overall conversion (what's likely happening in your real code)
                    print(f"\n=== OVERALL CONVERSION TEST ===")
                    converted_overall = pd.to_datetime(
                        updatetime_values, errors="coerce"
                    )
                    total_nans = converted_overall.isnull().sum()
                    print(f"Total NaNs when converting all values: {total_nans}")

                    # Show which specific values became NaN
                    for i, (orig, conv) in enumerate(
                        zip(updatetime_values, converted_overall)
                    ):
                        if pd.notnull(orig) and pd.isnull(conv):
                            print(f"  PROBLEM: Row {i} '{orig}' -> NaT")

                    # Add to output for further analysis
                    output_df["updatetime_converted"] = converted_overall

                # Add required columns
                output_df["start_date_converted"] = pd.to_datetime(
                    input_df["start_date"]
                )
                output_df["start_date_offset"] = output_df[
                    "start_date_converted"
                ] - pd.DateOffset(years=1)

                return output_df

            return pd.DataFrame()

        mock_add_offset.side_effect = format_detection_side_effect

        mock_build_dict.return_value = {
            f"P{str(i).zfill(3)}": (datetime(2020, i, 1), datetime(2021, i, 1))
            for i in range(1, 6)
        }

        print("\n" + "=" * 70)
        print("TESTING NON-ISO DATETIME FORMATS")
        print("=" * 70)

        with patch("builtins.print"):
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        self.assertTrue(mock_add_offset.called)
        self.assertTrue(config.individual_patient_window)

        print(f"\n=== RECOMMENDATIONS ===")
        print(
            "If you see 'PROBLEM' entries above, those datetime formats are causing NaNs."
        )
        print("Solutions:")
        print(
            "1. Standardize datetime formats in your source data to ISO format (YYYY-MM-DD HH:MM:SS)"
        )
        print(
            "2. Use pd.to_datetime() with specific format parameter instead of auto-detection"
        )
        print("3. Preprocess the updatetime column before passing to config_class")
        print("4. Use dayfirst=True parameter if your data uses DD/MM/YYYY format")

    @patch("pat2vec.util.config_pat2vec.build_patient_dict")
    @patch("pat2vec.util.config_pat2vec.add_offset_column")
    def test_datetime_format_solutions(self, mock_add_offset, mock_build_dict):
        """Test potential solutions for non-ISO datetime format issues."""

        # Test data with known problematic format (European DD/MM/YYYY)
        mock_df = pd.DataFrame(
            {
                "patient_id": ["P001", "P002", "P003"],
                "start_date": ["2020-01-01", "2020-02-01", "2020-03-01"],
                "updatetime": [
                    "15/01/2020 10:30:00",  # DD/MM/YYYY - problematic
                    "28/02/2020 14:45:00",  # DD/MM/YYYY - problematic
                    "10/03/2020 09:15:00",  # DD/MM/YYYY - problematic
                ],
            }
        )

        def solution_testing_side_effect(*args, **kwargs):
            """Test various solutions for datetime parsing issues."""

            if args and isinstance(args[0], pd.DataFrame):
                input_df = args[0]
                output_df = input_df.copy()

                print(f"\n=== TESTING DATETIME PARSING SOLUTIONS ===")

                if "updatetime" in input_df.columns:
                    updatetime_values = input_df["updatetime"]

                    print(f"Test data (European DD/MM/YYYY format):")
                    for i, val in enumerate(updatetime_values):
                        print(f"  [{i}] {repr(val)}")

                    # Solution 1: Default parsing (will likely fail)
                    print(f"\n--- Solution 1: Default pd.to_datetime() ---")
                    result1 = pd.to_datetime(updatetime_values, errors="coerce")
                    nans1 = result1.isnull().sum()
                    print(
                        f"Result: {nans1} NaNs out of {len(updatetime_values)} values"
                    )

                    # Solution 2: Using dayfirst=True for European format
                    print(f"\n--- Solution 2: pd.to_datetime(dayfirst=True) ---")
                    result2 = pd.to_datetime(
                        updatetime_values, dayfirst=True, errors="coerce"
                    )
                    nans2 = result2.isnull().sum()
                    print(
                        f"Result: {nans2} NaNs out of {len(updatetime_values)} values"
                    )
                    if nans2 < nans1:
                        print("✓ IMPROVEMENT: dayfirst=True reduced NaNs!")
                        for i, (orig, conv) in enumerate(
                            zip(updatetime_values, result2)
                        ):
                            if pd.notnull(conv):
                                print(f"  [{i}] {repr(orig)} -> {conv}")

                    # Solution 3: Explicit format specification
                    print(f"\n--- Solution 3: Explicit format='%d/%m/%Y %H:%M:%S' ---")
                    result3 = pd.to_datetime(
                        updatetime_values, format="%d/%m/%Y %H:%M:%S", errors="coerce"
                    )
                    nans3 = result3.isnull().sum()
                    print(
                        f"Result: {nans3} NaNs out of {len(updatetime_values)} values"
                    )
                    if nans3 < nans1:
                        print("✓ IMPROVEMENT: Explicit format reduced NaNs!")
                        for i, (orig, conv) in enumerate(
                            zip(updatetime_values, result3)
                        ):
                            if pd.notnull(conv):
                                print(f"  [{i}] {repr(orig)} -> {conv}")

                    # Solution 4: Custom preprocessing
                    print(f"\n--- Solution 4: Custom preprocessing ---")

                    def custom_datetime_parser(dt_str):
                        """Custom parser for problematic datetime formats."""
                        if pd.isnull(dt_str) or dt_str == "":
                            return pd.NaT

                        # Try to detect and convert DD/MM/YYYY format
                        if isinstance(dt_str, str) and "/" in dt_str:
                            try:
                                # Assume DD/MM/YYYY format and convert to YYYY-MM-DD
                                parts = dt_str.split(" ")
                                date_part = parts[0]
                                time_part = parts[1] if len(parts) > 1 else "00:00:00"

                                day, month, year = date_part.split("/")
                                iso_format = f"{year}-{month.zfill(2)}-{day.zfill(2)} {time_part}"
                                return pd.to_datetime(iso_format)
                            except:
                                pass

                        # Fall back to default parsing
                        return pd.to_datetime(dt_str, errors="coerce")

                    result4 = updatetime_values.apply(custom_datetime_parser)
                    nans4 = result4.isnull().sum()
                    print(
                        f"Result: {nans4} NaNs out of {len(updatetime_values)} values"
                    )
                    if nans4 < nans1:
                        print("✓ IMPROVEMENT: Custom preprocessing reduced NaNs!")
                        for i, (orig, conv) in enumerate(
                            zip(updatetime_values, result4)
                        ):
                            if pd.notnull(conv):
                                print(f"  [{i}] {repr(orig)} -> {conv}")

                    # Store the best result
                    best_result = (
                        result2 if nans2 == 0 else (result3 if nans3 == 0 else result4)
                    )
                    output_df["updatetime_converted"] = best_result

                # Add required columns
                output_df["start_date_converted"] = pd.to_datetime(
                    input_df["start_date"]
                )
                output_df["start_date_offset"] = output_df[
                    "start_date_converted"
                ] - pd.DateOffset(years=1)

                return output_df

            return pd.DataFrame()

        mock_add_offset.side_effect = solution_testing_side_effect

        mock_build_dict.return_value = {
            "P001": (datetime(2020, 1, 1), datetime(2021, 1, 1)),
            "P002": (datetime(2020, 2, 1), datetime(2021, 2, 1)),
            "P003": (datetime(2020, 3, 1), datetime(2021, 3, 1)),
        }

        print("\n" + "=" * 70)
        print("TESTING DATETIME FORMAT SOLUTIONS")
        print("=" * 70)

        with patch("builtins.print"):
            config = config_class(
                start_date=self.base_start_date,
                years=1,
                months=0,
                days=0,
                individual_patient_window=True,
                individual_patient_window_df=mock_df,
                individual_patient_window_start_column_name="start_date",
                individual_patient_id_column_name="patient_id",
                testing=True,
            )

        self.assertTrue(mock_add_offset.called)
        self.assertTrue(config.individual_patient_window)


if __name__ == "__main__":
    unittest.main(verbosity=2)
