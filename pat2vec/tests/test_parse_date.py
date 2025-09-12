import unittest

from pat2vec.util.parse_date import validate_input_dates

class TestDateValidationForElasticsearch(unittest.TestCase):
    """
    Unit tests for the validate_input_dates function, ensuring its output
    is suitable for Elasticsearch/Kibana range queries.
    """

    def test_zfill_on_single_digit_month_and_day(self):
        """
        Tests that single-digit months and days are correctly zero-padded.
        """
        result = validate_input_dates(
            start_year=2024, start_month=9, start_day=5,
            end_year=2025, end_month=1, end_day=2
        )
        expected = ('2024', '09', '05', '2025', '01', '02')
        self.assertEqual(result, expected)

    def test_no_change_on_double_digit_dates(self):
        """
        Tests that dates with double-digit months and days are returned correctly.
        """
        result = validate_input_dates(
            start_year=2024, start_month=10, start_day=15,
            end_year=2025, end_month=12, end_day=25
        )
        expected = ('2024', '10', '15', '2025', '12', '25')
        self.assertEqual(result, expected)

    def test_invalid_start_date_raises_value_error(self):
        """
        Tests that an impossible date (e.g., September 31st) raises a ValueError.
        """
        with self.assertRaisesRegex(ValueError, "Invalid start date component"):
            validate_input_dates(
                start_year=2024, start_month=9, start_day=31,
                end_year=2025, end_month=1, end_day=1
            )

    def test_invalid_end_date_raises_value_error(self):
        """
        Tests that an impossible month (e.g., month 13) raises a ValueError.
        """
        with self.assertRaisesRegex(ValueError, "Invalid end date component"):
            validate_input_dates(
                start_year=2024, start_month=1, start_day=1,
                end_year=2025, end_month=13, end_day=1
            )

    def test_valid_leap_year_date(self):
        """
        Tests that a valid leap day (Feb 29) is accepted.
        """
        result = validate_input_dates(
            start_year=2024, start_month=2, start_day=29,
            end_year=2024, end_month=3, end_day=1
        )
        self.assertEqual(result[2], '29')
        self.assertEqual(result[1], '02')

    def test_invalid_non_leap_year_date(self):
        """
        Tests that an invalid leap day (Feb 29 in a non-leap year) raises an error.
        """
        with self.assertRaisesRegex(ValueError, "day is out of range for month"):
            validate_input_dates(
                start_year=2025, start_month=2, start_day=29,
                end_year=2025, end_month=3, end_day=1
            )

    def test_output_types_are_strings(self):
        """
        Ensures all returned values are strings, as required for query building.
        """
        result = validate_input_dates(
            start_year=2024, start_month=1, start_day=1,
            end_year=2025, end_month=1, end_day=1
        )
        self.assertTrue(all(isinstance(item, str) for item in result))

    def test_string_inputs_are_handled_correctly(self):
        """
        Tests that numeric strings for all components are validated and formatted.
        """
        result = validate_input_dates(
            start_year="2024", start_month="9", start_day="5",
            end_year="2025", end_month="12", end_day="20"
        )
        expected = ('2024', '09', '05', '2025', '12', '20')
        self.assertEqual(result, expected)

    def test_mixed_int_and_string_inputs(self):
        """
        Tests that a mix of integer and string inputs are handled correctly.
        """
        result = validate_input_dates(
            start_year=2024, start_month="9", start_day=5,
            end_year="2025", end_month=1, end_day="8"
        )
        expected = ('2024', '09', '05', '2025', '01', '08')
        self.assertEqual(result, expected)

    def test_non_numeric_string_raises_error(self):
        """
        Tests that a non-numeric string raises a ValueError during int conversion.
        """
        with self.assertRaisesRegex(ValueError, "Invalid start date component"):
            validate_input_dates(
                start_year=2024, start_month=9, start_day="five",
                end_year=2025, end_month=1, end_day=1
            )

        with self.assertRaisesRegex(ValueError, "Invalid end date component"):
            validate_input_dates(
                start_year=2024, start_month=1, start_day=1,
                end_year=2025, end_month="abc", end_day=1
            )

if __name__ == '__main__':
    unittest.main(verbosity=2)
