import unittest
from pat2vec.pat2vec_search.matcher import match_terms_in_text, find_all_matches


class TestMatcher(unittest.TestCase):
    """Unit tests for the matcher utility module."""

    def test_match_terms_in_text_basic(self):
        """Test basic term matching (case-insensitive, whole words)."""
        text = "The patient has a history of asthma and diabetes."
        terms = ["asthma", "diabetes", "hypertension"]
        expected = ["asthma", "diabetes"]
        result = match_terms_in_text(text, terms)
        self.assertCountEqual(result, expected)

    def test_match_terms_in_text_case_insensitivity(self):
        """Test case-insensitivity of term matching."""
        text = "Patient reports FEVER and cough."
        terms = ["fever", "cough"]
        expected = ["fever", "cough"]
        result = match_terms_in_text(text, terms)
        self.assertCountEqual(result, expected)

    def test_match_terms_in_text_partial_words(self):
        """Test that partial word matches are not returned."""
        text = "The patient is asthmatic."
        terms = ["asthma"]
        expected = []
        result = match_terms_in_text(text, terms)
        self.assertCountEqual(result, expected)

    def test_match_terms_in_text_empty_inputs(self):
        """Test behavior with empty text or term lists."""
        self.assertCountEqual(match_terms_in_text("", ["term"]), [])
        self.assertCountEqual(match_terms_in_text("text", []), [])
        self.assertCountEqual(match_terms_in_text("", []), [])

    def test_find_all_matches_basic(self):
        """Test finding all matches for given patterns."""
        text = "Patient has a fever of 38.5C. Also, a cough."
        patterns = {
            "temperature": r"\d+\.\d+C",
            "symptom": r"fever|cough",
            "diagnosis": r"cancer",
        }
        expected = {"temperature": ["38.5C"], "symptom": ["fever", "cough"]}
        result = find_all_matches(text, patterns)
        self.assertDictEqual(result, expected)

    def test_find_all_matches_no_match(self):
        """Test when no patterns match the text."""
        text = "No symptoms reported."
        patterns = {"temperature": r"\d+\.\d+C"}
        result = find_all_matches(text, patterns)
        self.assertDictEqual(result, {})


if __name__ == "__main__":
    unittest.main()
