import unittest
import pandas as pd
from pat2vec.util.ethnicity_abstractor import EthnicityAbstractor


class TestEthnicityAbstractor(unittest.TestCase):
    """Comprehensive tests for pat2vec.util.ethnicity_abstractor."""

    def test_abstract_ethnicity_standard(self):
        """Test standard ethnicity mapping for common groups."""
        data = {
            "client_idcode": ["P1", "P2", "P3", "P4", "P5"],
            "ethnicity_text": [
                "White British",
                "Black African",
                "Indian",
                "Mixed White and Black Caribbean",
                "Arab",
            ],
        }
        df = pd.DataFrame(data)
        result = EthnicityAbstractor.abstractEthnicity(df, "", "ethnicity_text")

        self.assertEqual(result.at[0, "census"], "white")
        self.assertEqual(
            result.at[1, "census"], "black_african_caribbean_or_black_british"
        )
        self.assertEqual(result.at[2, "census"], "asian_or_asian_british")
        self.assertEqual(result.at[3, "census"], "mixed_or_multiple_ethnic_groups")
        self.assertEqual(result.at[4, "census"], "other_ethnic_group")

    def test_abstract_ethnicity_nationalities(self):
        """Test mapping of country names and nationalities to default census groups."""
        data = {
            "client_idcode": ["P_UK", "P_NGA", "P_CHN", "P_POL", "P_BRA"],
            "ethnicity": ["British", "Nigerian", "China", "Poland", "Brazil"],
        }
        df = pd.DataFrame(data)
        result = EthnicityAbstractor.abstractEthnicity(df, "", "ethnicity")

        self.assertEqual(result.at[0, "census"], "white")
        self.assertEqual(
            result.at[1, "census"], "black_african_caribbean_or_black_british"
        )
        self.assertEqual(result.at[2, "census"], "asian_or_asian_british")
        self.assertEqual(result.at[3, "census"], "white")
        self.assertEqual(result.at[4, "census"], "other_ethnic_group")

    def test_abstract_ethnicity_precedence(self):
        """Test that explicit racial labels override synonym-based country defaults."""
        # 'Asian Caribbean': 'Asian' is explicit, 'Caribbean' maps to Black.
        data = {
            "client_idcode": ["P_AC", "P_WN"],
            "ethnicity": ["Asian Caribbean", "White Nigerian"],
        }
        df = pd.DataFrame(data)
        result = EthnicityAbstractor.abstractEthnicity(df, "", "ethnicity")

        self.assertEqual(result.at[0, "census"], "asian_or_asian_british")
        self.assertEqual(result.at[1, "census"], "white")

    def test_abstract_ethnicity_null_handling(self):
        """Test handling of missing or unspecified values."""
        data = {
            "client_idcode": ["P1", "P2", "P3"],
            "ethnicity": [None, "Not Specified", "Unknown"],
        }
        df = pd.DataFrame(data)
        result = EthnicityAbstractor.abstractEthnicity(df, "", "ethnicity")

        # All should fall back to other_ethnic_group
        self.assertTrue((result["census"] == "other_ethnic_group").all())

    def test_abstract_ethnicity_case_insensitivity(self):
        """Test that mapping ignores character case."""
        data = {"client_idcode": ["P1", "P2"], "ethnicity": ["indian", "INDIAN"]}
        df = pd.DataFrame(data)
        result = EthnicityAbstractor.abstractEthnicity(df, "", "ethnicity")

        self.assertEqual(result.at[0, "census"], "asian_or_asian_british")
        self.assertEqual(result.at[1, "census"], "asian_or_asian_british")

    def test_abstract_ethnicity_edge_cases(self):
        """Test specific edge cases like North American nationalities."""
        data = {"client_idcode": ["P1", "P2"], "ethnicity": ["Canadian", "USA"]}
        df = pd.DataFrame(data)
        result = EthnicityAbstractor.abstractEthnicity(df, "", "ethnicity")

        # Canadian is explicitly in the extraWhite list
        self.assertEqual(result.at[0, "census"], "white")
        self.assertEqual(result.at[1, "census"], "white")


if __name__ == "__main__":
    unittest.main()
