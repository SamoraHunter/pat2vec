import os

import numpy as np
import pandas as pd
import pytest
from pat2vec.util.anonymize_column import (
    ColumnAnonymizer,
    anonymize_column,
    deanonymize_column,
)


class TestColumnAnonymizer:
    """Test suite for the ColumnAnonymizer class."""

    @pytest.fixture
    def sample_df(self):
        """Create a sample DataFrame with client_idcode values."""
        return pd.DataFrame(
            {
                "client_idcode": ["A001", "B002", "C003", "D004", "E005"],
                "value_column": [10, 20, 30, 40, 50],
                "text_column": ["foo", "bar", "baz", "qux", "quux"],
            }
        )

    @pytest.fixture
    def anonymizer(self):
        """Create a ColumnAnonymizer instance with a fixed key."""
        return ColumnAnonymizer(key="test_secret_key_12345")

    def test_initialization_with_key(self, anonymizer):
        """Test that the anonymizer initializes correctly with a secret key."""
        assert anonymizer.key == "test_secret_key_12345"

    def test_initialization_without_key(self):
        """Test that the anonymizer generates a default key when none is provided."""
        anon = ColumnAnonymizer()
        assert anon.key is not None
        assert len(anon.key) > 0

    def test_anonymize_creates_mapping(self, sample_df, anonymizer):
        """Test that anonymization creates a consistent mapping."""
        df_anon, mapping = anonymizer.anonymize(sample_df)

        assert isinstance(mapping, dict)
        assert len(mapping) == len(
            sample_df["client_idcode"].unique(),
        )

        for original_value in sample_df["client_idcode"]:
            hash_value = mapping.get(str(original_value))
            assert hash_value is not None
            assert len(hash_value) > 0

    def test_anonymize_replaces_values(self, sample_df, anonymizer):
        """Test that anonymization actually replaces the values."""
        df_anon, mapping = anonymizer.anonymize(sample_df)

        assert "client_idcode" in df_anon.columns
        assert len(df_anon) == len(sample_df)

        for original_value, anon_value in zip(
            sample_df["client_idcode"],
            df_anon["client_idcode"],
        ):
            assert str(original_value) != str(anon_value)

    def test_deanonymize_reverses_mapping(self, sample_df, anonymizer):
        """Test that de-anonymization correctly restores original values."""
        df_anon, mapping = anonymizer.anonymize(sample_df)
        df_deanon = anonymizer.deanonymize(df_anon, mapping)

        pd.testing.assert_series_equal(
            sample_df["client_idcode"],
            df_deanon["client_idcode"],
        )

    def test_mapping_consistency(self, sample_df, anonymizer):
        """Test that the same input produces the same hash."""
        _, mapping1 = anonymizer.anonymize(sample_df)
        _, mapping2 = anonymizer.anonymize(sample_df)

        assert mapping1 == mapping2

    def test_different_keys_produce_different_hashes(self, sample_df):
        """Test that different keys produce different hashes for the same value."""
        anon1 = ColumnAnonymizer(key="key_one")
        anon2 = ColumnAnonymizer(key="key_two")

        _, mapping1 = anon1.anonymize(sample_df)
        _, mapping2 = anon2.anonymize(sample_df)

        assert mapping1 != mapping2

    def test_empty_dataframe(self, anonymizer):
        """Test handling of empty DataFrame."""
        df_empty = pd.DataFrame({"client_idcode": [], "value_column": []})

        df_anon, mapping = anonymizer.anonymize(df_empty)

        assert len(df_anon) == 0
        assert len(mapping) == 0

    def test_missing_values(self, anonymizer):
        """Test handling of missing/null values in the column."""
        df_with_nan = pd.DataFrame(
            {
                "client_idcode": ["A001", None, "B002", np.nan, "C003"],
                "other_col": [1, 2, 3, 4, 5],
            },
        )

        df_anon, mapping = anonymizer.anonymize(df_with_nan)

        assert len(mapping) >= 3
        assert pd.isna(df_anon.loc[1, "client_idcode"])
        assert pd.isna(df_anon.loc[3, "client_idcode"])

    def test_custom_column_name(self, sample_df, anonymizer):
        """Test anonymization of a column with a custom name."""
        df_custom = sample_df.rename(columns={"client_idcode": "patient_id"})

        df_anon, mapping = anonymizer.anonymize(df_custom, column_name="patient_id")

        assert "patient_id" in df_anon.columns
        assert "client_idcode" not in df_anon.columns

    def test_missing_column_error(self, sample_df, anonymizer):
        """Test that a missing column raises an error."""
        with pytest.raises(KeyError) as excinfo:
            anonymizer.anonymize(sample_df, column_name="nonexistent")

        assert "nonexistent" in str(excinfo.value)

    def test_deanonymize_missing_column_error(self, sample_df, anonymizer):
        """Test that deanonymization raises error for missing column."""
        df_anon, mapping = anonymizer.anonymize(sample_df)

        with pytest.raises(KeyError) as excinfo:
            anonymizer.deanonymize(df_anon, mapping, column_name="nonexistent")

        assert "nonexistent" in str(excinfo.value)

    def test_key_persistence(self, sample_df, tmp_path):
        """Test saving and loading keys from files."""
        anon = ColumnAnonymizer(key="persistence_test")
        key_file = tmp_path / "test_key.json"

        _, mapping = anon.anonymize(sample_df)
        anon.save_mapping(mapping, str(key_file))

        assert os.path.exists(str(key_file))

        loaded_mapping = anon.load_mapping(str(key_file))
        assert loaded_mapping == mapping

    def test_feature_columns_unchanged(self, sample_df, anonymizer):
        """Test that only the target column is anonymized."""
        df_anon, _ = anonymizer.anonymize(sample_df)

        assert list(df_anon.columns) == [
            "client_idcode",
            "value_column",
            "text_column",
        ]

        pd.testing.assert_series_equal(
            sample_df["value_column"],
            df_anon["value_column"],
        )
        pd.testing.assert_series_equal(
            sample_df["text_column"].astype(str),
            df_anon["text_column"].astype(str),
        )


class TestAnonymizeColumnFunctions:
    """Test suite for convenience functions."""

    @pytest.fixture
    def sample_df(self):
        """Create a sample DataFrame with client_idcode values."""
        return pd.DataFrame(
            {
                "client_idcode": ["A001", "B002", "C003", "D004", "E005"],
                "value_column": [10, 20, 30, 40, 50],
                "text_column": ["foo", "bar", "baz", "qux", "quux"],
            }
        )

    def test_anonymize_column_convenience(self, sample_df):
        """Test the anonymize_column convenience function."""

        df_anon, mapping = anonymize_column(sample_df)

        assert isinstance(df_anon, pd.DataFrame)
        assert len(mapping) > 0

    def test_deanonymize_column_convenience(self, sample_df):
        """Test the deanonymize_column convenience function."""

        df_anon, mapping = anonymize_column(sample_df)
        df_deanon = deanonymize_column(df_anon, mapping)

        pd.testing.assert_series_equal(
            sample_df["client_idcode"],
            df_deanon["client_idcode"],
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
