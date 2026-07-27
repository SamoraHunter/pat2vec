import hmac
import hashlib
import json
import os
from typing import Dict, Mapping, Optional, Tuple

import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class ColumnAnonymizer:
    """Provides deterministic pseudonymisation of DataFrame column values using a keyed hash.

    This class allows you to anonymize sensitive data (like client IDs) in a pandas DataFrame.
    Reversibility is achieved by storing the generated mapping, which maps original values
    to their pseudonymized hashes.

    Key Features:
        - Deterministic hashing with keyed HMAC for consistent results
        - Store/load mappings for consistency across runs
        - Support for custom column names (defaults to 'client_idcode')
        - Preserves data integrity while hiding sensitive values

    Example:
        >>> anonymizer = ColumnAnonymizer(key="my_secret_key")
        >>> df_anon, mapping = anonymizer.anonymize(df)
        >>> df_deanon = anonymizer.deanonymize(df_anon, mapping)
    """

    def __init__(self, key: Optional[str] = None):
        """Initialize the ColumnAnonymizer.

        Args:
            key: Secret key for generating consistent hashes. If None and no
                 environment variable is set, a secure random key will be generated.
                 Keep this secret if you need to maintain consistent mappings across sessions.
        """
        self.key = key or os.environ.get("ANONYMIZATION_KEY")

        if self.key is None:
            import secrets

            self.key = secrets.token_hex(32)

    def _hash_value(self, value: str) -> str:
        """Generate a deterministic hash for a given value using the secret key.

        Args:
            value: The string value to hash

        Returns:
            A deterministic hash string combining the key and value
        """
        return hmac.new(
            self.key.encode(),
            value.encode(),
            hashlib.sha256,
        ).hexdigest()[:32]

    def _create_mapping(self, values: pd.Series) -> Dict[str, str]:
        """Create a mapping from original values to anonymized values.

        Args:
            values: A pandas Series containing unique values to map

        Returns:
            Dictionary mapping original values to their pseudonymized versions
        """
        unique_values = values.dropna().unique()

        mapping = {}
        for val in unique_values:
            if pd.isna(val):
                continue
            str_val = str(val)
            mapping[str_val] = self._hash_value(str_val)

        return mapping

    def anonymize(
        self, df: pd.DataFrame, column_name: str = "client_idcode"
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """Anonymizes the values in a specified column using deterministic hashing.

        Args:
            df: The input DataFrame containing the column to anonymize
            column_name: Name of the column to anonymize (default: 'client_idcode')

        Returns:
            Tuple of (anonymized_dataframe, mapping_dictionary)
            The mapping dictionary can be used to deanonymize the data later.

        Raises:
            KeyError: If the specified column doesn't exist in the DataFrame
        """
        if column_name not in df.columns:
            raise KeyError(column_name)

        original_values = df[column_name]
        mapping = self._create_mapping(original_values)

        anonymized_df = df.copy()

        def apply_anonymize(val):
            if pd.isna(val):
                return val
            str_val = str(val)
            return mapping.get(str_val, val)

        anonymized_df[column_name] = original_values.apply(apply_anonymize)

        logger.info("Anonymized '%s': %d unique values", column_name, len(mapping))

        return anonymized_df, mapping

    def deanonymize(
        self,
        df: pd.DataFrame,
        mapping: Mapping[str, str],
        column_name: str = "client_idcode",
    ) -> pd.DataFrame:
        """De-anonymizes values in a specified column using a provided mapping.

        Args:
            df: The anonymized DataFrame
            mapping: Dictionary containing the original -> hash mappings from anonymize()
            column_name: Name of the column to deanonymize (default: 'client_idcode')

        Returns:
            A new DataFrame with_deanonymized values in the specified column

        Raises:
            KeyError: If the specified column doesn't exist in the DataFrame
        """
        if column_name not in df.columns:
            raise KeyError(column_name)

        reverse_mapping = {v: k for k, v in mapping.items()}
        deanonymized_df = df.copy()

        original_values = df[column_name]

        def apply_deanonymize(val):
            original_str = str(val) if not pd.isna(val) else None
            hash_val = reverse_mapping.get(original_str)

            if hash_val is not None:
                return hash_val

            if pd.isna(val):
                return val

            return val

        deanonymized_df[column_name] = original_values.apply(apply_deanonymize)

        unknown_count = sum(
            1
            for val in df[column_name]
            if not pd.isna(val) and str(val) not in reverse_mapping
        )

        if unknown_count > 0:
            logger.warning(
                "%d values could not be de-anonymized (not found in mapping)",
                unknown_count,
            )

        return deanonymized_df

    def save_mapping(self, mapping: Mapping[str, str], filepath: str) -> None:
        """Save the anonymization mapping to a file.

        Args:
            mapping: The mapping dictionary to save (original -> hash)
            filepath: Path where the mapping will be saved (JSON format)
        """
        output = {str(k): str(v) for k, v in mapping.items()}

        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        logger.info("Anonymization mapping saved to: %s", filepath)

    def load_mapping(self, filepath: str) -> Dict[str, str]:
        """Load an anonymization mapping from a file.

        Args:
            filepath: Path to the saved mapping file

        Returns:
            The loaded mapping dictionary
        """
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)


def anonymize_column(
    df: pd.DataFrame,
    column_name: str = "client_idcode",
    key: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Convenience function to anonymize a DataFrame column with deterministic hashing.

    This is a wrapper around ColumnAnonymizer that provides a simple interface
    for one-off anonymization operations without needing to manage class instances.

    Args:
        df: The input DataFrame containing the column to anonymize
        column_name: Name of the column to anonymize (default: 'client_idcode')
        key: Optional secret key for consistent hashing. If None, a secure random key is generated.

    Returns:
        Tuple of (anonymized_dataframe, mapping_dictionary)

    Example:
        >>> df = pd.DataFrame({'client_idcode': ['A001', 'B002', 'C003']})
        >>> df_anon, mapping = anonymize_column(df)
        >>> print(df_a_non)
           client_idcode
        0     a1b2c3d4e5f6...
        1     f6e5d4c3b2a1...
        2     1a2b3c4d5e6f...
    """
    anonymizer = ColumnAnonymizer(key=key)
    return anonymizer.anonymize(df, column_name)


def deanonymize_column(
    df: pd.DataFrame,
    mapping: Mapping[str, str],
    column_name: str = "client_idcode",
) -> pd.DataFrame:
    """Convenience function to de-anonymize a DataFrame column.

    This is a wrapper around ColumnAnonymizer.deanonymize for simple use cases.

    Args:
        df: The anonymized DataFrame
        mapping: Dictionary from the anonymization operation (original -> hash)
        column_name: Name of the column to deanonymize (default: 'client_idcode')

    Returns:
        A new DataFrame with de-anonymized values

    Example:
        >>> df_anon = pd.DataFrame({'client_idcode': ['a1b2c3d4e5f6...', ...]})
        >>> df_deanon = deanonymize_column(df_anon, mapping)
    """
    anonymizer = ColumnAnonymizer()
    return anonymizer.deanonymize(df, mapping, column_name)
