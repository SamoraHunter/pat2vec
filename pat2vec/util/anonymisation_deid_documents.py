import os
import logging
from typing import List, Union, Optional, Dict, Any, Tuple, Type
from pathlib import Path
import pandas as pd

try:
    from medcat.utils.ner.deid import DeIdModel
    import spacy

    MEDCAT_AVAILABLE = True
except ImportError:
    MEDCAT_AVAILABLE = False  # type: ignore
    DeIdModel = None  # type: ignore


class DeIdAnonymizer:
    """A class for anonymizing clinical text using MedCAT's DeIdModel.

    This class encapsulates the functionality for loading a de-identification
    model, anonymizing text data in various formats (single string, list of
    strings, pandas DataFrame columns), and providing utilities for inspection
    and reporting.

    Attributes:
        model: The loaded MedCAT DeIdModel instance.
        model_path: The path to the loaded model pack.
        is_loaded: A boolean indicating if a model is successfully loaded.
        pii_labels: A list of PII labels the loaded model is configured to redact.
        anonymization_log: A list of dictionaries logging each operation.
        logger: A configured logger instance for the class.
    """

    def __init__(
        self, model_path: Optional[Union[str, Path]] = None, log_level: str = "INFO"
    ):
        """Initializes the DeIdAnonymizer.

        Args:
            model_path: Optional path to the MedCAT DeIdModel pack. If provided,
                the model is loaded upon initialization.
            log_level: The logging level for the instance (e.g., "INFO", "DEBUG").
        """
        self.model: Optional[Type[DeIdModel]] = None
        self.model_path = model_path
        self.is_loaded: bool = False
        self.pii_labels: List[str] = []
        self.anonymization_log: List[Dict[str, Any]] = []

        # Setup logging
        self.logger = logging.getLogger(f"{__name__}.DeIdAnonymizer")

        # Check if MedCAT is available
        if not MEDCAT_AVAILABLE:
            self.logger.error(
                "MedCAT is not installed. Please install it using: "
                "pip install medcat"
            )
            raise ImportError("MedCAT is required but not installed")

        # Auto-load model if path provided
        if model_path:
            self.load_model(model_path)

    def load_model(self, model_path: Union[str, Path]) -> bool:
        """Loads a pre-trained DeIdModel from a specified path.

        Args:
            model_path: The path to the model pack (directory or .zip file).

        Returns:
            True if the model was loaded successfully, False otherwise.
        """
        try:
            model_path_p = Path(model_path)
            if not model_path.exists():
                self.logger.error(f"Model path does not exist: {model_path}")
                return False

            self.logger.info(f"Loading DeIdModel from: {model_path}")
            self.model = DeIdModel.load_model_pack(str(model_path))
            self.model_path = model_path
            self.is_loaded = self.model is not None

            # Hotfix for a spaCy extension error that can occur with some MedCAT models.
            # The 'link_candidates' attribute is expected by a serialization pipe
            # but may not be registered if the model pack doesn't include a linker.
            # We register it here with a safe default to prevent crashes.
            if not spacy.tokens.Span.has_extension("link_candidates"):
                self.logger.info(
                    "Registering missing 'link_candidates' spaCy extension to prevent serialization errors."
                )
                spacy.tokens.Span.set_extension("link_candidates", default=[])

            # Inspect the model for PII labels it's configured to redact
            self.pii_labels = getattr(self.model, "pii_labels", [])
            self.logger.info("DeIdModel loaded successfully")
            self.logger.info(
                f"Model configured to redact PII labels: {self.pii_labels}"
            )
            if not self.pii_labels:
                self.logger.warning(
                    "The loaded model has an empty 'pii_labels' list. "
                    "This means no PII will be redacted. Please check the model configuration."
                )
            self._log_operation(
                "model_loaded", {"path": str(model_path), "pii_labels": self.pii_labels}
            )
            return True

        except ValueError as e:
            self.logger.error(f"Error loading DeIdModel: {e}")
            self.logger.error(
                "Please ensure the path corresponds to a valid DeId model"
            )
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during model loading: {e}")
            return False

    def _check_model_loaded(self) -> None:
        """Checks if a model is loaded, raising a RuntimeError if not."""
        if not self.is_loaded or self.model is None:
            raise RuntimeError(
                "DeIdModel not loaded. Please call load_model() first or "
                "provide model_path during initialization."
            )

    def _log_operation(self, operation: str, details: Dict[str, Any]) -> None:  # type: ignore
        """Log an anonymization operation for audit purposes."""
        log_entry = {
            "operation": operation,
            "timestamp": pd.Timestamp.now(),
            "details": details,
        }
        self.anonymization_log.append(log_entry)

    def anonymize_text(
        self, text: str, redact: bool = True, verify: bool = False
    ) -> Union[str, Tuple[str, Dict[str, Any]]]:
        """Anonymizes a single text string.

        Args:
            text: The input text to anonymize.
            redact: If True, replaces PII with asterisks ('\***'). If False,
                replaces PII with type tags (e.g., '<PERSON>').
            verify: If True, returns a tuple containing the anonymized text and
                a dictionary with verification information.

        Returns:
            If `verify` is False, returns the anonymized text string.
            If `verify` is True, returns a tuple of (anonymized_text, verification_info).
        """
        self._check_model_loaded()

        try:
            anonymized = self.model.deid_text(text, redact=redact)

            self._log_operation(
                "single_text",
                {
                    "redact": redact,
                    "original_length": len(text),
                    "anonymized_length": len(anonymized),
                },
            )

            if verify:
                verification_info = self._verify_single_text(text, anonymized)
                return anonymized, verification_info

            return anonymized

        except Exception as e:
            self.logger.error(f"Error anonymizing text: {e}")
            raise

    def anonymize_texts(
        self,
        texts: List[str],
        redact: bool = True,
        n_process: int = 1,
        batch_size: int = 100,
        verify_sample: bool = False,
        sample_size: int = 10,
    ) -> Union[List[str], Tuple[List[str], Dict]]:
        """Anonymizes a list of text strings, with parallel processing support.

        Args:
            texts: A list of input texts to anonymize.
            redact: If True, replaces PII with asterisks. If False, uses type tags.
            n_process: The number of processes to use for parallel execution.
            batch_size: The number of texts to process in each batch.
            verify_sample: If True, verifies a random sample of the results and
                returns a report.
            sample_size: The size of the random sample to verify if `verify_sample`
                is True.

        Returns:
            If `verify_sample` is False, returns a list of anonymized texts.
            If `verify_sample` is True, returns a tuple of
            (anonymized_texts, verification_report).
        """
        self._check_model_loaded()

        try:
            anonymized: List[str] = self.model.deid_multi_texts(
                texts, redact=redact, n_process=n_process, batch_size=batch_size
            )

            self._log_operation(
                "multiple_texts",
                {
                    "count": len(texts),
                    "redact": redact,
                    "n_process": n_process,
                    "batch_size": batch_size,
                },
            )

            if verify_sample:
                verification_report = self._verify_multiple_texts(
                    texts, anonymized, sample_size
                )
                return anonymized, verification_report

            return anonymized

        except Exception as e:
            self.logger.error(f"Error anonymizing multiple texts: {e}")
            raise

    def anonymize_dataframe(
        self,
        df: pd.DataFrame,
        text_columns: List[str],
        redact: bool = True,
        inplace: bool = False,
        suffix: str = "_anonymized",
        n_process: int = 1,
        batch_size: int = 100,
    ) -> pd.DataFrame:
        """Anonymizes specified text columns in a pandas DataFrame.

        Args:
            df: The input DataFrame.
            text_columns: A list of column names containing the text to be
                anonymized.
            redact: If True, replaces PII with asterisks. If False, uses type tags.
            inplace: If True, modifies the DataFrame in place by overwriting the
                original text columns. If False, returns a new DataFrame with
                anonymized columns added.
            suffix: The suffix to add to new anonymized column names. This is
                ignored if `inplace` is True.
            n_process: The number of processes for parallel execution.
            batch_size: The number of texts to process in each batch.

        Returns:
            A DataFrame with the specified text columns anonymized.
        """
        self._check_model_loaded()

        # Validate columns exist
        missing_cols = [col for col in text_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

        # Create working copy if not inplace
        result_df = df if inplace else df.copy()
        total_texts_processed = 0

        for col in text_columns:
            self.logger.info(f"Anonymizing column: {col}")

            # Handle NaN values
            texts_to_process = df[col].fillna("").astype(str).tolist()
            total_texts_processed += len(texts_to_process)

            # Anonymize texts
            anonymized_texts = self.model.deid_multi_texts(
                texts_to_process,
                redact=redact,
                n_process=n_process,
                batch_size=batch_size,
            )

            # Update DataFrame
            if inplace:
                result_df[col] = anonymized_texts
            else:
                result_df[f"{col}{suffix}"] = anonymized_texts

        self._log_operation(
            "dataframe",
            {
                "columns": text_columns,
                "rows": len(df),
                "total_texts": total_texts_processed,
                "redact": redact,
                "inplace": inplace,
            },
        )

        return result_df

    def inspect_text(self, text: str) -> List[Dict[str, Any]]:
        """Inspects text to find and log PII entities without anonymizing.

        This method is useful for debugging and understanding what the loaded
        model is capable of detecting in a given piece of text.

        Args:
            text: The text to inspect.

        Returns:
            A list of dictionaries, each representing a found PII entity.
        """
        self._check_model_loaded()
        self.logger.info(f"Inspecting text for PII entities...")
        entities = self.get_structured_annotations(text)

        if not entities:
            self.logger.info("No PII entities found in the text.")
        else:
            self.logger.info(f"Found {len(entities)} PII entities:")
            for ent in entities:
                self.logger.info(
                    f"  - Text: '{ent['text']}', "
                    f"Label: {ent['label']}, "
                    f"Confidence: {ent.get('confidence', 'N/A'):.2f}"
                )

        return entities

    def get_structured_annotations(self, text: str) -> List[Dict[str, Any]]:
        """Gets structured annotations for PII entities in a text.

        Args:
            text: The input text to analyze.

        Returns:
            A list of dictionaries, where each dictionary contains details
            (text, label, start, end, confidence) for an identified PII entity.
        """
        self._check_model_loaded()

        try:
            doc = self.model(text)
            entities = []

            for ent in doc.ents:
                entities.append(
                    {
                        "text": ent.text,
                        "label": ent.label_,
                        "start": ent.start_char,
                        "end": ent.end_char,
                        "confidence": getattr(
                            ent, "_.acc", None
                        ),  # Confidence if available
                    }
                )

            return entities

        except Exception as e:
            self.logger.error(f"Error getting structured annotations: {e}")
            raise

    def _verify_single_text(self, original: str, anonymized: str) -> Dict[str, Any]:
        """Verifies anonymization quality for a single text."""
        entities = self.get_structured_annotations(original)

        verification = {
            "entities_found": len(entities),
            "entity_types": list(set(ent["label"] for ent in entities)),
            "original_length": len(original),
            "anonymized_length": len(anonymized),
            "entities": entities,
        }

        return verification

    def _verify_multiple_texts(
        self, original_texts: List[str], anonymized_texts: List[str], sample_size: int
    ) -> Dict[str, Any]:
        """Verifies anonymization quality for a sample of multiple texts."""
        import random

        # Sample texts for verification
        indices = random.sample(
            range(len(original_texts)), min(sample_size, len(original_texts))
        )

        total_entities = 0
        all_entity_types = set()

        for i in indices:
            verification = self._verify_single_text(
                original_texts[i], anonymized_texts[i]
            )
            total_entities += verification["entities_found"]
            all_entity_types.update(verification["entity_types"])

        report = {
            "sample_size": len(indices),
            "total_texts": len(original_texts),
            "total_entities_in_sample": total_entities,
            "unique_entity_types": list(all_entity_types),
            "avg_entities_per_text": total_entities / len(indices) if indices else 0,
        }

        return report

    def generate_report(self) -> Dict[str, Any]:
        """Generates a summary report of all operations performed.

        Returns:
            A dictionary containing statistics about the anonymization
            operations, model details, and total texts processed.
        """
        if not self.anonymization_log:
            return {"message": "No anonymization operations performed yet"}

        # Count operations by type
        operation_counts = {}
        for log_entry in self.anonymization_log:
            op_type = log_entry["operation"]
            operation_counts[op_type] = operation_counts.get(op_type, 0) + 1

        # Calculate total texts processed
        total_texts = 0
        for log_entry in self.anonymization_log:
            if log_entry["operation"] == "single_text":
                total_texts += 1
            elif log_entry["operation"] == "multiple_texts":
                total_texts += log_entry["details"]["count"]
            elif log_entry["operation"] == "dataframe":
                total_texts += log_entry["details"].get("total_texts", 0)

        report = {
            "model_path": str(self.model_path) if self.model_path else None,
            "model_loaded": self.is_loaded,
            "pii_labels_in_use": self.pii_labels,
            "total_operations": len(self.anonymization_log),
            "operation_breakdown": operation_counts,
            "total_texts_processed": total_texts,
            "first_operation": (
                self.anonymization_log[0]["timestamp"]
                if self.anonymization_log
                else None
            ),
            "last_operation": (
                self.anonymization_log[-1]["timestamp"]
                if self.anonymization_log
                else None
            ),
        }

        return report

    def save_log(self, filepath: Union[str, Path]) -> None:
        """Saves the anonymization operation log to a JSON file.

        Args:
            filepath: The path where the log file will be saved.
        """
        import json

        filepath = Path(filepath)

        # Convert timestamps to strings for JSON serialization
        log_data = []
        for entry in self.anonymization_log:
            entry_copy = entry.copy()
            entry_copy["timestamp"] = entry_copy["timestamp"].isoformat()
            log_data.append(entry_copy)

        with open(filepath, "w") as f:
            json.dump(log_data, f, indent=2)

        self.logger.info(f"Anonymization log saved to: {filepath}")


# Convenience functions for quick usage
def anonymize_single_text(
    text: str, model_path: Union[str, Path], redact: bool = True
) -> str:
    """A convenience function to quickly anonymize a single text string.

    Args:
        text: The input text to anonymize.
        model_path: The path to the DeIdModel pack.
        redact: If True, replaces PII with asterisks. If False, uses type tags.

    Returns:
        The anonymized text.
    """
    anonymizer = DeIdAnonymizer(model_path)
    return anonymizer.anonymize_text(text, redact=redact)


def anonymize_dataframe_quick(
    df: pd.DataFrame,
    text_columns: List[str],
    model_path: Union[str, Path],
    redact: bool = True,
) -> pd.DataFrame:
    """A convenience function to quickly anonymize columns in a DataFrame.

    Args:
        df: The input DataFrame.
        text_columns: A list of column names to anonymize.
        model_path: The path to the DeIdModel pack.
        redact: If True, replaces PII with asterisks. If False, uses type tags.

    Returns:
        A new DataFrame with anonymized text columns.
    """
    anonymizer = DeIdAnonymizer(model_path)
    return anonymizer.anonymize_dataframe(df, text_columns, redact=redact)


# # Example usage and testing
# if __name__ == "__main__":
#     # --- IMPORTANT ---
#     # This example requires a pre-trained MedCAT DeIdModel pack.
    # # # You need to provide the path to your model pack below.
    # # # For demonstration, we use a placeholder path.
    # # # Replace this with the actual path to your model.
    # # model_path = "/path/to/your/deid_model_pack"

    # # # Check if the model path is a placeholder
    # # if model_path == "/path/to/your/deid_model_pack" or not os.path.exists(model_path):
    # #     logger.warning("=" * 80)
    # #     logger.warning("WARNING: De-identification model path is not set or is invalid.")
    # #     logger.warning(f"Please update 'model_path' in the __main__ block of this script.")
    # #     logger.warning(f"Current path: {model_path}")
    # #     logger.warning("Skipping anonymization example.")
    # #     logger.warning("=" * 80)
    # # else:
    # #     # Initialize anonymizer
    # #     anonymizer = DeIdAnonymizer(model_path)

    # #     # Test single text anonymization
    # #     test_text = (
    # #         "Patient John Doe, born on 1980-01-15, visited Dr. Smith on 2023-10-26."
    # #     )
    # #     anonymized, verification = anonymizer.anonymize_text(
    # #         test_text, redact=True, verify=True
    # #     )
    # #     logger.info("--- Single Text Anonymization ---")
    # #     logger.info(f"Original: {test_text}")
    # #     logger.info(f"Anonymized: {anonymized}")
    # #     logger.info(f"Verification: {verification}")

    # #     # Add diagnostic check if anonymization did not change the text
    # #     if anonymized == test_text:
    # #         logger.warning("\n" + "=" * 20 + " DIAGNOSTIC " + "=" * 20)
    # #         logger.warning("WARNING: Anonymized text is identical to the original text.")
    # #         logger.warning("This indicates that no PII was redacted.")
    # #         logger.warning("Running inspection to see what PII entities the model detected...")
    # #         anonymizer.inspect_text(test_text)
    # #         logger.warning("\nPossible reasons for no redaction:")
    # #         logger.warning(
    # #             "1. The model did not detect any PII in the text (see inspection results above)."
    # #         )
    # #         logger.warning(
    # #             "2. The model's `pii_labels` list is empty or misconfigured. "
    # #             f"Current labels: {anonymizer.pii_labels}"
    # #         )
    # #         logger.warning("=" * 52)

    # #     logger.info("\n" + "=" * 50 + "\n")

    # #     # Test DataFrame anonymization
    # #     test_df = pd.DataFrame(
    # #         {
    # #             "id": [1, 2, 3],
    # #             "body_analysed": [
    # #                 "Patient Jane Smith was seen on 2023-01-15",
    # #                 "Dr. Brown reviewed case P12345 at Kings Hospital",
    # #                 "Contact number: 07123456789, address: 123 Oak Street",
    # #             ],
    # #         }
    # #     )

    # #     logger.info("--- DataFrame Anonymization ---")
    # #     logger.info("Original DataFrame:")
    # #     logger.info(test_df)

    # #     anonymized_df = anonymizer.anonymize_dataframe(test_df, ["body_analysed"])

    # #     logger.info("\nAnonymized DataFrame:")
    # #     logger.info(anonymized_df)

    # #     # Add diagnostic check for the DataFrame
    # #     if anonymized_df["body_analysed_anonymized"].equals(test_df["body_analysed"]):
    # #         logger.warning("\n" + "=" * 20 + " DIAGNOSTIC " + "=" * 20)
    # #         logger.warning("WARNING: Anonymized DataFrame column is identical to the original.")
    # #         logger.warning("Running inspection on the first row of the DataFrame...")
    # #         anonymizer.inspect_text(test_df["body_analysed"].iloc[0])
    # #         logger.warning("=" * 52)

    # #     logger.info("\n" + "=" * 50 + "\n")

    # #     # Generate and print a report
    # #     report = anonymizer.generate_report()
    # #     logger.info("--- Anonymization Report ---")
    # #     logger.info(report)

    # #     # Save the log
    # #     anonymizer.save_log("anonymization_log.json")

    # # logger.info("\nDeIdAnonymizer module execution finished.")
