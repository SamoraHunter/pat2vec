import gc
import logging
from contextlib import contextmanager
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
    spacy = None  # type: ignore

try:
    from tqdm.auto import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


# Default PII labels from the MedCAT DeId model description.
# Used as a fallback when the loaded model's pii_labels attribute is empty,
# which can happen due to MedCAT version mismatches.
_DEFAULT_PII_LABELS = [
    "Address Line",
    "Date",
    "Date Of Birth",
    "Email",
    "Hospital Number",
    "Initials",
    "Name",
    "Nhs Number",
    "Postcode",
    "Telephone Number",
    "Gmc Number",
    "HCPC Number",
    "Accession Number",
]


@contextmanager
def suppress_gc_collect():
    """Neutralizes gc.collect() calls for the duration of the block.

    MedCAT's internal pipeline (medcat/pipe.py -> _ensure_serializable /
    serialize_entities) calls gc.collect() once per document. In long-running
    Python sessions with large objects already resident in memory (e.g. a big
    DataFrame), each of these calls walks the whole tracked object graph and
    can dominate runtime -- in testing this accounted for >90% of wall-clock
    time on a GPU-bound inference workload, making it look like the GPU was
    never being used when in fact it was idle waiting on gc.collect().

    Note: gc.disable() alone does NOT fix this, because MedCAT calls
    gc.collect() explicitly rather than relying on automatic triggering.
    This context manager patches gc.collect itself to a no-op instead.

    Safe for inference-only workloads (no long-lived reference cycles are
    being created here). If you run this over a very large DataFrame for a
    long time, prefer the chunked methods below, which call a real
    gc.collect() between chunks.
    """
    real_collect = gc.collect
    gc.collect = lambda *args, **kwargs: 0
    try:
        yield
    finally:
        gc.collect = real_collect


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
        self,
        model_path: Optional[Union[str, Path]] = None,
        log_level: str = "INFO",
        disable_chunking: bool = True,
        chunking_overlap_window: Optional[int] = None,
    ):
        """Initializes the DeIdAnonymizer.

        Args:
            model_path: Optional path to the MedCAT DeIdModel pack. If provided,
                the model is loaded upon initialization.
            log_level: The logging level for the instance (e.g., "INFO", "DEBUG").
            disable_chunking: If True (default), disables the chunking overlap
                window on the transformer NER component. Note: disabling chunking
                means only the first ~512 tokens of very long documents will be
                scanned for PII -- anything beyond that is NOT redacted, which is
                a real information-governance risk for long clinical notes.

                IMPORTANT: the hang risk this guards against is specifically tied
                to *multiprocessing* (n_process > 1) -- see the warning in
                MedCAT's own deid.py source. If you always call with n_process=1
                (recommended when using a GPU, since multiple processes fight
                over CUDA context anyway), you do NOT need to disable chunking.
                Consider setting disable_chunking=False and passing
                chunking_overlap_window explicitly, or call `enable_chunking()`
                after construction once you've confirmed n_process=1 works
                without hanging in your environment.
            chunking_overlap_window: If disable_chunking is False, sets this
                stride/overlap value (in tokens) on the transformer NER
                component so long documents are scanned in overlapping windows
                rather than truncated at ~512 tokens. A small positive value
                (e.g. 32-50) is a reasonable starting point. Ignored if
                disable_chunking is True.
        """
        self.model: Optional[Type[DeIdModel]] = None
        self.model_path = model_path
        self.is_loaded: bool = False
        self.pii_labels: List[str] = []
        self.anonymization_log: List[Dict[str, Any]] = []
        self.disable_chunking = disable_chunking
        self.chunking_overlap_window = chunking_overlap_window

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
            # FIX: assign the Path object so .exists() is available
            model_path = Path(model_path)
            if not model_path.exists():
                self.logger.error(f"Model path does not exist: {model_path}")
                return False

            self.logger.info(f"Loading DeIdModel from: {model_path}")
            self.model = DeIdModel.load_model_pack(str(model_path))
            self.model_path = model_path
            self.is_loaded = self.model is not None

            # Hotfix for a spaCy extension error that can occur with some MedCAT models.
            if not spacy.tokens.Span.has_extension("link_candidates"):
                self.logger.info(
                    "Registering missing 'link_candidates' spaCy extension."
                )
                spacy.tokens.Span.set_extension("link_candidates", default=[])

            # FIX: pii_labels is often empty due to MedCAT version mismatches.
            # Fall back to the known default labels from the model description.
            self.pii_labels = getattr(self.model, "pii_labels", [])
            if not self.pii_labels:
                self.logger.warning(
                    "Model's 'pii_labels' attribute is empty. "
                    "Falling back to default PII label list. "
                    "Override `anonymizer.pii_labels` if your model targets different concepts."
                )
                self.pii_labels = _DEFAULT_PII_LABELS
                self.model.pii_labels = self.pii_labels

            self.logger.info(
                f"Model configured to redact PII labels: {self.pii_labels}"
            )

            # FIX: disable chunking overlap to prevent multiprocessing hanging.
            # The warning from MedCAT itself recommends this approach.
            if self.disable_chunking:
                self._apply_chunking_fix()
            elif self.chunking_overlap_window is not None:
                # User explicitly wants chunking enabled with a given overlap
                # window -- apply it now (only safe with n_process=1; see
                # docstring on __init__ and enable_chunking()).
                self.enable_chunking(self.chunking_overlap_window)

            self._log_operation(
                "model_loaded",
                {"path": str(model_path), "pii_labels": self.pii_labels},
            )
            self.logger.info("DeIdModel loaded successfully")
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

    def _apply_chunking_fix(self) -> None:
        """Disables the chunking overlap window on the transformer NER component.

        This prevents multiprocessing from hanging, which is a known issue with
        some MedCAT model configurations. Disabling chunking means only the first
        ~512 tokens of long documents will be de-identified.

        The fix navigates model.cat._addl_ner[0] to reach the TransformersNER
        component where the chunking config lives.
        """
        try:
            ner = self.model.cat._addl_ner[0]
            ner.config.general.chunking_overlap_window = None
            ner.create_eval_pipeline()
            self.logger.info(
                "Chunking overlap window disabled on TransformersNER to prevent "
                "multiprocessing hang. Documents longer than ~512 tokens will only "
                "have their first ~512 tokens de-identified."
            )
        except (AttributeError, IndexError) as e:
            self.logger.warning(
                f"Could not apply chunking fix (model structure may differ): {e}. "
                "If processing hangs, inspect `model.cat._addl_ner` manually."
            )

    def enable_chunking(
        self, overlap_window: int = 32, silence_repeated_warning: bool = True
    ) -> None:
        """Enables chunking on the transformer NER component so long documents
        are scanned in overlapping windows instead of being truncated at ~512
        tokens.

        SAFETY NOTE: only use this with n_process=1 in all subsequent
        anonymize_* calls. The known hang risk documented in MedCAT's own
        deid.py source is specifically about *multiprocessing*
        (n_process > 1) combined with chunking -- not single-process use.
        If you need multiple processes, leave chunking disabled instead, or
        thoroughly test for hangs in your specific environment first.

        Args:
            overlap_window: The stride/overlap size in tokens. A small
                positive value (e.g. 32-50) works well for typical clinical
                notes. Must be >= 0 (None disables chunking).
            silence_repeated_warning: MedCAT's own deid_multi_texts() logs a
                warning about the multiprocessing hang risk on EVERY call --
                which, when processing a DataFrame in many chunks, means the
                same warning repeats constantly. Since this method's own
                log message above already communicates that information once,
                silence_repeated_warning=True (default) raises the level of
                MedCAT's specific 'medcat.utils.ner.deid' logger to ERROR so
                it stops repeating. This does not affect any other MedCAT
                logging. Set False to keep seeing MedCAT's own warning on
                every call.
        """
        self._check_model_loaded()
        try:
            ner = self.model.cat._addl_ner[0]
            ner.config.general.chunking_overlap_window = overlap_window
            ner.create_eval_pipeline()
            self.disable_chunking = False
            self.chunking_overlap_window = overlap_window
            self.logger.info(
                f"Chunking ENABLED with overlap_window={overlap_window}. "
                "Long documents will now be scanned in full via overlapping "
                "windows rather than truncated at ~512 tokens. "
                "Ensure all anonymize_* calls use n_process=1."
            )
            if silence_repeated_warning:
                logging.getLogger("medcat.utils.ner.deid").setLevel(logging.ERROR)
                self.logger.info(
                    "Silenced MedCAT's repeated per-call chunking warning "
                    "(medcat.utils.ner.deid logger set to ERROR level). "
                    "Call `silence_medcat_chunking_warning(False)` to restore it."
                )
        except (AttributeError, IndexError) as e:
            self.logger.error(
                f"Could not enable chunking (model structure may differ): {e}"
            )
            raise

    def silence_medcat_chunking_warning(self, silence: bool = True) -> None:
        """Toggles MedCAT's own repeated per-call chunking-hang warning.

        This warning is emitted by MedCAT itself (not this wrapper class)
        every time deid_multi_texts() is called while chunking is enabled --
        which becomes noisy across many DataFrame chunks. This only affects
        the specific 'medcat.utils.ner.deid' logger, not other MedCAT logging.

        Args:
            silence: If True, raises that logger to ERROR level (hiding the
                warning). If False, restores it to WARNING level.
        """
        level = logging.ERROR if silence else logging.WARNING
        logging.getLogger("medcat.utils.ner.deid").setLevel(level)
        self.logger.info(
            f"MedCAT chunking warning {'silenced' if silence else 'restored'} "
            f"(medcat.utils.ner.deid logger set to {logging.getLevelName(level)})."
        )

    def get_gpu_status(self) -> Dict[str, Any]:
        """Reports whether the underlying transformer NER model is on GPU.

        Returns:
            A dictionary with device info for the transformer component,
            or an explanatory message if it could not be determined.
        """
        self._check_model_loaded()
        try:
            ner = self.model.cat._addl_ner[0]
            info: Dict[str, Any] = {}
            if hasattr(ner, "ner_pipe"):
                info["ner_pipe_device"] = str(ner.ner_pipe.device)
            try:
                import torch

                info["model_param_device"] = str(next(ner.model.parameters()).device)
                info["cuda_available"] = torch.cuda.is_available()
                if torch.cuda.is_available():
                    info["cuda_device_name"] = torch.cuda.get_device_name(0)
                    info["cuda_memory_allocated_mb"] = round(
                        torch.cuda.memory_allocated(0) / 1024**2, 1
                    )
            except ImportError:
                pass
            return info
        except (AttributeError, IndexError) as e:
            return {"error": f"Could not determine GPU status: {e}"}

    def _check_model_loaded(self) -> None:
        """Checks if a model is loaded, raising a RuntimeError if not."""
        if not self.is_loaded or self.model is None:
            raise RuntimeError(
                "DeIdModel not loaded. Please call load_model() first or "
                "provide model_path during initialization."
            )

    def _log_operation(self, operation: str, details: Dict[str, Any]) -> None:
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
        r"""Anonymizes a single text string.

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
        suppress_gc: bool = True,
    ) -> Union[List[str], Tuple[List[str], Dict]]:
        """Anonymizes a list of text strings, with parallel processing support.

        Args:
            texts: A list of input texts to anonymize.
            redact: If True, replaces PII with asterisks. If False, uses type tags.
            n_process: The number of processes to use for parallel execution.
                Defaults to 1. Use caution with values > 1 if chunking is enabled.
            batch_size: The number of texts to process in each batch.
            verify_sample: If True, verifies a random sample of the results and
                returns a report.
            sample_size: The size of the random sample to verify if `verify_sample`
                is True.
            suppress_gc: If True (default), neutralizes MedCAT's internal
                per-document gc.collect() calls for the duration of this call.
                This can be a 10-20x speedup in long-running sessions with large
                objects in memory, since gc.collect() cost scales with total
                tracked objects, not just this operation's own allocations.
                Safe for inference; set False if you suspect memory growth from
                reference cycles on a very long-running process.

        Returns:
            If `verify_sample` is False, returns a list of anonymized texts.
            If `verify_sample` is True, returns a tuple of
            (anonymized_texts, verification_report).
        """
        self._check_model_loaded()

        try:
            ctx = suppress_gc_collect() if suppress_gc else _null_context()
            with ctx:
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
                    "suppress_gc": suppress_gc,
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
        text_columns: Union[str, List[str]],
        redact: bool = True,
        inplace: bool = False,
        suffix: str = "_anonymized",
        n_process: int = 1,
        batch_size: int = 100,
        suppress_gc: bool = True,
        quiet_medcat_progress: bool = False,
    ) -> pd.DataFrame:
        """Anonymizes specified text columns in a pandas DataFrame.

        For very large DataFrames where you want progress visibility and
        crash-safety, prefer `anonymize_dataframe_chunked` instead -- this
        method processes everything in one pass with no intermediate
        checkpointing.

        Args:
            df: The input DataFrame.
            text_columns: A column name (str) or list of column names containing
                the text to be anonymized.
            redact: If True, replaces PII with asterisks. If False, uses type tags.
            inplace: If True, modifies the DataFrame in place by overwriting the
                original text columns. If False, returns a new DataFrame with
                anonymized columns added.
            suffix: The suffix to add to new anonymized column names. Ignored
                if `inplace` is True.
            n_process: The number of processes for parallel execution. Keep at 1
                if using a GPU -- multiprocessing workers each try to grab CUDA
                context independently, which causes conflicts/CPU fallback.
            batch_size: The number of texts to process in each batch.
            suppress_gc: If True (default), neutralizes MedCAT's internal
                per-document gc.collect() calls for the duration of this call.
                See `suppress_gc_collect` docstring for details. This is often
                the difference between the run appearing to hang at 0% GPU
                utilization and running at full GPU utilization.
            quiet_medcat_progress: If True, silences MedCAT's own internal
                tqdm bar (one per column). Default False here since this
                method only makes one call per column (not per-chunk), so
                there's less bar-spam than in anonymize_dataframe_chunked.

        Returns:
            A DataFrame with the specified text columns anonymized.
        """
        self._check_model_loaded()

        # FIX: accept a plain string column name as well as a list
        if isinstance(text_columns, str):
            text_columns = [text_columns]

        # Validate columns exist
        missing_cols = [col for col in text_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

        result_df = df if inplace else df.copy()
        total_texts_processed = 0

        gc_ctx = suppress_gc_collect() if suppress_gc else _null_context()
        progress_ctx = (
            suppress_medcat_progress() if quiet_medcat_progress else _null_context()
        )
        with gc_ctx, progress_ctx:
            for col in text_columns:
                self.logger.info(f"Anonymizing column: {col}")

                texts_to_process = df[col].fillna("").astype(str).tolist()
                total_texts_processed += len(texts_to_process)

                anonymized_texts = self.model.deid_multi_texts(
                    texts_to_process,
                    redact=redact,
                    n_process=n_process,
                    batch_size=batch_size,
                )

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
                "suppress_gc": suppress_gc,
            },
        )

        return result_df

    def anonymize_dataframe_chunked(
        self,
        df: pd.DataFrame,
        text_columns: Union[str, List[str]],
        redact: bool = True,
        suffix: str = "_anonymized",
        n_process: int = 1,
        batch_size: int = 100,
        chunk_size: int = 500,
        suppress_gc: bool = True,
        checkpoint_dir: Optional[Union[str, Path]] = None,
        checkpoint_prefix: str = "deid_chunk",
        show_progress: bool = True,
        quiet_medcat_progress: bool = True,
        resume_from_checkpoint: bool = True,
    ) -> pd.DataFrame:
        """Anonymizes a large DataFrame in row-chunks, with progress and optional
        checkpointing to disk.

        Recommended over `anonymize_dataframe` for large datasets: MedCAT's
        internal batching (by character count, via `pipe_batch_size_in_chars`)
        can cause its own progress bar to stay at 0% for the entire run even
        though it's working correctly. Processing in explicit row-chunks here
        gives you a progress bar that actually ticks, periodic real garbage
        collection between chunks, and (optionally) resumable checkpoints.

        Args:
            df: The input DataFrame. Always processed in a new copy (inplace
                is not supported here since chunk results are concatenated).
            text_columns: A column name (str) or list of column names to anonymize.
            redact: If True, replaces PII with asterisks. If False, uses type tags.
            suffix: Suffix appended to new anonymized column names.
            n_process: Number of processes for parallel execution. Keep at 1
                if using a GPU.
            batch_size: Batch size passed through to MedCAT's deid_multi_texts.
            chunk_size: Number of DataFrame rows processed per chunk. Smaller
                = more frequent progress updates and checkpoints, more gc
                overhead between chunks. Tune based on average text length;
                500 is a reasonable starting point for typical clinical notes.

                IMPORTANT: if resuming across runs, chunk_size, chunk_size,
                and the row order/length of `df` must match the original run
                exactly, since chunk boundaries (and therefore checkpoint
                filenames) are derived from row position. Don't reorder,
                filter, or resize `df` between an interrupted run and its
                resume, or chunk N will no longer correspond to the same rows.
            suppress_gc: If True (default), neutralizes gc.collect() calls
                *within* each chunk's processing (a real gc.collect() still
                runs *between* chunks regardless, for cleanup).
            checkpoint_dir: If provided, each completed chunk is written to this
                directory as a parquet file, allowing you to resume or recover
                partial results if the run is interrupted. If None, no
                checkpointing occurs and results are only concatenated in memory.
            checkpoint_prefix: Filename prefix used for checkpoint parquet files.
            show_progress: If True and tqdm is available, displays a chunk-level
                progress bar.
            quiet_medcat_progress: If True (default), silences MedCAT's own
                internal per-call tqdm bar so only this method's chunk-level
                bar is visible. Otherwise you'll see a new short-lived bar
                flash by for every column, in every chunk.
            resume_from_checkpoint: If True (default) and checkpoint_dir is
                given, any chunk whose checkpoint file already exists on disk
                is loaded from that file instead of being recomputed. This is
                what makes checkpointing actually useful for crash recovery --
                re-running the same call after an interruption will skip
                everything already completed and pick up where it left off.
                Set False to force full recomputation even if checkpoints
                exist (e.g. if you changed the model or redact settings and
                want a clean re-run).

        Returns:
            A new DataFrame with the specified text columns anonymized
            (original rows preserved, plus new `<col><suffix>` columns).
        """
        self._check_model_loaded()

        if isinstance(text_columns, str):
            text_columns = [text_columns]

        missing_cols = [col for col in text_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

        if checkpoint_dir is not None:
            checkpoint_dir = Path(checkpoint_dir)
            checkpoint_dir.mkdir(parents=True, exist_ok=True)

        n_rows = len(df)
        n_chunks = (n_rows + chunk_size - 1) // chunk_size
        chunk_ranges = list(range(0, n_rows, chunk_size))

        iterator = enumerate(chunk_ranges)
        if show_progress and TQDM_AVAILABLE:
            iterator = tqdm(
                iterator,
                total=len(chunk_ranges),
                desc="Anonymizing chunks",
                unit="chunk",
            )
        elif show_progress and not TQDM_AVAILABLE:
            self.logger.warning("tqdm not installed; proceeding without progress bar.")

        results = []
        total_texts_processed = 0
        n_resumed = 0
        n_computed = 0

        for i, start in iterator:
            expected_len = min(chunk_size, n_rows - start)
            checkpoint_path = (
                checkpoint_dir / f"{checkpoint_prefix}_{i:05d}.parquet"
                if checkpoint_dir is not None
                else None
            )

            if (
                resume_from_checkpoint
                and checkpoint_path is not None
                and checkpoint_path.exists()
            ):
                try:
                    cached_chunk = pd.read_parquet(checkpoint_path)
                    expected_cols = set(df.columns) | {
                        f"{col}{suffix}" for col in text_columns
                    }
                    if len(cached_chunk) == expected_len and expected_cols.issubset(
                        set(cached_chunk.columns)
                    ):
                        results.append(cached_chunk)
                        n_resumed += 1
                        continue
                    else:
                        self.logger.warning(
                            f"Checkpoint {checkpoint_path.name} exists but doesn't "
                            "match expected shape/columns for this run -- "
                            "recomputing this chunk instead of trusting it."
                        )
                except Exception as e:
                    self.logger.warning(
                        f"Could not read checkpoint {checkpoint_path.name} "
                        f"({e}) -- recomputing this chunk."
                    )

            chunk = df.iloc[start : start + chunk_size].copy()

            # Ensure all columns are serializable to parquet by converting
            # all object columns to string. This prevents Arrow conversion errors
            # from mixed types (e.g., strings and NaN as float).
            for col in chunk.columns:
                if chunk[col].dtype == object:
                    chunk[col] = chunk[col].astype(str)

            gc_ctx = suppress_gc_collect() if suppress_gc else _null_context()
            progress_ctx = (
                suppress_medcat_progress() if quiet_medcat_progress else _null_context()
            )
            with gc_ctx, progress_ctx:
                for col in text_columns:
                    texts_to_process = chunk[col].fillna("").astype(str).tolist()
                    total_texts_processed += len(texts_to_process)

                    anonymized_texts = self.model.deid_multi_texts(
                        texts_to_process,
                        redact=redact,
                        n_process=n_process,
                        batch_size=batch_size,
                    )
                    chunk[f"{col}{suffix}"] = anonymized_texts

            if checkpoint_path is not None:
                chunk.to_parquet(checkpoint_path, index=False)

            results.append(chunk)
            n_computed += 1

            # Real cleanup between chunks (not the neutralized version).
            gc.collect()

        result_df = pd.concat(results, ignore_index=True)

        self._log_operation(
            "dataframe_chunked",
            {
                "columns": text_columns,
                "rows": n_rows,
                "total_texts": total_texts_processed,
                "redact": redact,
                "chunk_size": chunk_size,
                "n_chunks": n_chunks,
                "n_resumed_from_checkpoint": n_resumed,
                "n_computed": n_computed,
                "suppress_gc": suppress_gc,
                "checkpoint_dir": str(checkpoint_dir) if checkpoint_dir else None,
            },
        )

        if n_resumed:
            self.logger.info(
                f"Resumed {n_resumed}/{n_chunks} chunks from existing checkpoints; "
                f"computed {n_computed} new chunk(s)."
            )

        return result_df

    def inspect_text(self, text: str) -> List[Dict[str, Any]]:
        """Inspects text to find and log PII entities without anonymizing.

        Args:
            text: The text to inspect.

        Returns:
            A list of dictionaries, each representing a found PII entity.
        """
        self._check_model_loaded()
        self.logger.info("Inspecting text for PII entities...")
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
            A list of dictionaries with details (text, label, start, end,
            confidence) for each identified PII entity.
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
                        "confidence": getattr(ent, "_.acc", None),
                    }
                )

            return entities

        except Exception as e:
            self.logger.error(f"Error getting structured annotations: {e}")
            raise

    def _verify_single_text(self, original: str, anonymized: str) -> Dict[str, Any]:
        """Verifies anonymization quality for a single text."""
        entities = self.get_structured_annotations(original)

        return {
            "entities_found": len(entities),
            "entity_types": list(set(ent["label"] for ent in entities)),
            "original_length": len(original),
            "anonymized_length": len(anonymized),
            "entities": entities,
        }

    def _verify_multiple_texts(
        self, original_texts: List[str], anonymized_texts: List[str], sample_size: int
    ) -> Dict[str, Any]:
        """Verifies anonymization quality for a sample of multiple texts."""
        import random

        indices = random.sample(
            range(len(original_texts)), min(sample_size, len(original_texts))
        )

        total_entities = 0
        all_entity_types: set = set()

        for i in indices:
            verification = self._verify_single_text(
                original_texts[i], anonymized_texts[i]
            )
            total_entities += verification["entities_found"]
            all_entity_types.update(verification["entity_types"])

        return {
            "sample_size": len(indices),
            "total_texts": len(original_texts),
            "total_entities_in_sample": total_entities,
            "unique_entity_types": list(all_entity_types),
            "avg_entities_per_text": total_entities / len(indices) if indices else 0,
        }

    def generate_report(self) -> Dict[str, Any]:
        """Generates a summary report of all operations performed.

        Returns:
            A dictionary containing statistics about the anonymization
            operations, model details, and total texts processed.
        """
        if not self.anonymization_log:
            return {"message": "No anonymization operations performed yet"}

        operation_counts: Dict[str, int] = {}
        for log_entry in self.anonymization_log:
            op_type = log_entry["operation"]
            operation_counts[op_type] = operation_counts.get(op_type, 0) + 1

        total_texts = 0
        for log_entry in self.anonymization_log:
            if log_entry["operation"] == "single_text":
                total_texts += 1
            elif log_entry["operation"] in (
                "multiple_texts",
                "dataframe",
                "dataframe_chunked",
            ):
                total_texts += log_entry["details"].get("count") or log_entry[
                    "details"
                ].get("total_texts", 0)

        return {
            "model_path": str(self.model_path) if self.model_path else None,
            "model_loaded": self.is_loaded,
            "pii_labels_in_use": self.pii_labels,
            "chunking_disabled": self.disable_chunking,
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

    def save_log(self, filepath: Union[str, Path]) -> None:
        """Saves the anonymization operation log to a JSON file.

        Args:
            filepath: The path where the log file will be saved.
        """
        import json

        filepath = Path(filepath)

        log_data = []
        for entry in self.anonymization_log:
            entry_copy = entry.copy()
            entry_copy["timestamp"] = entry_copy["timestamp"].isoformat()
            log_data.append(entry_copy)

        with open(filepath, "w") as f:
            json.dump(log_data, f, indent=2)

        self.logger.info(f"Anonymization log saved to: {filepath}")


@contextmanager
def suppress_medcat_progress():
    """Silences MedCAT's own internal tqdm progress bar.

    medcat/cat.py's get_entities_multi_texts() wraps its document loop in its
    own tqdm() call. When you call deid_multi_texts repeatedly yourself (e.g.
    once per chunk, per column, in anonymize_dataframe_chunked), each call
    spawns a *new* short-lived progress bar, which looks like a flicker of
    unrelated bars appearing every N rows alongside your own chunk-level bar.
    This patches medcat.cat.tqdm to a passthrough no-op for the duration of
    the block so only your own progress bar is visible.

    This only affects the medcat.cat module's tqdm reference (found by
    inspecting its namespace at call-time), so other tqdm usages elsewhere
    in your code/session are unaffected.
    """
    try:
        import medcat.cat as _medcat_cat_module
    except ImportError:
        # MedCAT not importable in this environment -- nothing to patch.
        yield
        return

    has_tqdm_attr = hasattr(_medcat_cat_module, "tqdm")
    real_tqdm = getattr(_medcat_cat_module, "tqdm", None)

    def _passthrough_tqdm(iterable=None, *args, **kwargs):
        return iterable if iterable is not None else iter([])

    if has_tqdm_attr:
        _medcat_cat_module.tqdm = _passthrough_tqdm
    try:
        yield
    finally:
        if has_tqdm_attr:
            _medcat_cat_module.tqdm = real_tqdm


@contextmanager
def _null_context():
    """A no-op context manager, used when suppress_gc=False."""
    yield


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------


def anonymize_single_text(
    text: str, model_path: Union[str, Path], redact: bool = True
) -> str:
    """Quickly anonymize a single text string.

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
    text_columns: Union[str, List[str]],
    model_path: Union[str, Path],
    redact: bool = True,
    suffix: str = "_anonymized",
    inplace: bool = False,
    n_process: int = 1,
    batch_size: int = 100,
) -> pd.DataFrame:
    """Quickly anonymize one or more columns in a DataFrame.

    Args:
        df: The input DataFrame.
        text_columns: A column name (str) or list of column names to anonymize.
        model_path: The path to the DeIdModel pack.
        redact: If True, replaces PII with asterisks. If False, uses type tags.
        suffix: Suffix appended to new anonymized column names (ignored if inplace).
        inplace: If True, overwrites the original columns instead of adding new ones.
        n_process: Number of processes for parallel execution (default 1).
        batch_size: Number of texts per batch (default 100).

    Returns:
        A DataFrame with the specified text columns anonymized.

    Example:
        >>> result = anonymize_dataframe_quick(
        ...     df,
        ...     text_columns='body_analysed',
        ...     model_path='/path/to/model.zip',
        ... )
    """
    anonymizer = DeIdAnonymizer(model_path)
    return anonymizer.anonymize_dataframe(
        df,
        text_columns=text_columns,
        redact=redact,
        suffix=suffix,
        inplace=inplace,
        n_process=n_process,
        batch_size=batch_size,
    )
