import os
from pathlib import Path
from typing import List
import logging


logger = logging.getLogger(__name__)

class PathsClass:
    """Manages the creation and organization of directory paths for the pipeline."""

    def __init__(self, root_path: str, suffix: str, output_folder: str) -> None:
        """Initializes the PathsClass.

        Sets up and creates all necessary directory paths for a processing run.

        Args:
            root_path: The root directory path where all paths will be based.
            suffix: The suffix to be appended to each directory name for
                differentiation.
            output_folder: The name of the main output folder (e.g., 'outputs').

        Attributes:
            root_path (str): The provided root directory path.
            suffix (str): The provided suffix for directory names.
            all_paths (List[str]): A list of all generated absolute directory paths.
            output_folder_path (str): The absolute path to the main output
                directory.
        """
        self.root_path = root_path
        self.suffix = suffix

        # These paths are relative to the root_path
        relative_paths = [
            f"current_pat_annots_parts{self.suffix}/",
            f"current_pat_annots_mrc_parts{self.suffix}/",
            f"current_pat_documents_annotations_batches{self.suffix}/",
            f"current_pat_documents_annotations_batches_mct{self.suffix}/",
            f"current_pat_documents_annotations_batches_reports{self.suffix}/",
            f"current_pat_document_batches{self.suffix}/",
            f"current_pat_document_batches_mct{self.suffix}/",
            f"current_pat_document_batches_reports{self.suffix}/",
            f"current_pat_bloods_batches{self.suffix}/",
            f"current_pat_drugs_batches{self.suffix}/",
            f"current_pat_diagnostics_batches{self.suffix}/",
            f"current_pat_news_batches{self.suffix}/",
            f"current_pat_obs_batches{self.suffix}/",
            f"current_pat_bmi_batches{self.suffix}/",
            f"current_pat_demo_batches{self.suffix}/",
            # f'current_pat_misc_batches{self.suffix}/', #Dynamically created
            f"current_pat_lines_parts{self.suffix}/",
            f"current_pat_appointments_batches{self.suffix}/",
            f"current_pat_textual_obs_document_batches{self.suffix}/",
            f"current_pat_textual_obs_annotation_batches{self.suffix}/",
            f"merged_input_pat_batches{self.suffix}/",
        ]

        # Create absolute paths
        self.all_paths = [os.path.join(self.root_path, p) for p in relative_paths]

        # Handle the main output folder separately for clarity
        self.output_folder_path = os.path.join(self.root_path, output_folder)
        self.all_paths.append(self.output_folder_path)

        self._create_directories()
        self._print_paths()

    def _create_directories(self) -> None:
        """Creates directories from a list of paths.

        Args:
            paths: A list of relative directory paths to be created under the
                root path.
        """
        for path in self.all_paths:
            Path(path).mkdir(parents=True, exist_ok=True)

    def _print_paths(self) -> None:
        """Prints all created absolute paths for verification.

        This is intended for verification and debugging purposes to show the
        fully resolved paths that have been configured.
        """
        logger.info("Created and verified the following paths:")
        for path in sorted(self.all_paths):
            logger.info(path)
