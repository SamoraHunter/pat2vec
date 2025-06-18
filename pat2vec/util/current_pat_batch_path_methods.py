import os
from pathlib import Path


class PathsClass:
    def __init__(self, root_path, suffix, output_folder):
        """
        Initializes the PathsClass with specified root path, suffix, and output folder.

        Parameters:
            root_path (str): The root directory path where all paths will be based.
            suffix (str): The suffix to be appended to each directory name for differentiation.
            output_folder (str): The specific output folder where results will be stored.

        Attributes:
            root_path (str): Stores the provided root directory path.
            suffix (str): Stores the provided suffix for directory names.
            paths (list): A list of paths generated using the suffix, including paths for
                        annotations, documents, batches, and more.
            output_folder (str): The output directory path, appended to the paths list.

        The method also ensures that all directories in the paths list are created and
        prints the paths for verification.
        """

        self.root_path = root_path
        self.suffix = suffix

        self.paths = [
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
            output_folder,
        ]

        self.output_folder = os.path.join(self.root_path, "outputs")
        self.paths.append(self.output_folder)

        self._create_directories(self.paths)
        self._print_paths()

    def _create_directories(self, paths):
        for path in paths:
            Path(os.path.join(self.root_path, path)).mkdir(parents=True, exist_ok=True)

    def _print_paths(self):
        for attribute in dir(self):
            if "path" in attribute:
                # print(getattr(self, attribute))
                print(os.path.join(self.root_path, str(getattr(self, attribute))))
