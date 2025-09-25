import os
import sys
import logging
from typing import Any, Optional, Union

from pat2vec.util.get_dummy_data_medcat_annotation import dummy_CAT


logger = logging.getLogger(__name__)

def get_cat(config_obj: Any) -> Optional[Union["CAT", dummy_CAT]]:  # type: ignore
    """Loads and returns a MedCAT model instance based on configuration.

    This function determines which MedCAT model to load based on the provided

    This function determines which MedCAT model to load based on the provided
    configuration object. It supports several loading strategies:

    1.  **Dummy Model**: If `config_obj.testing` and `config_obj.dummy_medcat_model`
        are `True`, it returns a `dummy_CAT` instance for testing without a real model.
    2.  **Automatic Path Detection**: If `config_obj.override_medcat_model_path` is
        set to "auto", it searches `sys.path` for a `medcat_models` directory and
        loads the first `.zip` model pack found.
    3.  **Explicit Path**: It checks for a model path in this order:
        a.  `medcat_path` variable imported from a local `paths.py` file.
        b.  `config_obj.override_medcat_model_path`.
    4.  **No Model**: If `config_obj.medcat` is `False`, it returns `None`.

    Args:
        config_obj: The configuration object containing settings like `testing`,
            `medcat`, and model paths.

    Returns:
        An instance of a MedCAT `CAT` object, a `dummy_CAT` object for testing,
        or `None` if MedCAT processing is disabled.

    Raises:
        ValueError: If MedCAT is enabled but no valid model path can be found. # type: ignore
    """
    if config_obj.verbosity >= 1:
        logger.info(f"Override MedCAT model path: {config_obj.override_medcat_model_path}")

    if config_obj.testing:

        if config_obj.dummy_medcat_model:
            logger.info("Returning dummy_CAT for testing.")

            cat = dummy_CAT()

            return cat

    model_path = None

    if config_obj.medcat:

        medcat_path = None

        # Check if the file exists
        if os.path.exists("paths.py") and config_obj.override_medcat_model_path == None:
            if config_obj.verbosity >= 1:
                logger.info("paths.py file found, importing medcat_path...")
            # If the file exists, try to import the variable
            try:
                from paths import medcat_path

                logger.info(
                    "Variable 'medcat_path' imported successfully from 'paths.py' file."
                )
                # Now you can use medcat_path variable here
                logger.info(f"medcat_path from paths.py: {medcat_path}")
            except ImportError:
                logger.error("Error: Could not import 'medcat_path' from 'paths.py' file.")
        else:
            logger.info("The 'paths.py' file does not exist or is being overridden.")

        from medcat.cat import CAT

        path_found = False

        if config_obj.override_medcat_model_path == "auto":
            if config_obj.verbosity >= 1:
                logger.info("override_medcat_model_path is set to 'auto'. Searching sys.path.")
            # Search for 'medcat_models/' in each directory in sys.path
            for directory in sys.path:
                medcat_models_path = os.path.join(directory, "medcat_models")
                if os.path.exists(medcat_models_path):
                    files_in_dir = os.listdir(medcat_models_path)
                    zip_files = [file for file in files_in_dir if file.endswith(".zip")]
                    if zip_files:
                        model_path = os.path.join(medcat_models_path, zip_files[0])
                    else:
                        # Handle case where no zip files are found
                        logger.warning(f"No .zip files found in {medcat_models_path}.")
                else:
                    # Handle case where medcat_models_path doesn't exist
                    logger.debug(
                        f"Path does not exist: {medcat_models_path}"
                    )
                if model_path:
                    path_found = True # type: ignore
                    break # type: ignore
            else:
                logger.warning(
                    "Directory 'medcat_models/' not found in any directory in sys.path." # type: ignore
                )

        if path_found == False:
            if medcat_path is not None or medcat_path is "auto":
                model_path = medcat_path
            elif config_obj.override_medcat_model_path is not None:
                model_path = config_obj.override_medcat_model_path
            elif config_obj.testing:
                model_path = "medcat_models\medcat_model_pack_422d1d38fc58f158.zip"

        if model_path is not None:
            if config_obj.verbosity > 0:
                logger.info(f"Loading MedCAT model from: {model_path}")

            cat = CAT.load_model_pack(model_path)
            return cat
        else:
            # Handle the case where none of the conditions matched
            raise ValueError("No valid model path found in the configuration.")
    else:

        return None
