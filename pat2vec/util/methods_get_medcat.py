import os
import sys


def get_cat(config_obj):
    model_path = None

    if config_obj.medcat:

        medcat_path = None

        # Check if the file exists
        if os.path.exists("paths.py"):
            # If the file exists, try to import the variable
            try:
                from paths import medcat_path

                print(
                    "Variable 'medcat_path' imported successfully from 'paths.py' file."
                )
                # Now you can use medcat_path variable here
                print("medcat_path:", medcat_path)
            except ImportError:
                print("Error: Could not import 'medcat_path' from 'paths.py' file.")
        else:
            print("The 'paths.py' file does not exist.")

        from medcat.cat import CAT

        path_found = False

        if medcat_path == "auto":

            # Search for 'medcat_models/' in each directory in sys.path
            for directory in sys.path:
                medcat_models_path = os.path.join(directory, "medcat_models")
                if os.path.exists(medcat_models_path):
                    model_path = medcat_models_path
                    print(
                        "auto selected: Path to 'medcat_models/':", medcat_models_path
                    )
                    path_found = True
                    break
            else:
                print(
                    "Directory 'medcat_models/' not found in any directory in sys.path."
                )

        if path_found == False:
            if medcat_path is not None and medcat_path is not "auto":
                model_path = medcat_path
            elif config_obj.override_medcat_model_path is not None:
                model_path = config_obj.override_medcat_model_path
            elif config_obj.testing:
                model_path = "medcat_models\medcat_model_pack_422d1d38fc58f158.zip"
            elif config_obj.aliencat:
                model_path = "/home/aliencat/samora/HFE/HFE/medcat_models/medcat_model_pack_316666b47dfaac07.zip"
            elif config_obj.dgx:
                model_path = "/data/AS/Samora/HFE/HFE/v18/medcat_models/20230328_trained_model_hfe_redone/medcat_model_pack_316666b47dfaac07"
            elif config_obj.dhcap:
                model_path = "/home/jovyan/work/medcat_models/medcat_model_pack_316666b47dfaac07.zip"
            elif config_obj.dhcap02:
                model_path = "/home/samorah/_data/medcat_models/medcat_model_pack_316666b47dfaac07.zip"

        if model_path:
            if config_obj.verbosity > 0:
                print(f"Loading medcat model at {model_path}")

            cat = CAT.load_model_pack(model_path)
            return cat
        else:
            # Handle the case where none of the conditions matched
            raise ValueError("No valid model path found in the configuration.")
    else:

        return None
