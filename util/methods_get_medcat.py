

def get_cat(config_obj):
    model_path = None

    if (config_obj.medcat):

        from medcat.cat import CAT
        if(config_obj.override_medcat_model_path is not None):
            model_path = config_obj.override_medcat_model_path
        elif config_obj.testing:
            model_path = 'medcat_models\medcat_model_pack_422d1d38fc58f158.zip'
        elif config_obj.aliencat:
            model_path = '/home/aliencat/samora/HFE/HFE/medcat_models/medcat_model_pack_316666b47dfaac07.zip'
        elif config_obj.dgx:
            model_path = '/data/AS/Samora/HFE/HFE/v18/medcat_models/20230328_trained_model_hfe_redone/medcat_model_pack_316666b47dfaac07'
        elif config_obj.dhcap:
            model_path = '/home/jovyan/work/medcat_models/medcat_model_pack_316666b47dfaac07.zip'
        elif config_obj.dhcap02:
            model_path = '/home/cogstack/samora/_data/medcat_models/medcat_model_pack_316666b47dfaac07.zip'

        if model_path:
            if (config_obj.verbosity > 0):
                print(f"Loading medcat model at {model_path}")

            cat = CAT.load_model_pack(model_path)
            return cat
        else:
            # Handle the case where none of the conditions matched
            raise ValueError("No valid model path found in the configuration.")
    else:

        return None
