

from pathlib import Path

from post_processing import retrieve_pat_annots_mct_epr
from tqdm import tqdm


def build_merged_epr_mct_annot_df(config_obj):
    """
    Build a merged DataFrame containing annotations for multiple patients using MCT and EPR data.

    Parameters:
    - config_obj (ConfigObject): An object containing configuration settings, including project name,
                                patient list, etc.

    Returns:
    None

    This function creates a directory for merged batches, retrieves annotations for each patient,
    and writes the merged annotations to a CSV file named 'annots_mct_epr.csv'.
    If the output file already exists, subsequent batches are appended to it.

    Example usage:
    ```
    config = ConfigObject(...)  # Create or load your configuration object
    build_merged_epr_mct_annot_df(config)
    ```

    """
    directory_path = config_obj.proj_name + "/" + "merged_batches/"
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    output_file_path = directory_path + 'annots_mct_epr.csv'

    all_pat_list = config_obj.all_pat_list

    for i in tqdm(range(0, len(all_pat_list)), total=len(all_pat_list)):
        current_pat_idcode = all_pat_list[i]
        all_annots = retrieve_pat_annots_mct_epr(
            current_pat_idcode, config_obj)

        if i == 0:
            # Create the output file and write the first batch directly
            all_annots.to_csv(output_file_path, index=False)
        else:
            # Append each result to the output file
            all_annots.to_csv(output_file_path, mode='a',
                              header=False, index=False)
