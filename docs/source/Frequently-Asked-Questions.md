# Frequently Asked Questions (FAQ)

This page answers common questions about setting up and using `pat2vec`.

---

## General

### What is `pat2vec`?

`pat2vec` is a Python-based tool designed to transform raw electronic health records (EHR) into structured, time-series feature vectors. This process makes the data suitable for machine learning tasks, particularly binary classification. It can aggregate data at the patient level or construct detailed longitudinal timelines.

---

## Installation & Setup

### I'm behind a corporate proxy. How do I install?

The `install_pat2vec.sh` script for Unix/Linux includes a `--proxy` flag specifically for this purpose. This flag tells `pip` to use your organization's internal package mirror.

```shell
./install_pat2vec.sh --proxy
```

If you are using Windows or the basic `install.sh` script, you will need to configure `pip` to use your proxy manually. This is typically done by setting the `http_proxy` and `https-proxy` environment variables or by creating and configuring a `pip.conf`/`pip.ini` file.

### Where do I get a MedCAT model and where do I put it?

You need to have a pre-trained MedCAT model pack (`.zip` file). These are typically pretrained trained and then fine tuned with exports from MedCAT trainer for your specific use case and data.

Once you have the model pack, place it in the `medcat_models/` directory, which should be in the same parent folder as your `pat2vec` repository clone. The installation script creates this directory for you. See https://github.com/CogStack/MedCAT.

```
your_project_folder/
├── medcat_models/
│   └── your_model.zip  <-- Place it here
└── pat2vec/
```

### Where do my Elasticsearch credentials go?

Your credentials should be placed in a file named `credentials.py` in the parent directory of your `pat2vec` clone. The `install_pat2vec.sh` script automatically copies a template for you. If you installed manually, you can copy `pat2vec/pat2vec/config/credentials_template.py` to the parent directory and edit it.

**IMPORTANT**: This file contains sensitive information and should **never** be committed to version control. The root `.gitignore` file of this project should already be configured to ignore `credentials.py`.

The structure should look like this:
```
your_project_folder/
├── credentials.py      <-- Edit this file
└── pat2vec/
```

### What is the `snomed_methods` repository?

`snomed_methods` is a helper repository containing utility functions and methods related to SNOMED-CT and other clinical terminologies used in conjunction with this project. It is a dependency for certain feature extraction methods and is cloned automatically by the `install_pat2vec.sh` script.

### The installation script failed. What should I do?

1.  **Check Python Version**: Ensure you are using Python 3.10 or higher.
2.  **Check `venv`**: Make sure the `python3-venv` package (or your OS equivalent) is installed.
3.  **Run with `--force`**: If you have a partially completed or corrupted installation, try running the script again with the `--force` flag. This will remove the existing `pat2vec_env` directory and perform a clean installation.
    ```shell
    ./install_pat2vec.sh --force
    ```
4.  **Check Permissions**: Ensure you have write permissions in the directory where you are running the script. The script needs to create directories and files one level above the `pat2vec` directory.
5.  **Review Logs**: Read the error messages in the terminal carefully. They often point to the exact package or command that failed.

---

## Usage

### What format does my input data need to be in?

Your primary input should be a CSV file. The only strict requirement is that this file must contain a column named `client_idcode` which holds the unique identifiers for each patient in your cohort.

If you are performing time-series analysis, you will also need a column containing the reference date for each patient (e.g., a diagnosis date) to align the data correctly.

### How do I choose which features to extract?

How do I choose which features to extract?
Feature extraction is controlled via the main_options_dict dictionary in your configuration file. Each feature type can be enabled or disabled by setting it to True or False. The modular design allows you to easily enable or disable features based on your research needs.
Example configuration snippet:


```python

main_options_dict = {
    'demo': True,           # Enable demographic information
    'bmi': True,            # Enable BMI information
    'bloods': True,         # Enable blood-related information
    'drugs': True,          # Enable drug-related information
    'diagnostics': True,    # Enable diagnostic information
    'core_02': True,        # Enable core_02 information
    'bed': True,            # Enable bed information
    'vte_status': True,     # Enable VTE status information
    'hosp_site': True,      # Enable hospital site information
    'core_resus': True,     # Enable core resuscitation information
    'news': True,           # Enable NEWS (National Early Warning Score)
    'smoking': True,        # Enable smoking-related information
    'annotations': True,    # Enable EPR document annotations via MedCat
    'annotations_mrc': True,# Enable MRC annotations via MedCat
    'negated_presence_annotations': False,  # Disable negated presence annotations
    'appointments': False,  # Disable appointments information
    'annotations_reports': False,  # Disable reports information
    'textual_obs': False,   # Disable textual observations
}
```
# Pass this dictionary to your config_class
config_obj = config_class(
    main_options=main_options_dict,
    # ... other configuration parameters
)
This dictionary is then passed to the config_class constructor via the main_options parameter to control which features are extracted during processing.
