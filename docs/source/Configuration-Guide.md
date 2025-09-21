# Comprehensive Configuration Guide for pat2vec

Configuration for pat2vec is managed at multiple levels: during installation, through files in your project directory, and at runtime within your analysis notebooks. This guide provides a detailed overview of all configuration options.

## 1. Installation-Time Configuration

The `install_pat2vec.sh` script prepares your environment. You can customize its behavior with the following command-line flags:

| Flag | Alias | Description |
|------|-------|-------------|
| `--proxy` | `-p` | Configures pip to use a corporate proxy/mirror for installing Python packages. |
| `--dev` | | Installs development dependencies like pytest and nbmake, which are required for running tests and contributing to the project. |
| `--all` | `-a` | Installs all optional dependencies required for every feature extractor. The default "lite" installation only includes core dependencies. |
| `--force` | `-f` | Performs a clean installation by removing any existing pat2vec_env virtual environment. |
| `--no-clone` | | Skips cloning the snomed_methods helper repository. Use this if you already have it. |

### Example Usage

To set up a full development environment behind a corporate proxy, you would run:

```bash
./install_pat2vec.sh --proxy --dev --all
```

## 2. Environment and File-Based Configuration

The installation script sets up a specific directory structure and creates several configuration files that you must edit. The pipeline expects this layout to be in the parent directory of your pat2vec clone.

### Project Directory Structure

```
your_project_folder/
├── credentials.py              # <-- MUST EDIT: Your Elasticsearch credentials
├── medcat_models/
│   └── your_model.zip          # <-- MUST ADD: Your MedCAT model pack
├── snomed_methods/             # <-- Cloned automatically
└── pat2vec/                    # <-- This repository
    └── ...
```

> **Note:** The `install_pat2vec.sh` script also creates a `notebooks/paths.py` file, but it is now recommended to set the MedCAT model path directly in `config_class` using `override_medcat_model_path` for better clarity.

### Key Configuration Files

#### credentials.py

- **Location:** `your_project_folder/credentials.py`
- **Purpose:** Stores your sensitive Elasticsearch credentials (host, username, password).
- **Setup:** The `install_pat2vec.sh` script copies a template to this location. You must edit this file to add your actual credentials. The path to this file can be specified in `config_class` with the `credentials_path` argument.
- **Security:** This file is critical and should never be committed to version control.

#### medcat_models/ directory

- **Location:** `your_project_folder/medcat_models/`
- **Purpose:** Stores your pre-trained MedCAT model packs (.zip files) used for clinical text annotation.
- **Setup:** The installation script creates this directory. You must manually place your model pack(s) inside it.

## 3. Runtime Configuration (config_class)

The `config_class` is the central Python object used within your Jupyter notebook (e.g., `example_usage.ipynb`) to control the behavior of a pipeline run. It is highly detailed, allowing for fine-grained control over the entire process.

### Project and Path Configuration

These parameters define the project's file structure and I/O.

- **`proj_name` (str):** The name of your project. This is used to create a root directory for all outputs (e.g., `new_project/`).
- **`suffix` (str):** An optional suffix appended to output sub-folders, allowing you to distinguish between different runs within the same project (e.g., `_run1`).
- **`treatment_doc_filename` (str):** The path to your input CSV file containing the initial patient cohort. This file must contain a column with patient identifiers.
- **`patient_id_column_name` (str):** The name of the column in your cohort CSV that contains the unique patient identifiers (default: `'client_idcode'`).
- **`root_path` (str):** The absolute path to the project's root output directory. If not set, it defaults to `os.getcwd()/proj_name/`.
- **`override_medcat_model_path` (str):** The direct path to the MedCAT model pack (.zip) you want to use. This is the recommended way to specify the model.

### Execution and Operational Control

These flags control how the pipeline runs, its verbosity, and its behavior for testing and performance.

- **`testing` (bool):** Set to `True` to run in testing mode, which uses dummy data generators for rapid debugging. Set to `False` for production runs.
- **`strip_list` (bool):** If `True` (default), the pipeline checks for already processed patients and skips them to avoid redundant work.
- **`verbosity` (int):** Sets the logging level (0-9). A higher number provides more detailed output. 3 is a good default.
- **`random_seed_val` (int):** A seed for random operations to ensure reproducibility.
- **`calculate_vectors` (bool):** If `True` (default), the pipeline generates the final feature vector CSVs. If `False`, it only pre-fetches and saves the raw data batches, which can be useful for debugging the data extraction step.
- **`prefetch_pat_batches` (bool):** If `True`, all raw data for the entire cohort is fetched and stored in memory before processing begins. This can speed up processing but requires significant RAM. It is not compatible with `individual_patient_window`.

### Temporal Window Configuration

This is one of the most critical parts of the configuration, defining how patient data is sliced over time.

#### Global Time Windows
Used when all patients are analyzed over the same fixed time period.

- **`start_date` (datetime):** The anchor date for the time window calculation.
- **`years`, `months`, `days` (int):** The total duration of the time window relative to `start_date`.
- **`lookback` (bool):** Determines the direction of the window. If `True` (default), the window extends backward from `start_date`. If `False`, it extends forward.
- **`time_window_interval_delta` (relativedelta):** The step size for each time slice. For example, `relativedelta(months=1)` creates one feature vector per patient per month.

#### Individual Patient Windows (IPW)
Used for patient-specific time windows, typically anchored to a clinical event (e.g., diagnosis date).

- **`individual_patient_window` (bool):** Set to `True` to enable IPW mode.
- **`individual_patient_window_df` (pd.DataFrame):** A DataFrame containing patient IDs and their corresponding event dates.
- **`individual_patient_id_column_name` (str):** The name of the patient ID column in the `individual_patient_window_df`.
- **`individual_patient_window_start_column_name` (str):** The name of the column containing the anchor dates in the `individual_patient_window_df`.

#### Global Data Boundaries
These parameters set the absolute earliest and latest dates for any data retrieval from Elasticsearch, acting as a hard filter.

- **`global_start_year`, `global_start_month`, `global_start_day` (int/str)**
- **`global_end_year`, `global_end_month`, `global_end_day` (int/str)**

### Feature Selection (main_options)

You can precisely control which features are extracted by creating a dictionary and passing it to the `config_class`. Set a feature to `True` to enable it or `False` to disable it.

```python
# 1. Define your feature set
main_options_dict = {
    'demo': True,           # Demographic information
    'bmi': True,            # BMI information
    'bloods': True,         # Blood-related information
    'drugs': True,          # Drug-related information
    'diagnostics': True,    # Diagnostic information
    'core_02': True,        # core_02 information
    'bed': True,            # Bed information
    'vte_status': True,     # VTE status information
    'hosp_site': True,      # Hospital site information
    'core_resus': True,     # Core resuscitation information
    'news': True,           # NEWS (National Early Warning Score)
    'smoking': True,        # Smoking-related information
    'annotations': True,    # EPR document annotations via MedCAT
    'annotations_mrc': True,# MRC annotations via MedCAT
    'negated_presence_annotations': False,  # Negated presence annotations
    'appointments': False,  # Appointments information
    'annotations_reports': False,  # Reports information
    'textual_obs': False,   # Textual observations
}

# 2. Pass the dictionary to your config_class instance
config_obj = config_class(
    main_options=main_options_dict,
    # ... other configuration parameters
)
```

### Cohort and Sampling

- **`use_controls` (bool):** If `True`, generates a control group for a case-control study.
- **`treatment_control_ratio_n` (int):** The ratio of control patients to treatment patients (e.g., 2 for a 2:1 ratio).
- **`all_epr_patient_list_path` (str):** Path to a CSV file containing a master list of all possible patient IDs, used for sampling controls.
- **`sample_treatment_docs` (int):** If set to a number greater than 0, a random sample of that size will be taken from the initial cohort. Useful for quick tests.
- **`shuffle_pat_list` (bool):** If `True`, shuffles the final patient list before processing.

### Advanced and Technical Parameters

- **`split_clinical_notes` (bool):** If `True`, the pipeline attempts to parse dates within long clinical notes and split them into smaller, date-stamped documents for more accurate temporal analysis.
- **`add_icd10` / `add_opc4s` (bool):** If `True`, appends linked ICD-10 or OPCS-4 codes to the MedCAT annotation outputs.
- **`annot_filter_options` (dict):** A dictionary to fine-tune MedCAT annotation filtering, allowing you to set thresholds for confidence, accuracy, and filter by concept types or meta-annotations (e.g., `Presence: True`).
- **`data_type_filter_dict` (dict):** A dictionary to apply term-based filtering on raw data before feature extraction (e.g., only include specific blood tests).
- **`gpu_mem_threshold` (int):** The minimum free GPU memory (in MB) required for MedCAT to be loaded onto a specific GPU.
- **`remote_dump` (bool):** If `True`, saves output files to a remote server via SFTP. Requires hostname, username, and password to be set.
