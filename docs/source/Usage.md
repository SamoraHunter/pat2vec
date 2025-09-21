# Usage

This guide outlines the steps to run a `pat2vec` analysis after completing the installation.

## 1. Finalize Project Setup

Before running an analysis, ensure your project directory is set up correctly. If you used the `install_pat2vec.sh` script, much of this is done for you.

1.  **Populate `credentials.py`**: In the parent directory of your `pat2vec` clone, edit `credentials.py` with your Elasticsearch credentials.
2.  **Add MedCAT Model**: Copy your MedCAT model pack (`.zip`) into the `medcat_models` directory.

Your final directory structure should look like this:

```
your_project_folder/
├── credentials.py              # <-- Populated with your credentials
├── medcat_models/
│   └── your_model.zip          # <-- Your MedCAT model pack
├── snomed_methods/             # <-- Cloned helper repository
└── pat2vec/                    # <-- This repository
    ├── notebooks/
    │   └── example_usage.ipynb
    └── ...
```

## 2. Prepare Input Data

Create a CSV file containing your patient cohort. This file must include:
- A column named `client_idcode` with unique patient identifiers.
- Any other relevant columns, such as a diagnosis date for aligning time series data.

Place this file in an accessible location, such as a new `data` folder inside `pat2vec/notebooks/`.

## 3. Configure and Run

The `example_usage.ipynb` notebook provides a template for running the pipeline.

1.  **Open the Notebook**: Navigate to `pat2vec/notebooks/` and open `example_usage.ipynb`.
2.  **Select the Kernel**: Ensure the `pat2vec_env` Jupyter kernel is active.
3.  **Configure the Analysis**: In the notebook, locate the `config_class`. This object controls all parameters for your run. You will need to set:
    - Paths to your input cohort CSV and output directories.
    - The list of features to extract.
    - Time windows for data extraction (look-back/look-forward periods).
4.  **Run the Pipeline**: Execute the cells in the notebook to process your data.

> **Note:** When working with real patient data, ensure the `testing` flag in the `config_class` is set to `False`.
