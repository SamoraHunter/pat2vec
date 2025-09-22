[![Documentation Status](https://github.com/SamoraHunter/pat2vec/actions/workflows/docs.yml/badge.svg)](https://samorahunter.github.io/pat2vec/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)

## Table of Contents
- [Overview](#overview)
- [Documentation](#documentation)
- [Example Use Cases](#example-use-cases)
  - [1. Patient-Level Aggregation](#1-patient-level-aggregation)
  - [2. Longitudinal Time Series Construction](#2-longitudinal-time-series-construction)
- [Requirements](#requirements)
- [Features](#features)
- [📊 Diagrams](#-diagrams)
  - [System Architecture & Configuration](#system-architecture--configuration)
  - [Data Pipelines](#data-pipelines)
  - [Methods & Post-Processing](#methods--post-processing)
  - [Feature Extraction](#feature-extraction)
- [Installation](#installation)
  - [Windows](#windows)
  - [Unix/Linux](#unixlinux)
- [Usage](#usage)
- [FAQ](#faq)
- [Citation](#citation)
- [Contributing](#contributing)
- [Code of Conduct](#code-of-conduct)
- [License](#license)


# Overview

This tool converts individual patient records into structured time-interval feature vectors, making them suitable for filtering, aggregation, and assembly into a data matrix **D** for binary classification machine learning tasks.

## Documentation

The full API documentation for `pat2vec` is automatically generated and hosted on GitHub Pages.

**View the Live Documentation**

## Example Use Cases

### 1. Patient-Level Aggregation
Compute summary statistics (e.g., the mean of *n* variables) for each unique patient, resulting in one row per patient. This is ideal for models requiring a single representation per individual.

### 2. Longitudinal Time Series Construction
Generate a monthly time series for each patient that includes:

- Biochemistry results
- Demographic attributes
- MedCat-derived clinical text annotations

The time series spans up to 25 years retrospectively, aligned to each patient's diagnosis date, enabling a consistent retrospective view across varying start times.

## Requirements

**Core Services:**
- **CogStack**: An operational instance for data retrieval. The required client libraries are now bundled with this project.
- **Elasticsearch**: The backend for CogStack.
- **MedCAT**: For medical concept annotation.

**Local Setup:**
- **Python**: Version 3.10 or higher.
- **Virtual Environment**: Requires the `python3-venv` package (or equivalent for your OS).
- For all other Python packages, see `requirements.txt`.

## Features

`pat2vec` offers a flexible suite of tools for processing and analyzing patient data.

**Patient Processing**
- **Single & Batch Processing**: Process individual patients for detailed analysis or run large batches for cohort-level studies.

**Cohort Management**
- **Cohort Search & Creation**: Define and build patient cohorts using flexible search criteria.
- **Automated Control Matching**: Automatically generate random control groups for case-control studies.

**Flexible Feature Engineering**
- **Modular Feature Selection**: Choose from a wide range of feature extractors to build a custom feature space tailored to your research question.
- **Temporal Windowing**: Define precise time windows for data extraction relative to a key event (e.g., diagnosis date), including look-back and look-forward periods.

## 📊 Diagrams

<details>
<summary>Click to view project diagrams</summary>

This project includes a collection of diagrams illustrating the system architecture, data pipelines, and feature extraction workflows. You can view the Mermaid definitions or the rendered diagrams below.

#### 📂 System Architecture & Configuration
| Diagram | Mermaid | Image |
|---|---|---|
| **System Architecture** | [assets/system_architecture.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/system_architecture.mmd) | ![System Architecture](https://github.com/SamoraHunter/pat2vec/blob/main/assets/system_architecture.png) |
| **Configuration** | [assets/config.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/config.mmd) | ![Configuration](https://github.com/SamoraHunter/pat2vec/blob/main/assets/config.svg) |

#### 🛠️ Data Pipelines
| Diagram | Mermaid | Image |
|---|---|---|
| **Data Pipeline** | [assets/data_pipeline.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/data_pipeline.mmd) | ![Data Pipeline](https://github.com/SamoraHunter/pat2vec/blob/main/assets/data_pipeline.png) |
| **Main Batch Processing** | [assets/main_batch.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/main_batch.mmd) | ![Main Batch](https://github.com/SamoraHunter/pat2vec/blob/main/assets/main_batch.svg) |
| **Example Ingestion** | [assets/example_ingestion.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/example_ingestion.mmd) | ![Example Ingestion](https://github.com/SamoraHunter/pat2vec/blob/main/assets/example_ingestion.png) |

#### 🧩 Methods & Post-Processing
| Diagram | Mermaid | Image |
|---|---|---|
| **Methods Annotation** | [assets/methods_annotation.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/methods_annotation.mmd) | ![Methods Annotation](https://github.com/SamoraHunter/pat2vec/blob/main/assets/methods_annotation.png) |
| **Post-Processing Build Methods** | [assets/post_processing_build_methods.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/post_processing_build_methods.mmd) | ![Post-Processing Build Methods](https://github.com/SamoraHunter/pat2vec/blob/main/assets/post_processing_build_methods.svg) |
| **Post-Processing Anonymisation** | [assets/post_processing_anonymisation_high_level.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/post_processing_anonymisation_high_level.mmd) | ![Post-Processing Anonymisation](https://github.com/SamoraHunter/pat2vec/blob/main/assets/post_processing_anonymisation_high_level.svg) |

#### 🔍 Feature Extraction
| Diagram | Mermaid | Image |
|---|---|---|
| **Ethnicity Abstractor** | [assets/ethnicity_abstractor.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/ethnicity_abstractor.mmd) | ![Ethnicity Abstractor](https://github.com/SamoraHunter/pat2vec/blob/main/assets/ethnicity_abstractor.svg) |
| **Get BMI** | [assets/get_bmi.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_bmi.mmd) | ![Get BMI](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_bmi.svg) |
| **Get Demographics** | [assets/get_demographics.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_demographics.mmd) | ![Get Demographics](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_demographics.svg) |
| **Get Diagnostics** | [assets/get_diagnostics.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_diagnostics.mmd) | ![Get Diagnostics](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_diagnostics.svg) |
| **Get Drugs** | [assets/get_drugs.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_drugs.mmd) | ![Get Drugs](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_drugs.svg) |
| **Get Smoking** | [assets/get_smoking.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_smoking.mmd) | ![Get Smoking](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_smoking.svg) |
| **Get News** | [assets/get_news.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_news.mmd) | ![Get News](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_news.svg) |
| **Get Dummy Data Cohort Searcher** | [assets/get_dummy_data_cohort_searcher.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_dummy_data_cohort_searcher.mmd) | ![Get Dummy Data Cohort Searcher](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_dummy_data_cohort_searcher.svg) |
| **Get Method Bloods** | [assets/get_method_bloods.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_method_bloods.mmd) | ![Get Method Bloods](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_method_bloods.svg) |
| **Get Method Patient Annotations** | [assets/get_method_pat_annotations.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_method_pat_annotations.mmd) | ![Get Method Patient Annotations](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_method_pat_annotations.svg) |
| **Get Treatment Docs (No Terms Fuzzy)** | [assets/get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy.mmd](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy.mmd) | ![Get Treatment Docs (No Terms Fuzzy)](https://github.com/SamoraHunter/pat2vec/blob/main/assets/get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy.svg) |

</details>


## Installation

### Windows

1.  **Clone the repository:**
    Navigate to the directory where you want to store your projects. It's recommended to have a parent directory to hold `pat2vec` and its related assets.

    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    ```

2.  **Run the installation script:**
    Navigate into the cloned repository and run the batch script. This will create a Python virtual environment, install dependencies from `requirements.txt`, and set up a Jupyter kernel.

    ```shell
    cd pat2vec
    install.bat
    ```

3.  **Activate the environment:**
    To use the installed packages, activate the virtual environment:
    ```shell
    pat2vec_env\Scripts\activate
    ```

4.  **Set up for your IDE/Notebook:**
    If you are using an IDE like VS Code or a Jupyter Notebook, make sure to select the `pat2vec_env` kernel to run your code.

5.  **Post-Installation Setup:**
    The script sets up the Python environment, but you must manually arrange other project assets. In the parent directory of your `pat2vec` clone, you will need to:
    -   **Clone the helper repository**:
        ```shell
        git clone https://github.com/SamoraHunter/snomed_methods.git
        ```
    -   **Add MedCAT model**: Create a `medcat_models` directory and copy your MedCAT model pack (`.zip`) into it.
    -   **Add credentials**: Create a `credentials.py` file. You can use `pat2vec/pat2vec/config/credentials_template.py` as a starting point.

    Your final directory structure should look like the one described in the Usage section.

### Unix/Linux

There are two installation scripts provided for Unix-like systems. The comprehensive script (`install_pat2vec.sh`) is recommended as it handles the complete project setup.

#### **Option 1: Comprehensive Installation (Recommended)**

The `install_pat2vec.sh` script automates the full setup, including:
- Creating a Python virtual environment (`pat2vec_env`).
- Installing Python dependencies (including development and testing tools).
- Cloning the `snomed_methods` helper repository.
- Creating required directories and template files (e.g., for MedCAT models and credentials).

**Prerequisites**

Before running, you will need:
- A MedCAT model pack (`.zip` file).
- Your CogStack/Elasticsearch credentials.

**Installation Steps**

1.  **Clone the repository:**
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    cd pat2vec
    ```

2.  **Run the installation script:**
    Grant execution permissions and run the script. It must be run from within the `pat2vec` directory.

    ```shell
    chmod +x install_pat2vec.sh
    ./install_pat2vec.sh
    ```

    The script supports several options:
    -   `--proxy`: Use if you are behind a corporate proxy that mirrors Python packages.
    -   `--dev`: Installs development dependencies (e.g., `pytest`, `nbmake`) for running tests.
    -   `--all`: Installs all optional feature dependencies.
    -   `--force`: Removes any existing virtual environment and performs a clean installation.
    -   `--no-clone`: Skips cloning the `snomed_methods` repository if you already have it.

    For example, to install for development behind a proxy:
    ```shell
    ./install_pat2vec.sh --proxy --dev
    ```

3.  **Post-Installation Setup:**
    The script creates a directory structure in the parent folder of `pat2vec`.
    -   **Place MedCAT model:** Copy your model pack into the `medcat_models` directory created by the script.
    -   **Populate credentials:** Edit the `credentials.py` file created by the script and fill in your details.

4.  **Activate the environment:**
    ```shell
    source pat2vec_env/bin/activate
    ```

#### **Option 2: Basic Installation**

The `install.sh` script provides a basic setup. It creates a virtual environment and installs Python packages from `requirements.txt`. It does **not** clone helper repositories or create configuration files.

1.  **Clone the repository:**
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    cd pat2vec
    ```

2.  **Run the installation script:**
    This script requires `python3` and the `venv` module to be available.
    ```shell
    chmod +x install.sh
    ./install.sh
    ```

3.  **Manual Setup:**
    You will need to manually clone `snomed_methods` and create the `credentials.py` file if you use this method.

4.  **Activate the environment:**
    ```shell
    source pat2vec_env/bin/activate
    ```

## Usage

This guide outlines the steps to run a `pat2vec` analysis after completing the installation.

### 1. Finalize Project Setup

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

### 2. Prepare Input Data

Create a CSV file containing your patient cohort. This file must include:
- A column named `client_idcode` with unique patient identifiers.
- Any other relevant columns, such as a diagnosis date for aligning time series data.

Place this file in an accessible location, such as a new `data` folder inside `pat2vec/notebooks/`.

### 3. Configure and Run

The `example_usage.ipynb` notebook provides a template for running the pipeline.

1.  **Open the Notebook**: Navigate to `pat2vec/notebooks/` and open `example_usage.ipynb`.
2.  **Select the Kernel**: Ensure the `pat2vec_env` Jupyter kernel is active.
3.  **Configure the Analysis**: In the notebook, locate the `config_class`. This object controls all parameters for your run. You will need to set:
    - Paths to your input cohort CSV and output directories.
    - The list of features to extract.
    - Time windows for data extraction (look-back/look-forward periods).
4.  **Run the Pipeline**: Execute the cells in the notebook to process your data.

> **Note:** When working with real patient data, ensure the `testing` flag in the `config_class` is set to `False`.


## Building the Documentation

This project uses Sphinx to generate documentation from the source code's docstrings.

1.  **Install development dependencies:**
    If you haven't already, run the installation script with the `--dev` flag to install Sphinx and its extensions.
    ```shell
    ./install_pat2vec.sh --dev
    ```

2.  **Activate the virtual environment:**
    ```shell
    source pat2vec_env/bin/activate
    ```

3.  **Build the HTML documentation:**
    Navigate to the `docs/` directory and use the provided `Makefile`.
    ```shell
    cd docs
    make html
    ```

4.  **View the documentation:**
    The generated files will be in `docs/build/html/`. You can open the main page in your browser:
    ```
    open docs/build/html/index.html
    ```

## FAQ

For answers to common questions, troubleshooting tips, and more detailed explanations of project concepts, please see our Frequently Asked Questions page.
- [Frequently Asked Questions](./docs/source/Frequently-Asked-Questions.md)

## Citation

If you use `pat2vec` in your research, please cite it. This helps to credit the work and allows others to find the tool.

```bibtex
@software{hunter_pat2vec_2024,
  author = {Hunter, Samora and Others},
  title = {pat2vec: A tool for transforming EHR data into feature vectors for machine learning},
  year = {2024},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/SamoraHunter/pat2vec}}
}
```

## Contributing

Contributions are welcome! Please see the contributing guidelines for more information.

## Code of Conduct

This project and everyone participating in it is governed by a Code of Conduct. By participating, you are expected to uphold this code. Please report any unacceptable behavior.

## License
This project is licensed under the MIT License - see the LICENSE file for details
