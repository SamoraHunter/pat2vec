# Installation and Setup Guide

This guide provides detailed instructions for installing and configuring `pat2vec`.

## Requirements

**Core Services:**
- **CogStack**: An operational instance for data retrieval.
- **Elasticsearch**: The backend for CogStack.
- **MedCAT**: For medical concept annotation.

**Local Setup:**
- **Python**: Version 3.10 or higher.
- **Virtual Environment**: Requires the `python3-venv` package (or equivalent for your OS).

---

## Installation on Unix/Linux (Recommended)

The `install_pat2vec.sh` script automates the full setup.

**Prerequisites**
- A MedCAT model pack (`.zip` file).
- Your CogStack/Elasticsearch credentials.

**Steps**
1.  **Clone the repository:**
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    cd pat2vec
    ```

2.  **Run the installation script:**
    ```shell
    chmod +x install_pat2vec.sh
    ./install_pat2vec.sh
    ```
    The script supports several options:
    -   `--proxy`: Use if you are behind a corporate proxy.
    -   `--dev`: Installs development dependencies (e.g., `pytest`).
    -   `--force`: Performs a clean installation, removing any existing environment.

3.  **Activate the environment:**
    ```shell
    source ../pat2vec_env/bin/activate
    ```

---

## Installation on Windows

1.  **Clone the repository:**
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    ```

2.  **Run the installation script:**
    This script sets up the Python virtual environment.
    ```shell
    cd pat2vec
    install.bat
    ```

3.  **Activate the environment:**
    ```shell
    pat2vec_env\Scripts\activate
    ```

---

## Post-Installation Configuration

After running the installation script, you must configure your environment.

### 1. Elasticsearch Credentials

Your credentials should be placed in a file named `credentials.py` in the parent directory of your `pat2vec` clone. The `install_pat2vec.sh` script automatically copies a template for you.

**IMPORTANT**: This file contains sensitive information and should **never** be committed to version control.

The structure should look like this:
