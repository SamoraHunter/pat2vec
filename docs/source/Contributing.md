# Contributing to pat2vec

Thank you for your interest in contributing to `pat2vec`! Whether you're reporting a bug, suggesting a feature, or writing code, your contributions are welcome.

## How to Contribute

### Reporting Bugs
If you encounter a bug, please open an issue on our GitHub issue tracker. Provide as much detail as possible, including:
- Steps to reproduce the bug.
- The expected behavior.
- The actual behavior and any error messages.
- Your operating system and Python version.

### Suggesting Enhancements
If you have an idea for a new feature or an improvement, please open an issue using the "Feature Request" template. Describe your idea clearly, explaining the problem it solves and its potential benefits.

## Development Setup

To contribute code, you'll need to set up a development environment. The `install_pat2vec.sh` script makes this easy.

1.  **Clone the repository:**
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    cd pat2vec
    ```

2.  **Run the installation script with the `--dev` flag:**
    This flag installs all necessary development dependencies, such as `pytest` and `nbmake`.
    ```shell
    ./install_pat2vec.sh --dev
    ```
    *Use the `--proxy` flag as well if you are behind a corporate proxy.*

3.  **Activate the virtual environment:**
    ```shell
    source pat2vec_env/bin/activate
    ```

## Running Tests

Before submitting a pull request, please ensure all tests pass. `pat2vec` uses `pytest` for testing Python code and `nbmake` for testing Jupyter Notebooks.

1.  **Run Python unit tests:**
    From the root of the `pat2vec` directory, run:
    ```shell
    pytest
    ```

2.  **Run Notebook tests:**
    To ensure the example notebooks run without errors, use `nbmake`:
    ```shell
    pytest --nbmake notebooks/
    ```

## Pull Request Process

1.  **Fork** the repository and create a new branch for your feature or bugfix.
2.  Make your changes and add or update tests as appropriate.
3.  Ensure all tests pass locally.
4.  Update the documentation (README, wiki, docstrings) if your changes affect it.
5.  Submit a pull request to the `main` branch, detailing the changes you have made.
