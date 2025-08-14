import pytest
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import os


def test_notebook():
    notebook_filename = "notebooks/example_usage.ipynb"
    notebook_dir = os.path.dirname(notebook_filename)

    # Get absolute path to ensure proper directory resolution
    notebook_abs_path = os.path.abspath(notebook_filename)
    notebook_abs_dir = os.path.dirname(notebook_abs_path)

    with open(notebook_filename) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=600, kernel_name="pat2vec_env")

    # Change to the notebook directory before execution
    original_cwd = os.getcwd()
    try:
        os.chdir(notebook_abs_dir)
        # Execute the notebook with the correct working directory
        ep.preprocess(nb, {"metadata": {"path": notebook_abs_dir}})
    finally:
        # Always restore the original working directory
        os.chdir(original_cwd)
