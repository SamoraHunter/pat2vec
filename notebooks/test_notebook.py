import pytest
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import os


def test_notebook():
    notebook_filename = "notebooks/example_usage.ipynb"
    notebook_dir = os.path.dirname(notebook_filename)

    with open(notebook_filename) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=600, kernel_name="pat2vec_env")

    # Set the working directory for the notebook execution
    ep.preprocess(nb, {"metadata": {"path": notebook_dir}})
