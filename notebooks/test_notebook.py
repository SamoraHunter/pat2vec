import pytest
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor


def test_notebook():
    with open("notebooks/example_usage.ipynb") as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb)  # Will raise an error if the notebook fails
