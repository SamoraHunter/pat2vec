# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
import typing

# -- Path setup --------------------------------------------------------------
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

# This allows Sphinx to understand forward-referenced type hints (e.g., 'ClassName'
# instead of ClassName), which is common in modern Python.
autodoc_typehints = "description"

# Point to the project root (one level up from docs/source)
sys.path.insert(0, os.path.abspath('../..'))


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'pat2vec'
copyright = '2024, Samora Hunter'
author = 'Samora Hunter'

# The full version, including alpha/beta/rc tags
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',  # Core library to pull in documentation from docstrings
    'sphinx.ext.autosummary',  # Create summary tables
    'sphinx.ext.napoleon',  # Support for Google and NumPy style docstrings
    'sphinx.ext.viewcode',  # Add links to highlighted source code
    'sphinx_autodoc_typehints',  # Use the compatible version from pyproject.toml
    'myst_parser',  # For Markdown support
    'sphinxcontrib.mermaid'
]

# Tell Sphinx to parse both reStructuredText and Markdown files.
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

templates_path = ['_templates']
exclude_patterns = ['_Sidebar.md']

# -- Options for autodoc -----------------------------------------------------
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'undoc-members': False,  # Set to True if you want to see items without docstrings
    'show-inheritance': True,
}

# -- Options for Napoleon ----------------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
