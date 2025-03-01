#!/bin/bash

# Set paths
VENV_DIR="$(pwd)/pat2vec_env"
REQUIREMENTS_FILE="$(pwd)/requirements_lite_proxy.txt"

# Create virtual environment
python3 -m venv "$VENV_DIR"

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Upgrade pip
echo "Upgrading pip..."
python -m pip install --upgrade --trusted-host dh-cap02 -i http://dh-cap02:8000/mirrors/pat2vec pip
echo "Pip upgrade completed."

# Install requirements
echo "Installing requirements..."

pip install --trusted-host dh-cap02 -i http://dh-cap02:8000/mirrors/pat2vec -r "$REQUIREMENTS_FILE"

echo "Requirements installation completed."

# Install ipykernel
pip install --trusted-host dh-cap02 -i http://dh-cap02:8000/mirrors/pat2vec ipykernel

# Add virtual environment to Jupyter kernelspec
echo "Adding virtual environment to Jupyter kernelspec..."
python -m ipykernel install --user --name=pat2vec_env
echo "Virtual environment added to Jupyter kernelspec."

# Deactivate virtual environment
deactivate

echo "Installation completed."
