#!/bin/bash

VENV_DIR="$(pwd)/pat2vec_env"
PROXY_MODE=false
CLONE_REPOS=true
FORCE_CLEAN=false
INSTALL_MODE="lite"  # Default to lite installation
DEV_MODE=false
# Store the absolute path to global_files directory (one level up from pat2vec)
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
GLOBAL_FILES_DIR="$(dirname "$SCRIPT_DIR")"

show_help() {
    echo "Usage: ./install_pat2vec.sh [OPTIONS]"
    echo "Options:"
    echo "  -h, --help           Show this help message"
    echo "  -p, --proxy          Install with proxy support"
    echo "  --no-clone           Skip git clone operations"
    echo "  -f, --force          Remove existing files and perform fresh install"
    echo "  -a, --all            Install all components (overrides default lite installation)"
    echo "  --dev                Install development dependencies"
}

setup_medcat_models() {
    echo "Checking if medcat_models directory exists inside global_files..."

    # Check if the directory already exists
    if [ ! -d "$GLOBAL_FILES_DIR/medcat_models" ]; then
        echo "Directory medcat_models does not exist, creating it..."
        mkdir -p "$GLOBAL_FILES_DIR/medcat_models" || { echo "Failed to create medcat_models directory"; return 1; }
    else
        echo "medcat_models directory already exists, skipping creation."
    fi

    # Create the placeholder file or skip if it's already there
    echo "Place your MedCAT model pack in this directory" > "$GLOBAL_FILES_DIR/medcat_models/put_medcat_modelpack_here.txt"
}

create_paths_file() {
    echo "Setting up paths.py file..."
    local paths_dir="notebooks"
    local paths_file="$paths_dir/paths.py"

    # Create notebooks directory if it doesn't exist
    if [ ! -d "$paths_dir" ]; then
        echo "Creating notebooks directory..."
        mkdir -p "$paths_dir" || { echo "ERROR: Failed to create notebooks directory"; return 1; }
    fi

    # Create or overwrite paths.py
    echo "Creating paths.py file..."
    echo "medcat_path = 'put your model pack path here'" > "$paths_file"

    if [ $? -eq 0 ]; then
        echo "paths.py created successfully at: $paths_file"
        ls -l "$paths_file"  # Verify the file exists and show its details
    else
        echo "ERROR: Failed to create paths.py file"
        return 1
    fi
}

copy_credentials() {
    echo "Starting credentials copy process..."
    echo "Target directory: $GLOBAL_FILES_DIR"

    local source_file="$GLOBAL_FILES_DIR/pat2vec/pat2vec/util/credentials.py"
    local target_file="$GLOBAL_FILES_DIR/credentials.py"

    echo "Looking for credentials at: $source_file"

    # Check if source file exists
    if [ ! -f "$source_file" ]; then
        echo "ERROR: Source credentials file not found at: $source_file"
        return 1
    fi

    echo "Source file found at: $source_file"

    # Check if target file already exists
    if [ -f "$target_file" ]; then
        echo "Target credentials file already exists at: $target_file"
        if [ "$FORCE_CLEAN" = true ]; then
            echo "Force clean enabled, removing existing credentials file..."
            rm "$target_file" || { echo "ERROR: Failed to remove existing credentials file"; return 1; }
        else
            echo "Skipping credentials copy (use -f to force overwrite)"
            return 0
        fi
    fi

    # Attempt to copy the file
    echo "Copying credentials file..."
    cp "$source_file" "$target_file"

    if [ $? -eq 0 ]; then
        echo "Credentials file copied successfully to: $target_file"
        echo "IMPORTANT: Make sure to populate the credentials file with your actual credentials!"
        ls -l "$target_file"  # Verify the file exists and show its details
    else
        echo "ERROR: Failed to copy credentials file"
        return 1
    fi
}

clone_repositories() {
    # Save current directory
    local current_dir=$(pwd)

    # Change to global_files directory
    cd "$GLOBAL_FILES_DIR" || { echo "Error: Could not change to global_files directory"; return 1; }

    local snomed_repo_url="https://github.com/SamoraHunter/snomed_methods.git"

    local repos=(
        "$snomed_repo_url"
    )

    # Save current proxy settings
    local saved_http_proxy="$http_proxy"
    local saved_https_proxy="$https_proxy"
    local saved_git_http_proxy=""
    local saved_git_https_proxy=""

    # Get current git proxy settings if they exist
    saved_git_http_proxy=$(git config --global --get http.proxy 2>/dev/null || echo "")
    saved_git_https_proxy=$(git config --global --get https.proxy 2>/dev/null || echo "")

    echo "Temporarily disabling proxy for Git operations..."

    # Unset environment proxy variables for git operations
    unset http_proxy
    unset https_proxy
    unset HTTP_PROXY
    unset HTTPS_PROXY

    # Unset git proxy configuration temporarily
    git config --global --unset http.proxy 2>/dev/null || true
    git config --global --unset https.proxy 2>/dev/null || true

    for repo in "${repos[@]}"; do
        local repo_name=$(basename "$repo" .git)
        if [ ! -d "$repo_name" ]; then
            echo "Cloning $repo (bypassing proxy)..."
            if ! git clone "$repo"; then
                echo "WARNING: Failed to clone $repo_name. This might be due to a permission issue or network problem. Continuing installation..."
            fi
        else
            echo "$repo_name already exists, skipping..."
        fi
    done

    # Restore proxy settings
    echo "Restoring proxy settings..."
    if [ -n "$saved_http_proxy" ]; then
        export http_proxy="$saved_http_proxy"
    fi
    if [ -n "$saved_https_proxy" ]; then
        export https_proxy="$saved_https_proxy"
    fi
    if [ -n "$saved_git_http_proxy" ]; then
        git config --global http.proxy "$saved_git_http_proxy"
    fi
    if [ -n "$saved_git_https_proxy" ]; then
        git config --global https.proxy "$saved_git_https_proxy"
    fi

    # Return to original directory
    cd "$current_dir" || { echo "Error: Could not return to original directory"; return 1; }
}

main() {

(
    # Run in a subshell with -e to exit immediately on error without killing the parent shell.
    # This makes error handling cleaner than manually checking every command.
    set -e

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--proxy) PROXY_MODE=true; shift;;
        --no-clone) CLONE_REPOS=false; shift;;
        -f|--force) FORCE_CLEAN=true; shift;;
        -a|--all) INSTALL_MODE="all"; shift;;
        --dev) DEV_MODE=true; shift;;
        -h|--help) show_help; return 0;;
        *) echo "Unknown option: $1"; show_help; exit 1;;
    esac
done

# Verify we're in the pat2vec directory
if [[ ! "$(basename "$(pwd)")" == "pat2vec" ]]; then
    echo "Error: This script must be run from the pat2vec directory"
    exit 1
fi

# Pre-flight check for write permissions in the global files directory
if [ ! -w "$GLOBAL_FILES_DIR" ]; then
    echo "ERROR: No write permission in the target directory: '$GLOBAL_FILES_DIR'." >&2
    echo "Please run this script as a user with write permissions, or specify a writable path with a future '--global-files-dir' option." >&2
    exit 1
fi

# Clone additional repositories if needed
if [ "$CLONE_REPOS" = true ]; then
    clone_repositories
fi

# Setup MedCAT models directory in global_files
setup_medcat_models

# Create paths.py file
create_paths_file

# Copy the credentials if needed
copy_credentials

if [ "$FORCE_CLEAN" = true ] && [ -d "$VENV_DIR" ]; then
    echo "Force clean enabled, removing existing virtual environment at $VENV_DIR..."
    rm -rf "$VENV_DIR"
fi

echo "Creating virtual environment..."
python3 -m venv "$VENV_DIR"

echo "Activating virtual environment..."
if [ ! -f "$VENV_DIR/bin/activate" ]; then
    echo "ERROR: Virtual environment activation script not found at $VENV_DIR/bin/activate"
    exit 1
fi
source "$VENV_DIR/bin/activate" || { echo "ERROR: Failed to activate virtual environment"; return 1; }

echo "Upgrading pip..."
pip_upgrade_args=("--upgrade" "pip")
if [ "$PROXY_MODE" = true ]; then
    pip_upgrade_args+=("--trusted-host" "dh-cap02" "-i" "http://dh-cap02:8008/mirrors/pat2vec")
fi
python -m pip install "${pip_upgrade_args[@]}"

echo "Installing project dependencies..."

extras=""
if [ "$INSTALL_MODE" = "all" ]; then
    extras="all"
fi
if [ "$DEV_MODE" = true ]; then
    [ -n "$extras" ] && extras+=","
    extras+="dev"
fi

INSTALL_TARGET="."
[ -n "$extras" ] && INSTALL_TARGET=".[$extras]"

echo "Running pip install -e \"$INSTALL_TARGET\""
pip_install_args=("-e" "$INSTALL_TARGET")
if [ "$PROXY_MODE" = true ]; then
    pip_install_args+=("--trusted-host" "dh-cap02" "-i" "http://dh-cap02:8008/mirrors/pat2vec" "--retries" "5" "--timeout" "60")
fi

pip install "${pip_install_args[@]}"

if [ "$DEV_MODE" = true ]; then
    echo "Installing documentation dependencies (Sphinx)..."
    pip_doc_args=("sphinx" "sphinx-rtd-theme" "sphinx-autodoc-typehints")
    [ "$PROXY_MODE" = true ] && pip_doc_args+=("--trusted-host" "dh-cap02" "-i" "http://dh-cap02:8008/mirrors/pat2vec")
    pip install "${pip_doc_args[@]}"
fi

echo "Installing SpaCy model..."
SPACY_MODEL_URL="https://github.com/explosion/spacy-models/releases/download/en_core_web_md-3.7.1/en_core_web_md-3.7.1-py3-none-any.whl"
pip_spacy_args=()
if [ "$PROXY_MODE" = true ]; then
    # If using proxy, install the package by name from the local mirror index.
    pip_spacy_args+=("en-core-web-md==3.6.0")
    pip_spacy_args+=("--trusted-host" "dh-cap02")
    pip_spacy_args+=("-i" "http://dh-cap02:8008/mirrors/pat2vec")
else
    # Otherwise, install directly from the public URL.
    pip_spacy_args+=("$SPACY_MODEL_URL")
fi

pip install "${pip_spacy_args[@]}"

echo "Adding virtual environment to Jupyter kernelspec..."
python -m ipykernel install --user --name=pat2vec_env

echo "Deactivating virtual environment..."
deactivate

echo ""
echo "----------------------------------------------------"
echo "Installation completed successfully!"
echo "To activate the environment, run: source $VENV_DIR/bin/activate"
echo "----------------------------------------------------"

)
local status=$?
return $status

}

# Pass all script arguments to the main function
main "$@"
