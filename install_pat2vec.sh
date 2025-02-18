#!/bin/bash

PROXY_MODE=false
CLONE_REPOS=true
FORCE_CLEAN=false

# Store the absolute path to global_files directory (one level up from pat2vec)
GLOBAL_FILES_DIR="$(dirname "$(pwd)")"

show_help() {
    echo "Usage: ./install.sh [OPTIONS]"
    echo "Options:"
    echo "  -h, --help           Show this help message"
    echo "  -p, --proxy          Install with proxy support"
    echo "  --no-clone           Skip git clone operations"
    echo "  -f, --force          Remove existing files and perform fresh install"
}

setup_medcat_models() {
    echo "Checking if medcat_models directory exists inside global_files..."
    
    # Check if the directory already exists
    if [ ! -d "$GLOBAL_FILES_DIR/medcat_models" ]; then
        echo "Directory medcat_models does not exist, creating it..."
        mkdir -p "$GLOBAL_FILES_DIR/medcat_models" || { echo "Failed to create medcat_models directory"; exit 1; }
    else
        echo "medcat_models directory already exists, skipping creation."
    fi
    
    # Create the placeholder file or skip if it's already there
    echo "Place your MedCAT model pack in this directory" > "$GLOBAL_FILES_DIR/medcat_models/put_medcat_modelpack_here.txt"
}

copy_credentials() {
    local target_file="$GLOBAL_FILES_DIR/credentials.py"
    if [ ! -f "$target_file" ] && [ -f "$GLOBAL_FILES_DIR/pat2vec/util/credentials.py" ]; then
        cp "$GLOBAL_FILES_DIR/pat2vec/util/credentials.py" "$target_file"
        echo "Credentials file copied successfully. Make sure you populate this!"
    fi
}

clone_repositories() {
    # Save current directory
    local current_dir=$(pwd)
    
    # Change to global_files directory
    cd "$GLOBAL_FILES_DIR" || { echo "Error: Could not change to global_files directory"; exit 1; }
    
    local repos=(
        "https://github.com/SamoraHunter/cogstack_search_methods.git"
        "https://github.com/SamoraHunter/clinical_note_splitter.git"
    )
    
    for repo in "${repos[@]}"; do
        local repo_name=$(basename "$repo" .git)
        if [ ! -d "$repo_name" ]; then
            echo "Cloning $repo_name..."
            git clone "$repo" || exit 1
        else
            echo "$repo_name already exists, skipping..."
        fi
    done
    
    # Return to original directory
    cd "$current_dir" || { echo "Error: Could not return to original directory"; exit 1; }
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--proxy) PROXY_MODE=true; shift;;
        --no-clone) CLONE_REPOS=false; shift;;
        -f|--force) FORCE_CLEAN=true; shift;;
        -h|--help) show_help; exit 0;;
        *) echo "Unknown option: $1"; show_help; exit 1;;
    esac
done

# Verify we're in the pat2vec directory
if [[ ! "$(basename "$(pwd)")" == "pat2vec" ]]; then
    echo "Error: This script must be run from the pat2vec directory"
    exit 1
fi

# Clone additional repositories if needed
if [ "$CLONE_REPOS" = true ]; then
    clone_repositories
fi

# Setup MedCAT models directory in global_files
setup_medcat_models

# Copy the credentials if needed
copy_credentials

# Run the appropriate pat2vec install script
if [ "$PROXY_MODE" = true ]; then
    if [ -f "install_proxy.sh" ]; then
        chmod +x install_proxy.sh
        ./install_proxy.sh
    else
        echo "Error: install_proxy.sh not found"
        exit 1
    fi
else
    if [ -f "install.sh" ]; then
        chmod +x install.sh
        ./install.sh
    else
        echo "Error: install.sh not found"
        exit 1
    fi
fi

echo "Installation completed successfully!"
echo "Press Enter to exit..."; read
