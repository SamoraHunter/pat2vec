#!/bin/bash

PROXY_MODE=false
CLONE_REPOS=true
FORCE_CLEAN=false
INSTALL_MODE="lite"  # Default to lite installation
# Store the absolute path to global_files directory (one level up from pat2vec)
GLOBAL_FILES_DIR="$(dirname "$(pwd)")"

show_help() {
    echo "Usage: ./install.sh [OPTIONS]"
    echo "Options:"
    echo "  -h, --help           Show this help message"
    echo "  -p, --proxy          Install with proxy support"
    echo "  --no-clone           Skip git clone operations"
    echo "  -f, --force          Remove existing files and perform fresh install"
    echo "  -a, --all            Install all components (overrides default lite installation)"
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
    cd "$GLOBAL_FILES_DIR" || { echo "Error: Could not change to global_files directory"; exit 1; }
    
    local repos=(
        "https://github.com/SamoraHunter/cogstack_search_methods.git"
        "https://github.com/SamoraHunter/clinical_note_splitter.git"
        "https://github.com/SamoraHunter/snomed_methods.git"
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
        -a|--all) INSTALL_MODE="all"; shift;;
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

# Create paths.py file
create_paths_file || echo "Warning: Paths file setup encountered issues"

# Copy the credentials if needed
copy_credentials || echo "Warning: Credentials setup encountered issues"

# Run the appropriate pat2vec install script
if [ "$INSTALL_MODE" == "all" ]; then
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
else
    if [ "$PROXY_MODE" = true ]; then
        if [ -f "install_lite_proxy.sh" ]; then
            chmod +x install_lite_proxy.sh
            ./install_lite_proxy.sh
        else
            echo "Error: install_lite_proxy.sh not found"
            exit 1
        fi
    else
        if [ -f "install_lite.sh" ]; then
            chmod +x install_lite.sh
            ./install_lite.sh
        else
            echo "Error: install_lite.sh not found"
            exit 1
        fi
    fi
fi

echo "Installation completed successfully!"