#!/bin/bash

# Default configuration
PROXY_MODE=false
CLONE_REPOS=true
INSTALL_DIR=$(pwd)
GLOBAL_FILES_DIR="gloabl_files"
FORCE_CLEAN=false

# Function to display usage
show_help() {
    echo "Usage: ./install.sh [OPTIONS]"
    echo "Options:"
    echo "  -h, --help           Show this help message"
    echo "  -p, --proxy         Install with proxy support"
    echo "  --no-clone          Skip git clone operations"
    echo "  -d, --directory DIR  Specify installation directory"
    echo "  -f, --force         Remove existing files and perform fresh install"
}

# Function to setup medcat_models directory
setup_medcat_models() {
    echo "Setting up medcat_models directory..."
    mkdir -p "$GLOBAL_FILES_DIR/medcat_models"
    echo "Place your MedCAT model pack in this directory" > "$GLOBAL_FILES_DIR/medcat_models/put_medcat_modelpack_here.txt"
}

# Function to copy credentials file if it does not already exist
copy_credentials() {
    echo "Checking credentials file in global_files..."
    local target_file="$GLOBAL_FILES_DIR/credentials.py"
    
    if [ -f "$target_file" ]; then
        echo "Warning: credentials.py already exists in global_files. Not overwriting."
    else
        if [ -f "$GLOBAL_FILES_DIR/pat2vec/util/credentials.py" ]; then
            cp "$GLOBAL_FILES_DIR/pat2vec/util/credentials.py" "$target_file"
            echo "Credentials file copied successfully. Make sure you populate this!"
        else
            echo "Warning: credentials.py not found in pat2vec/util"
        fi
    fi
}

# Function to safely clone a repository
clone_single_repo() {
    local repo_url=$1
    local repo_name=$(basename "$repo_url" .git)
    
    if [ -d "$repo_name" ]; then
        if [ "$FORCE_CLEAN" = true ]; then
            echo "Removing existing $repo_name directory..."
            rm -rf "$repo_name"
        else
            echo "Directory $repo_name already exists. Using existing directory."
            return 0
        fi
    fi
    
    echo "Cloning $repo_url..."
    if ! git clone "$repo_url"; then
        echo "Failed to clone $repo_url"
        return 1
    fi
}

# Function to handle git clone operations
clone_repositories() {
    local repos=(
        "https://github.com/SamoraHunter/pat2vec.git"
        "https://github.com/SamoraHunter/snomed_methods.git"
        "https://github.com/SamoraHunter/cogstack_search_methods.git"
        "https://github.com/SamoraHunter/clinical_note_splitter.git"
    )
    
    # Handle existing global_files directory
    if [ -d "$GLOBAL_FILES_DIR" ]; then
        if [ "$FORCE_CLEAN" = true ]; then
            echo "Removing existing $GLOBAL_FILES_DIR directory..."
            rm -rf "$GLOBAL_FILES_DIR"
        else
            echo "Directory $GLOBAL_FILES_DIR already exists. Using existing directory."
        fi
    fi
    
    # Create and enter global_files directory
    mkdir -p "$GLOBAL_FILES_DIR"
    cd "$GLOBAL_FILES_DIR" || exit 1
    echo "Using gloabl_files directory at: $(pwd)"

    for repo in "${repos[@]}"; do
        if ! clone_single_repo "$repo"; then
            cd ..
            return 1
        fi
    done

    # Return to parent directory
    cd ..
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--proxy)
            PROXY_MODE=true
            shift
            ;;
        --no-clone)
            CLONE_REPOS=false
            shift
            ;;
        -d|--directory)
            INSTALL_DIR="$2"
            shift 2
            ;;
        -f|--force)
            FORCE_CLEAN=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Create and move to installation directory
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR" || exit 1

# Store the base directory
BASE_DIR=$(pwd)

# Clone repositories if needed
if [ "$CLONE_REPOS" = true ]; then
    echo "Setting up repositories in gloabl_files directory..."
    if ! clone_repositories; then
        echo "Repository setup failed"
        exit 1
    fi
fi

# Setup medcat_models directory and copy credentials
setup_medcat_models
copy_credentials

# Debug information
echo "Current directory: $(pwd)"
echo "Looking for install script in: $BASE_DIR/$GLOBAL_FILES_DIR/pat2vec/"
ls -la "$BASE_DIR/$GLOBAL_FILES_DIR/pat2vec/"

# Run the appropriate installation
if [ "$PROXY_MODE" = true ]; then
    echo "Installing with proxy support..."
    cd "$BASE_DIR/$GLOBAL_FILES_DIR/pat2vec" || exit 1
    if [ -f "install_proxy.sh" ]; then
        echo "Found install_proxy.sh, executing..."
        chmod +x install_proxy.sh
        ./install_proxy.sh
    else
        echo "Error: install_proxy.sh not found"
        exit 1
    fi
else
    echo "Installing without proxy..."
    cd "$BASE_DIR/$GLOBAL_FILES_DIR/pat2vec" || exit 1
    if [ -f "install.sh" ]; then
        echo "Found install.sh, executing..."
        chmod +x install.sh
        ./install.sh
    else
        echo "Error: install.sh not found"
        exit 1
    fi
fi

echo "Installation completed successfully!"