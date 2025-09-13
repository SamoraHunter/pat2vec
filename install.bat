@echo off
setlocal

REM ============================================================================
REM Pat2Vec Installation Script for Windows
REM
REM This script automates the setup of the Pat2Vec project, including
REM creating a virtual environment, installing dependencies, and setting up
REM necessary project files and directories.
REM
REM Usage:
REM   install.bat [OPTIONS]
REM
REM Options:
REM   /h, /?         - Show this help message.
REM   /p, /proxy     - Install with proxy support using a local package mirror.
REM   /no-clone      - Skip git clone operations for additional repositories.
REM   /f, /force     - Remove existing virtual environment and perform a fresh install.
REM   /a, /all       - Install all optional dependencies.
REM   /dev           - Install development dependencies.
REM ============================================================================

REM --- Configuration ---
set SCRIPT_DIR=%~dp0
pushd "%SCRIPT_DIR%.."
set GLOBAL_FILES_DIR=%CD%
popd
set VENV_DIR=%SCRIPT_DIR%pat2vec_env
set SPACY_MODEL_URL=https://github.com/explosion/spacy-models/releases/download/en_core_web_md-3.7.1/en_core_web_md-3.7.1-py3-none-any.whl
set PROXY_PIP_ARGS=--trusted-host dh-cap02 -i http://dh-cap02:8008/mirrors/pat2vec

REM --- Default Flags ---
set PROXY_MODE=false
set CLONE_REPOS=true
set FORCE_CLEAN=false
set INSTALL_MODE=lite
set DEV_MODE=false

REM --- Argument Parsing ---
:arg_loop
if "%1"=="" goto :parse_end
if /I "%1"=="/h" (goto :show_help)
if /I "%1"=="/?" (goto :show_help)
if /I "%1"=="/p" (set PROXY_MODE=true)
if /I "%1"=="/proxy" (set PROXY_MODE=true)
if /I "%1"=="/no-clone" (set CLONE_REPOS=false)
if /I "%1"=="/f" (set FORCE_CLEAN=true)
if /I "%1"=="/force" (set FORCE_CLEAN=true)
if /I "%1"=="/a" (set INSTALL_MODE=all)
if /I "%1"=="/all" (set INSTALL_MODE=all)
if /I "%1"=="/dev" (set DEV_MODE=true)
shift
goto :arg_loop
:parse_end

REM --- Prerequisite Checks ---
echo Checking prerequisites...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not found. Please install Python and add it to your PATH.
    goto :fatal_error
)
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Git is not found. Please install Git and add it to your PATH.
    goto :fatal_error
)
echo Prerequisites found.

REM --- Pre-flight check for write permissions ---
echo.
echo Checking write permissions in %GLOBAL_FILES_DIR%...
echo. > "%GLOBAL_FILES_DIR%\perm.tmp" 2>nul
if %errorlevel% neq 0 (
    echo ERROR: No write permission in the target directory: '%GLOBAL_FILES_DIR%'.
    echo Please run this script from a location where you have write permissions.
    goto :fatal_error
)
del "%GLOBAL_FILES_DIR%\perm.tmp"
echo Write permissions OK.

REM --- Main Installation Logic ---
if "%CLONE_REPOS%"=="true" (call :clone_repositories)
call :setup_medcat_models
call :create_paths_file
call :copy_credentials

if "%FORCE_CLEAN%"=="true" (
    if exist "%VENV_DIR%" (
        echo Force clean enabled, removing existing virtual environment...
        rmdir /s /q "%VENV_DIR%"
    )
)

echo.
echo Creating virtual environment...
if exist "%VENV_DIR%" (
    echo Virtual environment already exists. Skipping creation.
) else (
    python -m venv "%VENV_DIR%"
    if %errorlevel% neq 0 (
        echo ERROR: Failed to create virtual environment.
        goto :fatal_error
    )
)

echo.
echo Activating virtual environment...
call "%VENV_DIR%\Scripts\activate.bat"
if not defined VIRTUAL_ENV (
    echo ERROR: Failed to activate virtual environment.
    goto :fatal_error
)

echo.
echo Upgrading pip...
set PIP_UPGRADE_ARGS=--upgrade pip
if "%PROXY_MODE%"=="true" (set PIP_UPGRADE_ARGS=%PIP_UPGRADE_ARGS% %PROXY_PIP_ARGS%)
python -m pip install %PIP_UPGRADE_ARGS%
if errorlevel 1 (
    echo ERROR: Failed to upgrade pip.
    goto :deactivate_and_exit
)

echo.
echo Installing project dependencies...
set EXTRAS=
if "%INSTALL_MODE%"=="all" (set EXTRAS=all)
if "%DEV_MODE%"=="true" (
    if defined EXTRAS (set EXTRAS=%EXTRAS%,dev) else (set EXTRAS=dev)
)

set INSTALL_TARGET=.
if defined EXTRAS (set INSTALL_TARGET=.[%EXTRAS%])

echo Running: pip install -e "%INSTALL_TARGET%"
set PIP_INSTALL_ARGS=-e "%INSTALL_TARGET%"
if "%PROXY_MODE%"=="true" (set PIP_INSTALL_ARGS=%PIP_INSTALL_ARGS% %PROXY_PIP_ARGS% --retries 5 --timeout 60)

pip install "%PIP_INSTALL_ARGS%"
if %errorlevel% neq 0 (
    echo ERROR: Failed to install project dependencies.
    goto :deactivate_and_exit
)

echo.
echo Installing SpaCy model...
set PIP_SPACY_ARGS=
if "%PROXY_MODE%"=="true" (
    set PIP_SPACY_ARGS=en-core-web-md==3.6.0 %PROXY_PIP_ARGS%
) else (
    set PIP_SPACY_ARGS=%SPACY_MODEL_URL%
)
pip install "%PIP_SPACY_ARGS%"
if %errorlevel% neq 0 (
    echo WARNING: Failed to install SpaCy model. You may need to install it manually.
)

echo.
echo Adding virtual environment to Jupyter as 'Python (pat2vec)'...
python -m ipykernel install --user --name=pat2vec_env --display-name "Python (pat2vec)"

echo.
echo ----------------------------------------------------
echo Installation completed successfully!
echo ----------------------------------------------------

:deactivate_and_exit
echo.
echo Deactivating virtual environment...
call deactivate

echo.
echo To activate the environment, run:
echo   call "%VENV_DIR%\Scripts\activate.bat"
goto :eof

REM ============================================================================
REM --- Subroutines ---
REM ============================================================================

:show_help
echo Usage: install.bat [OPTIONS]
echo.
echo Options:
echo   /h, /?         Show this help message.
echo   /p, /proxy     Install with proxy support.
echo   /no-clone      Skip git clone operations.
echo   /f, /force     Remove existing venv and perform a fresh install.
echo   /a, /all       Install all optional dependencies.
echo   /dev           Install development dependencies.
goto :eof

:clone_repositories
echo.
echo Cloning additional repositories...
set SNOMED_REPO_URL=https://github.com/SamoraHunter/snomed_methods.git

pushd "%GLOBAL_FILES_DIR%"

set REPO_NAME=snomed_methods
if not exist "%REPO_NAME%" (
    echo Cloning %SNOMED_REPO_URL%...
    git clone "%SNOMED_REPO_URL%"
    if %errorlevel% neq 0 (
        echo WARNING: Failed to clone %REPO_NAME%. This might be due to a permission issue or network problem.
    )
) else (
    echo %REPO_NAME% already exists, skipping clone.
)

popd
goto :eof

:setup_medcat_models
echo.
echo Setting up MedCAT models directory...
set MEDCAT_MODELS_DIR=%GLOBAL_FILES_DIR%\medcat_models
if not exist "%MEDCAT_MODELS_DIR%" (
    echo Creating directory: %MEDCAT_MODELS_DIR%
    mkdir "%MEDCAT_MODELS_DIR%"
    if %errorlevel% neq 0 (
        echo ERROR: Failed to create medcat_models directory.
        goto :deactivate_and_exit
    )
) else (
    echo medcat_models directory already exists.
)
echo Place your MedCAT model pack in this directory > "%MEDCAT_MODELS_DIR%\put_medcat_modelpack_here.txt"
goto :eof

:create_paths_file
echo.
echo Setting up paths.py file...
set "NOTEBOOKS_DIR=%SCRIPT_DIR%notebooks"
set "PATHS_FILE=%NOTEBOOKS_DIR%\paths.py"

if not exist "%NOTEBOOKS_DIR%" (
    echo Creating notebooks directory...
    mkdir "%NOTEBOOKS_DIR%"
    if %errorlevel% neq 0 (
        echo ERROR: Failed to create notebooks directory.
        goto :deactivate_and_exit
    )
)

echo Creating/overwriting "%PATHS_FILE%"...
echo medcat_path = 'put your model pack path here' > "%PATHS_FILE%"
if errorlevel 1 (
    echo ERROR: Failed to create paths.py file.
    goto :deactivate_and_exit
)
echo paths.py created successfully.
goto :eof

:copy_credentials
echo.
echo Setting up credentials.py file...
set SOURCE_CREDS=%SCRIPT_DIR%pat2vec\util\credentials.py
set TARGET_CREDS=%GLOBAL_FILES_DIR%\credentials.py

echo DEBUG: SOURCE_CREDS = "%SOURCE_CREDS%"
echo DEBUG: TARGET_CREDS = "%TARGET_CREDS%"

if not exist "%SOURCE_CREDS%" (
    echo WARNING: Source credentials file not found at "%SOURCE_CREDS%". Skipping copy.
    goto :eof
)

if exist "%TARGET_CREDS%" (
    if "%FORCE_CLEAN%"=="true" (
        echo Force clean enabled, overwriting existing credentials file...
    ) else (
        echo credentials.py already exists. Skipping copy ^(use /f to force overwrite^).
        goto :eof
    )
)

echo Copying credentials template to "%TARGET_CREDS%"...
copy "%SOURCE_CREDS%" "%TARGET_CREDS%"
if errorlevel 1 (
    echo ERROR: Failed to copy credentials file.
    goto :deactivate_and_exit
)
echo IMPORTANT: Make sure to populate the new credentials.py file with your actual credentials!
goto :eof

:fatal_error
echo.
echo ============================================================================
echo                            INSTALLATION FAILED
echo ============================================================================
endlocal
exit /b 1
