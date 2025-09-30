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
set "PYTHON_EXE=python"
set "VENV_DIR=%~dp0pat2vec_env"
set "SPACY_MODEL_URL=https://github.com/explosion/spacy-models/releases/download/en_core_web_md-3.7.1/en_core_web_md-3.7.1-py3-none-any.whl"
set "PROXY_HOST=dh-cap02"
set "PROXY_PORT=8008"
set "PROXY_PIP_ARGS=--trusted-host %PROXY_HOST% -i http://%PROXY_HOST%:%PROXY_PORT%/mirrors/pat2vec"
set "SNOMED_REPO_URL=https://github.com/SamoraHunter/snomed_methods.git"

pushd "%~dp0.."
set "GLOBAL_FILES_DIR=%CD%"
popd

REM --- Default Flags ---
set "ESC="
for /F "tokens=1,2 delims=#" %%a in ('"prompt #$H#$E# & echo on & for %%b in (1) do rem"') do set "ESC=%%b"
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
if errorlevel 1 (
    echo %ESC%[91mERROR: Python is not found. Please install Python and add it to your PATH.%ESC%[0m
    goto :fatal_error
)
git --version >nul 2>&1
if errorlevel 1 (
    echo %ESC%[91mERROR: Git is not found. Please install Git and add it to your PATH.%ESC%[0m
    goto :fatal_error
)
echo %ESC%[92mPrerequisites found.%ESC%[0m

REM --- Pre-flight check for write permissions ---
echo.
echo Checking write permissions in %GLOBAL_FILES_DIR%...
echo. > "%GLOBAL_FILES_DIR%\perm.tmp" 2>nul
if errorlevel 1 (
    echo %ESC%[91mERROR: No write permission in the target directory: '%GLOBAL_FILES_DIR%'.%ESC%[0m
    echo %ESC%[91mPlease run this script from a location where you have write permissions.%ESC%[0m
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
        echo %ESC%[93mForce clean enabled, removing existing virtual environment...%ESC%[0m
        rmdir /s /q "%VENV_DIR%"
    )
)

echo.
echo Creating virtual environment...
if exist "%VENV_DIR%" (
    echo Virtual environment already exists in "%VENV_DIR%". Skipping creation.
) else (
    python -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo %ESC%[91mERROR: Failed to create virtual environment.%ESC%[0m
        goto :fatal_error
    )
)

echo.
echo Activating virtual environment...
call "%VENV_DIR%\Scripts\activate.bat"
if not defined VIRTUAL_ENV (
    echo %ESC%[91mERROR: Failed to activate virtual environment.%ESC%[0m
    goto :fatal_error
)

echo.
echo Upgrading pip...
set PIP_UPGRADE_ARGS=--upgrade pip
if "%PROXY_MODE%"=="true" (set PIP_UPGRADE_ARGS=%PIP_UPGRADE_ARGS% %PROXY_PIP_ARGS%)
python -m pip install %PIP_UPGRADE_ARGS%
if errorlevel 1 (
    echo %ESC%[91mERROR: Failed to upgrade pip.%ESC%[0m
    goto :deactivate_and_exit
)

echo.
echo Installing/upgrading build tools...
set PIP_BUILD_ARGS=--upgrade "setuptools>=61.0" wheel
if "%PROXY_MODE%"=="true" (set PIP_BUILD_ARGS=%PIP_BUILD_ARGS% %PROXY_PIP_ARGS%)
pip install %PIP_BUILD_ARGS%
if errorlevel 1 (
    echo %ESC%[91mERROR: Failed to install build tools.%ESC%[0m
    goto :deactivate_and_exit
)

echo.
echo Installing main project dependencies...
set MAIN_EXTRAS=
if "%INSTALL_MODE%"=="all" (set MAIN_EXTRAS=all)

set INSTALL_TARGET=.
if defined MAIN_EXTRAS (set INSTALL_TARGET=.[%MAIN_EXTRAS%])

echo Running: pip install --no-build-isolation -e "%INSTALL_TARGET%"
set PIP_INSTALL_ARGS=--no-build-isolation -e "%INSTALL_TARGET%"
if "%PROXY_MODE%"=="true" (set PIP_INSTALL_ARGS=%PIP_INSTALL_ARGS% %PROXY_PIP_ARGS% --retries 5 --timeout 60)

pip install %PIP_INSTALL_ARGS%
if errorlevel 1 (
    echo %ESC%[91mERROR: Failed to install project dependencies.%ESC%[0m
    goto :deactivate_and_exit
)

REM Install development dependencies separately from public PyPI
if "%DEV_MODE%"=="true" (
    echo.
    echo Installing development dependencies from public PyPI...
    set "DEV_DEPS="pytest" "nbformat" "nbconvert" "nbstripout" "nbmake" "pre-commit" "sphinx~=7.3.0" "myst-parser>=2.0.0" "sphinx-rtd-theme>=2.0.0" "sphinx-autodoc-typehints>=2.0.0""
    REM We do not use the proxy for these, as they are often missing from internal mirrors.
    pip install %DEV_DEPS%
    if errorlevel 1 (
        echo %ESC%[93mWARNING: Failed to install one or more dev dependencies. Docs build may fail.%ESC%[0m
    )
)

echo.
echo Installing SpaCy model...
set PIP_SPACY_ARGS=
if "%PROXY_MODE%"=="true" (
    set PIP_SPACY_ARGS=en-core-web-md==3.7.1 %PROXY_PIP_ARGS%
) else (
    set PIP_SPACY_ARGS=%SPACY_MODEL_URL%
)
pip install "%PIP_SPACY_ARGS%"
if errorlevel 1 (
    echo %ESC%[93mWARNING: Failed to install SpaCy model. You may need to install it manually.%ESC%[0m
)

echo.
echo Adding virtual environment to Jupyter as 'Python (pat2vec)'...
python -m ipykernel install --user --name=pat2vec_env --display-name "Python (pat2vec)"

echo.
echo ----------------------------------------------------
echo %ESC%[92mInstallation completed successfully!%ESC%[0m
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

pushd "%GLOBAL_FILES_DIR%"

set REPO_NAME=snomed_methods
if not exist "%REPO_NAME%" (
    echo Cloning %SNOMED_REPO_URL%...
    git clone "%SNOMED_REPO_URL%" "%REPO_NAME%"
    if errorlevel 1 (
        echo %ESC%[93mWARNING: Failed to clone %REPO_NAME%. This might be due to a permission issue or network problem.%ESC%[0m
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
    mkdir "%MEDCAT_MODELS_DIR%" >nul 2>&1
    if errorlevel 1 (
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
set "NOTEBOOKS_DIR=%~dp0notebooks"
set "PATHS_FILE=%NOTEBOOKS_DIR%\paths.py"

if not exist "%NOTEBOOKS_DIR%" (
    echo Creating notebooks directory...
    mkdir "%NOTEBOOKS_DIR%"
    if errorlevel 1 (
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
set SOURCE_CREDS=%~dp0pat2vec\util\credentials.py
set TARGET_CREDS=%GLOBAL_FILES_DIR%\credentials.py

if not exist "%SOURCE_CREDS%" (
    echo %ESC%[93mWARNING: Source credentials file not found at "%SOURCE_CREDS%". Skipping copy.%ESC%[0m
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
echo %ESC%[93mIMPORTANT: Make sure to populate the new credentials.py file with your actual credentials!%ESC%[0m
goto :eof

:fatal_error
echo.
echo ============================================================================
echo                            INSTALLATION FAILED
echo ============================================================================
endlocal
exit /b 1
