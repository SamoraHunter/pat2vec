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
set "VENV_DIR=%~dp0pat2vec_env"
set "SPACY_MODEL_URL=https://github.com/explosion/spacy-models/releases/download/en_core_web_md-3.7.1/en_core_web_md-3.7.1-py3-none-any.whl"

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

REM --- Proxy Configuration Validation ---
if "%PROXY_MODE%"=="true" (
    if "%INTERNAL_PROXY_HOST%"=="" (
        echo %ESC%[91mERROR: Proxy mode requested but INTERNAL_PROXY_HOST is not set.%ESC%[0m
        goto :fatal_error
    )
    if "%INTERNAL_PYPI_MIRROR%"=="" (
        echo %ESC%[91mERROR: Proxy mode requested but INTERNAL_PYPI_MIRROR is not set.%ESC%[0m
        goto :fatal_error
    )
    set "PROXY_PIP_ARGS=--trusted-host %INTERNAL_PROXY_HOST% --extra-index-url %INTERNAL_PYPI_MIRROR%"
)

REM --- Prerequisite Checks ---
echo Checking prerequisites...

echo Detecting Python interpreter...
set "PYTHON_EXE="
for %%P in (python3.11 python3.10 python3 python) do (
    if not defined PYTHON_EXE (
        %%P --version >nul 2>&1 && set "PYTHON_EXE=%%P"
    )
)

if not defined PYTHON_EXE (
    echo %ESC%[91mERROR: Python is not installed or not found in PATH.%ESC%[0m
    goto :fatal_error
)
echo Using Python interpreter: %PYTHON_EXE%

git --version >nul 2>&1 || (echo %ESC%[91mERROR: Git is not found.%ESC%[0m & goto :fatal_error)
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

REM Verify we're in the pat2vec directory
for %%I in ("%CD%") do set "CURRENT_DIR_NAME=%%~nxI"
if /I NOT "%CURRENT_DIR_NAME%"=="pat2vec" (
    echo %ESC%[91mERROR: This script must be run from the pat2vec directory.%ESC%[0m
    echo Current directory is: %CD%
    goto :fatal_error
)

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
    %PYTHON_EXE% -m venv "%VENV_DIR%"
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
set "EXTRAS="
if "%INSTALL_MODE%"=="all" (set "EXTRAS=all")
if "%DEV_MODE%"=="true" (
    if defined EXTRAS (set "EXTRAS=%EXTRAS%,dev") else (set "EXTRAS=dev")
)

set "INSTALL_TARGET=."
if defined EXTRAS (set "INSTALL_TARGET=.[%EXTRAS%]")

echo Running: pip install --no-build-isolation -e "%INSTALL_TARGET%"
set PIP_INSTALL_ARGS=--no-build-isolation -e "%INSTALL_TARGET%"
if "%PROXY_MODE%"=="true" (set PIP_INSTALL_ARGS=%PIP_INSTALL_ARGS% %PROXY_PIP_ARGS% --retries 5 --timeout 60)

pip install %PIP_INSTALL_ARGS%
if errorlevel 1 (
    echo %ESC%[91mERROR: Failed to install project dependencies.%ESC%[0m
    goto :deactivate_and_exit
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
if defined VIRTUAL_ENV call deactivate

echo.
echo To activate the environment, run:
echo   Command Prompt: call "%VENV_DIR%\Scripts\activate.bat"
echo   PowerShell:     %VENV_DIR%\Scripts\Activate.ps1
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

REM Save current environment proxy settings and global git config
set "SAVE_HTTP_PROXY=%http_proxy%"
set "SAVE_HTTPS_PROXY=%https_proxy%"
for /f "tokens=*" %%i in ('git config --global --get http.proxy 2^>nul') do set "SAVE_GIT_HTTP_PROXY=%%i"
for /f "tokens=*" %%i in ('git config --global --get https.proxy 2^>nul') do set "SAVE_GIT_HTTPS_PROXY=%%i"

if "%PROXY_MODE%"=="false" (
    echo Temporarily disabling proxy for Git operations...
    set http_proxy=
    set https_proxy=
    set HTTP_PROXY=
    set HTTPS_PROXY=
    git config --global --unset http.proxy >nul 2>&1
    git config --global --unset https.proxy >nul 2>&1
)

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

REM Restore proxy settings
if "%PROXY_MODE%"=="false" (
    if defined SAVE_HTTP_PROXY set "http_proxy=%SAVE_HTTP_PROXY%"
    if defined SAVE_HTTPS_PROXY set "https_proxy=%SAVE_HTTPS_PROXY%"
    if defined SAVE_GIT_HTTP_PROXY git config --global http.proxy "%SAVE_GIT_HTTP_PROXY%"
    if defined SAVE_GIT_HTTPS_PROXY git config --global https.proxy "%SAVE_GIT_HTTPS_PROXY%"
)
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
