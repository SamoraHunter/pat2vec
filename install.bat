@echo off

REM Set paths
set PROJECT_DIR=%~dp0
set VENV_DIR=%PROJECT_DIR%pat2vec_env
set REQUIREMENTS_FILE=%PROJECT_DIR%requirements.txt

REM Create virtual environment
python -m venv %VENV_DIR%

REM Activate virtual environment
call %VENV_DIR%\Scripts\activate

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip
echo Pip upgrade completed.

REM Install requirements
echo Installing requirements...
for /f "delims=" %%i in (%REQUIREMENTS_FILE%) do (
    pip install "%%i" || (
        echo Failed to install package: %%i
        echo Continuing with the next package...
    )
)
echo Requirements installation completed.

REM Install ipykernel
pip install ipykernel

REM Add virtual environment to Jupyter kernelspec
echo Adding virtual environment to Jupyter kernelspec...
python -m ipykernel install --user --name=pat2vec_env
echo Virtual environment added to Jupyter kernelspec.

REM Deactivate virtual environment
deactivate

echo Installation completed.
