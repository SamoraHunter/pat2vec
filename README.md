# pat2vec
Converts individual patient data into time interval feature vectors, suitable for filtering and concatenation into a data matrix D for binary classification machine learning tasks.


Example use case 1: I aim to compute the mean of n variables for each unique patient, resulting in a single row representing each patient.

Example use case 2: I intend to generate a monthly time series comprising patient data encompassing biochemistry, demographic details, and textual annotations (MedCat annotations) spanning the last 25 years. Each patient's data begins from a distinct start date (diagnosis date), providing a retrospective view.



## Table of Contents
- [Requirements](#requirements)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Notable requirements:

- CogStack (cogstack_v8_lite) ([cogstack_search_methods](https://github.com/SamoraHunter/cogstack_search_methods))
- Elasticsearch
- MedCat https://github.com/CogStack/MedCAT

## Dependencies:
# Notable dependencies, for the full list see requirements.txt

transformers==4.45.2
datasets==2.17.1
huggingface-hub==0.19.1
protobuf==3.20.3
accelerate==0.23.0
scikit-learn==1.5.2
torch==2.4.1
torchvision==0.17.1
torchaudio==2.4.1
tensorflow==2.13.1
keras==2.13.1
nltk==3.8.1
spacy==3.6.1
gensim==4.2.0
fasttext==0.9.2
flair==0.12.2
pandas==1.5.3
polars==1.9.0
pandas-profiling==3.6.6
ydata-profiling==4.5.0
matplotlib==3.7.3
seaborn==0.12.2
plotly==5.17.0
bokeh==3.2.2
scipy==1.11.3
lifelines==0.27.10
statsmodels==0.14.0
xgboost==1.7.6
catboost==1.3.1
lightgbm==4.1.0
numpy==1.24.3
pillow==10.0.1
pydantic==1.10.12
requests==2.31.0
tqdm==4.66.1
pyyaml==6.0.1
sqlalchemy==2.0.21
protobuf==4.24.3
pydub==0.25.1
audiofile==1.2.0
librosa==0.10.4
boto3==1.28.65
pathy==0.10.2
smart-open==7.0.1
thinc==8.1.13
preshed==3.0.9
blis==0.7.10
cython==3.0.2
joblib==1.3.2
numba==0.57.1
dask==2023.10.1
smart-open==7.0.1
pyarrow==17.0.0
tiledb==0.21.7
triton==3.14.0
tensorboard==2.14.1
beautifulsoup4==4.12.2
sqlparse==0.4.4
nvidia-cublas-cu12==12.2.6.1
nvidia-cuda-nvrtc-cu12==12.3.55
nvidia-cuda-runtime-cu12==12.3.55
nvidia-cudnn-cu12==8.9.4.31
nvidia-cufft-cu12==11.0.9.91
nvidia-cusolver-cu12==11.6.2.12
nvidia-cusparse-cu12==12.1.0.56
nvidia-nccl-cu12==2.18.1
nvidia-nvtx-cu12==12.1.105
nvidia-ml-py==11.8.0


See requirements.txt

## Features:

- Single patient
- Batch patient
- Cohort search and creation
- Automated random controls
- Modular feature space selection
- Look back
- Look forward
- Individual patient time windows. 

## Installation

### Windows:

1. **Clone the repository:**
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    cd pat2vec
    ```

    **Run the installation script:**
    ```shell
    install.bat
    ```

2. **Add the `pat2vec` directory to the Python path:**

   Before importing `pat2vec` in your Python script, add the following lines to the script, replacing `/path/to/pat2vec` with the actual path to the `pat2vec` directory inside your project:
   
    ```python
    import sys
    sys.path.append('/path/to/pat2vec')
    ```

3. **Import `pat2vec` in your Python script:**

    ```python
    import pat2vec
    ```

### Unix/Linux:

1. **Clone the repository:**
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    ```
    
    . **Run the installation script:**
    
    ```shell
    (Requires python3 on path and venv)
    chmod +x install.sh
    ./install.sh
    ```
    
    cd pat2vec
    ```

2. **Add the `pat2vec` directory to the Python path:**

   Before importing `pat2vec` in your Python script, add the following lines to the script, replacing `/path/to/pat2vec` with the actual path to the `pat2vec` directory inside your project:
   
    ```python
    import sys
    sys.path.append('/path/to/pat2vec')
    ```

3. **Import `pat2vec` in your Python script:**

    ```python
    import pat2vec
    ```


## Usage:

- Set paths, gloabl_files/medcat_models/modelpack.zip, gloabl_files/snomed_methods, gloabl_files/..

- gloabl_files/
    - medcat_models/
        - modelpack.zip
    - SNOMED_methods/snomed_methods_v1.py**
    - pat2vec/
    - pat2vec_projects/
        - project_01/
            - example_usage.ipynb
            - treatment_docs.csv
 
*treatment_docs.csv should contain a column 'client_idcode' with your UUID's. 
**https://github.com/SamoraHunter/SNOMED_methods.git

- Configure options

- Run all

- Examine example_usage.ipynb for additional functionality and usecases. 

## Contributing
Contributions are welcome! Please see the contributing guidelines for more information.

## License
This project is licensed under the MIT License - see the LICENSE file for details

![Slide1](https://github.com/SamoraHunter/pat2vec/assets/44898312/f60dcf43-7fbe-4d96-8f33-9603694641b4)


![Slide2](https://github.com/SamoraHunter/pat2vec/assets/44898312/f93f47bb-46ad-4830-a010-4d6880a1bae6)


