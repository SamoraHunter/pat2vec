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

## Requirements:

- CogStack (cogstack_v8_lite)
- Elasticsearch
- MedCat
- Numpy
- Pandas
- Scipy
- COGStats
- Datetime
- IO
- Tqdm
- Colorama
- Many more

See util/requirements.txt

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
    
    . **Run the installation script:**
    ```shell
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

- Configure options

- Run all


## Contributing
Contributions are welcome! Please see the contributing guidelines for more information.

## License
This project is licensed under the MIT License - see the LICENSE file for details

![Slide1](https://github.com/SamoraHunter/pat2vec/assets/44898312/f60dcf43-7fbe-4d96-8f33-9603694641b4)


![Slide2](https://github.com/SamoraHunter/pat2vec/assets/44898312/f93f47bb-46ad-4830-a010-4d6880a1bae6)


