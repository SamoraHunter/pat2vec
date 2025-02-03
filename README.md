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
    cd to gloabl_files
   
    ```shell
    git clone https://github.com/SamoraHunter/pat2vec.git
    cd pat2vec
    ```

    **Run the installation script:**
    ```shell
    install.bat
    ```

3. **Add the `pat2vec` directory to the Python path:**

   Before importing `pat2vec` in your Python script, add the following lines to the script, replacing `/path/to/pat2vec` with the actual path to the `pat2vec` directory inside your project:
   
    ```python
    import sys
    sys.path.append('/path/to/pat2vec')
    ```

4. **Import `pat2vec` in your Python script:**

    ```python
    import pat2vec
    ```

### Unix/Linux:

### **Option 1: Install All Requirements Automatically**  
This option installs `pat2vec` along with its dependencies, including:  
- `pat2vec_env` (virtual environment)  
- `snomed_methods`  
- `cogstack_search_methods`  
- `clinical_note_splitter`  

Before running the installation, ensure you:  
- Place the **model pack** in the appropriate directory  gloabl_files/medcat_models/%modelpack%.zip
- Populate the **credentials file**  under gloabl_files/credentials.py
- *(Optional)* Add a **SNOMED file** if needed  gloabl_files/.. 'snomed', 'SnomedCT_InternationalRF2_PRODUCTION_20231101T120000Z', 'SnomedCT_InternationalRF2_PRODUCTION_20231101T120000Z', 'Full', 'Terminology', 'sct2_StatedRelationship_Full_INT_20231101.txt'

### **Installation Steps:**  

1. Copy the `install_pat2vec.sh` file to your installation directory.  
2. Grant execution permissions:  
   ```sh
   chmod +x install_pat2vec.sh
   ```  
3. Run the installation using one of the following options:  

   - **Standard installation:**  
     ```sh
     ./install_pat2vec.sh
     ```  
   - **Installation with proxy mirror support:**  
     ```sh
     ./install_pat2vec.sh --proxy
     ```  
   - **Install to a specific directory:**  
     ```sh
     ./install_pat2vec.sh --directory /path/to/install
     ```  
   - **Skip cloning repositories (if already cloned manually):**  
     ```sh
     ./install_pat2vec.sh --no-clone
     ```  

### **Repositories Installed by This Script:**  
The script will clone the following repositories:  
- [`pat2vec`](https://github.com/SamoraHunter/pat2vec.git)  
- [`snomed_methods`](https://github.com/SamoraHunter/snomed_methods.git)  
- [`cogstack_search_methods`](https://github.com/SamoraHunter/cogstack_search_methods.git)  
- [`clinical_note_splitter`](https://github.com/SamoraHunter/clinical_note_splitter.git)  

---

## **Option 2: Manual Installation**

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
    - snomed_methods/snomed_methods_v1.py**
    - pat2vec/
    - pat2vec_projects/
        - project_01/
            - example_usage.ipynb
            - treatment_docs.csv
 
*treatment_docs.csv should contain a column 'client_idcode' with your UUID's. 
**https://github.com/SamoraHunter/SNOMED_methods.git

- Configure options

- Run all

- Examine example_usage.ipynb for additional functionality and use cases.

- open example_usage.ipynb and hit run all.

- If testing in a live environment ensure the testing flag is set to False in the config_obj.

## Contributing
Contributions are welcome! Please see the contributing guidelines for more information.

## License
This project is licensed under the MIT License - see the LICENSE file for details

![Slide1](https://github.com/SamoraHunter/pat2vec/assets/44898312/f60dcf43-7fbe-4d96-8f33-9603694641b4)


![Slide2](https://github.com/SamoraHunter/pat2vec/assets/44898312/f93f47bb-46ad-4830-a010-4d6880a1bae6)


