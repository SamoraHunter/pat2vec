---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

---
name: Bug Report
about: Report an issue with pat2vec
title: "[Bug] <Brief description>"
labels: bug
assignees: ''
---

**Description**
A clear and concise description of the issue.

**Steps to Reproduce**
1. What command or code did you run?
2. What input data did you use?
3. What error messages or behaviors did you observe?

# Example code that caused the issue
```bash
example_usage.ipynb 
get_treatment_docs_by_iterative_multi_term_cohort_searcher_no_terms_fuzzy(pat2vec_obj=pat2vec_obj,
                                                                          term_list=term_list,
                                                                          overwrite=False,
                                                                          append=False,
                                                                          verbose=9,
                                                                          mct=True, #
                                                                          textual_obs=True,
                                                                          additional_filters=None,
                                                                          all_fields=False
                                                                          )
```

## Expected Behaviour
Describe what you expected to happen.

## Actual Behaviour
Describe what actually happened, including any error messages.

## Environment Details
- **Operating System:** (e.g., Ubuntu 20.04, Windows 11)  
- **Python Version:** (e.g., 3.8.10)  
- **pat2vec Version:** (e.g., v4.4.6)  
- **Installation Method:** (e.g., `install.sh`, `install_pat2vec.sh`, `pip`, `conda`)  

## Additional Context
Add any other context about the problem here, such as:  
- **Dependencies used:** (e.g., [SNOMED_methods](https://github.com/SamoraHunter/SNOMED_methods), [MedCAT](https://github.com/CogStack/MedCAT))  
- **Configuration files**  
- **Relevant logs or screenshots**  


# Example code that caused the issue
example_usage.ipynb
```bash
config_obj = config_class(
    treatment_doc_filename='test_files/treatment_docs.csv',
    treatment_control_ratio_n=1,  # Ratio for treatment to control
     shuffle_pat_list=False,  # Flag for shuffling patient list
    time_window_interval_delta = relativedelta(years=1), #specify the time window to collapse each feature vector into, years=1 is one vector per year within the global time window
    split_clinical_notes=True, #will split clinical notes by date and treat as individual documents with extracted dates. Requires note splitter module. 
    lookback = True, # when calculating individual patient window from table of start dates, will calculate backwards in time if true. Else Forwards. When calculating from global start date, will calculate backwards or forwards respectively. 
    add_icd10 = False, #append icd 10 codes to annot batches. Can be found under current_pat_documents_annotations/%client_idcode%.csv.
    add_opc4s=False, # needs icd10 true also. Can be found under current_pat_documents_annotations/%client_idcode%.csv
    override_medcat_model_path = None, #Force medcat model path, if None uses defaults for env. #Can be set in paths.py with medcat_path = %path to medcat model pack.zip"
    data_type_filter_dict = None, # Dictionary for data type filter, see examples above. 
    filter_split_notes = True # If enabled, will reapply global time window filter post clinical note splitting. Recommended to enable if split notes enabled.
```
  ...
