# Configuration Class (`config_class`)

The `config_class` is the central configuration object that controls all aspects of the pat2vec pipeline. It serves as a comprehensive settings container that defines how data is extracted, processed, and transformed into feature vectors.

## Overview

The configuration class manages three primary areas:

1. **Feature Selection**: Which clinical data types to extract and process
2. **Time Window Definition**: How to slice patient data temporally  
3. **Processing Parameters**: Technical settings for data handling and output

## Core Configuration Parameters

### Feature Selection (`main_options`)

Feature extraction is controlled through the `main_options_dict` dictionary, where each clinical data type can be enabled or disabled:

```python
main_options_dict = {
    'demo': True,           # Demographics (age, ethnicity, death status)
    'bmi': True,            # Body Mass Index measurements
    'bloods': True,         # Blood test results and biochemistry
    'drugs': True,          # Medication orders and prescriptions
    'diagnostics': True,    # Diagnostic orders and procedures
    'core_02': True,        # Core oxygen saturation data
    'bed': True,            # Bed occupancy information
    'vte_status': True,     # Venous thromboembolism status
    'hosp_site': True,      # Hospital site/location data
    'core_resus': True,     # Core resuscitation information
    'news': True,           # National Early Warning Score
    'smoking': True,        # Smoking status and history
    'annotations': True,    # Clinical note annotations via MedCat
    'annotations_mrc': True,# MRC clinical observations
    'appointments': False,  # Appointment scheduling data
    'textual_obs': False,   # Free-text clinical observations
}
```

### Time Window Configuration

The time window system defines how patient data is temporally organized:

#### Global Time Windows
- **`start_date`**: Anchor point for time window calculations
- **`years`, `months`, `days`**: Duration of each patient's time window
- **`time_window_interval_delta`**: Step size for creating multiple vectors per patient
- **`lookback`**: Direction of time window calculation (True = backward, False = forward)

```python
config_obj = config_class(
    start_date=datetime(2019, 1, 1),    # Starting point
    years=2,                            # 2-year window
    months=0,
    days=0,
    time_window_interval_delta=relativedelta(months=6),  # 6-month intervals
    lookback=False                      # Forward-looking from start_date
)
```

#### Individual Patient Windows (IPW)
For patient-specific time windows based on clinical events:

```python
config_obj = config_class(
    individual_patient_window=True,
    individual_patient_window_df=patient_dates_df,
    individual_patient_window_start_column_name='event_date',
    individual_patient_id_column_name='patient_id',
    # ... other parameters
)
```

### Project Organization

- **`proj_name`**: Creates project-specific directories for data storage
- **`suffix`**: Distinguishes multiple runs within the same project
- **`treatment_doc_filename`**: Input file containing patient cohort list

### Data Processing Controls

#### Filtering Options
```python
# Filter specific document types
data_type_filter_dict = {
    'filter_term_lists': {
        'epr_docs': ['Discharge Summary', 'Progress Note'],
        'bloods': ['hemoglobin', 'glucose']
    }
}

# Annotation filtering
annot_filter_arguments = {
    'acc': 0.8,  # Minimum MedCat accuracy
    'types': ['finding', 'disorder', 'procedure'],
    'Presence_Value': ['True'],
    'Presence_Confidence': 0.8
}
```

#### Processing Modes
- **`testing`**: Uses dummy data generators for development
- **`batch_mode`**: Standard operational mode (required)
- **`calculate_vectors`**: Generate feature vectors (vs. just extract raw batches)
- **`split_clinical_notes`**: Parse dates from clinical notes for temporal accuracy

## Common Configuration Patterns

### Research Study Configuration
For a typical clinical research study analyzing medication effects:

```python
config_obj = config_class(
    proj_name='medication_study_2024',
    treatment_doc_filename='cohort_patients.csv',
    main_options={
        'demo': True,
        'drugs': True, 
        'bloods': True,
        'annotations': True,
        'diagnostics': True,
        # Disable unnecessary features
        'bmi': False,
        'appointments': False
    },
    start_date=datetime(2020, 1, 1),
    years=3,
    time_window_interval_delta=relativedelta(months=3),
    lookback=False
)
```

### Testing and Development
For development work with dummy data:

```python
config_obj = config_class(
    proj_name='test_run',
    testing=True,
    dummy_medcat_model=True,
    sample_treatment_docs=10,  # Use only 10 patients
    verbosity=5,               # Maximum debug output
    main_options=get_test_options_dict()  # Only implemented features
)
```

### High-Performance Processing
For large-scale production runs:

```python
config_obj = config_class(
    proj_name='full_cohort_analysis',
    strip_list=True,           # Skip already processed patients
    prefetch_pat_batches=False, # Avoid memory issues
    verbosity=1,               # Minimal logging
    shuffle_pat_list=False     # Maintain consistent ordering
)
```

## Validation and Error Handling

The config class includes built-in validation:

- **Date validation**: Ensures global start dates precede end dates
- **Feature compatibility**: Warns about incompatible parameter combinations
- **Memory management**: Prevents configurations likely to cause out-of-memory errors
- **Testing mode enforcement**: Restricts features to those with dummy data implementations

## Advanced Features

### Annotation Processing
- **MedCat integration**: Automatic clinical note processing with configurable confidence thresholds
- **ICD-10 code extraction**: Appends diagnostic codes to annotation output
- **Temporal annotation filtering**: Separates recent vs. historical clinical findings

### Control Patient Sampling
```python
config_obj = config_class(
    use_controls=True,
    treatment_control_ratio_n=2,  # 2:1 control:treatment ratio
    all_epr_patient_list_path='all_patients.csv'
)
```

The configuration class is designed to be both comprehensive and user-friendly, with sensible defaults for most parameters while allowing fine-grained control over every aspect of the data processing pipeline.