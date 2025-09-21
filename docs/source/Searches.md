# Blood Test Data Search

The `pat2vec` library provides comprehensive functionality for searching and retrieving blood test data from clinical records. This section explains how to use the blood test search methods to extract laboratory results for your patient cohorts.

## Overview

The blood test search functionality allows you to:
- Retrieve laboratory results for specific patients or patient cohorts
- Filter results by date ranges
- Extract standardized blood test measurements
- Access both numeric values and metadata

## Data Fields

When searching for blood test data, the following fields are retrieved:

| Field | Description |
|-------|-------------|
| `client_idcode` | Unique patient identifier |
| `basicobs_itemname_analysed` | Name/type of the blood test (e.g., "Hemoglobin", "Creatinine") |
| `basicobs_value_numeric` | Numeric result value |
| `basicobs_entered` | Date and time when the result was recorded |
| `clientvisit_serviceguid` | Unique identifier for the clinical visit |
| `updatetime` | Timestamp when the record was last updated |

## Basic Usage

### 1. Initialize Pat2vec

First, set up your `pat2vec` configuration and initialize the main object:

```python
import pat2vec
from pat2vec.util.config_pat2vec import config_class
from pat2vec.main_pat2vec import main

# Configure pat2vec
config_obj = config_class(
    testing=False,  # Set to False for production use
    verbosity=9,
    proj_name='blood_analysis_project'
)

# Initialize pat2vec object
pat2vec_obj = main(config_obj=config_obj, cogstack=True)
```

### 2. Search Blood Test Data

```python
# Define patient IDs
patient_ids = ['PATIENT001', 'PATIENT002', 'PATIENT003']

# Search for blood test data
df_bloods = pat2vec.pat2vec_get_methods.get_method_bloods.search_bloods_data(
    cohort_searcher_with_terms_and_search=pat2vec_obj.cohort_searcher_with_terms_and_search,
    client_id_codes=patient_ids
)

# View the results
print(df_bloods.head())
```

## Advanced Parameters

### Date Range Filtering

You can specify custom date ranges to focus on specific time periods:

```python
df_bloods = pat2vec.pat2vec_get_methods.get_method_bloods.search_bloods_data(
    cohort_searcher_with_terms_and_search=pat2vec_obj.cohort_searcher_with_terms_and_search,
    client_id_codes=patient_ids,
    start_year=2020,
    start_month=1,
    start_day=1,
    end_year=2023,
    end_month=12,
    end_day=31
)
```

### Custom Search Filters

Add additional search criteria using the `additional_custom_search_string` parameter:

```python
# Search for specific blood tests only
df_bloods = pat2vec.pat2vec_get_methods.get_method_bloods.search_bloods_data(
    cohort_searcher_with_terms_and_search=pat2vec_obj.cohort_searcher_with_terms_and_search,
    client_id_codes=patient_ids,
    additional_custom_search_string='AND basicobs_itemname_analysed:("Hemoglobin" OR "Creatinine")'
)
```

### Alternative Field Mapping

If your data uses different field names, you can specify them:

```python
df_bloods = pat2vec.pat2vec_get_methods.get_method_bloods.search_bloods_data(
    cohort_searcher_with_terms_and_search=pat2vec_obj.cohort_searcher_with_terms_and_search,
    client_id_codes=patient_ids,
    client_idcode_name="patient_id.keyword",  # Different patient ID field
    bloods_time_field="test_date"             # Different timestamp field
)
```

## Function Parameters Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cohort_searcher_with_terms_and_search` | callable | None | **Required.** The pat2vec search function |
| `client_id_codes` | str or list | None | **Required.** Patient ID(s) to search for |
| `client_idcode_name` | str | "client_idcode.keyword" | Field name for patient IDs |
| `bloods_time_field` | str | "basicobs_entered" | Field name for timestamps |
| `start_year` | int | 1995 | Start year for date range |
| `start_month` | int | 1 | Start month for date range |
| `start_day` | int | 1 | Start day for date range |
| `end_year` | int | 2025 | End year for date range |
| `end_month` | int | 12 | End month for date range |
| `end_day` | int | 12 | End day for date range |
| `additional_custom_search_string` | str | None | Additional Elasticsearch query string |

## Working with Results

### Basic Data Exploration

```python
# Check the shape of results
print(f"Retrieved {len(df_bloods)} blood test records")

# View unique test types
print("Available blood tests:")
print(df_bloods['basicobs_itemname_analysed'].unique())

# Check date range of results
print(f"Date range: {df_bloods['basicobs_entered'].min()} to {df_bloods['basicobs_entered'].max()}")
```

### Data Processing Examples

```python
# Convert date strings to datetime
df_bloods['basicobs_entered'] = pd.to_datetime(df_bloods['basicobs_entered'])

# Filter for specific test types
hemoglobin_results = df_bloods[
    df_bloods['basicobs_itemname_analysed'].str.contains('Hemoglobin', na=False)
]

# Group by patient and test type
patient_summary = df_bloods.groupby(['client_idcode', 'basicobs_itemname_analysed']).agg({
    'basicobs_value_numeric': ['count', 'mean', 'std'],
    'basicobs_entered': ['min', 'max']
}).round(2)
```

## Error Handling

The function includes built-in validation and will raise helpful error messages:

```python
try:
    df_bloods = pat2vec.pat2vec_get_methods.get_method_bloods.search_bloods_data(
        cohort_searcher_with_terms_and_search=pat2vec_obj.cohort_searcher_with_terms_and_search,
        client_id_codes=patient_ids
    )
except ValueError as e:
    print(f"Search error: {e}")
```

## Integration with Other Pat2vec Methods

Blood test data can be combined with other clinical data types:

```python
# Get multiple data types for the same patients
df_demographics = pat2vec.pat2vec_get_methods.get_method_demo.search_demographics(
    cohort_searcher_with_terms_and_search=pat2vec_obj.cohort_searcher_with_terms_and_search,
    client_id_codes=patient_ids
)

df_drugs = pat2vec.pat2vec_get_methods.get_method_drugs.search_drug_orders(
    cohort_searcher_with_terms_and_search=pat2vec_obj.cohort_searcher_with_terms_and_search,
    client_id_codes=patient_ids
)

# Combine datasets for comprehensive patient profiles
# (Further processing would depend on your specific analysis needs)
```

## Best Practices

1. **Date Range Optimization**: Use appropriate date ranges to avoid retrieving unnecessary historical data
2. **Batch Processing**: For large cohorts, process patients in smaller batches to manage memory usage
3. **Data Validation**: Always check the retrieved data for completeness and expected ranges
4. **Field Mapping**: Verify that field names match your Elasticsearch index schema
5. **Error Handling**: Implement proper error handling for production use

## Troubleshooting

**Common Issues:**

- **No results returned**: Check patient IDs exist in the database and date ranges are appropriate
- **Field not found errors**: Verify field names match your Elasticsearch schema
- **Memory issues**: Reduce date ranges or process smaller patient batches
- **Timeout errors**: Consider implementing retry logic for large queries
