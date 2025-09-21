# Welcome to the pat2vec Wiki!

This wiki contains comprehensive documentation for `pat2vec`.

## Overview

`pat2vec` is a Python-based tool designed to transform raw electronic health records (EHR) into structured, time-series feature vectors. This process makes the data suitable for machine learning tasks, particularly binary classification. It can aggregate data at the patient level or construct detailed longitudinal timelines.

## Example Use Cases

### 1. Patient-Level Aggregation
Compute summary statistics (e.g., the mean of *n* variables) for each unique patient, resulting in one row per patient. This is ideal for models requiring a single representation per individual.

### 2. Longitudinal Time Series Construction
Generate a monthly time series for each patient that includes:

- Biochemistry results
- Demographic attributes
- MedCat-derived clinical text annotations

The time series spans up to 25 years retrospectively, aligned to each patient's diagnosis date, enabling a consistent retrospective view across varying start times.
