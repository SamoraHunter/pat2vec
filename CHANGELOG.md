# CHANGELOG

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased (dev branch) - Since v0.3.4

This document summarizes all changes made to the pat2vec repository since the last release (v0.3.4). The development branch (`dev`) contains unreleased features, fixes, and improvements that may be included in future releases.

### Added

- Add ascribe_translog support with NHS number search (`3d2b3fd`, SamoraHunter, 2026-09-11)
- Implement consistent feature structure for empty annotation results (`41a420d`, Samora Hunter, 2026-08-23)
- Add `warn_on_missing=False` parameter to suppress warnings for missing tables (`7f79be6`, SamoraHunter, 2026-08-22)
- Add new test notebooks and credentials (`d8e9b3f`, Samora Hunter, 2026-08-18)
- Implement JSON packing for SQLite column limits (`2d1bdac`, SamoraHunter, 2026-07-21)

### Changed

- Support Epic data sources and improve merging logic (`f50c5c5`, Samora Hunter, 2026-08-27)
- Standardize time field naming across ES and DB schemas to `document_UpdatedWhen` (`7ba3674`, SamoraHunter, 2026-08-26)
- Centralize Elasticsearch index configuration (`4c5c0fd`, Samora Hunter, 2026-08-27)

### Fixed

- Handle missing `client_dob` column in `demo_to_latest` (`b713e45`, SamoraHunter, 2026-09-11)
- Handle remaining edge cases and `generate_date_list` (`aa117f9`, Samora Hunter, 2026-09-01)
- Improve data handling and notebook stability (`effec1a`, Samora Hunter, 2026-08-28)
- Correct default values and table names in config (`4190364`, SamoraHunter, 2026-08-18)
- Correct indentation in `anonymisation_deid_documents.py` (`586c78f`, Samora Hunter, 2026-08-17)
- Update MAPPINGS for epic tables to use `updatetime` instead of `document_CreatedWhen` (`16e47fe`, SamoraHunter, 2026-08-08)
- Add UTC timezone to all datetime parsing (`e30a8da`, Samora Hunter, 2026-07-21)

### Refactored

- Remove debug print statements and fix logic (`c0babfd`, SamoraHunter, 2026-09-11)
- Remove redundant Elasticsearch container setup (`0d13383`, Samora Hunter, 2026-09-11)
- Implement client_idcode ↔ NHS number conversion functions (`d419d1f`, `99f2c6b`, SamoraHunter, 2026-09-09)
- Improve credential handling and test reliability (`a6d2afd`, Samora Hunter, 2026-09-09)
- Reduce debug verbosity and consolidate tests (`d36088c`, SamoraHunter, 2026-09-08)
- Add database caching support and fix data handling (`c120aee`, Samora Hunter, 2026-09-01)
- Rename `basicobs_guid` to `observation_guid` with alias (`e4e72cc`, SamoraHunter, 2026-09-01)
- Add temporary print statements for pat_maker debugging (`f5c9572`, Samora Hunter, 2026-09-01)
- Fix duplicate `updatetime` column removal logic (`fd5dd37`, SamoraHunter, 2026-09-01)
- Replace print debug statements with proper logger calls (`894551c`, Samora Hunter, 2026-08-27)
- Normalize datetime handling and update annotation flags (`08c0881`, SamoraHunter, 2026-08-27)
- Add epic clinical notes, imaging reports and medical history pipelines (`c1483bf`, Samora Hunter, 2026-08-27)
- Simplify date range handling and improve test coverage (`f92dfb8`, SamoraHunter, 2026-08-27)
- Adjust verbosity thresholds for debug logging (`e91bce0`, Samora Hunter, 2026-08-27)
- Improve error handling with explicit exceptions (`cebda03`, SamoraHunter, 2026-08-27)
- Standardize annotation method naming convention (`0dfd4c2`, Samora Hunter, 2026-08-06)
- Extract problem list features from `epic_medical_history` (`1a5e865`, SamoraHunter, 2026-08-26)
- Improve test isolation with dynamic temp directories (`39c62ba`, `b3911fc`, Samora Hunter, 2026-08-26)
- Improve test infrastructure and fix get_method issues (`f336595`, SamoraHunter, 2026-08-25)
- Improve feature validation and test infrastructure (`93823c0`, SamoraHunter, 2026-08-24)
- Normalize smoking labels and fix NA handling (`37a7e91`, Samora Hunter, 2026-08-24)
- Add vector validation and clean up debug code (`036818c`, SamoraHunter, 2026-08-24)
- Update bloods test validation and config storage backend handling (`51dbf08`, Samora Hunter, 2026-08-23)
- Improve table error detection and merge builder validation (`278e170`, SamoraHunter, 2026-08-22)
- Standardize notebook test infrastructure with canonical merge builders (`794c8a9`, Samora Hunter, 2026-08-22)
- Simplify core_02 test notebook merge logic (`bb539bb`, SamoraHunter, 2026-08-22)
- Correct patient_id variable name in annotation methods (`addf9ee`, Samora Hunter, 2026-08-21)
- Use shared merge functions and auto-sync annotation raw data options (`460173c`, SamoraHunter, 2026-08-21)
- Correct OBS item names and standardize test notebook merges (`101ee50`, Samora Hunter, 2026-08-20)
- Add database storage fallback and improve logging for annotation methods (`df33c2b`, SamoraHunter, 2026-08-20)
- Replace local merge functions with shared post_processing_build_methods (`a83bdca`, `3e4b801`, Samora Hunter, 2026-08-20)
- Add epic_medical_history_annotations support (`5281ca5`, SamoraHunter, 2026-08-20)
- Migrate test infrastructure to fixture-based setup (`d3a6ada`, Samora Hunter, 2026-08-19)
- Improve credential loading with multiple fallback paths (`9eb00af`, SamoraHunter, 2026-08-19)
- Rename database tables for consistency (`be6aad7`, Samora Hunter, 2026-08-19)
- Improve utility functions and configuration (`1afa37d`, SamoraHunter, 2026-08-18)
- Add new get method functions (`4ce6419`, Samora Hunter, 2026-08-18)
- Logging and pathlib modernization (`c5662d9`, `f8840fe`, SamoraHunter, 2026-08-17)
- Apply ruff formatter fixes (`9046aca`, Samora Hunter, 2026-08-16)
- Modernize Python codebase with trailing commas and cleanup docstrings (`ca4060f`, SamoraHunter, 2026-08-16)
- Replace subprocess arguments with `capture_output=True` and `check=False` (`bc45454`, Samora Hunter, 2026-08-11)
- Migrate Ruff config to `pyproject.toml` with comprehensive exclude patterns; clean up generate_init.py and list_functions.py (`11cbfb8`, SamoraHunter, 2026-08-16)
- Modernize epic clinical notes annotations batch processing (`6b546aa`, `9826d8d`, `2e1afe0`, Samora Hunter, 2026-08-08)
- Modernize notebook examples and utility modules (`bfe0a69`, `c244c74`, `4d33a2d`, `7927a1d`, SamoraHunter, 2026-08-08)
- Modernize imports and type hints in `main_pat2vec.py` to PEP 604 syntax (`0693e96`, Samora Hunter, 2026-08-08)
- Add automatic patient ID column detection fallback for Epic-style IDs (`c8414d7`, `7ce92f4`, SamoraHunter, 2026-08-06)
- Handle None/empty fields_list with defaults in Epic data generators (`5d6ee15`, `c6427b2`, Samora Hunter, 2026-08-06)
- Add Elasticsearch fallback for Epic data when not in database (`a635493`, SamoraHunter, 2026-08-06)
- Add epic_medical_history and epic_imaging_reports annotation options (`cd28131`, Samora Hunter, 2026-08-06)
- Replace direct raw fetches with annotation-based batch configs (`b3a7e54`, SamoraHunter, 2026-08-06)
- Support column standardization and Epic data sources (`f3a4b1c`, Samora Hunter, 2026-08-05)

### Removed

- Remove epic_clinical_notes_results.csv from commit (`b4697f9`, SamoraHunter, 2026-08-27)
- Remove legacy test artifacts and reorganize structure (`5e1c32b`, Samora Hunter, 2026-08-18)
- Remove test artifacts (`d38224a`, SamoraHunter, 2026-08-17)

### Deprecated

- deleted e2e test, too slow to execute. (`2a4594e`, Samora Hunter, 2026-08-21)

### Security

- No security changes in this release.

---

## [0.3.4] - 2026-06-02

### Added

- **LLM Validation Notebooks**: Added example notebooks for LLM-based clinical annotation validation (`57f0170`, Samora Hunter, 2026-05-31)
- **Architecture Documentation**: Added architecture diagrams and documentation for LLM modules (`385f9a5`, Samora Hunter, 2026-05-31)

### Changed

- **Robust Data Handling**: Standardized on ISO8601 for robust timestamp parsing; implemented fixes to handle mixed timezone-aware and naive timestamps during data filtering (`4530680`, `552ebc7`, SamoraHunter, 2026-06-01)
- **Infrastructure Abstraction**: Removed hardcoded internal addresses and refactored setup logic to improve robustness across varying environments (`d8deaee`, Samora Hunter, 2026-06-01)

### Fixed

- **CI/CD Connectivity**: Robustified local-setup actions to better support corporate networks and Gitea mirrors (`611e2ad`, SamoraHunter, 2026-06-01)
- **Logging Improvements**: Updated validator documentation, added environment logging to notebook tests, removed deprecated notebook initialization content (`a0d9b81`, `b7545a4`, Samora Hunter, 2026-06-01)

### Refactored

- **Progress Bar Logging**: Improved progress bar logging and removed unused variables (`9d3ccf6`, Samora Hunter, 2026-05-21)
- **Standardized CI/CD**: Standardized CI/CD configurations and update distribution package management (`324094d`, SamoraHunter, 2026-06-01)

---

## [0.3.2] - 2026-05-08

### Added

- **Search Result Caching**: Implemented search result caching and project-folder awareness in search methods (`2a420af`, Samora Hunter, 2026-04-13)
- **Enhanced Gitea Synchronization**: Enhanced Gitea release synchronization robustness and configuration (`f89baa2`, SamoraHunter, 2026-04-15)

### Fixed

- **Bloods Output Path**: Fixed bloods output to save to `root_path/proj_name/` instead of `root_path/` (`96eadc2`, Samora Hunter, 2026-04-15)
- **Time Calculation Bug**: Improved robustness and fixed time calculation bug in `get_method_bloods` (`27798e4`, SamoraHunter, 2026-04-15)
- **Field List Formatting**: Improved patient existence check robustness and field list formatting (`3bac724`, Samora Hunter, 2026-04-14)

### Refactored

- **Optimized Annotation Pipeline**: Optimized annotation pipeline for large-scale cohort processing (`7bc3bdd`, SamoraHunter, 2026-04-29)
- **Post-processing Optimization**: Optimized post-processing and version bump to 0.3.1 (`a3c4e5b`, Samora Hunter, 2026-04-13)

### Packaging & Distribution

- **Package Data**: Included package data and fixed test file path resolution (`2a9a252`, SamoraHunter, 2026-05-08)
- **Documentation Updates**: Updated search guides and configuration examples (`0bace5f`, Samora Hunter, 2026-04-13)

---

## [0.3.1] - 2026-04-10

### CI/CD & Automation

- **Automated GitHub Releases**: Integrated `softprops/action-gh-release` to automatically create releases and attach build binaries (.whl and .tar.gz) upon tag push (`533bfdd`, Samora Hunter, 2026-04-10)
- **Artifact Persistence**: Added workflow steps to store distribution packages as GitHub Action artifacts (`81900ac`, SamoraHunter, 2026-04-10)
- **Formatting**: Added `black` to the build environment to ensure generated package metadata is consistently formatted

### Testing

- **Elasticsearch Testing Framework**: Added reference implementation for new Elasticsearch testing framework via `test_integration_elastic.py`

---

## [0.3.0] - 2026-04-10

### New Features

- **Integrated Elasticsearch Testing Framework**: Implemented comprehensive end-to-end integration testing using Dockerized Elasticsearch instances (`f3b4711`, Samora Hunter, 2026-04-07)
- **Automated Synthetic Data Seeding**: New utilities to seed test clusters with realistic, timestamped patient records and automated schema management via `elastic_schemas.json`
- **Testing Infrastructure**: Enhanced testing framework for robust development workflows (`96ddfab`, SamoraHunter, 2026-04-08)

### Improvements & Bug Fixes

- **Data Ingestion Safety**: Implemented safety guardrails to block accidental data ingestion into production hosts during testing (`c22e8fa`, Samora Hunter, 2026-04-08)
- **Standardized Date Formatting**: Optimized synthetic data generators to use strict ISO 8601 formatting for improved Elasticsearch dynamic mapping accuracy
- **Robustness**: Enhanced index bounds checking in `main_pat2vec` and improved sanitization logic for large patient cohorts (`deda277`, SamoraHunter, 2026-04-07)
- **CI/CD Optimization**: Full support for local runner environments (via `act`), including automated Docker configuration for notebook tests

### CI/CD Enhancements

- **Runner Detection**: Refactored runner detection and improved Docker setup (`90f7aef`, Samora Hunter, 2026-04-09)
- **PyPI Workflow**: Added PyPI publish workflow for package distribution
- **Test Hardening**: Hardened notebook tests and elasticsearch integration for self-hosted runners (`5216e58`, SamoraHunter, 2026-04-08)

---

## [0.2.0] - 2026-03-23

### Database Backend Implementation

This release introduces a robust database backend using SQLAlchemy, which replaces the legacy file-based system as the default storage mechanism.

### New Features

- **Database Support**: Added support for SQLite (default) and PostgreSQL. Defaults to a local `{project_name}.db` SQLite database if no connection string is provided. Supports in-memory SQLite for testing (`a1cb206`, SamoraHunter, 2026-03-20)
- **Schema Management**: Automatic table creation and schema updates for Raw Data, Annotations, and Features (using JSON serialization for sparse/high-dimensional data)
- **Migration Utility**: Added `pat2vec/util/migrate_to_db.py` to migrate existing file-based projects to the new database structure
- **CRUD Helpers**: Implemented comprehensive CRUD operations for database-backed workflows (`e9fc954`, SamoraHunter, 2026-03-20)
- **Patient Existence Validation**: Added patient existence validation and enhanced backend flexibility (`2d1e8da`, Samora Hunter, 2026-03-20)

### Configuration Changes

- Added `storage_backend` option to `config_class` (values: `'database'`, `'file'`)
- Added `db_connection_string` option to `config_class`

### Technical Improvements

- **Centralized Data Retrieval**: Implemented `get_df_from_db` and updated `retrieve_patient_data` to abstract data access (`7f28572`, Samora Hunter, 2026-03-20)
- **Unified Data Pipeline**: Integrated database backend into main pipeline with improved imports (`0971c8a`, SamoraHunter, 2026-03-20)
- **Post-processing Support**: Added database support to post-processing build methods and utilities (`cc10e57`, `ab5f863`, Samora Hunter, 2026-03-20)
- **Batch Operations**: Implemented batch operations for IPW builder and optimized concatenation (`88f68d8`, `dd42c2a`, SamoraHunter, 2026-03-20)
- **Annotation Support**: Added database support for annotations and refactored dataframe creation (`21375f2`, `1bbeb3c`, Samora Hunter, 2026-03-20)

### Testing

- **Integration Tests**: Added full flow integration tests for database backend (`1812b99`, `d246370`, SamoraHunter, 2026-03-20)
- **Unit Tests**: Implemented comprehensive unit tests for database backend operations (`06c62f3`, Samora Hunter, 2026-03-20)

### Examples & Documentation

- **Database Architecture Diagrams**: Added database architecture and schema diagrams (`3b2b844`, SamoraHunter, 2026-03-20)
- **Example Notebooks**: Updated example notebooks for database backend usage (`89d375f`, Samora Hunter, 2026-03-20)

### Bug Fixes

- **Schema Mismatch**: Fixed schema mismatch in clinical note splitter (`87b2924`, SamoraHunter, 2026-03-21)
- **Import Issues**: Resolved duplicate imports in `__init__.py` generation and linter errors (`7444875`, Samora Hunter, 2026-03-13)

### CI/CD & Maintenance

- **Python Download Robustness**: Made Python download robust to missing certificates (`d0d9c96`, SamoraHunter, 2026-03-15)
- **CI Refactoring**: Improved workflow environment detection and reliability (`a969911`, `d0d9c96`, Samora Hunter, 2026-03-15)

---

## [0.1.1] - 2025-09-22

### Initial Release (with fixes and improvements through v0.1.x)

This is the earliest tagged release in the repository. The following changes have been incorporated into subsequent releases:

### Core Features (incorporated from early development)

- **Cohort extraction with search functions**
- **Feature engineering pipeline for medical data**
- **Local runner support via `act`**
- **Documentation workflow migration to GitHub Pages**

### Bug Fixes & Maintenance

- **Trailing whitespace fixes** (`e9aceb0`, SamoraHunter, 2025-09-30)
- **Artifact upload improvements for various runner environments** (`27d3aab`, `ccc1666`, Samora Hunter, 2025-09-30)

---

### Legend

| Type | Description |
|------|-------------|
| Added | New features or functionality added |
| Changed | Changes in existing functionality |
| Fixed | Bug fixes |
| Refactored | Code restructuring without functional changes |
| Removed | Deprecated features or code removed |
| Deprecated | Features marked for future removal |
| Security | Security-related improvements |

### Contributors

- Samora Hunter
- SamoraHunter (git commits prior to 2026-03-20)
