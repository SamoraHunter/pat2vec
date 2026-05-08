# Changelog

All notable changes to this project will be documented in this file.

## [0.3.2] - 2024-05-24

### Packaging & Distribution
- **Asset Bundling**: Enabled `include-package-data` to ensure Jupyter notebooks and test files are included in the distributed Wheel and Source distributions.
- **Path Resolution**: Refactored `config_class` to use absolute path resolution for default test files, fixing "File Not Found" errors when running the package from a site-packages installation.

### Improvements
- **Notebooks**: Included example notebooks in the package for easier user onboarding.

## [0.3.1] - 2024-05-23

### CI/CD & Automation
- **Automated GitHub Releases**: Integrated `softprops/action-gh-release` to automatically create releases and attach build binaries (.whl and .tar.gz) upon tag push.
- **Artifact Persistence**: Added workflow steps to store distribution packages as GitHub Action artifacts.
- **Formatting**: Added `black` to the build environment to ensure generated package metadata is consistently formatted.

### Improvements
- **Testing**: Added `test_integration_elastic.py` to provide a reference implementation for the new Elasticsearch testing framework.

---

## [0.3.0] - 2024-05-22

### New Features
- **Integrated Elasticsearch Testing**: A powerful new framework for end-to-end integration testing using Dockerized Elasticsearch instances. This allows developers to validate pipelines against real search indices rather than static mocks.
- **Automated Synthetic Data Seeding**: New utilities to seed test clusters with realistic, timestamped patient records and automated schema management via `elastic_schemas.json`.
- **Automated Versioning**: Package versioning is now automatically synchronized between `pyproject.toml` and package metadata.

### Improvements & Bug Fixes
- **Data Ingestion Safety**: Implemented safety guardrails to block accidental data ingestion into production hosts during testing.
- **Standardized Date Formatting**: Optimized synthetic data generators to use strict ISO 8601 formatting, improving Elasticsearch dynamic mapping accuracy.
- **Robustness**: Enhanced index bounds checking in `main_pat2vec` and improved sanitization logic for large patient cohorts to prevent `TypeErrors` with mixed-type IDs.
- **CI/CD Optimization**: Full support for local runner environments (via `act`), including automated Docker configuration for notebook tests.

---

## [0.2.0] - 2024-03-12

### Database Backend Implementation
This release introduces a robust database backend using SQLAlchemy, which replaces the legacy file-based system as the default storage mechanism.

### New Features
- **Database Support**: Added support for SQLite (default) and PostgreSQL. Defaults to a local `{project_name}.db` SQLite database if no connection string is provided. Supports in-memory SQLite for testing.
- **Schema Management**: Automatic table creation and schema updates for Raw Data, Annotations, and Features (using JSON serialization for sparse/high-dimensional data).
- **Migration Utility**: Added `pat2vec/util/migrate_to_db.py` to migrate existing file-based projects to the new database structure.

### Configuration Changes
- Added `storage_backend` option to `config_class` (values: `'database'`, `'file'`).
- Added `db_connection_string` option to `config_class`.

### Technical Improvements
- **Centralized Data Retrieval**: Implemented `get_df_from_db` and updated `retrieve_patient_data` to abstract data access.
- **Performance**: Implemented batch insertion and automatic index creation on primary keys (e.g., `client_idcode`, timestamps) to improve query performance.
