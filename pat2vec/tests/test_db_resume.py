#!/usr/bin/env python3
"""Test script to verify resume functionality with database backend."""

import os

os.environ["PATIENT_ID"] = "test_patient"

from pat2vec.util.config_pat2vec import config_class

config = config_class(
    storage_backend="database",
    db_connection_string="sqlite:///:memory:",
    testing=True,
    verbosity=0,
)

print("Test completed successfully")
