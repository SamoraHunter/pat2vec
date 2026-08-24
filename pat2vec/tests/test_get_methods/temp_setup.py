"""Temporary output-directory setup for pat2vec pytest tests.

Calling ``setup_test_temp_dir()`` redirects all relative-path artifacts
(.project directories, SQLite databases, credentials files, logs) into a
per-test temporary folder instead of the repository working tree.

Usage (uniform first fixture for every test class):

    @pytest.fixture(autouse=True, scope="class")
    def _setup_tempdir(self):
        from temp_setup import setup_test_temp_dir, cleanup_test_temp_dir
        cls = type(self)
        cls.temp_dir, cls.repo_root = setup_test_temp_dir()

        # ... rest of your test setup

Yield:
-----
        cleanup_test_temp_dir(cls.temp_dir)

The location of the per-test temp dir can be moved by setting the
``P2V_TEST_OUTPUT_DIR`` environment variable (a fresh subdirectory is
still created for every test run).

"""

import os
import shutil
import sys
import tempfile

#: Directory containing this helper (pat2vec/tests/test_get_methods).
TEST_DIR = os.path.dirname(os.path.abspath(__file__))

#: Repository root (two levels above pat2vec/tests/test_get_methods).
REPO_ROOT = os.path.abspath(os.path.join(TEST_DIR, os.pardir, os.pardir))

#: pat2vec package directory.
PAT2VEC_PKG_DIR = os.path.join(REPO_ROOT, "pat2vec")


def setup_test_temp_dir(prefix: str = "pat2vec_test_") -> tuple:
    """Create a per-test temp output directory, chdir into it, fix sys.path.

    Args:
    ----
        prefix: Prefix for the generated temp directory name.

    Returns:
    -------
        tuple: (temp_dir, repo_root) as absolute paths.

    Raises:
    ------
        OSError: If the temp directory cannot be created or entered.

    """
    base = os.environ.get("P2V_TEST_OUTPUT_DIR", "").strip()
    if base:
        os.makedirs(base, exist_ok=True)
        temp_dir = tempfile.mkdtemp(prefix=prefix, dir=base)
    else:
        temp_dir = tempfile.mkdtemp(prefix=prefix)

    os.chdir(temp_dir)

    for path in (PAT2VEC_PKG_DIR, REPO_ROOT, TEST_DIR):
        if path not in sys.path:
            sys.path.insert(0, path)

    return temp_dir, REPO_ROOT


def cleanup_test_temp_dir(temp_dir: str) -> None:
    """Remove the temp output directory (no-op if it no longer exists).

    Args:
    ----
        temp_dir: Absolute path of the temp directory to remove.

    """
    if temp_dir and os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)
