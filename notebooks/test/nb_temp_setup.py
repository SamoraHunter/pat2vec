"""Temporary output-directory setup for pat2vec test notebooks.

Calling ``setup_nb_temp_dir()`` in an early notebook cell redirects all
relative-path artifacts (project directories, SQLite databases, credentials
files, logs, batch CSVs) into a per-run temporary folder instead of the
repository working tree, regardless of where the notebook is launched from.

Usage (uniform first setup cell for every test notebook):

    import os
    import sys

    _here = os.getcwd()
    for _c in (_here, os.path.join(_here, "notebooks", "test")):
        if os.path.exists(os.path.join(_c, "nb_temp_setup.py")):
            if _c not in sys.path:
                sys.path.insert(0, _c)
            break
    else:
        raise FileNotFoundError(
            "nb_temp_setup.py not found in the working directory or "
            "notebooks/test. Run this notebook from notebooks/test or the "
            "repository root."
        )

    from nb_temp_setup import cleanup_nb_temp_dir, setup_nb_temp_dir

    nb_temp_dir, repo_root = setup_nb_temp_dir()

The location of the per-run temp dir can be moved by setting the
``P2V_NOTEBOOK_OUTPUT_DIR`` environment variable (a fresh subdirectory is
still created for every run).
"""

import os
import shutil
import sys
import tempfile

#: Directory containing this helper (notebooks/test).
NB_DIR = os.path.dirname(os.path.abspath(__file__))

#: Repository root (two levels above notebooks/test).
REPO_ROOT = os.path.abspath(os.path.join(NB_DIR, os.pardir, os.pardir))

#: pat2vec package directory.
PAT2VEC_PKG_DIR = os.path.join(REPO_ROOT, "pat2vec")


def setup_nb_temp_dir(prefix: str = "pat2vec_nb_") -> tuple:
    """Create a per-run temp output directory, chdir into it, fix sys.path.

    Ensures the pat2vec package dir, the repo root, and the notebooks/test
    dir are importable no matter which directory the kernel started in.

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
    base = os.environ.get("P2V_NOTEBOOK_OUTPUT_DIR", "").strip()
    if base:
        os.makedirs(base, exist_ok=True)
        temp_dir = tempfile.mkdtemp(prefix=prefix, dir=base)
    else:
        temp_dir = tempfile.mkdtemp(prefix=prefix)

    os.chdir(temp_dir)

    for path in (PAT2VEC_PKG_DIR, REPO_ROOT, NB_DIR):
        if path not in sys.path:
            sys.path.insert(0, path)

    return temp_dir, REPO_ROOT


def cleanup_nb_temp_dir(temp_dir: str) -> None:
    """Remove the temp output directory (no-op if it no longer exists).

    Args:
    ----
        temp_dir: Absolute path of the temp directory to remove.

    """
    if temp_dir and os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)
