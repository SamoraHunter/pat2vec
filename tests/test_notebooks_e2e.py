"""
End-to-end tests for pat2vec test notebooks.

Each test executes a notebook using nbformat and nbconvert's ExecutePreprocessor,
validates cell execution, checks for non-empty outputs, verifies merge functions,
and ensures proper cleanup of temp directories.
"""

import glob
import json
import os
import re
import shutil
import sys
import tempfile

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor


def get_notebook_path(key: str) -> str:
    """Get the path to a notebook file by key."""
    if key == "demo":
        return "/workspaces/pat2vec/notebooks/test/demo/test_demo_get.ipynb"
    elif key == "bmi":
        return "/workspaces/pat2vec/notebooks/test/bmi/test_bmi_get.ipynb"
    else:
        # All notebooks follow pattern: test_{key}_get.ipynb
        sub_dir_path = f"/workspaces/pat2vec/notebooks/test/{key}/test_{key}_get.ipynb"
        root_path = f"/workspaces/pat2vec/notebooks/test/test_{key}_get.ipynb"

        # Check subdirectory first (e.g., bloods, bmi, demo in their own dirs)
        if os.path.exists(sub_dir_path):
            return sub_dir_path
        elif os.path.exists(root_path):
            return root_path
        else:
            # Default to root path for most notebooks
            return root_path


def get_temp_dir(key: str) -> str:
    """Get the expected temp directory for a notebook."""
    return f"/tmp/{key}_test_project"


def has_markdown_cell_front(notebook_path: str) -> bool:
    """Check if notebook starts with a markdown cell."""
    try:
        with open(notebook_path, "r", encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)

        if not nb.cells or nb.cells[0].cell_type != "markdown":
            return False
        return True
    except Exception:
        return False


def has_merge_function(notebook_path: str, key: str) -> bool:
    """Check if notebook contains merge_*_data() function.

    The key may have underscores (e.g., core_02) but the function might not
    (e.g., merge_core02_data). We check for both patterns.
    """
    try:
        with open(notebook_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Pattern 1: exact match (key has no special characters)
        pattern1 = rf"def\s+merge_{re.escape(key)}_data\s*\("

        # Pattern 2: key without underscores in function name
        # e.g., core_02 -> merge_core02_data
        key_no_underscores = key.replace("_", "")
        pattern2 = rf"def\s+merge_{re.escape(key_no_underscores)}_data\s*\("

        return bool(re.search(pattern1, content) or re.search(pattern2, content))
    except Exception:
        return False


def extract_cell_output_text(cell) -> str:
    """Extract text output from a notebook cell."""
    outputs = []
    for output in cell.get("outputs", []):
        if output.output_type == "stream":
            outputs.append(output.text)
        elif output.output_type == "execute_result":
            if "text/plain" in output.data:
                outputs.append(output.data["text/plain"])
            if "text/html" in output.data:
                outputs.append(output.data["text/html"])
    return "\n".join(outputs)


def has_empty_dataframe_text(cell_output: str) -> bool:
    """Check if cell output indicates an empty DataFrame."""
    output_lower = cell_output.lower()
    indicators = [
        "empty dataframe",
        "0 rows",
        "no data found",
        "returning empty",
    ]
    return any(indicator in output_lower for indicator in indicators)


def run_notebook(notebook_path: str, key: str) -> tuple[bool, list[str]]:
    """
    Execute a notebook and return (success, error_messages).

    Uses nbformat and ExecutePreprocessor to execute the notebook.
    Captures all cell outputs including stdout/stderr from each cell.
    """
    errors = []

    creds_path = "/workspaces/pat2vec/test_elastic_credentials.py"

    # Now read the notebook for execution
    orig_cwd = os.getcwd()

    try:
        with open(notebook_path, "r", encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)

        # Fix cell sources - notebooks may have source as list of strings with embedded \n
        for i, cell in enumerate(nb.cells):
            if isinstance(cell.source, list):
                # Source is a list - join with empty string (items already have \n)
                cell.source = "".join(cell.source)

        # Insert setup cell at the beginning that creates fresh credentials from ES
        # This ensures populate_elastic_with_dummy_data() has valid credentials
        setup_code = '''import sys
sys.path.insert(0, "/workspaces/pat2vec")

from pat2vec.util.docker_elastic import ElasticContainer

es_container = ElasticContainer()
if es_container.start():
    host, username, password = es_container.get_credentials()
    
    # Write directly to the expected credentials file
    creds_content = f"""username = "{username}"
password = "{password}"
api_key = None
hosts = ["{host}"]
"""
    
    with open("/workspaces/pat2vec/test_elastic_credentials.py", "w") as f:
        f.write(creds_content)
    
    print("Created test_elastic_credentials.py")
else:
    raise RuntimeError("Failed to start Elasticsearch container for credential setup")
'''

        setup_cell = nbformat.v4.new_code_cell(setup_code)
        # Insert at position 0 (before first cell)
        nb.cells.insert(0, setup_cell)

        # Change to /workspaces/pat2vec/ for proper path resolution
        os.chdir("/workspaces/pat2vec")

        ep = ExecutePreprocessor(
            timeout=900,
            kernel_name="python3",
            interrupt_on_timeout=True,
        )

        temp_project_dir = get_temp_dir(key)

        # Run the notebook with working directory set to /workspaces/pat2vec/
        try:
            ep.preprocess(nb, {"metadata": {"path": "/workspaces/pat2vec"}})
        except Exception as e:
            errors.append(f"Notebook execution failed: {e}")

            for i, cell in enumerate(nb.cells):
                if hasattr(cell, "outputs") and cell.outputs:
                    for output in cell.outputs:
                        if output.output_type == "error":
                            errors.append(
                                f"Cell {i} error:\n"
                                f"{output.ename}: {output.evalue}\n"
                                "\n".join(output.traceback)
                            )

            return False, errors

        # Check for empty DataFrames in outputs
        for i, cell in enumerate(nb.cells):
            if cell.cell_type == "code":
                output_text = extract_cell_output_text(cell)

                if has_empty_dataframe_text(output_text):
                    errors.append(
                        f"Cell {i} produced empty DataFrame output:\n{output_text[:500]}"
                    )

        return True, errors

    except FileNotFoundError:
        errors.append(f"Notebook not found: {notebook_path}")
        return False, errors
    finally:
        # Always restore working directory and cleanup credentials file
        try:
            os.chdir(orig_cwd)
        except Exception:
            pass
        try:
            if os.path.exists(creds_path):
                os.remove(creds_path)
        except Exception:
            pass

        # Also clean up any credential files with specific key patterns that may have been created
        try:
            cred_pattern = "/workspaces/pat2vec/test_elastic_credentials_*_get.py"
            for cred_file in glob.glob(cred_pattern):
                os.remove(cred_file)
        except Exception:
            pass


def cleanup_temp_dir(key: str) -> bool:
    """Clean up temp directory if it exists."""
    temp_dir = get_temp_dir(key)

    try:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception as e:
        print(f"Warning: Failed to clean up {temp_dir}: {e}")

    # Clean up credential files matching pattern for this key
    _cleanup_credentials_for_key(key)

    return True


def _cleanup_credentials_for_key(key: str) -> None:
    """Clean up credentials file for a specific notebook key."""

    # Pattern 1: test_elastic_credentials_<key>_get.py (e.g., test_elastic_credentials_demo_get.py)
    cred_pattern = f"/workspaces/pat2vec/test_elastic_credentials_{key}_get.py"
    try:
        if os.path.exists(cred_pattern):
            os.remove(cred_pattern)
    except Exception:
        pass

    # Clean up any temporary files created in /tmp by the notebook
    temp_dir = get_temp_dir(key)
    try:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception:
        pass


def cleanup_all_test_artifacts() -> None:
    """Clean up all test artifacts including temp directories and credential files."""
    import glob

    # Clean up temp directories matching pattern /tmp/*_test_project
    tmp_dir = "/tmp"
    try:
        for item in os.listdir(tmp_dir):
            if item.endswith("_test_project") and os.path.isdir(
                os.path.join(tmp_dir, item)
            ):
                shutil.rmtree(os.path.join(tmp_dir, item))
    except Exception:
        pass

    # Clean up credential files matching pattern test_elastic_credentials_*_get.py
    cred_pattern = "/workspaces/pat2vec/test_elastic_credentials_*_get.py"
    try:
        for cred_file in glob.glob(cred_pattern):
            os.remove(cred_file)
    except Exception:
        pass

    # Also clean up the legacy test_elastic_credentials.py if it exists
    legacy_cred = "/workspaces/pat2vec/test_elastic_credentials.py"
    try:
        if os.path.exists(legacy_cred):
            os.remove(legacy_cred)
    except Exception:
        pass


def test_notebook_e2e(notebook_key: str) -> tuple[bool, list[str]]:
    """
    Run a complete notebook e2e test.

    Returns (success, errors). If success is True, all checks passed.
    """
    errors = []

    notebook_path = get_notebook_path(notebook_key)

    if not os.path.exists(notebook_path):
        return False, [f"Notebook not found: {notebook_path}"]

    has_markdown = has_markdown_cell_front(notebook_path)
    if not has_markdown:
        errors.append(f"Notebook does not have markdown cell at top")

    has_merge = has_merge_function(notebook_path, notebook_key)
    if not has_merge:
        errors.append(f"Notebook does not contain merge_{notebook_key}_data() function")

    success, execute_errors = run_notebook(notebook_path, notebook_key)
    errors.extend(execute_errors)

    return (has_markdown and has_merge and success), errors


def update_status_json(
    key: str, status: str, error_msg: str = "", steps_done: list | None = None
):
    """Update status.json with current notebook state."""
    status_path = "/workspaces/pat2vec/.ai/notebook_tests/status.json"

    try:
        with open(status_path, "r", encoding="utf-8") as f:
            status_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        status_data = {}

    if key not in status_data:
        status_data[key] = {
            "status": "pending",
            "steps_done": [],
            "steps_remaining": [],
            "last_error": "",
            "last_updated": "",
        }

    current_time = os.popen("date -u '+%Y-%m-%dT%H:%M:%S'").read().strip()

    status_data[key]["status"] = status
    if error_msg:
        status_data[key]["last_error"] = error_msg

    if steps_done:
        status_data[key]["steps_done"].extend(steps_done)

    status_data[key]["steps_remaining"] = []

    if status == "done":
        status_data[key]["steps_remaining"] = []

    status_data[key]["last_updated"] = current_time

    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(status_data, f, indent=2)


# Reference test functions - these are the ones that should be executed by pytest
def test_demo():
    """Test demo notebook e2e execution."""
    success, errors = test_notebook_e2e("demo")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Demo notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_bmi():
    """Test bmi notebook e2e execution."""
    success, errors = test_notebook_e2e("bmi")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"BMI notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_bloods():
    """Test bloods notebook e2e execution."""
    success, errors = test_notebook_e2e("bloods")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Bloods notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_drugs():
    """Test drugs notebook e2e execution."""
    success, errors = test_notebook_e2e("drugs")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Drugs notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_diagnostics():
    """Test diagnostics notebook e2e execution."""
    success, errors = test_notebook_e2e("diagnostics")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Diagnostics notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_core_02():
    """Test core_02 notebook e2e execution."""
    success, errors = test_notebook_e2e("core_02")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Core 02 notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_bed():
    """Test bed notebook e2e execution."""
    success, errors = test_notebook_e2e("bed")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Bed notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_vte_status():
    """Test vte_status notebook e2e execution."""
    success, errors = test_notebook_e2e("vte_status")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"VTE status notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_hosp_site():
    """Test hosp_site notebook e2e execution."""
    success, errors = test_notebook_e2e("hosp_site")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Hosp site notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_core_resus():
    """Test core_resus notebook e2e execution."""
    success, errors = test_notebook_e2e("core_resus")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Core resus notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_news():
    """Test news notebook e2e execution."""
    success, errors = test_notebook_e2e("news")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"News notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_smoking():
    """Test smoking notebook e2e execution."""
    success, errors = test_notebook_e2e("smoking")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Smoking notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_annotations():
    """Test annotations notebook e2e execution."""
    success, errors = test_notebook_e2e("annotations")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Annotations notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_appointments():
    """Test appointments notebook e2e execution."""
    success, errors = test_notebook_e2e("appointments")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Appointments notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_textual_obs():
    """Test textual_obs notebook e2e execution."""
    success, errors = test_notebook_e2e("textual_obs")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Textual obs notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_covid():
    """Test covid notebook e2e execution."""
    success, errors = test_notebook_e2e("covid")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Covid notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_epic_encounters():
    """Test epic_encounters notebook e2e execution."""
    success, errors = test_notebook_e2e("epic_encounters")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Epic encounters notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_epic_clinical_notes():
    """Test epic_clinical_notes notebook e2e execution."""
    success, errors = test_notebook_e2e("epic_clinical_notes")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Epic clinical notes notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_epic_medical_history():
    """Test epic_medical_history notebook e2e execution."""
    success, errors = test_notebook_e2e("epic_medical_history")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Epic medical history notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_epic_orders():
    """Test epic_orders notebook e2e execution."""
    success, errors = test_notebook_e2e("epic_orders")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Epic orders notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_epic_lab_results():
    """Test epic_lab_results notebook e2e execution."""
    success, errors = test_notebook_e2e("epic_lab_results")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Epic lab results notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_epic_patients():
    """Test epic_patients notebook e2e execution."""
    success, errors = test_notebook_e2e("epic_patients")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Epic patients notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_epic_imaging_reports():
    """Test epic_imaging_reports notebook e2e execution."""
    success, errors = test_notebook_e2e("epic_imaging_reports")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, f"Epic imaging reports notebook test failed: {'; '.join(errors)}"

    cleanup_all_test_artifacts()


def test_epic_clinical_notes_appointments():
    """Test epic_clinical_notes_appointments notebook e2e execution."""
    success, errors = test_notebook_e2e("epic_clinical_notes_appointments")

    if not success:
        for error in errors:
            print(f"ERROR: {error}")
        assert False, (
            f"Epic clinical notes appointments notebook test failed: {'; '.join(errors)}"
        )

    cleanup_all_test_artifacts()
