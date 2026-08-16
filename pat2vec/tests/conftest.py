import glob
import os
import shutil
import sys

import pytest
import requests

# Block transformers from being used during tests. When use_GPT=False, tests
# should only use dummy/faker methods and not require the transformers library.
#
# This is achieved by pre-populating sys.modules with a mock transformers module
# that has pipeline() that raises RuntimeError when called.


class MockTransformers:
    """Mock transformers module that blocks pipeline()."""

    @staticmethod
    def pipeline(*args: object, **kwargs: object) -> None:
        msg = (
            "transformers.pipeline() is blocked during tests. "
            "Use use_GPT=False with dummy/faker data generation methods instead."
        )
        raise RuntimeError(msg)


# Store original function reference for cleanup
_original_generate_epr_documents_data = None


def _make_use_gpt_default_false(func):
    """Change a function's use_GPT default from True to False.

    This is used during test setup to prevent transformers pipeline() accidentally
    being called when tests run without explicitly setting use_GPT=False.
    """
    import inspect

    sig = inspect.signature(func)

    # Build new parameters list with use_GPT default changed to False
    new_params = []
    for name, param in sig.parameters.items():
        if name == "use_GPT":
            # Change the default from True to False
            param = param.replace(default=False)
        new_params.append(param)

    new_sig = sig.replace(parameters=new_params)

    # Create a wrapper function with the new signature
    def wrapper(*args, use_GPT=None, **kwargs):
        if use_GPT is None:
            use_GPT = False  # Default in tests
        return func(*args, use_GPT=use_GPT, **kwargs)

    # Preserve original function's metadata
    import functools

    wrapper = functools.update_wrapper(wrapper, func)
    wrapper.__signature__ = new_sig

    return wrapper


def is_huggingface_reachable():
    try:
        requests.get("https://huggingface.co", timeout=5)
        return True
    except Exception:
        return False


requires_huggingface = pytest.mark.skipif(
    not is_huggingface_reachable(),
    reason="HuggingFace is not reachable in this environment",
)


def pytest_configure(config):
    """Setup mocks to block transformers usage during tests.

    This configures:
    1. Mock transformers module that blocks pipeline() with RuntimeError
    2. Monkey-patches generate_epr_documents_data to use use_GPT=False by default

    This ensures tests that don't explicitly set use_GPT=False won't accidentally
    trigger transformers pipeline() calls.

    NOTE: The real transformers library is still loaded when other modules import it,
    but we replace sys.modules["transformers"] with a mock version. The mock's pipeline()
    raises RuntimeError instead of actually loading the GPT-2 model, avoiding warnings.
    """
    # Store reference to original transformers module before replacement
    # This is just for documentation - we won't use it as we're replacing entire modules

    _mock = MockTransformers()

    # Replace the transformers module with our mock in sys.modules
    # This ensures that even if sequence_generators.py does `from transformers import pipeline`,
    # it will get our mock instead of the real library
    sys.modules["transformers"] = _mock

    # Also replace specific submodules that might be accessed
    for name in list(sys.modules.keys()):
        if name.startswith("transformers."):
            del sys.modules[name]

    # Monkey-patch generate_epr_documents_data to default to use_GPT=False in tests
    global _original_generate_epr_documents_data

    from pat2vec.util.dummy_data_generation import epr_documents as ed

    _original_generate_epr_documents_data = ed.generate_epr_documents_data

    # Replace the function with our wrapper that has False default for use_GPT
    ed.generate_epr_documents_data = _make_use_gpt_default_false(
        ed.generate_epr_documents_data,
    )


def pytest_unconfigure(config):
    """Clean up mocks when pytest ends."""
    # Perform final cleanup of test artifacts
    _cleanup_test_artifacts()

    if "transformers" in sys.modules:
        try:
            import transformers as tf

            sys.modules["transformers"] = tf
        except ImportError:
            pass

    # Restore original function
    global _original_generate_epr_documents_data
    if (
        _original_generate_epr_documents_data
        and "pat2vec.util.dummy_data_generation.epr_documents" in sys.modules
    ):
        try:
            from pat2vec.util.dummy_data_generation import epr_documents as ed

            ed.generate_epr_documents_data = _original_generate_epr_documents_data
        except (ImportError, AttributeError):
            pass


def _cleanup_test_artifacts():
    """Clean up test artifacts including temp directories and credential files."""
    # Clean up temp directories matching pattern /tmp/*_test_project
    tmp_dir = "/tmp"
    for item in os.listdir(tmp_dir):
        if item.endswith("_test_project") and os.path.isdir(
            os.path.join(tmp_dir, item),
        ):
            try:
                shutil.rmtree(os.path.join(tmp_dir, item))
            except Exception:
                pass

    # Clean up temp directories matching pattern /workspaces/pat2vec/tmp/*_test_project
    tmp_proj_dir = "/workspaces/pat2vec/tmp"
    if os.path.exists(tmp_proj_dir):
        for item in os.listdir(tmp_proj_dir):
            if item.endswith("_test_project") and os.path.isdir(
                os.path.join(tmp_proj_dir, item),
            ):
                try:
                    shutil.rmtree(os.path.join(tmp_proj_dir, item))
                except Exception:
                    pass

    # Clean up credential files matching pattern test_elastic_credentials_*_get.py
    cred_pattern = "/workspaces/pat2vec/test_elastic_credentials_*_get.py"
    for cred_file in glob.glob(cred_pattern):
        try:
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
