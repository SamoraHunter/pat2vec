import unittest
from unittest.mock import patch, MagicMock, mock_open
import subprocess
from pat2vec.util.compile_requirements import (
    run_pip_compile,
    append_to_file,
    process_requirements,
)


class TestCompileRequirements(unittest.TestCase):
    @patch("pat2vec.util.compile_requirements.subprocess.run")
    def test_run_pip_compile(self, mock_run):
        """Verify subprocess call for pip-compile."""
        mock_run.return_value = MagicMock(returncode=0)
        self.assertTrue(run_pip_compile())

        mock_run.side_effect = subprocess.CalledProcessError(1, "cmd")
        self.assertFalse(run_pip_compile())

    @patch("builtins.open", new_callable=mock_open)
    def test_append_to_file(self, mock_file):
        """Verify file appending."""
        append_to_file("req.txt", "pandas")
        mock_file.assert_called_with("req.txt", "a")
        mock_file().write.assert_called_with("pandas\n")

    @patch("pat2vec.util.compile_requirements.run_pip_compile")
    @patch("pat2vec.util.compile_requirements.append_to_file")
    @patch("builtins.open")
    def test_process_requirements_retry_logic(
        self, mock_file_open, mock_append, mock_compile
    ):
        """Test iterative requirement compilation with failure recovery."""
        mock_file_open.return_value.__enter__.return_value.readlines.return_value = [
            "good\n",
            "bad\n",
        ]
        mock_compile.side_effect = [True, False]

        # Mock the requirements.in file reading for the revert step
        mock_in_file = mock_open(read_data="good\nbad\n")
        with patch("pat2vec.util.compile_requirements.open", mock_in_file):
            process_requirements()

        self.assertEqual(mock_compile.call_count, 2)
        # Check that 'bad' was recorded to failed_requirements.txt
        last_call_args = mock_in_file.call_args_list[-1]
        self.assertEqual(last_call_args[0][0], "failed_requirements.txt")
