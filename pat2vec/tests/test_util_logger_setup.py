import unittest
import os
import shutil
import tempfile
import logging
from pat2vec.util.logger_setup import setup_logger


class TestLoggerSetup(unittest.TestCase):
    """Unit tests for the logger configuration utility."""

    def setUp(self):
        """Set up a temporary directory for log files."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up the temporary directory and reset the logger."""
        shutil.rmtree(self.test_dir)
        # Reset the pat2vec logger to prevent handler accumulation across tests
        logger = logging.getLogger("pat2vec")
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    def test_setup_logger_initialization(self):
        """Test that setup_logger creates the directory and initializes handlers."""
        logs_dir = os.path.join(self.test_dir, "logs_test")
        logger = setup_logger(log_level="INFO", logs_dir=logs_dir)

        # Check directory creation
        self.assertTrue(os.path.exists(logs_dir))

        # Check handlers
        self.assertEqual(len(logger.handlers), 2)
        handler_types = [type(h) for h in logger.handlers]
        self.assertIn(logging.FileHandler, handler_types)
        self.assertIn(logging.StreamHandler, handler_types)

        # Verify a log file was actually created
        log_files = [f for f in os.listdir(logs_dir) if f.endswith(".log")]
        self.assertGreater(len(log_files), 0)

    def test_setup_logger_idempotency(self):
        """Test that calling setup_logger multiple times doesn't duplicate handlers."""
        setup_logger(logs_dir=self.test_dir)
        logger = setup_logger(logs_dir=self.test_dir)
        self.assertEqual(len(logger.handlers), 2)

    def test_setup_logger_library_silencing(self):
        """Verify that verbose libraries are silenced during setup."""
        setup_logger(logs_dir=self.test_dir)
        self.assertEqual(logging.getLogger("elasticsearch").level, logging.WARNING)
        self.assertEqual(logging.getLogger("urllib3").level, logging.WARNING)

    def test_setup_logger_custom_level(self):
        """Verify that the console handler respects the provided log level."""
        logger = setup_logger(log_level="ERROR", logs_dir=self.test_dir)
        console_handler = next(
            h for h in logger.handlers if type(h) is logging.StreamHandler
        )
        self.assertEqual(console_handler.level, logging.ERROR)


if __name__ == "__main__":
    unittest.main()
