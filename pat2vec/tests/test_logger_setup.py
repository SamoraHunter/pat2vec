import unittest
import logging
from pat2vec.util.logger_setup import setup_logger


class TestLoggerSetup(unittest.TestCase):
    def test_setup_logging_basic(self):
        """Test that logging is set up correctly."""
        setup_logger(log_level="DEBUG")
        logger = logging.getLogger("pat2vec")
        self.assertEqual(logger.level, logging.DEBUG)
        self.assertGreater(len(logger.handlers), 0)

    def test_setup_logging_idempotency(self):
        """Ensure multiple calls don't result in duplicate handlers."""
        setup_logger(log_level="INFO")
        initial_handler_count = len(logging.getLogger("pat2vec").handlers)
        setup_logger(log_level="INFO")
        self.assertEqual(
            len(logging.getLogger("pat2vec").handlers), initial_handler_count
        )
