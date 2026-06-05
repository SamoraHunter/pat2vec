import logging
import os
from datetime import datetime


def setup_logger(log_level: str = "INFO", logs_dir: str = "logs") -> logging.Logger:
    """Sets up a logger that writes to a file and the console.

    This function configures a logger with two handlers:

    1.  A file handler that saves DEBUG level logs to a timestamped file in a
        specified `logs` directory.
    2.  A stream handler that prints INFO level logs to the console.

    Returns:
        The configured logger instance.
    """
    # Ensure the logs directory exists
    os.makedirs(logs_dir, exist_ok=True)

    # Create a logger
    logger = logging.getLogger("pat2vec")
    logger.setLevel(
        getattr(logging, log_level.upper())
    )  # Set the logger's level to the requested level
    logger.propagate = False  # Prevent messages from being passed to the root logger

    # Always clear existing handlers to ensure a clean state for each setup
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    # --- File Handler ---
    current_date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(logs_dir, f"{current_date_time}_pat2vec.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)  # Log everything to the file
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # --- Console Handler ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Silence overly verbose libraries
    logging.getLogger("elasticsearch").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logger
