import logging
import os
from datetime import datetime
import sys
from IPython.core.getipython import get_ipython


def setup_logger():
    # Get the directory path of the current module
    """
    Sets up a logger for the application, directing log messages to both a log file and the console.

    The log file is stored in a 'logs' directory, two levels up from the current module's directory,
    and is named with the current date and time. The logger captures messages at the DEBUG level and above.
    Console messages display at the INFO level.

    Returns:
        logging.Logger: The configured logger instance.
    """

    module_dir = os.path.dirname(os.path.realpath(__file__))

    # Get the root directory of the notebook
    notebook_dir = os.path.dirname(
        get_ipython().config["IPKernelApp"]["connection_file"]
    )

    # Navigate up from the notebook directory to get the logs directory
    logs_dir = os.path.abspath(os.path.join(module_dir, "..", "..", "logs"))
    print("logs_dir", logs_dir)
    os.makedirs(logs_dir, exist_ok=True)

    # Get the current date and time
    current_date_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Set up logging with the current date and time in the log file name
    log_file = os.path.join(logs_dir, f"{current_date_time}_pat2vec.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create a logger
    logger = logging.getLogger(__name__)

    # Define a handler to print log messages to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Define a trace function for logging
    def tracefunc(frame, event, arg):
        # Only log events from files within the notebook directory
        if notebook_dir in frame.f_code.co_filename:
            if event == "line":
                logger.debug(
                    f"{event}: {frame.f_code.co_filename} - Line {frame.f_lineno}"
                )

        return tracefunc

    # Register the trace function globally for all events
    sys.settrace(tracefunc)

    # Redirect stdout to the logger
    class LoggerWriter:
        def __init__(self, logger, level):
            self.logger = logger
            self.level = level

        def write(self, message):
            if message.strip():
                self.logger.log(self.level, message.strip())

        def flush(self):
            pass

    sys.stdout = LoggerWriter(logger, logging.INFO)

    return logger
