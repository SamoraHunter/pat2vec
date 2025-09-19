import logging
import os
from datetime import datetime
import sys
from types import FrameType
from typing import IO, Any, Callable
from IPython.core.getipython import get_ipython


def setup_logger() -> logging.Logger:
    """Sets up a logger that writes to a file and redirects stdout.

    This function configures a logger with two handlers:

    1.  A file handler that saves DEBUG level logs to a timestamped file in a
        `logs` directory.
    2.  A stream handler that prints INFO level logs to the console.

    Crucially, it also replaces `sys.stdout` with a custom `TeeWriter` class.
    This class ensures that any output sent to `print()` is both displayed in
    the console (e.g., a Jupyter cell) and captured in the log file.

    Returns:
        The configured logger instance.
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

    logging.getLogger("elasticsearch").setLevel(logging.WARNING)
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

    # Store original stdout for restoration
    original_stdout = sys.stdout

    # Define a trace function for logging
    def tracefunc(frame: FrameType, event: str, arg: Any) -> Callable:
        # Only log events from files within the notebook directory
        if notebook_dir in frame.f_code.co_filename:
            if event == "line":
                logger.debug(
                    f"{event}: {frame.f_code.co_filename} - Line {frame.f_lineno}"
                )
        return tracefunc

    # Register the trace function globally for all events
    sys.settrace(tracefunc)

    # Create a custom stdout that both logs and preserves normal output
    class TeeWriter:
        """A file-like object that writes to a stream and a logger."""

        def __init__(self, original_stream: IO[str], logger: logging.Logger, level: int):
            """Initializes the TeeWriter.

            Args:
                original_stream: The original stream to write to (e.g., sys.stdout).
                logger: The logger instance to write to.
                level: The logging level to use for messages.
            """
            self.original_stream = original_stream
            self.logger = logger
            self.level = level

        def write(self, message: str) -> None:
            """Writes a message to the original stream and the logger.

            Args:
                message: The message to write.
            """
            # Write to original stdout (preserves Jupyter cell output)
            self.original_stream.write(message)
            # Also log the message (but only non-empty messages)
            if message.strip():
                self.logger.log(self.level, message.strip())

        def flush(self) -> None:
            """Flushes the original stream."""
            self.original_stream.flush()

        def __getattr__(self, name: str) -> Any:
            """Delegates any other attribute access to the original stream.

            Args:
                name: The name of the attribute to access.
            """
            return getattr(self.original_stream, name)

    # Replace stdout with our tee writer
    sys.stdout = TeeWriter(original_stdout, logger, logging.INFO)

    return logger
