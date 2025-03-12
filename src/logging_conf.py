import logging
import sys
from typing import Optional
from pathlib import Path

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOG_LEVEL = "INFO"


def configure_logging(log_level: Optional[str] = None):
    """Configures logging with a default format and level."""
    log_level = log_level or DEFAULT_LOG_LEVEL
    logging.basicConfig(level=log_level.upper(), format=LOG_FORMAT, stream=sys.stdout)


def add_file_handler(log_file: Path):
    """Adds a file handler to the root logger."""
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """Returns a logger instance with the given name."""
    return logging.getLogger(name)

