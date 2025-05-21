import logging
import sys
from typing import Optional
from pathlib import Path

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
ROOT_LOG_LEVEL = "DEBUG"
DEFAULT_LOG_LEVEL = "INFO"

def configure_core_logger(root_level: str = ROOT_LOG_LEVEL) -> None:
    """Configures core logging with a root log level."""
    logging.getLogger().setLevel(root_level.upper())

def add_console_handler(console_log_level: Optional[str] = None) -> None:
    """Adds a console handler to the root logger."""
    log_level = console_log_level or DEFAULT_LOG_LEVEL

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    console_handler.setLevel(log_level.upper())
    logging.getLogger().addHandler(console_handler)

def add_file_handler(log_file: Path, file_log_level: Optional[str] = None) -> None:
    """Adds a file handler to the root logger."""
    level_to_set = file_log_level or ROOT_LOG_LEVEL

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    file_handler.setLevel(level_to_set.upper())
    logging.getLogger().addHandler(file_handler)