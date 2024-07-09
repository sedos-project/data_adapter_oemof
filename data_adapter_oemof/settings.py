import logging
import os
import pathlib
from pathlib import Path

from data_adapter_oemof.utils import load_yaml

ROOT_DIR = pathlib.Path(__file__).parent

COLLECTIONS_DIR = (
    pathlib.Path(os.environ["COLLECTIONS_DIR"])
    if "COLLECTIONS_DIR" in os.environ
    else pathlib.Path.cwd() / "collections"
)
if not COLLECTIONS_DIR.exists():
    raise FileNotFoundError(
        f"Could not find collections directory '{COLLECTIONS_DIR}'. "
        "You should either create the collections folder or "
        "change path to collection folder by changing environment variable 'COLLECTIONS_DIR'.",
    )
STRUCTURES_DIR = (
    pathlib.Path(os.environ["STRUCTURES_DIR"])
    if "STRUCTURES_DIR" in os.environ
    else pathlib.Path.cwd() / "structures"
)
if not STRUCTURES_DIR.exists():
    raise FileNotFoundError(
        f"Could not find structure directory '{STRUCTURES_DIR}'. "
        "You should either create the structure folder or "
        "change path to structure folder by changing environment variable 'STRUCTURES_DIR'.",
    )

# Maps from oemof.tabular parameter names
# to ontological terms or to sedos nomenclature as fallback option
PARAMETER_MAP = load_yaml(Path(__file__).parent / "mappings" / "PARAMETER_MAP.yaml")
PROCESS_ADAPTER_MAP = load_yaml(
    Path(__file__).parent / "mappings" / "PROCESS_ADAPTER_MAP.yaml"
)
BUS_MAP = load_yaml(Path(__file__).parent / "mappings" / "BUS_MAP.yaml")


class CustomFormatter(logging.Formatter):
    """Logging colored formatter, adapted from https://stackoverflow.com/a/56944256/3638629"""

    grey = "\x1b[38;21m"
    blue = "\x1b[38;5;39m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    green = "'\x1b[38;5;82m'"

    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt
        self.FORMATS = {
            logging.DEBUG: self.grey + self.fmt + self.reset,
            logging.INFO: self.green + self.fmt + self.reset,
            logging.WARNING: self.yellow + self.fmt + self.reset,
            logging.ERROR: self.red + self.fmt + self.reset,
            logging.CRITICAL: self.bold_red + self.fmt + self.reset,
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = CustomFormatter("%(levelname)s: %(message)s")
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

# Initialize logging configuration
